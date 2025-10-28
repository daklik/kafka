'use strict';

const { WindowStore } = require('./store');

class MemoryWindowStore extends WindowStore {
  constructor(name = 'in-memory-window-store', options = {}) {
    super(name, { persistent: false, loggingEnabled: options.loggingEnabled ?? false, retention: options.retention, windowSize: options.windowSize });
    this._store = new Map();
    this._strategy = options.strategy ?? 'append';
  }

  async put(key, value, timestamp, metadata = {}) {
    if (key === null || key === undefined) {
      return;
    }
    const ts = Number(timestamp ?? Date.now());
    if (this._strategy === 'aggregate') {
      const entries = this._store.get(key) ?? new Map();
      entries.set(ts, { value, timestamp: ts, end: metadata.end ?? null });
      this._store.set(key, entries);
      return;
    }
    const entries = this._store.get(key) ?? [];
    entries.push({ value, timestamp: ts });
    entries.sort((a, b) => a.timestamp - b.timestamp);
    this._store.set(key, entries);
  }

  async fetch(key, from, to) {
    if (key === null || key === undefined) {
      return [];
    }
    const entries = this._strategy === 'aggregate'
      ? Array.from((this._store.get(key) ?? new Map()).values())
      : this._store.get(key) ?? [];
    const start = Number(from ?? Number.NEGATIVE_INFINITY);
    const end = Number(to ?? Number.POSITIVE_INFINITY);
    return entries.filter(entry => entry.timestamp >= start && entry.timestamp <= end);
  }

  async get(key, timestamp) {
    if (this._strategy !== 'aggregate') {
      const entries = this._store.get(key) ?? [];
      return entries.find(entry => entry.timestamp === timestamp) ?? null;
    }
    const entries = this._store.get(key) ?? new Map();
    return entries.get(timestamp) ?? null;
  }

  async delete(key, timestamp) {
    if (this._strategy === 'aggregate') {
      const entries = this._store.get(key);
      if (!entries) {
        return;
      }
      entries.delete(timestamp);
      if (!entries.size) {
        this._store.delete(key);
      }
      return;
    }
    const entries = this._store.get(key);
    if (!entries) {
      return;
    }
    const filtered = entries.filter(entry => entry.timestamp !== timestamp);
    if (filtered.length) {
      this._store.set(key, filtered);
    } else {
      this._store.delete(key);
    }
  }

  async purge(beforeTimestamp) {
    if (beforeTimestamp == null) {
      return;
    }
    const cutoff = Number(beforeTimestamp);
    for (const [key, entries] of this._store.entries()) {
      if (this._strategy === 'aggregate') {
        for (const [timestamp] of entries.entries()) {
          if (timestamp < cutoff) {
            entries.delete(timestamp);
          }
        }
        if (!entries.size) {
          this._store.delete(key);
        }
        continue;
      }
      const filtered = entries.filter(entry => entry.timestamp >= cutoff);
      if (filtered.length) {
        this._store.set(key, filtered);
      } else {
        this._store.delete(key);
      }
    }
  }

  async entries() {
    if (this._strategy === 'aggregate') {
      const result = [];
      for (const [key, entries] of this._store.entries()) {
        for (const entry of entries.values()) {
          result.push([key, entry]);
        }
      }
      return result;
    }
    const all = [];
    for (const [key, entries] of this._store.entries()) {
      for (const entry of entries) {
        all.push([key, entry]);
      }
    }
    return all;
  }

  async clear() {
    this._store.clear();
  }
}

module.exports = {
  MemoryWindowStore
};
