'use strict';

const { WindowStore } = require('./store');

class MemoryWindowStore extends WindowStore {
  constructor(name = 'in-memory-window-store', options = {}) {
    super(name, { persistent: false, loggingEnabled: options.loggingEnabled ?? false, retention: options.retention, windowSize: options.windowSize });
    this._store = new Map();
  }

  async put(key, value, timestamp) {
    if (key === null || key === undefined) {
      return;
    }
    const ts = Number(timestamp ?? Date.now());
    const entries = this._store.get(key) ?? [];
    entries.push({ value, timestamp: ts });
    entries.sort((a, b) => a.timestamp - b.timestamp);
    this._store.set(key, entries);
  }

  async fetch(key, from, to) {
    if (key === null || key === undefined) {
      return [];
    }
    const entries = this._store.get(key) ?? [];
    const start = Number(from ?? Number.NEGATIVE_INFINITY);
    const end = Number(to ?? Number.POSITIVE_INFINITY);
    return entries.filter(entry => entry.timestamp >= start && entry.timestamp <= end);
  }

  async purge(beforeTimestamp) {
    if (beforeTimestamp == null) {
      return;
    }
    const cutoff = Number(beforeTimestamp);
    for (const [key, entries] of this._store.entries()) {
      const filtered = entries.filter(entry => entry.timestamp >= cutoff);
      if (filtered.length) {
        this._store.set(key, filtered);
      } else {
        this._store.delete(key);
      }
    }
  }

  async clear() {
    this._store.clear();
  }
}

module.exports = {
  MemoryWindowStore
};
