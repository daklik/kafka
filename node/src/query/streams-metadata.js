'use strict';

const { HostInfo } = require('./host-info');

function normalizePartitions(values = []) {
  const set = new Set();
  for (const value of values) {
    const numeric = Number(value);
    if (!Number.isNaN(numeric)) {
      set.add(numeric);
    }
  }
  return Array.from(set).sort((a, b) => a - b);
}

class StreamsMetadata {
  constructor({
    hostInfo,
    storeNames = [],
    topicPartitions = [],
    standbyStoreNames = [],
    standbyTopicPartitions = []
  }) {
    this.hostInfo = hostInfo ? HostInfo.from(hostInfo) : null;
    this.storeNames = new Set(storeNames);
    this.standbyStoreNames = new Set(standbyStoreNames);
    this._activeTopicPartitions = new Map();
    this._standbyTopicPartitions = new Map();
    this.topicPartitions = [];
    this.standbyTopicPartitions = [];

    for (const entry of topicPartitions ?? []) {
      const storeName = entry.storeName ?? null;
      if (!storeName) {
        continue;
      }
      this.addStore(storeName);
      this.setTopicPartitions(storeName, {
        topic: entry.topic ?? null,
        partitions: entry.partitions ?? []
      });
    }

    for (const entry of standbyTopicPartitions ?? []) {
      const storeName = entry.storeName ?? null;
      if (!storeName) {
        continue;
      }
      this.addStore(storeName, { standby: true });
      this.setTopicPartitions(storeName, {
        topic: entry.topic ?? null,
        partitions: entry.partitions ?? []
      }, true);
    }
  }

  hasStore(storeName) {
    return this.storeNames.has(storeName) || this.standbyStoreNames.has(storeName);
  }

  isStandbyFor(storeName) {
    return this.standbyStoreNames.has(storeName) && !this.storeNames.has(storeName);
  }

  addStore(storeName, { standby = false } = {}) {
    if (!storeName) {
      return;
    }
    const target = standby ? this.standbyStoreNames : this.storeNames;
    target.add(storeName);
    this._ensureTopicMap(storeName, standby);
    this._rebuildTopicArrays();
  }

  removeStore(storeName) {
    if (!storeName) {
      return;
    }
    this.storeNames.delete(storeName);
    this.standbyStoreNames.delete(storeName);
    this._activeTopicPartitions.delete(storeName);
    this._standbyTopicPartitions.delete(storeName);
    this._rebuildTopicArrays();
  }

  removeStandbyStore(storeName) {
    if (!storeName) {
      return;
    }
    this.standbyStoreNames.delete(storeName);
    this._standbyTopicPartitions.delete(storeName);
    this._rebuildTopicArrays();
  }

  setTopicPartitions(storeName, { topic = null, partitions = [] } = {}, standby = false) {
    if (!storeName) {
      return;
    }
    const normalized = normalizePartitions(partitions);
    const entry = {
      topic,
      partitions: new Set(normalized)
    };
    const map = standby ? this._standbyTopicPartitions : this._activeTopicPartitions;
    map.set(storeName, entry);
    this.addStore(storeName, { standby });
    this._rebuildTopicArrays();
  }

  partitionsForStore(storeName, { standby = false } = {}) {
    if (!storeName) {
      return { topic: null, partitions: [] };
    }
    const map = standby ? this._standbyTopicPartitions : this._activeTopicPartitions;
    const entry = map.get(storeName);
    if (!entry) {
      return { topic: null, partitions: [] };
    }
    return {
      topic: entry.topic ?? null,
      partitions: Array.from(entry.partitions)
    };
  }

  toJSON() {
    return {
      hostInfo: this.hostInfo ? this.hostInfo.toJSON?.() ?? this.hostInfo : null,
      storeNames: Array.from(this.storeNames),
      standbyStoreNames: Array.from(this.standbyStoreNames),
      topicPartitions: this.topicPartitions,
      standbyTopicPartitions: this.standbyTopicPartitions
    };
  }

  _ensureTopicMap(storeName, standby) {
    const map = standby ? this._standbyTopicPartitions : this._activeTopicPartitions;
    if (!map.has(storeName)) {
      map.set(storeName, { topic: null, partitions: new Set() });
    }
  }

  _rebuildTopicArrays() {
    this.topicPartitions = Array.from(this._activeTopicPartitions.entries()).map(([storeName, entry]) => ({
      storeName,
      topic: entry.topic ?? null,
      partitions: Array.from(entry.partitions)
    }));
    this.standbyTopicPartitions = Array.from(this._standbyTopicPartitions.entries()).map(([storeName, entry]) => ({
      storeName,
      topic: entry.topic ?? null,
      partitions: Array.from(entry.partitions)
    }));
  }
}

module.exports = { StreamsMetadata };
