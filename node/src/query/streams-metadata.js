'use strict';

class StreamsMetadata {
  constructor({ hostInfo, storeNames = [], topicPartitions = [] }) {
    this.hostInfo = hostInfo ?? null;
    this.storeNames = new Set(storeNames);
    this.topicPartitions = topicPartitions.map(entry => ({
      topic: entry.topic,
      partitions: Array.from(new Set(entry.partitions ?? []))
    }));
  }

  hasStore(storeName) {
    return this.storeNames.has(storeName);
  }

  withStore(storeName) {
    const names = new Set(this.storeNames);
    names.add(storeName);
    return new StreamsMetadata({
      hostInfo: this.hostInfo,
      storeNames: Array.from(names),
      topicPartitions: this.topicPartitions
    });
  }

  toJSON() {
    return {
      hostInfo: this.hostInfo ? this.hostInfo.toJSON?.() ?? this.hostInfo : null,
      storeNames: Array.from(this.storeNames),
      topicPartitions: this.topicPartitions
    };
  }
}

module.exports = { StreamsMetadata };
