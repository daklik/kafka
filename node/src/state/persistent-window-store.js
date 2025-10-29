'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');

const { WindowStore } = require('./store');
const { serializeDatum, deserializeDatum, compareSerializedValues } = require('./serialization');

function loadRocksDBBinding(customBinding = null) {
  if (customBinding) {
    return customBinding;
  }
  try {
    // eslint-disable-next-line global-require
    return require('rocksdb-native');
  } catch (error) {
    const message = 'PersistentWindowStore requires the `rocksdb-native` package. ' +
      'Install it and ensure native dependencies (e.g. librocksdb, libatomic) are available.';
    const augmented = new Error(message);
    augmented.cause = error;
    throw augmented;
  }
}

function toNumber(value, fallback = null) {
  if (value === null || value === undefined) {
    return fallback;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? fallback : numeric;
}

function normalizeTimestamp(timestamp, fallback = Date.now()) {
  const numeric = toNumber(timestamp, null);
  if (numeric === null) {
    return Math.trunc(fallback);
  }
  return Math.trunc(numeric);
}

function serializeTimeRangeKey(key, start, end) {
  const keyBuffer = serializeDatum(key);
  const buffer = Buffer.allocUnsafe(keyBuffer.length + 16);
  keyBuffer.copy(buffer, 0);
  buffer.writeBigInt64BE(BigInt(start), keyBuffer.length);
  buffer.writeBigInt64BE(BigInt(end), keyBuffer.length + 8);
  return buffer;
}

function deserializeTimeRangeKey(buffer) {
  const { value: key, bytesRead } = deserializeDatum(buffer, 0);
  const start = Number(buffer.readBigInt64BE(bytesRead));
  const end = Number(buffer.readBigInt64BE(bytesRead + 8));
  return { key, start, end };
}

function isWithinWindow(entry, from, to) {
  if (from != null && entry.start < from) {
    return false;
  }
  if (to != null && entry.start > to) {
    return false;
  }
  return true;
}

class PersistentWindowStore extends WindowStore {
  constructor(name, options = {}) {
    super(name, {
      persistent: true,
      loggingEnabled: options.loggingEnabled ?? true,
      cachingEnabled: options.cachingEnabled ?? false,
      changelogConfig: options.changelogConfig ?? null,
      retention: options.retentionMs ?? options.retention ?? null,
      windowSize: options.windowSize ?? null
    });
    this._rocksdbFactory = options.rocksdb ?? null;
    this._dbOptions = options.dbOptions ?? {};
    this._directoryOverride = options.directory ?? null;
    this._retentionMs = toNumber(options.retentionMs ?? options.retention, null);
    this._graceMs = toNumber(options.gracePeriodMs ?? options.graceMs, 0);
    this._windowSize = toNumber(options.windowSize, null);
    this._db = null;
    this._directory = null;
  }

  async init(context = {}) {
    super.init(context);
    this._directory = await this._resolveDirectory(context);
    await fs.mkdir(this._directory, { recursive: true });
    const RocksDB = loadRocksDBBinding(this._rocksdbFactory);
    this._db = new RocksDB(this._directory, this._dbOptions);
    if (typeof this._db.ready === 'function') {
      await this._db.ready();
    }
  }

  async put(key, value, timestamp, metadata = {}) {
    if (key === null || key === undefined) {
      return;
    }
    const start = normalizeTimestamp(timestamp);
    const end = normalizeTimestamp(metadata.end, this._windowSize != null ? start + this._windowSize : start);
    const compositeKey = serializeTimeRangeKey(key, start, end);

    if (value === null || value === undefined) {
      await this._db.delete(compositeKey);
      return;
    }

    const payload = serializeDatum(value);
    await this._db.put(compositeKey, payload);
    await this._enforceRetention(end);
  }

  async fetch(key, from = null, to = null) {
    if (key === null || key === undefined) {
      return [];
    }
    const lower = toNumber(from, null);
    const upper = toNumber(to, null);
    const results = [];
    for await (const entry of this._iterate()) {
      if (compareSerializedValues(entry.key, key) !== 0) {
        continue;
      }
      if (!isWithinWindow(entry, lower, upper)) {
        continue;
      }
      results.push({ value: entry.value, timestamp: entry.start, end: entry.end });
    }
    results.sort((a, b) => a.timestamp - b.timestamp);
    return results;
  }

  async get(key, timestamp) {
    const start = normalizeTimestamp(timestamp, null);
    if (start == null) {
      return null;
    }
    const end = this._windowSize != null ? start + this._windowSize : start;
    const compositeKey = serializeTimeRangeKey(key, start, end);
    const value = await this._db.get(compositeKey);
    if (value === null || value === undefined) {
      return null;
    }
    const { value: deserialized } = deserializeDatum(value, 0);
    return { value: deserialized, timestamp: start, end };
  }

  async delete(key, timestamp, metadata = {}) {
    if (key === null || key === undefined) {
      return;
    }
    const start = normalizeTimestamp(timestamp, null);
    if (start == null) {
      return;
    }
    const end = normalizeTimestamp(metadata.end, this._windowSize != null ? start + this._windowSize : start);
    const compositeKey = serializeTimeRangeKey(key, start, end);
    await this._db.delete(compositeKey);
  }

  async purge(beforeTimestamp) {
    if (beforeTimestamp == null) {
      return;
    }
    const cutoff = normalizeTimestamp(beforeTimestamp);
    const batch = this._db.write ? this._db.write({ autoDestroy: true }) : null;
    try {
      for await (const entry of this._iterate()) {
        if (entry.end < cutoff) {
          if (batch) {
            batch.tryDelete(entry.compositeKey);
          } else {
            await this._db.delete(entry.compositeKey);
          }
        }
      }
      if (batch) {
        await batch.flush();
      }
    } finally {
      batch?.destroy?.();
    }
  }

  async entries() {
    const results = [];
    for await (const entry of this._iterate()) {
      results.push([entry.key, { value: entry.value, timestamp: entry.start, end: entry.end }]);
    }
    return results;
  }

  async clear() {
    const batch = this._db.write ? this._db.write({ autoDestroy: true }) : null;
    try {
      for await (const entry of this._iterate()) {
        if (batch) {
          batch.tryDelete(entry.compositeKey);
        } else {
          await this._db.delete(entry.compositeKey);
        }
      }
      if (batch) {
        await batch.flush();
      }
    } finally {
      batch?.destroy?.();
    }
  }

  async flush() {
    if (this._db?.flush) {
      await this._db.flush();
    }
  }

  async close() {
    if (!this._db) {
      return;
    }
    await this.flush();
    await this._db.close?.({ force: true });
    this._db = null;
  }

  async _enforceRetention(referenceEndTime) {
    if (this._retentionMs == null) {
      return;
    }
    const grace = this._graceMs ?? 0;
    const cutoff = referenceEndTime - this._retentionMs - grace;
    if (!Number.isFinite(cutoff)) {
      return;
    }
    await this.purge(cutoff);
  }

  async *_iterate() {
    if (!this._db?.iterator) {
      return;
    }
    const iterator = this._db.iterator();
    try {
      for await (const { key, value } of iterator) {
        const compositeKey = key;
        const { key: entryKey, start, end } = deserializeTimeRangeKey(compositeKey);
        const { value: deserialized } = deserializeDatum(value, 0);
        yield {
          compositeKey,
          key: entryKey,
          start,
          end,
          value: deserialized
        };
      }
    } finally {
      iterator?.destroy?.();
    }
  }

  async _resolveDirectory(context) {
    if (this._directoryOverride) {
      return path.resolve(this._directoryOverride);
    }
    if (context.storeDirectory) {
      return path.resolve(context.storeDirectory);
    }
    if (context.config?.resolveStateStoreDirectory) {
      return path.resolve(
        context.config.resolveStateStoreDirectory({
          stream: context.stream,
          storeName: context.storeName ?? this.name
        })
      );
    }
    if (context.stateDir) {
      const streamComponent = context.stream?.id ?? 'global';
      return path.join(context.stateDir, streamComponent, context.storeName ?? this.name);
    }
    throw new Error('PersistentWindowStore requires a state directory. Provide StreamsConfig.stateDir or a directory override.');
  }
}

module.exports = {
  PersistentWindowStore
};
