'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');

const { KeyValueStore } = require('./store');

function loadRocksDBBinding(customBinding = null) {
  if (customBinding) {
    return customBinding;
  }
  try {
    // eslint-disable-next-line global-require
    return require('rocksdb-native');
  } catch (error) {
    const message = 'PersistentKeyValueStore requires the `rocksdb-native` package. ' +
      'Install it and ensure native dependencies (e.g. librocksdb, libatomic) are available.';
    const augmented = new Error(message);
    augmented.cause = error;
    throw augmented;
  }
}

function estimateSize(value) {
  if (value == null) {
    return 0;
  }
  if (Buffer.isBuffer(value)) {
    return value.length;
  }
  if (typeof value === 'string') {
    return Buffer.byteLength(value);
  }
  if (typeof value === 'number') {
    return 8;
  }
  if (typeof value === 'boolean') {
    return 1;
  }
  if (typeof value === 'bigint') {
    return 8;
  }
  try {
    return Buffer.byteLength(JSON.stringify(value));
  } catch (err) {
    return 0;
  }
}

class PersistentKeyValueStore extends KeyValueStore {
  constructor(name, options = {}) {
    super(name, {
      persistent: true,
      loggingEnabled: options.loggingEnabled ?? true,
      cachingEnabled: options.cachingEnabled ?? false,
      changelogConfig: options.changelogConfig ?? null
    });
    this._rocksdbFactory = options.rocksdb ?? null;
    this._dbOptions = options.dbOptions ?? {};
    this._directoryOverride = options.directory ?? null;
    this._cacheMaxBytesOverride = options.cacheMaxBytes ?? null;
    this._cache = new Map();
    this._cacheBytes = 0;
    this._db = null;
    this._dbPath = null;
    this._cacheMaxBytes = null;
  }

  async init(context = {}) {
    super.init(context);
    const directory = await this._resolveDirectory(context);
    await fs.mkdir(directory, { recursive: true });
    const RocksDB = loadRocksDBBinding(this._rocksdbFactory);
    this._dbPath = directory;
    this._db = new RocksDB(directory, this._dbOptions);
    if (typeof this._db.ready === 'function') {
      await this._db.ready();
    }
    this._cacheBytes = 0;
    this._cache.clear();
    if (this.cachingEnabled) {
      const provided = this._cacheMaxBytesOverride ?? context.cacheMaxBytes ?? context.config?.getCacheMaxBytesBuffering?.();
      this._cacheMaxBytes = typeof provided === 'number' && provided >= 0 ? provided : null;
    } else {
      this._cacheMaxBytes = null;
    }
  }

  async get(key) {
    if (this.cachingEnabled && this._cache.has(key)) {
      const entry = this._cache.get(key);
      return entry.deleted ? undefined : entry.value;
    }
    if (!this._db?.get) {
      return undefined;
    }
    const value = await this._db.get(key);
    return value === null ? undefined : value;
  }

  async put(key, value) {
    if (!this.cachingEnabled) {
      await this._db.put(key, value);
      return;
    }
    this._stagePut(key, value);
    await this._maybeFlushCache();
  }

  async delete(key) {
    if (!this.cachingEnabled) {
      await this._db.delete(key);
      return;
    }
    this._stageDelete(key);
    await this._maybeFlushCache();
  }

  async entries() {
    await this._maybeFlushCache(true);
    if (!this._db?.iterator) {
      return [];
    }
    const entries = [];
    const iterator = this._db.iterator();
    try {
      for await (const { key, value } of iterator) {
        entries.push([key, value]);
      }
    } finally {
      iterator?.destroy?.();
    }
    return entries;
  }

  async flush() {
    await this._flushCache();
    if (this._db?.flush) {
      await this._db.flush();
    }
  }

  async close() {
    await this.flush();
    if (this._db) {
      await this._db.close?.({ force: true });
      this._db = null;
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
    throw new Error('PersistentKeyValueStore requires a state directory. Provide StreamsConfig.stateDir or a directory override.');
  }

  _stagePut(key, value) {
    this._removeCacheFootprint(key);
    const size = estimateSize(key) + estimateSize(value);
    this._cache.set(key, { value, deleted: false, size });
    this._cacheBytes += size;
  }

  _stageDelete(key) {
    this._removeCacheFootprint(key);
    const size = estimateSize(key);
    this._cache.set(key, { value: null, deleted: true, size });
    this._cacheBytes += size;
  }

  _removeCacheFootprint(key) {
    if (!this._cache.has(key)) {
      return;
    }
    const existing = this._cache.get(key);
    this._cacheBytes -= existing.size;
    this._cache.delete(key);
  }

  async _maybeFlushCache(force = false) {
    if (!this.cachingEnabled || !this._cache.size) {
      return;
    }
    if (!force && this._cacheMaxBytes != null && this._cacheMaxBytes > 0 && this._cacheBytes < this._cacheMaxBytes) {
      return;
    }
    await this._flushCache();
  }

  async _flushCache() {
    if (!this.cachingEnabled || !this._cache.size) {
      return;
    }
    const batch = this._db?.write ? this._db.write({ autoDestroy: true }) : null;
    try {
      if (!batch) {
        for (const [key, entry] of this._cache.entries()) {
          if (entry.deleted) {
            await this._db.delete(key);
          } else {
            await this._db.put(key, entry.value);
          }
        }
      } else {
        for (const [key, entry] of this._cache.entries()) {
          if (entry.deleted) {
            batch.tryDelete(key);
          } else {
            batch.tryPut(key, entry.value);
          }
        }
        await batch.flush();
      }
      this._cache.clear();
      this._cacheBytes = 0;
    } finally {
      batch?.destroy?.();
    }
  }
}

module.exports = {
  PersistentKeyValueStore
};
