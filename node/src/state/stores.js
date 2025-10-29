'use strict';

const { StoreBuilder } = require('./store-builder');
const { MemoryStateStore } = require('./memory-store');
const { MemoryWindowStore } = require('./memory-window-store');
const { PersistentKeyValueStore } = require('./persistent-key-value-store');

const Stores = {
  inMemoryKeyValueStore(name) {
    return new StoreBuilder({
      name,
      type: 'keyValue',
      supplier: () => new MemoryStateStore(name),
      loggingEnabled: false
    });
  },

  persistentKeyValueStore(name, options = {}) {
    const loggingEnabled = options.loggingEnabled ?? true;
    const cachingEnabled = options.cachingEnabled ?? false;
    const changelogConfig = options.changelogConfig ?? null;
    const retentionMs = options.retentionMs ?? null;
    const cacheMaxBytes = options.cacheMaxBytes ?? null;

    return new StoreBuilder({
      name,
      type: 'keyValue',
      supplier: context => new PersistentKeyValueStore(name, {
        ...options,
        loggingEnabled,
        cachingEnabled,
        changelogConfig
      }),
      loggingEnabled,
      cachingEnabled,
      changelogConfig,
      persistent: true,
      retentionMs,
      cacheMaxBytes
    });
  },

  keyValueStoreBuilder(name, supplier, options = {}) {
    return new StoreBuilder({
      name,
      type: 'keyValue',
      supplier,
      loggingEnabled: options.loggingEnabled ?? true,
      cachingEnabled: options.cachingEnabled ?? false,
      changelogConfig: options.changelogConfig
    });
  },

  inMemoryWindowStore(name, { retention, windowSize, strategy } = {}) {
    return new StoreBuilder({
      name,
      type: 'window',
      supplier: () => new MemoryWindowStore(name, { retention, windowSize, strategy }),
      loggingEnabled: false
    });
  },

  windowStoreBuilder(name, supplier, options = {}) {
    return new StoreBuilder({
      name,
      type: 'window',
      supplier,
      loggingEnabled: options.loggingEnabled ?? true,
      cachingEnabled: options.cachingEnabled ?? false,
      changelogConfig: options.changelogConfig
    });
  },

  custom(storeBuilder) {
    if (!(storeBuilder instanceof StoreBuilder)) {
      throw new Error('Stores.custom expects a StoreBuilder instance');
    }
    return storeBuilder;
  }
};

module.exports = {
  Stores
};
