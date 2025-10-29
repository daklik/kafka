'use strict';

const { StoreBuilder } = require('./store-builder');
const { MemoryStateStore } = require('./memory-store');
const { MemoryWindowStore } = require('./memory-window-store');
const { PersistentKeyValueStore } = require('./persistent-key-value-store');
const { PersistentWindowStore } = require('./persistent-window-store');
const { PersistentSessionStore } = require('./persistent-session-store');

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

  persistentWindowStore(name, options = {}) {
    const loggingEnabled = options.loggingEnabled ?? true;
    const cachingEnabled = options.cachingEnabled ?? false;
    const changelogConfig = options.changelogConfig ?? null;
    const retentionMs = options.retentionMs ?? options.retention ?? null;
    const cacheMaxBytes = options.cacheMaxBytes ?? null;
    const windowSize = options.windowSize ?? null;
    const gracePeriodMs = options.gracePeriodMs ?? options.graceMs ?? null;
    const segmentIntervalMs = options.segmentIntervalMs ?? null;

    return new StoreBuilder({
      name,
      type: 'window',
      supplier: context => new PersistentWindowStore(name, {
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
      cacheMaxBytes,
      additionalMetadata: {
        windowSize,
        gracePeriodMs,
        segmentIntervalMs
      }
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

  persistentSessionStore(name, options = {}) {
    const loggingEnabled = options.loggingEnabled ?? true;
    const cachingEnabled = options.cachingEnabled ?? false;
    const changelogConfig = options.changelogConfig ?? null;
    const retentionMs = options.retentionMs ?? options.retention ?? null;
    const cacheMaxBytes = options.cacheMaxBytes ?? null;
    const gracePeriodMs = options.gracePeriodMs ?? options.graceMs ?? null;

    return new StoreBuilder({
      name,
      type: 'session',
      supplier: context => new PersistentSessionStore(name, {
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
      cacheMaxBytes,
      additionalMetadata: {
        gracePeriodMs
      }
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
