'use strict';

const { StoreBuilder } = require('./store-builder');
const { MemoryStateStore } = require('./memory-store');

const Stores = {
  inMemoryKeyValueStore(name) {
    return new StoreBuilder({
      name,
      type: 'keyValue',
      supplier: () => new MemoryStateStore(name),
      loggingEnabled: false
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
