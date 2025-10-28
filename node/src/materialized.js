'use strict';

const { Named } = require('./named');

class Materialized {
  constructor({
    storeName,
    storeBuilder,
    store,
    keySerde,
    valueSerde,
    logging = true,
    caching = false,
    changelogConfig,
    named,
    emitOnUpdate = true,
    storeType = 'keyValue',
    retention,
    windowSize,
    strategy
  } = {}) {
    this.storeName = storeName;
    this.storeBuilder = storeBuilder;
    this.store = store;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.logging = logging;
    this.caching = caching;
    this.changelogConfig = changelogConfig;
    this.named = named;
    this.emitOnUpdate = emitOnUpdate;
    this.storeType = storeType;
    this.retention = retention;
    this.windowSize = windowSize;
    this.strategy = strategy;
  }

  static as(nameOrOptions) {
    const name = Named.from(nameOrOptions, undefined);
    if (!name) {
      throw new Error('Materialized.as requires a name');
    }
    return new Materialized({ storeName: name });
  }

  static with(options = {}) {
    return new Materialized(options);
  }

  withKeySerde(serde) {
    return new Materialized({ ...this, keySerde: serde });
  }

  withValueSerde(serde) {
    return new Materialized({ ...this, valueSerde: serde });
  }

  withLoggingDisabled() {
    return new Materialized({ ...this, logging: false });
  }

  withCachingEnabled() {
    return new Materialized({ ...this, caching: true });
  }

  withStoreBuilder(builder) {
    return new Materialized({ ...this, storeBuilder: builder, storeName: builder?.name ?? this.storeName });
  }

  withChangelogConfig(config) {
    return new Materialized({ ...this, changelogConfig: config });
  }

  withStore(storeSupplier) {
    return new Materialized({ ...this, store: storeSupplier });
  }

  withEmitOnUpdate(emitOnUpdate) {
    return new Materialized({ ...this, emitOnUpdate });
  }

  withStoreType(storeType) {
    return new Materialized({ ...this, storeType });
  }

  withRetention(retention) {
    return new Materialized({ ...this, retention });
  }

  withWindowSize(windowSize) {
    return new Materialized({ ...this, windowSize });
  }

  withStrategy(strategy) {
    return new Materialized({ ...this, strategy });
  }

  resolve(options = {}) {
    return {
      storeName: this.storeName ?? options.storeName,
      storeBuilder: this.storeBuilder ?? options.storeBuilder,
      store: this.store ?? options.store,
      keySerde: this.keySerde ?? options.keySerde,
      valueSerde: this.valueSerde ?? options.valueSerde,
      logging: this.logging ?? options.logging,
      caching: this.caching ?? options.caching,
      changelogConfig: this.changelogConfig ?? options.changelogConfig,
      named: this.named ?? options.named,
      emitOnUpdate: this.emitOnUpdate ?? options.emitOnUpdate,
      storeType: this.storeType ?? options.storeType,
      retention: this.retention ?? options.retention,
      windowSize: this.windowSize ?? options.windowSize,
      strategy: this.strategy ?? options.strategy
    };
  }
}

module.exports = {
  Materialized
};
