'use strict';

const { Named } = require('./named');

class Materialized {
  constructor({ storeName, storeBuilder, keySerde, valueSerde, logging = true, caching = false, changelogConfig } = {}) {
    this.storeName = storeName;
    this.storeBuilder = storeBuilder;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.logging = logging;
    this.caching = caching;
    this.changelogConfig = changelogConfig;
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

  resolve(options = {}) {
    return {
      storeName: this.storeName ?? options.storeName,
      storeBuilder: this.storeBuilder ?? options.storeBuilder,
      keySerde: this.keySerde ?? options.keySerde,
      valueSerde: this.valueSerde ?? options.valueSerde,
      logging: this.logging ?? options.logging,
      caching: this.caching ?? options.caching,
      changelogConfig: this.changelogConfig ?? options.changelogConfig
    };
  }
}

module.exports = {
  Materialized
};
