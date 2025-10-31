'use strict';

const { ChangelogConfig } = require('./changelog-config');

class StoreBuilder {
  constructor({
    name,
    supplier,
    type,
    loggingEnabled = true,
    cachingEnabled = false,
    changelogConfig = null,
    persistent = false,
    retentionMs = null,
    cacheMaxBytes = null,
    additionalMetadata = null
  }) {
    if (!name) {
      throw new Error('StoreBuilder requires a store name');
    }
    if (typeof supplier !== 'function') {
      throw new Error('StoreBuilder requires a supplier function');
    }
    this.name = name;
    this.type = type;
    this.supplier = supplier;
    this.loggingEnabled = loggingEnabled;
    this.cachingEnabled = cachingEnabled;
    this.changelogConfig = changelogConfig instanceof ChangelogConfig ? changelogConfig : (changelogConfig ? new ChangelogConfig(changelogConfig) : null);
    this.persistent = Boolean(persistent);
    this.retentionMs = retentionMs ?? null;
    this.cacheMaxBytes = cacheMaxBytes ?? null;
    this.additionalMetadata = additionalMetadata ?? null;
  }

  withCachingEnabled() {
    return new StoreBuilder({ ...this, cachingEnabled: true });
  }

  withLoggingDisabled() {
    return new StoreBuilder({ ...this, loggingEnabled: false });
  }

  withChangelogConfig(config) {
    return new StoreBuilder({ ...this, changelogConfig: config instanceof ChangelogConfig ? config : new ChangelogConfig(config) });
  }

  async build(context = {}) {
    const storeOrPromise = this.supplier(context);
    const store = await storeOrPromise;
    if (store && typeof store.init === 'function') {
      await store.init(context);
    }
    return store;
  }

  describe() {
    const metadata = {
      name: this.name,
      type: this.type,
      loggingEnabled: this.loggingEnabled,
      cachingEnabled: this.cachingEnabled,
      changelog: this.changelogConfig ? this.changelogConfig.toKafkaConfig(this.name) : null,
      persistent: this.persistent,
      retentionMs: this.retentionMs,
      cacheMaxBytes: this.cacheMaxBytes
    };
    if (this.additionalMetadata) {
      return { ...metadata, ...this.additionalMetadata };
    }
    return metadata;
  }
}

module.exports = {
  StoreBuilder
};
