'use strict';

const { ChangelogConfig } = require('./changelog-config');

class StoreBuilder {
  constructor({ name, supplier, type, loggingEnabled = true, cachingEnabled = false, changelogConfig = null }) {
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

  build(context) {
    const store = this.supplier();
    if (store && typeof store.then === 'function') {
      return store.then(instance => {
        instance.init?.(context);
        return instance;
      });
    }
    store.init?.(context);
    return store;
  }

  describe() {
    return {
      name: this.name,
      type: this.type,
      loggingEnabled: this.loggingEnabled,
      cachingEnabled: this.cachingEnabled,
      changelog: this.changelogConfig ? this.changelogConfig.toKafkaConfig(this.name) : null
    };
  }
}

module.exports = {
  StoreBuilder
};
