'use strict';

const { KeyValueStore } = require('./store');

class ChangeLoggingKeyValueStore extends KeyValueStore {
  constructor(inner, {
    manager,
    streamId,
    storeName,
    definition
  } = {}) {
    if (!inner) {
      throw new TypeError('ChangeLoggingKeyValueStore requires an underlying store.');
    }
    super(inner.name, {
      persistent: inner.persistent,
      loggingEnabled: true,
      cachingEnabled: inner.cachingEnabled,
      changelogConfig: inner.changelogConfig
    });
    this._inner = inner;
    this._manager = manager ?? null;
    this._streamId = streamId ?? null;
    this._storeName = storeName ?? inner.name;
    this._definition = definition ?? null;
    this._wrapped = true;
  }

  get inner() {
    return this._inner;
  }

  async init(context) {
    if (typeof this._inner.init === 'function') {
      await this._inner.init(context);
    }
    this.context = this._inner.context ?? context ?? null;
  }

  async get(key) {
    return this._inner.get(key);
  }

  async put(key, value) {
    await this._inner.put(key, value);
    await this._appendChange({ key, value });
  }

  async delete(key) {
    if (typeof this._inner.delete === 'function') {
      await this._inner.delete(key);
    }
    await this._appendChange({ key, value: null, tombstone: true });
  }

  async entries() {
    return this._inner.entries();
  }

  async flush() {
    if (typeof this._inner.flush === 'function') {
      await this._inner.flush();
    }
    if (this._manager) {
      await this._manager.flushChangelog({
        streamId: this._streamId,
        storeName: this._storeName
      });
    }
  }

  async close() {
    await this.flush();
    if (typeof this._inner.close === 'function') {
      await this._inner.close();
    }
  }

  async _appendChange({ key, value, tombstone = false }) {
    if (!this._manager) {
      return;
    }
    await this._manager.appendChangelogRecord({
      streamId: this._streamId,
      storeName: this._storeName,
      key,
      value,
      tombstone,
      context: this.context,
      definition: this._definition
    });
  }
}

module.exports = {
  ChangeLoggingKeyValueStore
};
