'use strict';

class StateStore {
  constructor(name, { persistent = false, loggingEnabled = true, cachingEnabled = false, changelogConfig = null } = {}) {
    if (!name) {
      throw new Error('StateStore requires a name');
    }
    this.name = name;
    this.persistent = persistent;
    this.loggingEnabled = loggingEnabled;
    this.cachingEnabled = cachingEnabled;
    this.changelogConfig = changelogConfig;
    this.context = null;
  }

  init(context) {
    this.context = context;
  }

  async flush() {}

  async close() {}
}

class KeyValueStore extends StateStore {
  async get() {
    throw new Error('KeyValueStore#get must be implemented by subclasses');
  }

  async put() {
    throw new Error('KeyValueStore#put must be implemented by subclasses');
  }

  async delete() {
    throw new Error('KeyValueStore#delete must be implemented by subclasses');
  }

  async entries() {
    throw new Error('KeyValueStore#entries must be implemented by subclasses');
  }
}

class WindowStore extends StateStore {
  constructor(name, options = {}) {
    super(name, options);
    this.windowSize = options.windowSize;
    this.retention = options.retention;
  }

  async fetch() {
    throw new Error('WindowStore#fetch must be implemented by subclasses');
  }

  async put() {
    throw new Error('WindowStore#put must be implemented by subclasses');
  }
}

class SessionStore extends StateStore {
  constructor(name, options = {}) {
    super(name, options);
    this.retention = options.retention;
  }

  async findSessions() {
    throw new Error('SessionStore#findSessions must be implemented by subclasses');
  }

  async put() {
    throw new Error('SessionStore#put must be implemented by subclasses');
  }
}

module.exports = {
  StateStore,
  KeyValueStore,
  WindowStore,
  SessionStore
};
