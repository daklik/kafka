'use strict';

class MemoryStateStore {
  constructor() {
    this._store = new Map();
  }

  async get(key) {
    return this._store.get(key);
  }

  async put(key, value) {
    this._store.set(key, value);
  }

  async delete(key) {
    this._store.delete(key);
  }

  async entries() {
    return Array.from(this._store.entries());
  }

  async clear() {
    this._store.clear();
  }
}

module.exports = {
  MemoryStateStore
};
