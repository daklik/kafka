'use strict';

class StubWriteBatch {
  constructor(db) {
    this._db = db;
    this._ops = [];
    this.destroyed = false;
  }

  tryPut(key, value) {
    this._ops.push({ type: 'put', key, value });
  }

  tryDelete(key) {
    this._ops.push({ type: 'delete', key });
  }

  async flush() {
    for (const op of this._ops) {
      if (op.type === 'put') {
        await this._db.put(op.key, op.value);
      } else if (op.type === 'delete') {
        await this._db.delete(op.key);
      }
    }
    this._ops = [];
  }

  destroy() {
    this.destroyed = true;
  }
}

class StubRocksDB {
  constructor(directory) {
    this.path = directory;
    this._data = new Map();
    this.flushes = 0;
    this.closed = false;
    this.readyCalled = false;
  }

  async ready() {
    this.readyCalled = true;
  }

  write() {
    return new StubWriteBatch(this);
  }

  async get(key) {
    const entry = this._data.get(Buffer.from(key).toString('hex'));
    return entry ? entry.value : null;
  }

  async put(key, value) {
    const storedKey = Buffer.from(key);
    this._data.set(storedKey.toString('hex'), { key: storedKey, value });
  }

  async delete(key) {
    this._data.delete(Buffer.from(key).toString('hex'));
  }

  iterator() {
    const entries = Array.from(this._data.values()).sort((a, b) => Buffer.compare(a.key, b.key));
    return {
      async *[Symbol.asyncIterator]() {
        for (const entry of entries) {
          yield { key: Buffer.from(entry.key), value: entry.value };
        }
      },
      destroy() {}
    };
  }

  async flush() {
    this.flushes += 1;
  }

  async close() {
    this.closed = true;
  }
}

module.exports = {
  StubRocksDB
};
