'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');

const { PersistentKeyValueStore } = require('../../src/state/persistent-key-value-store');

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
        this._db._data.set(op.key, op.value);
      } else if (op.type === 'delete') {
        this._db._data.delete(op.key);
      }
    }
    this._db.flushes += 1;
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
    return this._data.has(key) ? this._data.get(key) : null;
  }

  async put(key, value) {
    this._data.set(key, value);
  }

  async delete(key) {
    this._data.delete(key);
  }

  iterator() {
    const entries = Array.from(this._data.entries());
    return {
      async *[Symbol.asyncIterator]() {
        for (const [key, value] of entries) {
          yield { key, value };
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

function tempDirectory(prefix) {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

async function createStore({ name = 'persistent-store', cachingEnabled = false, cacheMaxBytes = null } = {}) {
  const baseDir = await tempDirectory('persistent-store-');
  const storeDirectory = path.join(baseDir, 'stream-1', name);
  const store = new PersistentKeyValueStore(name, {
    cachingEnabled,
    cacheMaxBytes,
    rocksdb: StubRocksDB
  });
  await store.init({
    stream: { id: 'stream-1' },
    storeName: name,
    storeDirectory,
    cacheMaxBytes,
    config: {
      getCacheMaxBytesBuffering: () => cacheMaxBytes,
      resolveStateStoreDirectory: () => storeDirectory
    }
  });
  return { store, baseDir };
}

test('PersistentKeyValueStore persists values with RocksDB binding', async () => {
  const { store, baseDir } = await createStore();

  await store.put('alpha', 'one');
  assert.equal(await store.get('alpha'), 'one');

  await store.put('beta', 'two');
  const entries = await store.entries();
  assert.deepEqual(entries.sort((a, b) => (a[0] > b[0] ? 1 : -1)), [
    ['alpha', 'one'],
    ['beta', 'two']
  ]);

  await store.delete('alpha');
  assert.equal(await store.get('alpha'), undefined);

  const binding = store._db;
  await store.close();
  assert.equal(binding.closed, true);

  await fs.rm(baseDir, { recursive: true, force: true });
});

test('PersistentKeyValueStore flushes cached entries when cache threshold is exceeded', async () => {
  const { store, baseDir } = await createStore({ cachingEnabled: true, cacheMaxBytes: 3 });

  await store.put('a', '1');
  assert.equal(store._db.flushes, 0);
  assert.equal(await store.get('a'), '1');

  await store.put('b', '2');
  assert.ok(store._db.flushes >= 1, 'cache should flush after exceeding threshold');
  assert.equal(await store.get('b'), '2');

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});

test('PersistentKeyValueStore respects cached tombstones during iteration', async () => {
  const { store, baseDir } = await createStore({ cachingEnabled: true, cacheMaxBytes: 64 });

  await store.put('keep', 'value');
  await store.put('drop', 'value');
  await store.delete('drop');
  await store.flush();

  const entries = await store.entries();
  assert.deepEqual(entries, [['keep', 'value']]);

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});
