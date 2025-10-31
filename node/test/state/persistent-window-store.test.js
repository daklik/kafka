'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');

const { PersistentWindowStore } = require('../../src/state/persistent-window-store');
const { StubRocksDB } = require('../helpers/stub-rocksdb');

async function tempDirectory(prefix) {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

async function createStore({
  name = 'persistent-window-store',
  windowSize = 1000,
  retentionMs = 10_000
} = {}) {
  const baseDir = await tempDirectory('persistent-window-');
  const storeDirectory = path.join(baseDir, 'stream-1', name);
  const store = new PersistentWindowStore(name, {
    windowSize,
    retentionMs,
    rocksdb: StubRocksDB
  });
  await store.init({
    stream: { id: 'stream-1' },
    storeName: name,
    storeDirectory,
    config: {
      resolveStateStoreDirectory: () => storeDirectory
    }
  });
  return { store, baseDir };
}

test('PersistentWindowStore persists and fetches windowed values', async () => {
  const { store, baseDir } = await createStore({ windowSize: 1000, retentionMs: 5000 });

  await store.put('alpha', 'one', 1000);
  await store.put('alpha', 'two', 2500);
  await store.put('beta', 'other', 1500);

  const alphaWindows = await store.fetch('alpha', 0, 4000);
  assert.deepEqual(alphaWindows, [
    { value: 'one', timestamp: 1000, end: 2000 },
    { value: 'two', timestamp: 2500, end: 3500 }
  ]);

  await store.delete('alpha', 1000);
  const remaining = await store.fetch('alpha', 0, 4000);
  assert.deepEqual(remaining, [
    { value: 'two', timestamp: 2500, end: 3500 }
  ]);

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});

test('PersistentWindowStore purges expired windows and lists entries', async () => {
  const { store, baseDir } = await createStore({ windowSize: 500, retentionMs: 1500 });

  await store.put('alpha', 'v1', 0);
  await store.put('alpha', 'v2', 600);
  await store.put('alpha', 'v3', 2000);

  await store.purge(1000);

  const entries = await store.entries();
  assert.deepEqual(entries, [
    ['alpha', { value: 'v2', timestamp: 600, end: 1100 }],
    ['alpha', { value: 'v3', timestamp: 2000, end: 2500 }]
  ]);

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});
