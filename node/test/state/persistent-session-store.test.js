'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const path = require('node:path');
const os = require('node:os');

const { PersistentSessionStore } = require('../../src/state/persistent-session-store');
const { StubRocksDB } = require('../helpers/stub-rocksdb');

async function tempDirectory(prefix) {
  return fs.mkdtemp(path.join(os.tmpdir(), prefix));
}

async function createStore({ name = 'persistent-session-store', retentionMs = 10_000 } = {}) {
  const baseDir = await tempDirectory('persistent-session-');
  const storeDirectory = path.join(baseDir, 'stream-1', name);
  const store = new PersistentSessionStore(name, {
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

test('PersistentSessionStore merges overlapping sessions and finds by key', async () => {
  const { store, baseDir } = await createStore({ retentionMs: 5000 });

  await store.put({ key: 'user-1', start: 0, end: 100 }, 'value-a');
  await store.put({ key: 'user-1', start: 80, end: 250 }, 'value-b');

  const sessions = await store.findSessions('user-1', 0, 500);
  assert.deepEqual(sessions, [
    { key: 'user-1', start: 0, end: 250, value: 'value-b' }
  ]);

  await store.put({ key: 'user-1', start: 0, end: 250 }, null);
  const afterDelete = await store.findSessions('user-1', 0, 500);
  assert.deepEqual(afterDelete, []);

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});

test('PersistentSessionStore supports key range queries and purging', async () => {
  const { store, baseDir } = await createStore({ retentionMs: 4000 });

  await store.put({ key: 'user-a', start: 0, end: 100 }, 'a1');
  await store.put({ key: 'user-b', start: 200, end: 400 }, 'b1');
  await store.put({ key: 'user-c', start: 500, end: 700 }, 'c1');

  const range = await store.findSessions('user-a', 'user-b', 0, 500);
  assert.deepEqual(range, [
    { key: 'user-a', start: 0, end: 100, value: 'a1' },
    { key: 'user-b', start: 200, end: 400, value: 'b1' }
  ]);

  await store.purge(350);
  const remaining = await store.entries();
  assert.deepEqual(remaining, [
    ['user-b', { start: 200, end: 400, value: 'b1' }],
    ['user-c', { start: 500, end: 700, value: 'c1' }]
  ]);

  await store.close();
  await fs.rm(baseDir, { recursive: true, force: true });
});
