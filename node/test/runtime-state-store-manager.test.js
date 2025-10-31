'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StateStoreManager } = require('../src/runtime/state-store-manager');
const {
  StoreBuilder,
  MemoryStateStore,
  ChangelogConfig,
  ChangeLoggingKeyValueStore
} = require('../src/state');

function createLoggingStoreBuilder(name = 'logging-store') {
  return new StoreBuilder({
    name,
    type: 'keyValue',
    supplier: () => new MemoryStateStore(name, { loggingEnabled: true }),
    loggingEnabled: true,
    changelogConfig: new ChangelogConfig({ topic: `${name}-changelog` })
  });
}

test('StateStoreManager wraps logging stores and forwards changelog records', async () => {
  const manager = new StateStoreManager();
  const stream = { id: 'stream-logging' };
  const builder = createLoggingStoreBuilder('logging-store');

  const producerCalls = [];
  let flushCount = 0;
  const producer = {
    async send(payload) {
      producerCalls.push(payload);
      return [{ partition: 0, offset: producerCalls.length }];
    },
    async flush() {
      flushCount += 1;
    }
  };

  manager.bindChangelogProducer({ stream, producer });

  const store = await manager.registerStore({ stream, builder, storeName: 'logging-store' });
  assert.ok(store instanceof ChangeLoggingKeyValueStore, 'store should be wrapped for change logging');

  await store.put('alpha', 'one');
  await store.delete('alpha');
  await store.flush();

  assert.equal(producerCalls.length, 2);
  const [firstPayload, secondPayload] = producerCalls;
  assert.equal(firstPayload.topic, 'logging-store-changelog');
  assert.ok(Buffer.isBuffer(firstPayload.messages[0].key));
  assert.ok(Buffer.isBuffer(firstPayload.messages[0].value));
  assert.equal(firstPayload.messages[0].key.toString(), 'alpha');
  assert.equal(firstPayload.messages[0].value.toString(), 'one');
  assert.equal(secondPayload.messages[0].value, null);
  assert.equal(flushCount, 1);

  const checkpoint = manager.getCheckpoint(stream.id, 'logging-store');
  assert.equal(checkpoint, 2);
});

test('StateStoreManager applies restore batches during registration', async () => {
  const manager = new StateStoreManager();
  const stream = { id: 'stream-restore-registration' };
  const builder = createLoggingStoreBuilder('registration-store');

  const restoreBatches = [
    {
      records: [
        { key: 'a', value: '1' },
        { key: 'b', value: '2' }
      ],
      batchEndOffset: 10,
      numRestored: 2
    },
    {
      records: [
        { key: 'a', value: null }
      ],
      batchEndOffset: 12,
      numRestored: 1
    }
  ];

  const store = await manager.registerStore({
    stream,
    builder,
    storeName: 'registration-store',
    restoreBatches,
    restoreOffsets: { partition: 0, startingOffset: 0, endingOffset: 12 }
  });

  const entries = await store.entries();
  assert.deepEqual(entries.sort(), [['b', '2']]);
  assert.equal(manager.getCheckpoint(stream.id, 'registration-store'), 12);
});

test('StateStoreManager.restoreStore replays batches onto existing stores without logging duplicates', async () => {
  const manager = new StateStoreManager();
  const stream = { id: 'stream-restore-existing' };
  const builder = createLoggingStoreBuilder('existing-store');

  const producerCalls = [];
  const producer = {
    async send(payload) {
      producerCalls.push(payload);
      return [{ partition: 0, offset: producerCalls.length }];
    }
  };

  manager.bindChangelogProducer({ stream, producer });

  const store = await manager.registerStore({ stream, builder, storeName: 'existing-store' });

  const restoreBatches = [
    {
      records: [
        { key: 'x', value: '1' },
        { key: 'y', value: '2' }
      ],
      batchEndOffset: 5,
      numRestored: 2
    }
  ];

  await manager.restoreStore({
    stream,
    storeName: 'existing-store',
    restoreBatches,
    restoreOffsets: { partition: 0, startingOffset: 0, endingOffset: 5 }
  });

  assert.equal(await store.get('x'), '1');
  assert.equal(await store.get('y'), '2');
  assert.equal(manager.getCheckpoint(stream.id, 'existing-store'), 5);
  assert.equal(producerCalls.length, 0);
});
