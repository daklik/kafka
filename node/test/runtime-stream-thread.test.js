'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  StreamThread,
  TaskManager,
  TaskState,
  StateStoreManager,
  StreamsConfig
} = require('../src');
const { TransactionManager } = require('../src/runtime/transaction-manager');

function createClock(start = 0) {
  let now = start;
  return {
    now: () => now,
    set: value => {
      now = value;
    }
  };
}

test('stream thread commits offsets based on commit interval', async () => {
  const clock = createClock();
  const config = new StreamsConfig({ applicationId: 'thread-app', commitInterval: 100 });
  const taskManager = new TaskManager({ clock });
  const stateStoreManager = new StateStoreManager();
  const consumerCommits = [];
  const consumer = {
    async commitOffsets(offsets) {
      consumerCommits.push(offsets);
    }
  };
  const producer = {};
  const stream = { id: 'stream-1', sourceTopic: 'input-topic' };

  const thread = new StreamThread({
    stream,
    taskManager,
    stateStoreManager,
    config,
    clock
  });
  thread.bindConsumer(consumer);
  thread.bindProducer(producer);

  taskManager.assign(stream.id, [{ partition: 0, state: TaskState.RESTORING }]);
  taskManager.updateState(stream.id, 0, TaskState.RUNNING);

  const handler = thread.wrapHandler(async payload => {
    await taskManager.recordProcessed(stream.id, payload.partition, payload.message.offset, Number(payload.message.timestamp));
  });

  await handler({ topic: 'input-topic', partition: 0, message: { offset: '0', timestamp: '0' } });
  assert.equal(consumerCommits.length, 1);
  assert.equal(consumerCommits[0][0].offset, '1');

  clock.set(50);
  await handler({ topic: 'input-topic', partition: 0, message: { offset: '1', timestamp: '1' } });
  assert.equal(consumerCommits.length, 1);

  clock.set(200);
  await handler({ topic: 'input-topic', partition: 0, message: { offset: '2', timestamp: '2' } });
  assert.equal(consumerCommits.length, 2);
  assert.equal(consumerCommits[1][0].offset, '3');
});

test('exactly-once transactions send offsets and commit', async () => {
  const clock = createClock();
  const config = new StreamsConfig({
    applicationId: 'eos-app',
    processingGuarantee: 'exactly_once_v2'
  });
  const taskManager = new TaskManager({ clock });
  const stateStoreManager = new StateStoreManager();
  const actions = [];
  const producer = {
    async beginTransaction() {
      actions.push('begin');
    },
    async sendOffsetsToTransaction(offsets) {
      actions.push({ type: 'sendOffsets', offsets });
    },
    async commitTransaction() {
      actions.push('commit');
    },
    async abortTransaction() {
      actions.push('abort');
    }
  };
  const consumer = {
    async commitOffsets() {
      throw new Error('commitOffsets should not be called in EOS mode');
    }
  };
  const stream = { id: 'stream-2', sourceTopic: 'input-topic' };
  const transactionManager = new TransactionManager({ guarantee: 'exactly_once_v2' });

  const thread = new StreamThread({
    stream,
    taskManager,
    stateStoreManager,
    config,
    clock,
    transactionManager
  });
  thread.bindConsumer(consumer);
  thread.bindProducer(producer);

  taskManager.assign(stream.id, [{ partition: 0, state: TaskState.RESTORING }]);
  taskManager.updateState(stream.id, 0, TaskState.RUNNING);

  const handler = thread.wrapHandler(async payload => {
    await taskManager.recordProcessed(stream.id, payload.partition, payload.message.offset, Number(payload.message.timestamp));
  });

  await handler({ topic: 'input-topic', partition: 0, message: { offset: '4', timestamp: '4' } });
  assert.deepEqual(actions, [
    'begin',
    { type: 'sendOffsets', offsets: [{ topic: 'input-topic', partition: 0, offset: '5' }] },
    'commit'
  ]);
});

test('transaction aborts on handler failure', async () => {
  const config = new StreamsConfig({
    applicationId: 'abort-app',
    processingGuarantee: 'exactly_once_v2'
  });
  const taskManager = new TaskManager();
  const stateStoreManager = new StateStoreManager();
  const actions = [];
  const producer = {
    async beginTransaction() {
      actions.push('begin');
    },
    async sendOffsetsToTransaction() {
      actions.push('sendOffsets');
    },
    async commitTransaction() {
      actions.push('commit');
    },
    async abortTransaction() {
      actions.push('abort');
    }
  };
  const stream = { id: 'stream-3', sourceTopic: 'input-topic' };

  const thread = new StreamThread({
    stream,
    taskManager,
    stateStoreManager,
    config,
    transactionManager: new TransactionManager({ guarantee: 'exactly_once_v2' })
  });
  thread.bindConsumer({});
  thread.bindProducer(producer);

  const handler = thread.wrapHandler(async () => {
    throw new Error('boom');
  });

  await assert.rejects(handler({ topic: 'input-topic', partition: 0, message: { offset: '0', timestamp: '0' } }), /boom/);
  assert.deepEqual(actions, ['begin', 'abort']);
});

test('state restoration emits lifecycle events and transitions task state', async () => {
  const taskManager = new TaskManager();
  const stateStoreManager = new StateStoreManager();
  const stream = { id: 'stream-restore', sourceTopic: 'input-topic' };
  const builder = {
    name: 'store',
    describe: () => ({ changelog: { topic: 'store-changelog' } }),
    async build() {
      return new Map();
    }
  };

  await stateStoreManager.registerStore({ stream, builder, storeName: 'store' });
  const restoreEvents = [];
  stateStoreManager.on('restore:batch', event => restoreEvents.push(event));

  const thread = new StreamThread({
    stream,
    taskManager,
    stateStoreManager,
    config: new StreamsConfig({ applicationId: 'restore-app' })
  });

  taskManager.assign(stream.id, [{ partition: 0, state: TaskState.RESTORING }]);

  await thread.restoreTask({
    partition: 0,
    storeName: 'store',
    restoreBatches: [{ batchEndOffset: 10, numRestored: 5 }],
    restoreOffsets: { partition: 0, startingOffset: 0, endingOffset: 10 }
  });

  const task = taskManager.snapshot(stream.id).find(t => t.partition === 0);
  assert.equal(task.state, TaskState.RUNNING);
  assert.equal(restoreEvents.length, 1);
  assert.equal(restoreEvents[0].numRestored, 5);
});
