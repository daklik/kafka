'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { TransactionManager } = require('../src/runtime/transaction-manager');

test('transaction manager uses transactional callback when available', async () => {
  const manager = new TransactionManager({ guarantee: 'exactly_once_v2' });
  const calls = [];
  const producer = {
    async transaction(fn) {
      calls.push('transaction');
      return fn({
        async sendOffsets(offsets) {
          calls.push({ type: 'sendOffsets', offsets });
        }
      });
    }
  };
  const result = await manager.withTransaction({
    producer,
    consumer: {},
    offsets: { topic: 'topic', partition: 0, offset: '3' },
    handler: async () => {
      calls.push('handler');
      return 'ok';
    }
  });
  assert.equal(result, 'ok');
  assert.deepEqual(calls, [
    'transaction',
    'handler',
    { type: 'sendOffsets', offsets: [{ topic: 'topic', partition: 0, offset: '3' }] }
  ]);
});

test('transaction manager propagates handler errors', async () => {
  const manager = new TransactionManager({ guarantee: 'exactly_once_v2' });
  const producer = {
    async beginTransaction() {},
    async abortTransaction() {}
  };
  await assert.rejects(
    manager.withTransaction({
      producer,
      handler: async () => {
        throw new Error('failure');
      }
    }),
    /failure/
  );
});
