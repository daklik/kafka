'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  processor: { ProcessorContext },
  Stores,
  ChangelogConfig,
  metrics: { MetricsReporter, StreamsMetrics }
} = require('../src');
const { StateStoreManager } = require('../src/runtime/state-store-manager');

class CapturingReporter extends MetricsReporter {
  constructor() {
    super();
    this.samples = [];
  }

  record(sample) {
    this.samples.push(sample);
  }
}

test('ProcessorContext register wires state stores and emits restore lifecycle', async () => {
  const reporter = new CapturingReporter();
  const metrics = new StreamsMetrics({ reporters: [reporter] });
  const manager = new StateStoreManager({ metrics });
  const stream = { id: 'stream-1' };
  const events = [];
  manager.on('restore:start', payload => events.push({ type: 'start', payload }));
  manager.on('restore:batch', payload => events.push({ type: 'batch', payload }));
  manager.on('restore:end', payload => events.push({ type: 'end', payload }));

  const builder = Stores.inMemoryKeyValueStore('custom-store').withChangelogConfig(new ChangelogConfig());
  const listenerCalls = [];
  const listener = {
    onRestoreStart: (...args) => listenerCalls.push({ type: 'start', args }),
    onBatchRestored: (...args) => listenerCalls.push({ type: 'batch', args }),
    onRestoreEnd: (...args) => listenerCalls.push({ type: 'end', args })
  };

  const context = new ProcessorContext({
    stateStoreRegistrar: payload => manager.registerStore({
      stream,
      storeName: payload.storeName,
      builder: payload.storeBuilder ?? builder,
      keySerde: payload.keySerde,
      valueSerde: payload.valueSerde,
      metadata: payload.metadata,
      stateRestoreListener: payload.stateRestoreListener,
      restoreBatches: payload.restoreBatches,
      restoreOffsets: payload.restoreOffsets
    }),
    stateStoreProvider: name => manager.getStore(stream.id, name)
  });

  const restoreOffsets = { startingOffset: 0, endingOffset: 99, partition: 3 };
  const restoreBatches = [{ batchEndOffset: 50, numRestored: 5 }, { batchEndOffset: 99, numRestored: 7 }];

  await context.register(builder, {
    metadata: { scope: 'processor-store' },
    stateRestoreListener: listener,
    restoreBatches,
    restoreOffsets
  });

  const store = context.getStateStore('custom-store');
  assert.ok(store, 'state store should be accessible after registration');
  assert.equal(typeof store.get, 'function');

  assert.equal(events.length, 2 + restoreBatches.length);
  assert.equal(events[0].type, 'start');
  assert.equal(events.at(-1).type, 'end');
  const batchEvents = events.filter(event => event.type === 'batch');
  assert.equal(batchEvents.length, restoreBatches.length);
  assert.equal(events[0].payload.storeName, 'custom-store');
  assert.equal(events[0].payload.streamId, 'stream-1');
  assert.equal(events[0].payload.topic, 'custom-store-changelog');

  assert.equal(listenerCalls.length, 4);
  const [startCall] = listenerCalls.filter(call => call.type === 'start');
  assert.equal(startCall.args[0], 'custom-store-changelog');
  assert.equal(startCall.args[1], 3);

  const batchCalls = listenerCalls.filter(call => call.type === 'batch');
  assert.equal(batchCalls.length, 2);
  assert.equal(batchCalls[0].args[3], 50);
  assert.equal(batchCalls[0].args[4], 5);
  assert.equal(batchCalls[1].args[3], 99);
  assert.equal(batchCalls[1].args[4], 7);

  const [endCall] = listenerCalls.filter(call => call.type === 'end');
  assert.equal(endCall.args[3], 12);

  const metricNames = reporter.samples.map(sample => sample.name);
  assert.ok(metricNames.includes('state.restore.start'));
  assert.ok(metricNames.includes('state.restore.batch'));
  assert.ok(metricNames.includes('state.restore.end'));
});

test('ProcessorContext getStateStore returns null when store missing', () => {
  const manager = new StateStoreManager();
  const stream = { id: 'missing-store-stream' };
  const context = new ProcessorContext({
    stateStoreRegistrar: () => {},
    stateStoreProvider: name => manager.getStore(stream.id, name)
  });

  assert.equal(context.getStateStore('unknown'), null);
});

test('Stores.persistentKeyValueStore describes persistent metadata', () => {
  const builder = Stores.persistentKeyValueStore('persistent', {
    cachingEnabled: true,
    cacheMaxBytes: 1024,
    retentionMs: 60000
  });

  const description = builder.describe();
  assert.equal(description.name, 'persistent');
  assert.equal(description.persistent, true);
  assert.equal(description.cachingEnabled, true);
  assert.equal(description.cacheMaxBytes, 1024);
  assert.equal(description.retentionMs, 60000);
});
