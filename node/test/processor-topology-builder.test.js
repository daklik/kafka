'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  StreamsBuilder,
  Serde,
  Stores
} = require('../src');

test('StreamsBuilder manual topology registration captures processors and stores', () => {
  const builder = new StreamsBuilder();

  builder.addSource('input-source', { topics: ['input-topic'], keySerde: Serde.string(), fromBeginning: true });

  builder.addProcessor('uppercase-processor', () => ({
    init: () => {},
    process: () => {}
  }), 'input-source');

  const storeBuilder = Stores.inMemoryKeyValueStore('custom-store');
  builder.addStateStore(storeBuilder, 'uppercase-processor');

  builder.addSink('output-sink', 'output-topic', ['uppercase-processor'], { valueSerde: Serde.string() });

  // Reconnect to ensure idempotent attachment paths do not duplicate entries.
  builder.connectProcessorAndStateStores('uppercase-processor', 'custom-store');

  const built = builder.build();
  const { processorTopology } = built;

  assert.ok(processorTopology, 'processor topology metadata should be available');
  const source = processorTopology.nodes.find(node => node.name === 'input-source');
  assert(source, 'manual source should be registered');
  assert.equal(source.type, 'source');
  assert.deepEqual(source.metadata.topics, ['input-topic']);

  const processor = processorTopology.nodes.find(node => node.name === 'uppercase-processor');
  assert(processor, 'manual processor should be registered');
  assert.ok(processor.parents.includes(source.id), 'processor should reference source as parent');
  assert.equal(processor.stores.length, 1, 'processor should expose attached state store');
  assert.equal(processor.stores[0].name, 'custom-store');

  const sink = processorTopology.nodes.find(node => node.name === 'output-sink');
  assert(sink, 'manual sink should be registered');
  assert.ok(sink.parents.includes(processor.id), 'sink should reference processor as parent');
  assert.equal(sink.metadata.topic, 'output-topic');

  const stateStore = processorTopology.stateStores.find(store => store.name === 'custom-store');
  assert(stateStore, 'state store should appear in processor topology metadata');
  assert.ok(stateStore.processors.includes('uppercase-processor'), 'state store should reference processor attachment');
});

test('DSL pipelines populate processor topology metadata', () => {
  const builder = new StreamsBuilder();

  builder
    .stream('orders', { keySerde: Serde.string(), valueSerde: Serde.json() })
    .mapValues(order => ({ ...order, status: 'processed' }))
    .to('processed-orders', { valueSerde: Serde.json() });

  const built = builder.build();
  const { processorTopology } = built;

  const sourceNode = processorTopology.nodes.find(node => node.name === 'source-orders');
  assert(sourceNode, 'DSL source should be represented in processor topology');
  assert.equal(sourceNode.type, 'source');

  const processorNode = processorTopology.nodes.find(node => node.metadata.operation === 'mapValues');
  assert(processorNode, 'DSL processor should capture operation metadata');
  assert.ok(processorNode.parents.includes(sourceNode.id), 'processor should reference source parent');

  const sinkNode = processorTopology.nodes.find(node => node.metadata.topic === 'processed-orders');
  assert(sinkNode, 'DSL sink should be represented in processor topology');
  assert.ok(sinkNode.parents.includes(processorNode.id), 'sink should reference preceding processor');
});
