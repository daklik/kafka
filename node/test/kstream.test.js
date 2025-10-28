'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, Serde, MemoryStateStore } = require('../src');

class InMemoryProducer {
  constructor() {
    this.produced = [];
  }

  async connect() {}
  async disconnect() {}

  async produce(event) {
    this.produced.push(event);
  }
}

test('map/filter/flatMapValues pipeline produces expected output', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.json() })
    .mapValues(value => ({ ...value, flagged: true }))
    .filter(value => value.include)
    .flatMapValues(value => value.items ?? [])
    .to('output-topic', { valueSerde: Serde.json() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {});
  const stream = topology.streams[0];
  const producer = new InMemoryProducer();

  const payload = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('key-1'),
      value: Serde.json().serialize({ include: true, items: [1, 2] }),
      headers: {}
    }
  };

  const records = await kafkaStreams._processMessage(stream, payload, producer, new Map());
  assert.equal(records.length, 2);
  assert.equal(producer.produced.length, 2);
  assert.deepEqual(
    producer.produced.map(entry => ({
      topic: entry.topic,
      value: JSON.parse(entry.message.value.toString())
    })),
    [
      { topic: 'output-topic', value: 1 },
      { topic: 'output-topic', value: 2 }
    ]
  );
});

test('groupBy/count aggregates values into a state store and emits updates', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.json() })
    .groupBy(value => value.category)
    .count({
      storeName: 'category-counts',
      store: () => new MemoryStateStore()
    })
    .to('counts-topic', { valueSerde: Serde.json() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {});
  const stream = topology.streams[0];
  const producer = new InMemoryProducer();

  const storeInstances = new Map();
  for (const definition of stream.stateStores) {
    storeInstances.set(definition.name, await definition.supplier());
  }

  const firstRecord = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('user-1'),
      value: Serde.json().serialize({ category: 'alpha' }),
      headers: {}
    }
  };

  const secondRecord = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('user-2'),
      value: Serde.json().serialize({ category: 'alpha' }),
      headers: {}
    }
  };

  await kafkaStreams._processMessage(stream, firstRecord, producer, storeInstances);
  await kafkaStreams._processMessage(stream, secondRecord, producer, storeInstances);

  assert.equal(producer.produced.length, 2);
  assert.deepEqual(
    producer.produced.map(event => ({
      key: event.message.key.toString(),
      value: JSON.parse(event.message.value.toString())
    })),
    [
      { key: 'alpha', value: 1 },
      { key: 'alpha', value: 2 }
    ]
  );

  const store = storeInstances.get('category-counts');
  assert.equal(await store.get('alpha'), 2);
});

test('through operation writes intermediate topic and continues downstream', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
    .through('intermediate-topic')
    .mapValues(value => `${value}-processed`)
    .to('output-topic', { valueSerde: Serde.string() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {});
  const stream = topology.streams[0];
  const producer = new InMemoryProducer();

  const payload = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('key-1'),
      value: Buffer.from('value-1'),
      headers: {}
    }
  };

  await kafkaStreams._processMessage(stream, payload, producer, new Map());

  assert.equal(producer.produced.length, 2);
  assert.deepEqual(
    producer.produced.map(event => ({
      topic: event.topic,
      key: event.message.key.toString(),
      value: event.message.value.toString()
    })),
    [
      { topic: 'intermediate-topic', key: 'key-1', value: 'value-1' },
      { topic: 'output-topic', key: 'key-1', value: 'value-1-processed' }
    ]
  );
});
