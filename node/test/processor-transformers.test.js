'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  StreamsBuilder,
  KafkaStreams,
  Serde,
  Stores,
  processor: { ValueTransformer }
} = require('../src');

class CountingValueTransformer extends ValueTransformer {
  constructor(storeBuilder) {
    super();
    this._storeBuilder = storeBuilder;
    this._store = null;
  }

  async init(context) {
    await context.register(this._storeBuilder);
    this._store = context.getStateStore(this._storeBuilder.name);
  }

  async transform(value) {
    const key = value.toLowerCase();
    const current = (await this._store.get(key)) ?? 0;
    const next = current + 1;
    await this._store.put(key, next);
    return `${key}:${next}`;
  }
}

class SummingProcessor {
  constructor(storeBuilder) {
    this._storeBuilder = storeBuilder;
    this._store = null;
    this._context = null;
  }

  async init(context) {
    this._context = context;
    await context.register(this._storeBuilder);
    this._store = context.getStateStore(this._storeBuilder.name);
  }

  async process(record) {
    if (record.key == null) {
      return;
    }
    const current = (await this._store.get(record.key)) ?? 0;
    const next = current + Number(record.value ?? 0);
    await this._store.put(record.key, next);
    await this._context.forward({ key: record.key, value: next });
  }
}

function createPayload({ topic, key, value, timestamp }) {
  return {
    topic,
    partition: 0,
    message: {
      key: key != null ? Buffer.from(key) : null,
      value: value != null ? Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)) : null,
      headers: {},
      timestamp: String(timestamp)
    }
  };
}

test('transformValues value transformer registers stores and emits transformed values', async () => {
  const builder = new StreamsBuilder();
  const storeBuilder = Stores.inMemoryKeyValueStore('value-counts');

  builder
    .stream('transform-input', { keySerde: Serde.string(), valueSerde: Serde.string() })
    .transformValues(() => new CountingValueTransformer(storeBuilder), { stateStore: storeBuilder })
    .to('transform-output', { valueSerde: Serde.string() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, { applicationId: 'processor-transform-values' });
  const stream = topology.streams.find(s => s.isSource);
  const stores = await kafkaStreams._ensureStateStores(stream);
  const produced = [];
  const producer = {
    async produce(event) {
      produced.push({
        topic: event.topic,
        key: event.message.key?.toString(),
        value: event.message.value?.toString()
      });
    }
  };

  const inputs = [
    { key: 'A', value: 'Alpha', timestamp: Date.now() },
    { key: 'B', value: 'Beta', timestamp: Date.now() + 1 },
    { key: 'C', value: 'Alpha', timestamp: Date.now() + 2 }
  ];

  for (const entry of inputs) {
    await kafkaStreams._processMessage(stream, createPayload({ topic: 'transform-input', ...entry }), producer, stores);
  }

  assert.equal(produced.length, 3);
  assert.deepEqual(produced.map(record => record.value), ['alpha:1', 'beta:1', 'alpha:2']);

  const stateStore = stores.get('value-counts');
  const entries = await stateStore.entries();
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  assert.deepEqual(entries, [['alpha', 2], ['beta', 1]]);
});

test('process processor forwards aggregated results with state store backing', async () => {
  const builder = new StreamsBuilder();
  const storeBuilder = Stores.inMemoryKeyValueStore('sums');

  builder
    .stream('process-input', { keySerde: Serde.string(), valueSerde: Serde.json() })
    .process(() => new SummingProcessor(storeBuilder), { stateStore: storeBuilder })
    .to('process-output', { valueSerde: Serde.json() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, { applicationId: 'processor-process-aggregation' });
  const stream = topology.streams.find(s => s.isSource);
  const stores = await kafkaStreams._ensureStateStores(stream);
  const produced = [];
  const producer = {
    async produce(event) {
      produced.push({
        topic: event.topic,
        key: event.message.key ? event.message.key.toString() : null,
        value: JSON.parse(event.message.value.toString())
      });
    }
  };

  const now = Date.now();
  const inputs = [
    { key: 'acct-1', value: 5, timestamp: now },
    { key: 'acct-1', value: 7, timestamp: now + 1 },
    { key: 'acct-2', value: 3, timestamp: now + 2 }
  ];

  for (const entry of inputs) {
    await kafkaStreams._processMessage(
      stream,
      createPayload({ topic: 'process-input', key: entry.key, value: entry.value, timestamp: entry.timestamp }),
      producer,
      stores
    );
  }

  assert.deepEqual(produced, [
    { topic: 'process-output', key: 'acct-1', value: 5 },
    { topic: 'process-output', key: 'acct-1', value: 12 },
    { topic: 'process-output', key: 'acct-2', value: 3 }
  ]);

  const stateStore = stores.get('sums');
  const entries = await stateStore.entries();
  entries.sort((a, b) => a[0].localeCompare(b[0]));
  assert.deepEqual(entries, [['acct-1', 12], ['acct-2', 3]]);
});

