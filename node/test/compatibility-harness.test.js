'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, Serde, MemoryStateStore } = require('../src');
const fixture = require('./fixtures/word-count.json');

class HarnessProducer {
  constructor() {
    this.produced = [];
  }

  async connect() {}
  async disconnect() {}

  async produce(event) {
    this.produced.push({
      topic: event.topic,
      key: event.message.key?.toString(),
      value: event.message.value?.toString()
    });
  }
}

test('word count compatibility harness matches Java expectations', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream(fixture.inputTopic, { keySerde: Serde.string(), valueSerde: Serde.string() })
    .flatMapValues(value => value.toLowerCase().split(/\s+/))
    .groupBy(word => word)
    .count({ storeName: 'word-counts', store: () => new MemoryStateStore('word-counts') });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, { applicationId: 'compat-harness' });

  const stream = topology.streams.find(s => s.sourceTopic === fixture.inputTopic);
  const storeInstances = new Map();
  for (const definition of stream.stateStores) {
    storeInstances.set(definition.name, await definition.builder.build({ stream }));
  }
  kafkaStreams._registerPrebuiltStateStores(stream, storeInstances);
  const producer = new HarnessProducer();

  for (const [index, record] of fixture.records.entries()) {
    const payload = {
      topic: fixture.inputTopic,
      partition: 0,
      message: {
        key: Buffer.from(record.key ?? `key-${index}`),
        value: Buffer.from(record.value),
        headers: {}
      }
    };
    await kafkaStreams._processMessage(stream, payload, producer, storeInstances);
  }

  const store = kafkaStreams.store('word-counts');
  const entries = await store.entries();
  const actual = entries
    .map(([key, value]) => ({ key, value }))
    .sort((a, b) => a.key.localeCompare(b.key));
  const expected = fixture.expected
    .map(entry => ({ key: entry.key, value: entry.value }))
    .sort((a, b) => a.key.localeCompare(b.key));

  assert.deepEqual(actual, expected);
});
