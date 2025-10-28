'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  StreamsBuilder,
  KafkaStreams,
  Serde,
  TimeWindows,
  SessionWindows,
  UnlimitedWindows,
  SlidingWindows,
  Suppressed,
  metrics
} = require('../src');
const fixtures = require('./fixtures/windowed-aggregations.json');

class InMemoryProducer {
  constructor() {
    this.produced = [];
  }

  async produce(event) {
    this.produced.push(event);
  }
}

class RecordingReporter extends metrics.MetricsReporter {
  constructor() {
    super();
    this.records = [];
  }

  record(sample) {
    this.records.push(sample);
  }
}

async function buildPipeline({ builder, configure, config = {} }) {
  configure(builder);
  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, { applicationId: 'windowed-aggregation-test', ...config });
  const stream = topology.streams.find(s => s.isSource);
  const producer = new InMemoryProducer();
  const storeInstances = new Map();

  for (const definition of stream.stateStores) {
    const instance = await definition.builder.build({ stream });
    storeInstances.set(definition.name, instance);
  }

  kafkaStreams._registerPrebuiltStateStores(stream, storeInstances);
  return { kafkaStreams, stream, producer, storeInstances };
}

function decodeProduced(producer) {
  return producer.produced.map(event => ({
    topic: event.topic,
    key: event.message.key ? JSON.parse(event.message.key.toString()) : null,
    value: JSON.parse(event.message.value.toString())
  }));
}

test('tumbling window count emits updates per window', async () => {
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(TimeWindows.of(5_000))
        .count({ keySerde: Serde.string(), storeName: 'tumbling-counts' })
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 1_000 },
      { key: 'user-2', value: 'alpha', timestamp: 3_500 },
      { key: 'user-3', value: 'alpha', timestamp: 6_500 }
    ];

    for (const payload of payloads) {
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);
    }

    const produced = decodeProduced(producer);
    assert.deepStrictEqual(produced.map(event => ({ key: event.key, value: event.value })), fixtures.tumbling);
  });
});

test('hopping window count writes each overlapping window', async () => {
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(TimeWindows.of(5_000).advanceBy(2_500))
        .count({ keySerde: Serde.string(), storeName: 'hopping-counts' })
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 2_000 },
      { key: 'user-2', value: 'alpha', timestamp: 3_600 }
    ];

    for (const payload of payloads) {
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);
    }

    const produced = decodeProduced(producer);
    assert.deepStrictEqual(produced.map(event => ({ key: event.key, value: event.value })), fixtures.hopping);
  });
});

test('sliding window aggregate maintains independent windows', async () => {
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(1_000, 1_000))
        .reduce((left, right) => right, { keySerde: Serde.string(), storeName: 'sliding-reduce' })
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 5_000 },
      { key: 'user-2', value: 'alpha', timestamp: 5_500 }
    ];

    for (const payload of payloads) {
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);
    }

    const produced = decodeProduced(producer);
    assert.deepStrictEqual(produced.map(event => ({ key: event.key, value: event.value })), fixtures.sliding);
  });
});

test('session window count merges sessions using default merger', async () => {
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(SessionWindows.with(1_000))
        .count({ keySerde: Serde.string(), storeName: 'session-counts' })
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 1_000 },
      { key: 'user-2', value: 'alpha', timestamp: 1_500 },
      { key: 'user-3', value: 'alpha', timestamp: 4_000 }
    ];

    for (const payload of payloads) {
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);
    }

    const produced = decodeProduced(producer);
    assert.deepStrictEqual(produced.map(event => ({ key: event.key, value: event.value })), fixtures.session);
  });
});

test('unlimited windows retain a single aggregate', async () => {
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(UnlimitedWindows.of())
        .reduce((left, right) => right, { keySerde: Serde.string(), storeName: 'unlimited-reduce' })
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 500 },
      { key: 'user-2', value: 'alpha', timestamp: 1_500 }
    ];

    for (const payload of payloads) {
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);
    }

    const produced = decodeProduced(producer);
    assert.deepStrictEqual(produced.map(event => ({ key: event.key.window ? { key: event.key.key, window: { start: Number.isFinite(event.key.window.start) ? event.key.window.start : Number.MIN_SAFE_INTEGER, end: event.key.window.end } } : event.key, value: event.value })), [
      { key: { key: 'alpha', window: { start: Number.MIN_SAFE_INTEGER, end: 500 } }, value: 'alpha' },
      { key: { key: 'alpha', window: { start: Number.MIN_SAFE_INTEGER, end: 1_500 } }, value: 'alpha' }
    ]);
  });
});

test('suppression buffers windowed results until the window closes', async () => {
  const reporter = new RecordingReporter();
  const builder = new StreamsBuilder();
  await buildPipeline({
    builder,
    config: { metrics: { reporters: [reporter] } },
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(TimeWindows.of(5_000).grace(1_000))
        .count({ keySerde: Serde.string(), storeName: 'suppressed-counts' })
        .suppress(Suppressed.untilWindowCloses())
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 1_000 },
      { key: 'user-2', value: 'alpha', timestamp: 3_000 },
      { key: 'user-3', value: 'alpha', timestamp: 7_000 },
      { key: 'user-4', value: 'alpha', timestamp: 12_000 }
    ];

    for (let index = 0; index < payloads.length; index += 1) {
      const payload = payloads[index];
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);

      if (index < 2) {
        assert.equal(producer.produced.length, 0);
      }

      if (index === 2) {
        const produced = decodeProduced(producer);
        assert.equal(produced.length, 1);
        assert.equal(produced[0].value, 2);
        assert.deepStrictEqual(produced[0].key.window, { start: 0, end: 5_000 });
      }

      if (index === 3) {
        const produced = decodeProduced(producer);
        assert.equal(produced.length, 2);
        assert.equal(produced[1].value, 1);
        assert.deepStrictEqual(produced[1].key.window, { start: 5_000, end: 10_000 });
      }
    }

    const suppressedMetrics = reporter.records.filter(sample => sample.name === 'stream.records.suppressed');
    assert.equal(suppressedMetrics.length, 4);
    const flushedMetrics = reporter.records.filter(sample => sample.name === 'stream.records.suppressed.flushed');
    assert.equal(flushedMetrics.length, 2);
  });
});

test('suppression drops late arrivals and flushes when the buffer fills', async () => {
  const reporter = new RecordingReporter();
  const builder = new StreamsBuilder();
  const bufferConfig = Suppressed.BufferConfig.bounded(1, { emitEarlyWhenFull: true });

  await buildPipeline({
    builder,
    config: { metrics: { reporters: [reporter] } },
    configure: (b) => {
      b
        .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
        .groupBy(value => value)
        .windowedBy(TimeWindows.of(10_000).grace(10_000))
        .count({ keySerde: Serde.string(), storeName: 'bounded-suppression' })
        .suppress(Suppressed.untilWindowCloses(bufferConfig))
        .to('output-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
    }
  }).then(async ({ kafkaStreams, stream, producer, storeInstances }) => {
    const payloads = [
      { key: 'user-1', value: 'alpha', timestamp: 1_000 },
      { key: 'user-2', value: 'alpha', timestamp: 12_000 },
      { key: 'user-3', value: 'alpha', timestamp: 35_000 },
      { key: 'user-4', value: 'alpha', timestamp: 5_000 }
    ];

    for (let index = 0; index < payloads.length; index += 1) {
      const payload = payloads[index];
      await kafkaStreams._processMessage(stream, {
        topic: 'input-topic',
        partition: 0,
        message: {
          key: Serde.string().serialize(payload.key),
          value: Serde.string().serialize(payload.value),
          headers: {},
          timestamp: String(payload.timestamp)
        }
      }, producer, storeInstances);

      if (index === 0) {
        assert.equal(producer.produced.length, 0);
      }

      if (index === 1) {
        const produced = decodeProduced(producer);
        assert.equal(produced.length, 1);
        assert.deepStrictEqual(produced[0].key.window, { start: 0, end: 10_000 });
      }

      if (index === 2) {
        const produced = decodeProduced(producer);
        assert.equal(produced.length, 2);
        assert.deepStrictEqual(produced[1].key.window, { start: 10_000, end: 20_000 });
      }

      if (index === 3) {
        const produced = decodeProduced(producer);
        assert.equal(produced.length, 2);
      }
    }

    const lateMetrics = reporter.records.filter(sample => sample.name === 'stream.records.late');
    assert.equal(lateMetrics.length, 1);
    const overflowMetrics = reporter.records.filter(sample => sample.name === 'stream.suppression.buffer.overflow');
    assert.equal(overflowMetrics.length, 0);
  });
});
