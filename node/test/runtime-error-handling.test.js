'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, Serde, errors, metrics } = require('../src');

class RecordingReporter extends metrics.MetricsReporter {
  constructor() {
    super();
    this.records = [];
  }

  record(sample) {
    this.records.push(sample);
  }
}

class SkippingDeserializationHandler extends errors.ExceptionHandler {
  async handle() {
    return errors.HandlerAction.CONTINUE;
  }
}

test('deserialization handler can skip corrupt records', async () => {
  const reporter = new RecordingReporter();
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { valueSerde: Serde.json() })
    .mapValues(value => value)
    .to('output-topic', { valueSerde: Serde.json() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'deser-app',
    metrics: { reporters: [reporter] },
    deserializationExceptionHandler: new SkippingDeserializationHandler()
  });

  const stream = topology.streams.find(s => s.isSource);
  const producer = { produce: async () => { throw new Error('should not be called'); } };

  const payload = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('key-1'),
      value: Buffer.from('not-json'),
      headers: {}
    }
  };

  const results = await kafkaStreams._processMessage(stream, payload, producer, new Map());
  assert.equal(results.length, 0);
  const skipped = reporter.records.filter(sample => sample.name === 'stream.records.skipped');
  assert.equal(skipped.length, 1);
});

class ContinueProductionHandler extends errors.ExceptionHandler {
  async handle() {
    return errors.HandlerAction.CONTINUE;
  }
}

test('production handler can swallow sink produce failures', async () => {
  const reporter = new RecordingReporter();
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
    .mapValues(value => value)
    .to('output-topic', { valueSerde: Serde.string() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'produce-app',
    metrics: { reporters: [reporter] },
    productionExceptionHandler: new ContinueProductionHandler()
  });

  const stream = topology.streams.find(s => s.isSource);
  let attempts = 0;
  const producer = {
    async produce() {
      attempts += 1;
      throw new Error('produce failure');
    }
  };

  const payload = {
    topic: 'input-topic',
    partition: 0,
    message: {
      key: Buffer.from('key-1'),
      value: Buffer.from('value-1'),
      headers: {}
    }
  };

  const records = await kafkaStreams._processMessage(stream, payload, producer, new Map());
  assert.equal(records.length, 1);
  assert.equal(attempts, 1);
  const productionErrors = reporter.records.filter(sample => sample.name === 'stream.production.errors');
  assert.equal(productionErrors.length, 1);
});
