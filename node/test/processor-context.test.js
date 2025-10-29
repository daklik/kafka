'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  StreamsBuilder,
  KafkaStreams,
  Serde,
  processor: {
    ProcessorContext,
    RecordContext,
    To,
    ForwardingDisabledException
  }
} = require('../src');

test('ProcessorContext forward normalizes key/value pairs and To options', async () => {
  const forwarded = [];
  const context = new ProcessorContext({
    forwarder: (record, to) => {
      forwarded.push({ record, to: to.describe() });
    }
  });

  const baseContext = new RecordContext({
    topic: 'input-topic',
    partition: 1,
    offset: 42,
    timestamp: 1_694_000_000_000,
    headers: { traceId: Buffer.from('abc123') }
  });
  context.setRecordContext(baseContext);

  await context.forward('key-1', 'value-1', To.child('processor-b'));

  assert.equal(forwarded.length, 1);
  const [{ record, to }] = forwarded;
  assert.equal(record.key, 'key-1');
  assert.equal(record.value, 'value-1');
  assert.deepEqual(to, { child: 'processor-b', children: null, topic: null, allChildren: false, headers: {} });
  assert.equal(record.recordContext, baseContext);
  assert.deepEqual(record.headers.traceId, baseContext.headers().traceId);
});

test('ProcessorContext forward uses To.all by default', async () => {
  const forwarded = [];
  const context = new ProcessorContext({
    forwarder: (record, to) => forwarded.push({ record, to: to.describe() })
  });
  context.setRecordContext(new RecordContext({ topic: 'topic', partition: 0 }));

  await context.forward({ key: 'k2', value: 'v2' });

  assert.equal(forwarded.length, 1);
  assert.deepEqual(forwarded[0].to, { child: null, children: null, topic: null, allChildren: true, headers: {} });
});

test('ProcessorContext disables forwarding with ForwardingDisabledException', async () => {
  const context = new ProcessorContext({
    forwarder: () => {}
  });
  context.disableForwarding();
  assert.throws(() => context.forward('key', 'value'), ForwardingDisabledException);
});

test('ProcessorContext commit and schedule delegates to callbacks', async () => {
  let committed = false;
  const scheduled = [];
  const context = new ProcessorContext({
    committer: () => { committed = true; },
    scheduler: (interval, punctuator, options) => {
      scheduled.push({ interval, punctuator, options });
      return { cancel: () => { scheduled.push({ cancelled: true }); } };
    }
  });

  await context.commit();
  assert.equal(committed, true);

  const handle = context.schedule(5000, () => 'tick', { type: 'WALL_CLOCK_TIME' });
  assert.equal(typeof handle.cancel, 'function');
  assert.equal(scheduled.length, 1);
  assert.deepEqual(scheduled[0], { interval: 5000, punctuator: scheduled[0].punctuator, options: { type: 'WALL_CLOCK_TIME' } });
  handle.cancel();
  assert.equal(scheduled.length, 2);
  assert.equal(scheduled[1].cancelled, true);
});

test('RecordContext exposes immutable metadata', () => {
  const context = new RecordContext({
    topic: 'orders',
    partition: 2,
    offset: '15',
    timestamp: 1234,
    headers: { customerId: Buffer.from('99') }
  });

  assert.equal(context.topic(), 'orders');
  assert.equal(context.partition(), 2);
  assert.equal(context.offset(), 15);
  assert.equal(context.timestamp(), 1234);
  assert.ok(Object.isFrozen(context.headers()));

  const updated = context.withUpdates({ headers: { customerId: Buffer.from('100') }, timestamp: 5678 });
  assert.equal(updated.topic(), 'orders');
  assert.equal(updated.timestamp(), 5678);
  assert.notEqual(updated.headers().customerId, context.headers().customerId);
});

test('KafkaStreams attaches RecordContext metadata to processed records', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
    .mapValues(value => value)
    .to('output-topic', { valueSerde: Serde.string() });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, { applicationId: 'record-context-test' });
  const stream = topology.streams.find(s => s.isSource);
  const producer = { produce: async () => {} };

  const payload = {
    topic: 'input-topic',
    partition: 3,
    message: {
      key: Buffer.from('key-A'),
      value: Buffer.from('value-A'),
      headers: { correlationId: Buffer.from('correlation') },
      offset: '27',
      timestamp: String(1_694_000_000_500)
    }
  };

  const results = await kafkaStreams._processMessage(stream, payload, producer, new Map());
  assert.equal(results.length, 1);
  const output = results[0];
  assert.equal(output.offset, 27);
  assert.equal(output.recordContext.topic(), 'input-topic');
  assert.equal(output.recordContext.partition(), 3);
  assert.equal(output.recordContext.offset(), 27);
  assert.equal(output.recordContext.timestamp(), 1_694_000_000_500);
  assert.ok(output.recordContext instanceof RecordContext);
  assert.equal(output.headers.correlationId.toString(), 'correlation');
});
