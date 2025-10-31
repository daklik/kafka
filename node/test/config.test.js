'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('node:path');
const os = require('node:os');

const { StreamsConfig, metrics, errors } = require('../src');

class RecordingReporter extends metrics.MetricsReporter {
  constructor() {
    super();
    this.records = [];
  }

  record(sample) {
    this.records.push(sample);
  }
}

test('StreamsConfig derives client and group identifiers with defaults', () => {
  const config = new StreamsConfig({
    applicationId: 'sample-app'
  });

  assert.equal(config.applicationId, 'sample-app');
  assert.equal(config.clientId, 'sample-app-client');
  assert.equal(config.getClientConfig().clientId, 'sample-app-client');
  assert.equal(config.getConsumerConfig().groupId, 'sample-app');
  assert.equal(config.resolveGroupId({ id: 'source' }), 'sample-app-source');
});

test('StreamsConfig enables exactly-once defaults when configured', () => {
  const config = new StreamsConfig({
    applicationId: 'sample-app',
    processingGuarantee: 'exactly_once_v2'
  });

  assert.equal(config.getProducerConfig().idempotent, true);
  assert.equal(config.getProducerConfig().transactionalId, 'sample-app-tx');
});

test('StreamsConfig wires metrics reporters and exception handlers', async () => {
  const reporter = new RecordingReporter();
  let handlerInvoked = false;
  const handler = new errors.LogAndContinueExceptionHandler({
    warn: () => {}
  });
  handler.handle = async context => {
    handlerInvoked = true;
    return errors.HandlerAction.CONTINUE;
  };

  const config = new StreamsConfig({
    applicationId: 'metrics-app',
    metrics: { reporters: [reporter] },
    deserializationExceptionHandler: handler
  });

  const metricsRegistry = config.getMetricsRegistry();
  metricsRegistry.record('stream.test', 1, { streamId: 'stream-1' });

  assert.equal(reporter.records.length, 1);
  const errorHandlers = config.getErrorHandlers();
  await errorHandlers.deserialization.handle({ error: new Error('boom') });
  assert.equal(handlerInvoked, true);
});

test('StreamsConfig rejects unsupported processing guarantee values', () => {
  assert.throws(() => new StreamsConfig({ applicationId: 'invalid', processingGuarantee: 'unknown' }));
});

test('StreamsConfig resolves state directories and cache buffering limits', () => {
  const config = new StreamsConfig({ applicationId: 'stateful-app' });
  const baseDir = config.getStateDirectory();
  assert.ok(baseDir.includes('kafka-streams-state'));
  const resolved = config.resolveStateStoreDirectory({ stream: { id: 'stream-1' }, storeName: 'store-A' });
  assert.equal(resolved, path.join(baseDir, 'stream-1', 'store-A'));
  assert.equal(config.getCacheMaxBytesBuffering(), 10 * 1024 * 1024);
});

test('StreamsConfig accepts custom state directory resolvers and cache overrides', () => {
  const customBase = path.join(os.tmpdir(), 'custom-state-dir');
  const resolver = ({ storeName }) => path.join(customBase, 'scoped', storeName);
  const config = new StreamsConfig({
    applicationId: 'custom-app',
    stateDir: customBase,
    resolveStateStoreDirectory: resolver,
    cacheMaxBytesBuffering: 2048
  });

  assert.equal(config.getStateDirectory(), path.resolve(customBase));
  assert.equal(
    config.resolveStateStoreDirectory({ storeName: 'store-B', stream: { id: 'ignored' } }),
    path.join(customBase, 'scoped', 'store-B')
  );
  assert.equal(config.getCacheMaxBytesBuffering(), 2048);
});
