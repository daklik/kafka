'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, Serde, MemoryStateStore } = require('../src');

class StubRpcClient {
  constructor(results = {}) {
    this.invocations = [];
    this.results = results;
  }

  async fetchKeyValue(payload) {
    this.invocations.push(payload);
    const key = payload.hostInfo?.host ?? 'default';
    if (this.results[key] instanceof Error) {
      throw this.results[key];
    }
    if (typeof this.results[key] === 'function') {
      return this.results[key](payload);
    }
    return { key: payload.key, value: `remote-${payload.key}` };
  }
}

test('interactive query service returns local store and metadata', async () => {
  const builder = new StreamsBuilder();
  builder
    .stream('iq-input', { keySerde: Serde.string(), valueSerde: Serde.json() })
    .groupBy(value => value.category)
    .count({ storeName: 'iq-counts', store: () => new MemoryStateStore('iq-counts') });

  const topology = builder.build();
  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'iq-test',
    applicationServer: { host: 'localhost', port: 7001 }
  });

  const stream = topology.streams.find(s => s.sourceTopic === 'iq-input');
  const storeInstances = new Map();
  for (const definition of stream.stateStores) {
    storeInstances.set(definition.name, await definition.builder.build({ stream }));
  }
  kafkaStreams._registerPrebuiltStateStores(stream, storeInstances);

  const store = kafkaStreams.store('iq-counts');
  assert.ok(store, 'store should be returned for interactive queries');
  await store.put('alpha', 3);
  assert.equal(await kafkaStreams.getInteractiveQueryService().get('iq-counts', 'alpha'), 3);

  const metadata = kafkaStreams.metadataForStore('iq-counts');
  assert.equal(metadata.length, 1);
  assert.equal(metadata[0].hostInfo.host, 'localhost');
  assert.equal(metadata[0].hostInfo.port, 7001);
});

test('interactive query service delegates to rpc client for remote stores', async () => {
  const builder = new StreamsBuilder();
  const topology = builder.build();
  const rpcClient = new StubRpcClient();
  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'iq-remote',
    applicationServer: { host: 'localhost', port: 7001 },
    interactiveQueries: { rpcClient }
  });

  kafkaStreams.registerRemoteStoreMetadata({
    storeName: 'remote-store',
    hostInfo: { host: 'remote-host', port: 8111 },
    topicPartitions: [{ topic: 'remote-topic', partitions: [0, 1] }]
  });

  const result = await kafkaStreams.getInteractiveQueryService().get('remote-store', 'alpha');
  assert.deepEqual(result, { key: 'alpha', value: 'remote-alpha' });
  assert.equal(rpcClient.invocations.length, 1);
  assert.equal(rpcClient.invocations[0].hostInfo.host, 'remote-host');
  assert.equal(rpcClient.invocations[0].options.role, 'active');
});

test('interactive query service falls back to standby replicas when remote fetch fails', async () => {
  const builder = new StreamsBuilder();
  const topology = builder.build();
  const rpcClient = new StubRpcClient({
    'remote-host': new Error('primary down'),
    'standby-host': payload => ({ key: payload.key, value: `standby-${payload.key}` })
  });
  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'iq-remote',
    applicationServer: { host: 'localhost', port: 7001 },
    interactiveQueries: { rpcClient }
  });

  kafkaStreams.registerRemoteStoreMetadata({
    storeName: 'remote-store',
    hostInfo: { host: 'remote-host', port: 8111 },
    topicPartitions: [{ topic: 'remote-topic', partitions: [0] }]
  });
  kafkaStreams.registerRemoteStoreMetadata({
    storeName: 'remote-store',
    hostInfo: { host: 'standby-host', port: 8222 },
    topicPartitions: [{ topic: 'remote-topic', partitions: [0] }],
    standby: true
  });

  const service = kafkaStreams.getInteractiveQueryService();
  const result = await service.get('remote-store', 'alpha', {
    partitioner: key => key === 'alpha' ? 0 : 1
  });

  assert.deepEqual(result, { key: 'alpha', value: 'standby-alpha' });
  assert.equal(rpcClient.invocations.length, 2);
  assert.equal(rpcClient.invocations[0].hostInfo.host, 'remote-host');
  assert.equal(rpcClient.invocations[1].hostInfo.host, 'standby-host');
  assert.equal(rpcClient.invocations[1].options.role, 'standby');
});
