'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, Serde, MemoryStateStore, query } = require('../src');

class StubRpcClient extends query.QueryRpcClient {
  constructor() {
    super();
    this.invocations = [];
  }

  async fetchKeyValue(payload) {
    this.invocations.push(payload);
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
});
