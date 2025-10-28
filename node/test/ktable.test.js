'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder, KafkaStreams, StreamsConfig, Serde, Materialized } = require('../src');

class NoopProducer {
  async connect() {}
  async disconnect() {}
  async produce() {}
}

test('table materializes records and exposes interactive queries', async () => {
  const builder = new StreamsBuilder();
  builder.table('users-topic', {
    keySerde: Serde.string(),
    valueSerde: Serde.json(),
    materialized: Materialized.as('users-store')
  });

  const topology = builder.build();
  const table = topology.streams.find(stream => stream.isTable && !stream.isGlobalKTable);
  assert(table, 'expected table topology to be registered');

  const kafkaStreams = new KafkaStreams(topology, {
    applicationId: 'table-test-app',
    applicationServer: { host: 'localhost', port: 7777 }
  });

  const producer = new NoopProducer();
  const storeInstances = new Map();
  for (const definition of table.stateStores) {
    storeInstances.set(definition.name, await definition.builder.build({ stream: table }));
  }
  kafkaStreams._registerPrebuiltStateStores(table, storeInstances);

  const keySerde = Serde.string();
  const valueSerde = Serde.json();

  await kafkaStreams._processMessage(
    table,
    {
      topic: 'users-topic',
      partition: 0,
      message: {
        key: keySerde.serialize('user-1'),
        value: valueSerde.serialize({ name: 'Alice' }),
        headers: {}
      }
    },
    producer,
    storeInstances
  );

  const store = kafkaStreams.store('users-store');
  assert(store, 'table store should be registered for interactive queries');
  assert.deepEqual(await store.get('user-1'), { name: 'Alice' });

  await kafkaStreams._processMessage(
    table,
    {
      topic: 'users-topic',
      partition: 0,
      message: {
        key: keySerde.serialize('user-1'),
        value: null,
        headers: {}
      }
    },
    producer,
    storeInstances
  );

  assert.equal(await store.get('user-1'), undefined);
  const metadata = kafkaStreams.metadataForStore('users-store');
  assert.equal(metadata.length, 1);
  assert(metadata[0].hasStore('users-store'));
});

test('globalTable uses a shared consumer group prefix and materializes from the beginning', () => {
  const builder = new StreamsBuilder();
  builder.globalTable('configs-topic', {
    keySerde: Serde.string(),
    valueSerde: Serde.json(),
    materialized: Materialized.as('configs-store')
  });

  const topology = builder.build();
  const table = topology.streams.find(stream => stream.isGlobalKTable);
  assert(table, 'expected global table to be present');
  assert.equal(table.fromBeginning, true);

  const config = new StreamsConfig({
    applicationId: 'global-test-app',
    applicationServer: { host: 'localhost', port: 7000 }
  });

  const groupId = config.resolveGroupId(table);
  assert.equal(groupId, 'global-test-app-global-configs-topic');
  assert.equal(table.materialized.storeName, 'configs-store');
});

test('table topology metadata records the changelog topic', () => {
  const builder = new StreamsBuilder();
  builder.table('inventory-topic', {
    materialized: Materialized.as('inventory-store'),
    changelogTopic: 'inventory-changelog'
  });

  const { topology } = builder.build();
  const tableNode = topology.nodes.find(node => node.type === 'table');
  assert(tableNode, 'table node should exist in topology description');
  assert.equal(tableNode.metadata.materialized.storeName, 'inventory-store');
  assert.equal(tableNode.metadata.materialized.changelogTopic, 'inventory-changelog');
});
