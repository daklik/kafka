'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { StreamsBuilder } = require('../src/streams-builder');
const { Materialized } = require('../src/materialized');
const { TimeWindows } = require('../src/windows/time-windows');
const { Suppressed } = require('../src/suppressed');
const { Serde } = require('../src/serde');

test('topology description surfaces joins, windowed processors, and state stores', () => {
  const builder = new StreamsBuilder();

  const orders = builder.stream('orders-topic', {
    keySerde: Serde.string(),
    valueSerde: Serde.json()
  });

  const customers = builder.table('customers-topic', {
    materialized: Materialized.as('customers-store'),
    keySerde: Serde.string(),
    valueSerde: Serde.json(),
    changelogTopic: 'customers-changelog'
  });

  orders
    .join(customers, (order, customer) => ({
      id: order.id,
      customerId: order.customerId,
      customer
    }), {
      joined: {
        keySerde: Serde.string(),
        valueSerde: Serde.json(),
        otherValueSerde: Serde.json()
      },
      named: 'orders-with-customers'
    })
    .groupBy(order => order.customerId, { named: 'orders-by-customer' })
    .windowedBy(TimeWindows.of(60_000).advanceBy(30_000))
    .count({
      keySerde: Serde.string(),
      valueSerde: Serde.string(),
      storeName: 'customer-window-counts',
      named: 'customer-window-counts-store'
    })
    .suppress(Suppressed.untilWindowCloses())
    .to('customer-counts-topic', {
      keySerde: Serde.json(),
      valueSerde: Serde.json(),
      named: 'customer-counts-sink'
    });

  const { topology } = builder.build();

  const sourceNode = topology.nodes.find(node => node.type === 'source' && node.metadata.topic === 'orders-topic');
  assert(sourceNode, 'expected source node for orders stream');
  assert.equal(sourceNode.metadata.keySerde, 'Serde');

  const tableNode = topology.nodes.find(node => node.type === 'table');
  assert(tableNode, 'expected table node to be registered');
  assert.equal(tableNode.metadata.materialized.storeName, 'customers-store');
  const tableStore = tableNode.stores.find(store => store.name === 'customers-store');
  assert(tableStore, 'table store metadata should be attached to topology');
  assert.equal(tableStore.partitioning, 'by-key');
  assert.equal(tableStore.changelogTopic, 'customers-changelog');

  const joinNode = topology.nodes.find(node => node.metadata.operation === 'join');
  assert(joinNode, 'join processor should be present');
  assert.equal(joinNode.metadata.options.joinType, 'inner');
  assert(joinNode.metadata.join, 'join metadata should be annotated');
  assert.equal(joinNode.metadata.join.partnerType, 'table');
  const joinStore = joinNode.stores.find(store => store.scope === 'table-lookup');
  assert(joinStore, 'join node should reference table lookup store');
  assert.equal(joinStore.partitioning, 'by-key');

  const aggregateNode = topology.nodes.find(node => node.metadata.operation === 'aggregate');
  assert(aggregateNode, 'aggregate processor should be present');
  const aggregateStore = aggregateNode.stores.find(store => store.scope === 'windowed-aggregation-store');
  assert(aggregateStore, 'aggregate node should expose windowed store metadata');
  assert.equal(aggregateStore.windowType, 'time');
  assert.equal(aggregateStore.partitioning, 'by-key');

  const sinkNode = topology.nodes.find(node => node.type === 'sink' && node.metadata.topic === 'customer-counts-topic');
  assert(sinkNode, 'sink node should be described');
  assert.equal(sinkNode.metadata.keySerde, 'Serde');

  const edgeToSink = topology.edges.find(edge => edge.to === sinkNode.id && edge.metadata.type === 'sink');
  assert(edgeToSink, 'sink edge should be captured with metadata');
});
