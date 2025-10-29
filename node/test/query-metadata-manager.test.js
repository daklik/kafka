'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { query } = require('../src');

function createStore() {
  return {
    async get(key) {
      return key;
    }
  };
}

test('metadata manager tracks assignments and standby replicas', () => {
  const manager = new query.QueryMetadataManager({
    hostInfo: { host: 'localhost', port: 7001 }
  });
  manager.registerLocalStore({
    storeName: 'store-A',
    store: createStore(),
    stream: { id: 'stream-A', sourceTopic: 'topic-A' }
  });
  manager.updateLocalAssignment({
    storeName: 'store-A',
    topic: 'topic-A',
    activePartitions: [0, 1],
    standbyPartitions: [2]
  });

  manager.registerRemoteStore({
    storeName: 'store-A',
    hostInfo: { host: 'remote-active', port: 9000 },
    topicPartitions: [{ topic: 'topic-A', partitions: [0, 1] }]
  });
  manager.registerRemoteStore({
    storeName: 'store-A',
    hostInfo: { host: 'remote-standby', port: 9001 },
    topicPartitions: [{ topic: 'topic-A', partitions: [0, 1] }],
    standby: true
  });

  const metadata = manager.getMetadataForStore('store-A');
  assert.equal(metadata.length, 3);
  assert(metadata[0].hasStore('store-A'));

  const candidates = manager.remoteCandidatesForStore({
    storeName: 'store-A',
    key: 'alpha',
    partitioner: () => 0
  });
  assert.equal(candidates.length, 2);
  assert.equal(candidates[0].hostInfo.host, 'remote-active');
  assert.equal(candidates[0].standby, false);
  assert.equal(candidates[1].hostInfo.host, 'remote-standby');
  assert.equal(candidates[1].standby, true);

  manager.refreshFromSnapshot({
    hosts: [{
      hostInfo: { host: 'snapshot-active', port: 9100 },
      stores: [{
        storeName: 'store-A',
        topicPartitions: [{ topic: 'topic-A', partitions: [0] }]
      }]
    }]
  });

  const afterSnapshot = manager.remoteCandidatesForStore({
    storeName: 'store-A',
    key: 'alpha',
    partitioner: () => 0
  });
  assert.equal(afterSnapshot.length, 1);
  assert.equal(afterSnapshot[0].hostInfo.host, 'snapshot-active');

  const route = manager.routeQuery({
    storeName: 'store-A',
    key: 'beta',
    partitioner: () => 1
  });
  assert.equal(route.type, 'local');
  assert.equal(typeof route.store.get, 'function');
});
