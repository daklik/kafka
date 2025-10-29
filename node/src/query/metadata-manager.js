'use strict';

const { HostInfo } = require('./host-info');
const { StreamsMetadata } = require('./streams-metadata');
const { QueryRpcClient } = require('./rpc-client');

class QueryMetadataManager {
  constructor({ applicationId, hostInfo, rpcClient } = {}) {
    this.applicationId = applicationId;
    this.localHost = hostInfo ? HostInfo.from(hostInfo) : null;
    this.rpcClient = rpcClient ?? new QueryRpcClient();
    this._metadataByHost = new Map();
    this._storeIndex = new Map();
    this._localStores = new Map();
  }

  setRpcClient(client) {
    if (client) {
      this.rpcClient = client;
    }
  }

  getRpcClient() {
    return this.rpcClient;
  }

  registerLocalStore({ storeName, store, stream, partitions = [], metadata = {} }) {
    if (!storeName || !store) {
      return;
    }
    this._localStores.set(storeName, { store, stream, metadata });
    if (this.localHost) {
      const topicPartitions = this._deriveTopicPartitions({ stream, partitions, storeName });
      this._applyHostMetadata({
        hostInfo: this.localHost,
        storeName,
        topicPartitions,
        standby: false
      });
    }
  }

  registerRemoteStore({ storeName, hostInfo, topicPartitions = [], standby = false }) {
    if (!storeName || !hostInfo) {
      return;
    }
    const host = HostInfo.from(hostInfo);
    this._applyHostMetadata({
      hostInfo: host,
      storeName,
      topicPartitions: this._normalizeTopicPartitions({ storeName, topicPartitions }),
      standby
    });
  }

  refreshFromSnapshot({ hosts = [] } = {}) {
    const nextMetadata = new Map();
    const nextIndex = new Map();
    for (const entry of hosts) {
      if (!entry?.hostInfo) {
        continue;
      }
      const host = HostInfo.from(entry.hostInfo);
      const hostKey = this._hostKey(host);
      const metadata = new StreamsMetadata({ hostInfo: host });
      for (const store of entry.stores ?? []) {
        if (!store?.storeName) {
          continue;
        }
        metadata.addStore(store.storeName, { standby: Boolean(store.standby) });
        const topicPartitions = this._normalizeTopicPartitions({
          storeName: store.storeName,
          topicPartitions: store.topicPartitions ?? [{
            topic: store.topic ?? null,
            partitions: store.partitions ?? []
          }]
        });
        for (const partitionEntry of topicPartitions) {
          metadata.setTopicPartitions(store.storeName, {
            topic: partitionEntry.topic,
            partitions: partitionEntry.partitions
          }, Boolean(store.standby));
        }
        this._indexStoreHost(nextIndex, store.storeName, hostKey, { standby: Boolean(store.standby) });
      }
      nextMetadata.set(hostKey, metadata);
    }
    this._metadataByHost = nextMetadata;
    this._storeIndex = nextIndex;

    if (this.localHost) {
      for (const [storeName, { stream }] of this._localStores.entries()) {
        const topicPartitions = this._deriveTopicPartitions({ stream, partitions: [], storeName });
        this._applyHostMetadata({
          hostInfo: this.localHost,
          storeName,
          topicPartitions,
          standby: false
        });
      }
    }
  }

  deregisterStore(storeName) {
    this._localStores.delete(storeName);
    if (!this.localHost) {
      return;
    }
    const hostKey = this._hostKey(this.localHost);
    const metadata = this._metadataByHost.get(hostKey);
    if (metadata) {
      metadata.removeStore(storeName);
      if (!metadata.storeNames.size && !metadata.standbyStoreNames.size) {
        this._metadataByHost.delete(hostKey);
      }
    }
    this._removeFromIndex(storeName, hostKey, { standby: false });
    this._removeFromIndex(storeName, hostKey, { standby: true });
  }

  updateLocalAssignment({ storeName, topic = null, activePartitions = [], standbyPartitions = [] } = {}) {
    if (!this.localHost || !storeName) {
      return;
    }
    const hostKey = this._hostKey(this.localHost);
    const metadata = this._ensureHostMetadata(hostKey, this.localHost);
    metadata.addStore(storeName);
    metadata.setTopicPartitions(storeName, { topic, partitions: activePartitions }, false);
    this._indexStoreHost(this._storeIndex, storeName, hostKey, { standby: false });

    if (standbyPartitions.length) {
      metadata.addStore(storeName, { standby: true });
      metadata.setTopicPartitions(storeName, { topic, partitions: standbyPartitions }, true);
      this._indexStoreHost(this._storeIndex, storeName, hostKey, { standby: true });
    } else {
      metadata.removeStandbyStore(storeName);
      this._removeFromIndex(storeName, hostKey, { standby: true });
    }
  }

  getLocalStore(storeName) {
    return this._localStores.get(storeName)?.store ?? null;
  }

  getMetadataForStore(storeName, { includeStandby = true } = {}) {
    const entry = this._storeIndex.get(storeName);
    if (!entry) {
      return [];
    }
    const result = [];
    for (const hostKey of entry.active ?? []) {
      const metadata = this._metadataByHost.get(hostKey);
      if (metadata) {
        result.push(metadata);
      }
    }
    if (includeStandby) {
      for (const hostKey of entry.standby ?? []) {
        if (entry.active?.has(hostKey)) {
          continue;
        }
        const metadata = this._metadataByHost.get(hostKey);
        if (metadata) {
          result.push(metadata);
        }
      }
    }
    return result;
  }

  routeQuery({ storeName, key, partitioner }) {
    const candidates = this.remoteCandidatesForStore({ storeName, key, partitioner });
    const hostKey = this.localHost ? this._hostKey(this.localHost) : null;
    const entry = this._storeIndex.get(storeName);
    const hasLocalActive = entry?.active?.has(hostKey);
    const hasLocalStandby = entry?.standby?.has(hostKey);
    const localStore = this.getLocalStore(storeName);

    if (hasLocalActive && localStore) {
      if (!partitioner || key === undefined || key === null) {
        return { type: 'local', store: localStore };
      }
      const partition = partitioner(key);
      if (partition === undefined || partition === null) {
        return { type: 'missing', reason: 'partitioner-returned-null' };
      }
      const metadata = this._metadataByHost.get(hostKey);
      const { partitions } = metadata?.partitionsForStore(storeName) ?? { partitions: [] };
      if (!partitions.length || partitions.includes(partition)) {
        return { type: 'local', store: localStore };
      }
    }

    if (candidates.length) {
      const primary = candidates[0];
      return {
        type: 'remote',
        hostInfo: primary.hostInfo,
        standby: primary.standby,
        candidates
      };
    }

    if (hasLocalStandby && localStore) {
      return { type: 'local', store: localStore };
    }

    if (localStore) {
      return { type: 'local', store: localStore };
    }

    return { type: 'missing', reason: 'no-metadata' };
  }

  remoteCandidatesForStore({ storeName, key, partitioner } = {}) {
    const entry = this._storeIndex.get(storeName);
    if (!entry) {
      return [];
    }
    const partition = partitioner && key !== undefined && key !== null ? partitioner(key) : null;
    if (partitioner && (partition === undefined || partition === null)) {
      return [];
    }

    const hostKey = this.localHost ? this._hostKey(this.localHost) : null;
    const seen = new Set();
    const candidates = [];
    for (const [standby, hosts] of [[false, entry.active], [true, entry.standby]]) {
      for (const keyCandidate of hosts ?? []) {
        if (keyCandidate === hostKey || seen.has(keyCandidate)) {
          continue;
        }
        const metadata = this._metadataByHost.get(keyCandidate);
        if (!metadata) {
          continue;
        }
        if (partition != null) {
          const { partitions } = metadata.partitionsForStore(storeName, { standby });
          if (partitions.length && !partitions.includes(partition)) {
            continue;
          }
        }
        candidates.push({
          hostInfo: metadata.hostInfo,
          standby,
          metadata
        });
        seen.add(keyCandidate);
      }
    }
    return candidates;
  }

  _deriveTopicPartitions({ stream, partitions, storeName }) {
    const resolvedPartitions = Array.isArray(partitions) ? partitions : [partitions].filter(p => p !== undefined);
    const topic = stream?.sourceTopic ?? null;
    return this._normalizeTopicPartitions({
      storeName,
      topicPartitions: resolvedPartitions.length ? [{ topic, partitions: resolvedPartitions }] : [{ topic, partitions: [] }]
    });
  }

  _normalizeTopicPartitions({ storeName, topicPartitions }) {
    return (topicPartitions ?? []).map(entry => ({
      storeName,
      topic: entry.topic ?? null,
      partitions: Array.from(new Set((entry.partitions ?? []).map(Number).filter(value => !Number.isNaN(value)))).sort((a, b) => a - b)
    }));
  }

  _applyHostMetadata({ hostInfo, storeName, topicPartitions = [], standby = false }) {
    if (!storeName) {
      return;
    }
    const host = HostInfo.from(hostInfo);
    const hostKey = this._hostKey(host);
    const metadata = this._ensureHostMetadata(hostKey, host);
    metadata.addStore(storeName, { standby });
    const entries = topicPartitions.length ? topicPartitions : [{ topic: null, partitions: [] }];
    for (const entry of entries) {
      metadata.setTopicPartitions(storeName, { topic: entry.topic, partitions: entry.partitions }, standby);
    }
    this._indexStoreHost(this._storeIndex, storeName, hostKey, { standby });
  }

  _ensureHostMetadata(hostKey, hostInfo) {
    let metadata = this._metadataByHost.get(hostKey);
    if (!metadata) {
      metadata = new StreamsMetadata({ hostInfo });
      this._metadataByHost.set(hostKey, metadata);
    }
    return metadata;
  }

  _indexStoreHost(targetIndex, storeName, hostKey, { standby }) {
    if (!storeName || !hostKey) {
      return;
    }
    let entry = targetIndex.get(storeName);
    if (!entry) {
      entry = { active: new Set(), standby: new Set() };
      targetIndex.set(storeName, entry);
    }
    const set = standby ? entry.standby : entry.active;
    set.add(hostKey);
  }

  _removeFromIndex(storeName, hostKey, { standby }) {
    const entry = this._storeIndex.get(storeName);
    if (!entry) {
      return;
    }
    const set = standby ? entry.standby : entry.active;
    set?.delete(hostKey);
    if (!entry.active?.size && !entry.standby?.size) {
      this._storeIndex.delete(storeName);
    }
  }

  _hostKey(host) {
    if (!host) {
      return 'local';
    }
    return `${host.host}:${host.port}`;
  }
}

module.exports = { QueryMetadataManager };
