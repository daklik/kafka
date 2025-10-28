'use strict';

const { HostInfo } = require('./host-info');
const { StreamsMetadata } = require('./streams-metadata');
const { QueryRpcClient } = require('./rpc-client');

class QueryMetadataManager {
  constructor({ applicationId, hostInfo, rpcClient } = {}) {
    this.applicationId = applicationId;
    this.localHost = HostInfo.from(hostInfo);
    this.rpcClient = rpcClient ?? new QueryRpcClient();
    this._storeMetadata = new Map();
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
    const hostKey = this._hostKey(this.localHost);
    const existing = this._storeMetadata.get(storeName) ?? new Map();
    const topicPartitions = this._deriveTopicPartitions({ stream, partitions });
    existing.set(hostKey, new StreamsMetadata({
      hostInfo: this.localHost,
      storeNames: [storeName],
      topicPartitions
    }));
    this._storeMetadata.set(storeName, existing);
  }

  registerRemoteStore({ storeName, hostInfo, topicPartitions = [] }) {
    if (!storeName || !hostInfo) {
      return;
    }
    const host = HostInfo.from(hostInfo);
    const hostKey = this._hostKey(host);
    const existing = this._storeMetadata.get(storeName) ?? new Map();
    existing.set(hostKey, new StreamsMetadata({
      hostInfo: host,
      storeNames: [storeName],
      topicPartitions
    }));
    this._storeMetadata.set(storeName, existing);
  }

  deregisterStore(storeName) {
    this._localStores.delete(storeName);
    const existing = this._storeMetadata.get(storeName);
    if (existing) {
      const hostKey = this._hostKey(this.localHost);
      existing.delete(hostKey);
      if (!existing.size) {
        this._storeMetadata.delete(storeName);
      }
    }
  }

  getLocalStore(storeName) {
    return this._localStores.get(storeName)?.store ?? null;
  }

  getMetadataForStore(storeName) {
    const storeMeta = this._storeMetadata.get(storeName);
    if (!storeMeta) {
      return [];
    }
    return Array.from(storeMeta.values());
  }

  routeQuery({ storeName, key, partitioner }) {
    const metadata = this.getMetadataForStore(storeName);
    if (!metadata.length) {
      return { type: 'missing', reason: 'no-metadata' };
    }

    if (metadata.length === 1) {
      const meta = metadata[0];
      if (!meta.hostInfo || (this.localHost && meta.hostInfo.equals(this.localHost))) {
        const local = this.getLocalStore(storeName);
        return local ? { type: 'local', store: local } : { type: 'missing', reason: 'store-not-registered' };
      }
      return { type: 'remote', hostInfo: meta.hostInfo };
    }

    if (!partitioner || key === undefined || key === null) {
      const local = this.getLocalStore(storeName);
      if (local) {
        return { type: 'local', store: local };
      }
      return { type: 'remote', hostInfo: metadata[0].hostInfo };
    }

    const partition = partitioner(key);
    if (partition === undefined || partition === null) {
      return { type: 'missing', reason: 'partitioner-returned-null' };
    }

    for (const meta of metadata) {
      if (!meta.topicPartitions.length) {
        continue;
      }
      for (const entry of meta.topicPartitions) {
        if (entry.partitions.includes(partition)) {
          if (!meta.hostInfo || (this.localHost && meta.hostInfo.equals(this.localHost))) {
            const local = this.getLocalStore(storeName);
            if (local) {
              return { type: 'local', store: local };
            }
          }
          return { type: 'remote', hostInfo: meta.hostInfo };
        }
      }
    }

    const local = this.getLocalStore(storeName);
    if (local) {
      return { type: 'local', store: local };
    }
    return { type: 'remote', hostInfo: metadata[0].hostInfo };
  }

  _deriveTopicPartitions({ stream, partitions }) {
    if (!stream) {
      return partitions.length ? [{ topic: null, partitions: Array.from(new Set(partitions)) }] : [];
    }
    const topic = stream.sourceTopic ?? null;
    const resolved = Array.isArray(partitions) ? partitions : [partitions].filter(p => p !== undefined);
    return resolved.length ? [{ topic, partitions: Array.from(new Set(resolved)) }] : [{ topic, partitions: [] }];
  }

  _hostKey(host) {
    if (!host) {
      return 'local';
    }
    return `${host.host}:${host.port}`;
  }
}

module.exports = { QueryMetadataManager };
