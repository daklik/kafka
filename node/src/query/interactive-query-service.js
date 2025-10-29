'use strict';

class InteractiveQueryService {
  constructor({ metadataManager } = {}) {
    if (!metadataManager) {
      throw new Error('InteractiveQueryService requires a metadata manager');
    }
    this.metadataManager = metadataManager;
  }

  store(storeName) {
    return this.metadataManager.getLocalStore(storeName);
  }

  metadataForStore(storeName) {
    return this.metadataManager.getMetadataForStore(storeName);
  }

  async get(storeName, key, options = {}) {
    const route = this.metadataManager.routeQuery({
      storeName,
      key,
      partitioner: options.partitioner
    });

    if (route.type === 'missing') {
      throw new Error(`No metadata available for store ${storeName} (${route.reason})`);
    }

    if (route.type === 'local') {
      const store = route.store;
      if (!store || typeof store.get !== 'function') {
        throw new Error(`Store ${storeName} does not support get operations`);
      }
      return store.get(key, options);
    }

    if (route.type === 'remote') {
      const client = this.metadataManager.getRpcClient();
      const candidates = route.candidates ?? [{ hostInfo: route.hostInfo, standby: route.standby }];
      let lastError = null;
      for (const candidate of candidates) {
        try {
          return await client.fetchKeyValue({
            hostInfo: candidate.hostInfo,
            storeName,
            key,
            options: { ...options, role: candidate.standby ? 'standby' : 'active' }
          });
        } catch (error) {
          lastError = error;
        }
      }
      if (lastError) {
        throw lastError;
      }
      throw new Error(`No remote candidates available for store ${storeName}`);
    }

    throw new Error(`Unsupported query route type: ${route.type}`);
  }
}

module.exports = { InteractiveQueryService };
