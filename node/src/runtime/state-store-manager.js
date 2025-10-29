'use strict';

const EventEmitter = require('events');

function toNumber(value) {
  if (value == null) {
    return null;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? null : numeric;
}

function ensureSet(candidate) {
  if (candidate instanceof Set) {
    return candidate;
  }
  if (Array.isArray(candidate)) {
    return new Set(candidate);
  }
  if (candidate == null) {
    return new Set();
  }
  return new Set([candidate]);
}

class StateStoreManager extends EventEmitter {
  constructor({ metrics, config } = {}) {
    super();
    this._metrics = metrics ?? null;
    this._config = config ?? null;
    this._definitions = new Map();
    this._instances = new Map();
  }

  registerDefinition(stream, definition) {
    if (!stream?.id) {
      throw new TypeError('StateStoreManager.registerDefinition requires a stream with an id.');
    }
    if (!definition?.name) {
      throw new TypeError('State store definitions require a name.');
    }

    let perStream = this._definitions.get(stream.id);
    if (!perStream) {
      perStream = new Map();
      this._definitions.set(stream.id, perStream);
    }

    let existing = perStream.get(definition.name);
    if (existing) {
      // Merge listener sets if the new definition includes additional listeners.
      if (definition.restoreListeners && definition.restoreListeners !== existing.restoreListeners) {
        const merged = ensureSet(existing.restoreListeners);
        for (const listener of ensureSet(definition.restoreListeners)) {
          merged.add(listener);
        }
        existing.restoreListeners = merged;
      }
      if (!existing.builderMetadata && definition.builderMetadata) {
        existing.builderMetadata = definition.builderMetadata;
      }
      existing.builder = existing.builder ?? definition.builder ?? null;
      existing.keySerde = existing.keySerde ?? definition.keySerde ?? null;
      existing.valueSerde = existing.valueSerde ?? definition.valueSerde ?? null;
      existing.metadata = existing.metadata ?? definition.metadata ?? {};
      return existing;
    }

    const normalized = {
      ...definition,
      restoreListeners: ensureSet(definition.restoreListeners),
      builderMetadata: definition.builderMetadata ?? definition.builder?.describe?.() ?? null
    };
    perStream.set(definition.name, normalized);
    return normalized;
  }

  getDefinition(streamId, storeName) {
    if (!streamId || !storeName) {
      return null;
    }
    return this._definitions.get(streamId)?.get(storeName) ?? null;
  }

  attachRestoreListener({ stream, streamId, storeName, listener }) {
    if (!listener) {
      return () => {};
    }

    const id = streamId ?? stream?.id;
    if (!id) {
      throw new TypeError('attachRestoreListener requires a streamId.');
    }
    const perStream = this._definitions.get(id);
    if (!perStream || !perStream.has(storeName)) {
      throw new Error(`State store ${storeName} is not registered for stream ${id}.`);
    }

    const definition = perStream.get(storeName);
    definition.restoreListeners = ensureSet(definition.restoreListeners);
    definition.restoreListeners.add(listener);

    return () => {
      definition.restoreListeners.delete(listener);
    };
  }

  async registerStore({
    stream,
    builder,
    storeName = null,
    keySerde = null,
    valueSerde = null,
    metadata = {},
    stateRestoreListener = null,
    restoreBatches = [],
    restoreOffsets = {}
  }) {
    if (!stream?.id) {
      throw new TypeError('registerStore requires a stream with an id.');
    }
    const name = storeName ?? builder?.name;
    if (!name) {
      throw new TypeError('registerStore requires a storeName or StoreBuilder.');
    }

    let definition;
    if (builder) {
      if (typeof builder.build !== 'function') {
        throw new TypeError('registerStore requires StoreBuilder instances to implement build().');
      }
      definition = this.registerDefinition(stream, {
        name,
        builder,
        keySerde,
        valueSerde,
        metadata,
        builderMetadata: builder.describe?.()
      });
    } else {
      definition = this.registerDefinition(stream, { name });
      if (!definition.builder) {
        throw new Error(`State store ${name} is not registered with a builder.`);
      }
      if (keySerde != null) {
        definition.keySerde = definition.keySerde ?? keySerde;
      }
      if (valueSerde != null) {
        definition.valueSerde = definition.valueSerde ?? valueSerde;
      }
      if (metadata && Object.keys(metadata).length && !definition.metadata) {
        definition.metadata = metadata;
      }
    }

    if (stateRestoreListener) {
      this.attachRestoreListener({ stream, storeName: definition.name, listener: stateRestoreListener });
    }

    return this.buildAndRegister({ stream, definition, restoreBatches, restoreOffsets });
  }

  async buildAndRegister({ stream, definition, storeInstances = null, restoreBatches = [], restoreOffsets = {} }) {
    if (!stream?.id) {
      throw new TypeError('buildAndRegister requires a stream with an id.');
    }
    if (!definition?.name) {
      throw new TypeError('buildAndRegister requires a state store definition.');
    }

    const existing = this.getStore(stream.id, definition.name);
    if (existing) {
      if (storeInstances) {
        storeInstances.set(definition.name, existing);
      }
      return existing;
    }

    if (!definition.builder || typeof definition.builder.build !== 'function') {
      throw new Error(`State store ${definition.name} is missing a builder.`);
    }

    const context = {
      stream,
      storeName: definition.name,
      config: this._config,
      stateDir: this._config?.getStateDirectory?.() ?? null,
      storeDirectory: this._config?.resolveStateStoreDirectory?.({
        stream,
        storeName: definition.name
      }) ?? null,
      cacheMaxBytes: this._config?.getCacheMaxBytesBuffering?.() ?? null,
      builderMetadata: definition.builderMetadata,
      definition
    };
    const store = await definition.builder.build(context);
    this._storeInstance({
      stream,
      definition,
      store,
      storeInstances,
      restoreBatches,
      restoreOffsets,
      emitLifecycle: true
    });
    return store;
  }

  async restoreStore({ stream, storeName, restoreBatches = [], restoreOffsets = {} }) {
    if (!stream?.id) {
      throw new TypeError('restoreStore requires a stream with an id.');
    }
    if (!storeName) {
      throw new TypeError('restoreStore requires a storeName.');
    }

    const definition = this.getDefinition(stream.id, storeName);
    if (!definition) {
      throw new Error(`State store ${storeName} is not registered for stream ${stream.id}.`);
    }

    const existing = this.getStore(stream.id, storeName);
    if (!existing) {
      await this.buildAndRegister({
        stream,
        definition,
        restoreBatches,
        restoreOffsets
      });
      return this.getStore(stream.id, storeName);
    }

    this._emitRestoreLifecycle({
      stream,
      definition,
      restoreBatches,
      restoreOffsets
    });

    return existing;
  }

  registerPrebuilt({ stream, storeName, store }) {
    if (!stream?.id) {
      throw new TypeError('registerPrebuilt requires a stream with an id.');
    }
    if (!storeName) {
      throw new TypeError('registerPrebuilt requires a storeName.');
    }
    this._storeInstance({
      stream,
      definition: this.registerDefinition(stream, { name: storeName }),
      store,
      emitLifecycle: false
    });
  }

  bindExisting(stream, storeInstances) {
    if (!stream?.id || !storeInstances) {
      return;
    }
    for (const [name, store] of storeInstances.entries()) {
      this.registerPrebuilt({ stream, storeName: name, store });
    }
  }

  getStore(streamId, storeName) {
    const perStream = this._instances.get(streamId);
    if (!perStream) {
      return null;
    }
    return perStream.get(storeName) ?? null;
  }

  listStores(streamId) {
    return Array.from(this._instances.get(streamId)?.entries() ?? []);
  }

  reset(streamId = null) {
    if (streamId == null) {
      this._instances.clear();
      return;
    }
    this._instances.delete(streamId);
  }

  _storeInstance({ stream, definition, store, storeInstances = null, restoreBatches = [], restoreOffsets = {}, emitLifecycle = false }) {
    const streamId = stream.id;
    let perStream = this._instances.get(streamId);
    if (!perStream) {
      perStream = new Map();
      this._instances.set(streamId, perStream);
    }
    perStream.set(definition.name, store);
    if (storeInstances) {
      storeInstances.set(definition.name, store);
    }

    if (emitLifecycle) {
      this._emitRestoreLifecycle({ stream, definition, restoreBatches, restoreOffsets });
    }
  }

  _emitRestoreLifecycle({ stream, definition, restoreBatches = [], restoreOffsets = {} }) {
    const listeners = Array.from(ensureSet(definition.restoreListeners));
    const topic = definition.builderMetadata?.changelog?.topic
      ?? definition.metadata?.changelogTopic
      ?? null;
    const partition = toNumber(restoreOffsets.partition);
    const startingOffset = toNumber(restoreOffsets.startingOffset);
    const endingOffset = toNumber(restoreOffsets.endingOffset);

    const totalRestored = restoreBatches.reduce((sum, batch) => sum + (toNumber(batch?.numRestored) ?? 0), 0);
    const payload = {
      streamId: stream.id,
      storeName: definition.name,
      topic,
      partition,
      startingOffset,
      endingOffset,
      totalRestored
    };

    this._metrics?.record('state.restore.start', 1, { streamId: stream.id, storeName: definition.name, topic });
    this.emit('restore:start', payload);
    for (const listener of listeners) {
      try {
        listener.onRestoreStart?.(topic, partition, definition.name, startingOffset ?? 0, endingOffset ?? 0);
      } catch (error) {
        // Swallow listener errors to avoid interrupting restoration.
      }
    }

    for (const batch of restoreBatches) {
      const batchEndOffset = toNumber(batch?.batchEndOffset);
      const numRestored = toNumber(batch?.numRestored) ?? 0;
      const batchPayload = {
        ...payload,
        batchEndOffset,
        numRestored
      };
      this._metrics?.record('state.restore.batch', numRestored, { streamId: stream.id, storeName: definition.name, topic });
      this.emit('restore:batch', batchPayload);
      for (const listener of listeners) {
        try {
          listener.onBatchRestored?.(topic, partition, definition.name, batchEndOffset, numRestored);
        } catch (error) {
          // Ignore listener errors
        }
      }
    }

    this._metrics?.record('state.restore.end', 1, { streamId: stream.id, storeName: definition.name, topic, totalRestored });
    this.emit('restore:end', { ...payload, totalRestored });
    for (const listener of listeners) {
      try {
        listener.onRestoreEnd?.(topic, partition, definition.name, totalRestored);
      } catch (error) {
        // Ignore listener errors
      }
    }
  }
}

module.exports = {
  StateStoreManager
};
