'use strict';

const EventEmitter = require('events');

const { ChangeLoggingKeyValueStore } = require('../state/change-logging-key-value-store');

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
    this._changelogMetadata = new Map();
    this._changelogProducers = new Map();
    this._checkpoints = new Map();
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
    const storeInstance = await definition.builder.build(context);
    const store = this._maybeWrapLoggingStore({ stream, definition, store: storeInstance });
    await this._applyRestoreBatches({
      stream,
      definition,
      store,
      restoreBatches,
      restoreOffsets
    });

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

    await this._applyRestoreBatches({
      stream,
      definition,
      store: existing,
      restoreBatches,
      restoreOffsets
    });

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

  bindChangelogProducer({ stream, producer } = {}) {
    if (!stream?.id || !producer) {
      return;
    }
    this._changelogProducers.set(stream.id, producer);
    const perStream = this._changelogMetadata.get(stream.id);
    if (perStream) {
      for (const entry of perStream.values()) {
        entry.producer = producer;
      }
    }
  }

  async appendChangelogRecord({
    streamId,
    storeName,
    key,
    value,
    tombstone = false,
    context = null,
    definition = null
  } = {}) {
    if (!streamId || !storeName) {
      return;
    }
    const entry = this._ensureChangelogEntry(streamId, storeName, { definition });
    if (!entry || !entry.topic || !entry.producer) {
      return;
    }

    const message = this._serializeChangelogRecord({ entry, key, value, tombstone, context });
    if (!message) {
      return;
    }

    const payload = {
      topic: entry.topic,
      messages: [message]
    };
    if (entry.partition != null) {
      payload.partition = entry.partition;
    }

    const result = await entry.producer.send(payload);
    const metadata = Array.isArray(result) ? result[0] : result;
    const partition = toNumber(metadata?.partition ?? entry.partition);
    const offset = metadata?.offset ?? metadata?.baseOffset ?? metadata?.offsets?.[0];
    if (offset != null) {
      this._updateCheckpoint({ streamId, storeName, partition, offset });
    }
  }

  async flushChangelog({ streamId, storeName } = {}) {
    if (!streamId || !storeName) {
      return;
    }
    const entry = this._ensureChangelogEntry(streamId, storeName);
    if (!entry?.producer?.flush) {
      return;
    }
    await entry.producer.flush();
  }

  getCheckpoint(streamId, storeName, partition = 0) {
    const perStream = this._checkpoints.get(streamId);
    if (!perStream) {
      return null;
    }
    const perStore = perStream.get(storeName);
    if (!perStore) {
      return null;
    }
    return perStore.get(Number(partition)) ?? null;
  }

  _maybeWrapLoggingStore({ stream, definition, store }) {
    if (!stream?.id || !definition?.name || !store) {
      return store;
    }
    if (store instanceof ChangeLoggingKeyValueStore) {
      return store;
    }
    if (!this._shouldLogStore({ definition, store })) {
      return store;
    }
    this._ensureChangelogEntry(stream.id, definition.name, { definition });
    return new ChangeLoggingKeyValueStore(store, {
      manager: this,
      streamId: stream.id,
      storeName: definition.name,
      definition
    });
  }

  _shouldLogStore({ definition, store }) {
    if (!store || !this._isKeyValueStore(store)) {
      return false;
    }
    const loggingEnabled = store.loggingEnabled ?? definition?.builderMetadata?.loggingEnabled;
    if (!loggingEnabled) {
      return false;
    }
    const topic = definition?.builderMetadata?.changelog?.topic
      ?? definition?.metadata?.changelogTopic
      ?? store.changelogConfig?.topic;
    if (!topic) {
      return false;
    }
    return true;
  }

  _isKeyValueStore(store) {
    return typeof store?.put === 'function' && typeof store?.delete === 'function';
  }

  _ensureChangelogEntry(streamId, storeName, { definition = null } = {}) {
    if (!streamId || !storeName) {
      return null;
    }
    let perStream = this._changelogMetadata.get(streamId);
    if (!perStream) {
      perStream = new Map();
      this._changelogMetadata.set(streamId, perStream);
    }
    let entry = perStream.get(storeName);
    const metadataSource = definition ?? this.getDefinition(streamId, storeName);
    if (!entry) {
      entry = {
        topic: null,
        partition: null,
        keySerde: null,
        valueSerde: null,
        producer: this._changelogProducers.get(streamId) ?? null
      };
      perStream.set(storeName, entry);
    }
    if (metadataSource) {
      entry.topic = entry.topic
        ?? metadataSource.builderMetadata?.changelog?.topic
        ?? metadataSource.metadata?.changelogTopic
        ?? metadataSource.changelogTopic
        ?? null;
      entry.partition = entry.partition
        ?? metadataSource.metadata?.changelogPartition
        ?? null;
      entry.keySerde = entry.keySerde ?? metadataSource.keySerde ?? null;
      entry.valueSerde = entry.valueSerde ?? metadataSource.valueSerde ?? null;
    }
    if (!entry.producer && this._changelogProducers.has(streamId)) {
      entry.producer = this._changelogProducers.get(streamId);
    }
    return entry;
  }

  _serializeChangelogRecord({ entry, key, value, tombstone, context }) {
    const keyBuffer = this._encodeKey(key, entry?.keySerde);
    const valueBuffer = tombstone ? null : this._encodeValue(value, entry?.valueSerde);
    if (keyBuffer == null && valueBuffer == null && !tombstone) {
      return null;
    }
    const headers = context?.recordContext?.()?.headers?.() ?? context?.headers ?? {};
    const normalizedHeaders = headers && typeof headers === 'object'
      ? Object.entries(headers).map(([headerKey, headerValue]) => ({
        key: headerKey,
        value: this._encodeBuffer(headerValue)
      }))
      : [];
    return {
      key: keyBuffer,
      value: valueBuffer,
      headers: normalizedHeaders
    };
  }

  _encodeKey(key, serde) {
    if (serde?.serialize) {
      return serde.serialize(key);
    }
    return this._encodeBuffer(key);
  }

  _encodeValue(value, serde) {
    if (serde?.serialize) {
      return serde.serialize(value);
    }
    return this._encodeBuffer(value);
  }

  _encodeBuffer(value) {
    if (value == null) {
      return null;
    }
    if (Buffer.isBuffer(value)) {
      return value;
    }
    if (typeof value === 'string') {
      return Buffer.from(value);
    }
    if (typeof value === 'number') {
      const buffer = Buffer.allocUnsafe(8);
      buffer.writeDoubleBE(value, 0);
      return buffer;
    }
    if (typeof value === 'boolean') {
      return Buffer.from([value ? 1 : 0]);
    }
    try {
      return Buffer.from(JSON.stringify(value));
    } catch (err) {
      return Buffer.from(String(value));
    }
  }

  _updateCheckpoint({ streamId, storeName, partition, offset }) {
    if (streamId == null || storeName == null || offset == null) {
      return;
    }
    const numericPartition = Number(partition ?? 0);
    let perStream = this._checkpoints.get(streamId);
    if (!perStream) {
      perStream = new Map();
      this._checkpoints.set(streamId, perStream);
    }
    let perStore = perStream.get(storeName);
    if (!perStore) {
      perStore = new Map();
      perStream.set(storeName, perStore);
    }
    perStore.set(numericPartition, typeof offset === 'number' ? offset : Number(offset));
  }

  async _applyRestoreBatches({ stream, definition, store, restoreBatches = [], restoreOffsets = {} }) {
    const targetStore = store instanceof ChangeLoggingKeyValueStore ? store.inner : store;
    if (!this._isKeyValueStore(targetStore) || !Array.isArray(restoreBatches) || !restoreBatches.length) {
      if (restoreOffsets?.endingOffset != null) {
        this._updateCheckpoint({
          streamId: stream.id,
          storeName: definition.name,
          partition: restoreOffsets.partition,
          offset: restoreOffsets.endingOffset
        });
      }
      return;
    }

    for (const batch of restoreBatches) {
      const records = Array.isArray(batch?.records) ? batch.records : [];
      for (const record of records) {
        if (!record) {
          continue;
        }
        if (record.value === null || record.value === undefined) {
          await targetStore.delete(record.key);
        } else {
          await targetStore.put(record.key, record.value);
        }
      }
      if (typeof targetStore.flush === 'function') {
        await targetStore.flush();
      }
      if (batch?.batchEndOffset != null) {
        this._updateCheckpoint({
          streamId: stream.id,
          storeName: definition.name,
          partition: batch.partition ?? restoreOffsets.partition,
          offset: batch.batchEndOffset
        });
      }
    }

    if (restoreOffsets?.endingOffset != null) {
      this._updateCheckpoint({
        streamId: stream.id,
        storeName: definition.name,
        partition: restoreOffsets.partition,
        offset: restoreOffsets.endingOffset
      });
    }
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
