'use strict';

const { v4: uuidv4 } = require('uuid');
const { Materialized } = require('./materialized');
const { Named } = require('./named');
const { StoreBuilder } = require('./state/store-builder');
const { MemoryStateStore } = require('./state/memory-store');

class KTable {
  constructor({
    id = uuidv4(),
    builder,
    sourceTopic,
    fromBeginning = true,
    keySerde,
    valueSerde,
    topology,
    materialized,
    nodeName,
    origin,
    isGlobal = false
  }) {
    if (!builder) {
      throw new Error('KTable must be created via StreamsBuilder');
    }
    if (!sourceTopic) {
      throw new Error('KTable requires a source topic');
    }

    this.id = id;
    this._builder = builder;
    this.sourceTopic = sourceTopic;
    this.fromBeginning = fromBeginning;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.isSource = true;
    this.isTable = true;
    this.isGlobalKTable = isGlobal;
    this.origin = origin ?? { type: isGlobal ? 'globalTable' : 'table', topic: sourceTopic };
    this.nodeName = nodeName ?? Named.from(origin?.name, `${isGlobal ? 'global-table' : 'table'}-${sourceTopic}`) ?? `${isGlobal ? 'global-table' : 'table'}-${sourceTopic}`;
    this.operations = [];
    this.sinks = [];
    this.stateStores = new Map();
    this._topology = topology;

    const resolvedMaterialization = this._resolveMaterialized(materialized, {
      storeName: `store-${sourceTopic}`,
      keySerde: keySerde,
      valueSerde: valueSerde,
      logging: true
    });

    const storeDefinition = this._registerMaterializedStore(resolvedMaterialization);
    this.materialized = {
      storeName: storeDefinition.name,
      keySerde: storeDefinition.keySerde,
      valueSerde: storeDefinition.valueSerde,
      changelogTopic: resolvedMaterialization.changelogTopic ?? this.sourceTopic,
      logging: resolvedMaterialization.logging !== false,
      caching: Boolean(resolvedMaterialization.caching)
    };

  }

  _resolveMaterialized(materializedOrOptions = {}, defaults = {}) {
    if (materializedOrOptions instanceof Materialized) {
      const resolved = materializedOrOptions.resolve(defaults);
      return {
        ...resolved,
        changelogTopic: defaults.changelogTopic ?? resolved.changelogTopic
      };
    }

    if (materializedOrOptions && materializedOrOptions.materialized instanceof Materialized) {
      const resolved = materializedOrOptions.materialized.resolve({
        ...defaults,
        ...materializedOrOptions
      });
      return {
        ...resolved,
        changelogTopic: materializedOrOptions.changelogTopic ?? resolved.changelogTopic ?? defaults.changelogTopic
      };
    }

    return { ...defaults, ...(materializedOrOptions ?? {}) };
  }

  _registerMaterializedStore({
    storeBuilder,
    store,
    storeName,
    keySerde,
    valueSerde,
    logging = true,
    caching = false,
    changelogConfig
  }) {
    const resolvedName = storeName ?? `ktable-${this.id}`;
    const builder = this._coerceStoreBuilder({
      storeBuilder,
      store,
      storeName: resolvedName,
      changelogConfig,
      logging
    });

    let configuredBuilder = builder;
    if (!logging) {
      configuredBuilder = configuredBuilder.withLoggingDisabled();
    }
    if (caching) {
      configuredBuilder = configuredBuilder.withCachingEnabled();
    }

    const definition = this._registerStateStore({
      name: resolvedName,
      storeBuilder: configuredBuilder,
      keySerde: keySerde ?? this.keySerde,
      valueSerde: valueSerde ?? this.valueSerde,
      metadata: {
        type: this.isGlobalKTable ? 'global-table-materialization' : 'table-materialization',
        logging: logging !== false,
        caching: Boolean(caching),
        changelog: changelogConfig?.toKafkaConfig?.(resolvedName) ?? null
      }
    });

    return definition;
  }

  _coerceStoreBuilder({ storeBuilder, store, storeName, changelogConfig, logging = true }) {
    if (storeBuilder instanceof StoreBuilder) {
      return storeBuilder;
    }

    if (store instanceof StoreBuilder) {
      return store;
    }

    const supplier = typeof store === 'function'
      ? store
      : () => new MemoryStateStore(storeName, { loggingEnabled: logging });

    return new StoreBuilder({
      name: storeName,
      type: 'keyValue',
      supplier,
      loggingEnabled: logging,
      changelogConfig
    });
  }

  _registerStateStore({ name, storeBuilder, keySerde, valueSerde, metadata = {} }) {
    if (!name) {
      throw new Error('Materialized state stores must include a name');
    }

    const definition = {
      name,
      keySerde,
      valueSerde,
      builder: storeBuilder,
      builderMetadata: storeBuilder.describe(),
      metadata,
      restoreListeners: new Set(),
      describe: () => ({
        name,
        type: storeBuilder.type,
        keySerde: Boolean(keySerde),
        valueSerde: Boolean(valueSerde),
        builder: storeBuilder.describe(),
        metadata
      })
    };

    this.stateStores.set(name, definition);
    return definition;
  }

  describe() {
    return {
      id: this.id,
      sourceTopic: this.sourceTopic,
      fromBeginning: this.fromBeginning,
      keySerde: this.keySerde,
      valueSerde: this.valueSerde,
      operations: this.operations.slice(),
      sinks: this.sinks.slice(),
      stateStores: Array.from(this.stateStores.values()).map(store => ({
        name: store.name,
        keySerde: store.keySerde,
        valueSerde: store.valueSerde,
        builder: store.builder,
        builderMetadata: store.builderMetadata ?? store.builder?.describe?.(),
        metadata: store.metadata ?? {},
        restoreListeners: Array.from(store.restoreListeners ?? [])
      })),
      isSource: true,
      isTable: true,
      isGlobalKTable: this.isGlobalKTable,
      origin: this.origin,
      materialized: { ...this.materialized }
    };
  }
}

class GlobalKTable extends KTable {
  constructor(options) {
    super({ ...options, isGlobal: true, fromBeginning: true });
  }
}

module.exports = {
  KTable,
  GlobalKTable
};
