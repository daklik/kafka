'use strict';

const { v4: uuidv4 } = require('uuid');
const { MemoryStateStore } = require('./state/memory-store');
const { StoreBuilder } = require('./state/store-builder');
const { Materialized } = require('./materialized');
const { Named } = require('./named');

class KStream {
  constructor({
    id,
    builder,
    sourceTopic,
    fromBeginning = false,
    keySerde,
    valueSerde,
    topology,
    isSource = false,
    origin = null,
    operations = [],
    sinks = [],
    stateStores = new Map(),
    parent = null,
    initialNodeId
  }) {
    if (!builder) {
      throw new Error('KStream must be created via StreamsBuilder');
    }

    this.id = id;
    this._builder = builder;
    this.sourceTopic = sourceTopic;
    this.fromBeginning = fromBeginning;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.operations = operations;
    this.sinks = sinks;
    this.stateStores = stateStores;
    this.isSource = isSource;
    this.origin = origin;
    this.parent = parent;
    this._topology = topology;
    this._lastNodeId = initialNodeId ?? id;
    this.nodeName = origin?.name ?? (isSource ? Named.from(origin, `source-${sourceTopic}`) ?? `source-${sourceTopic}` : `stream-${id}`);
  }

  cloneWith(options = {}) {
    return new KStream({
      id: uuidv4(),
      builder: this._builder,
      sourceTopic: options.sourceTopic ?? this.sourceTopic,
      fromBeginning: options.fromBeginning ?? this.fromBeginning,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      topology: this._topology,
      isSource: options.isSource ?? false,
      origin: options.origin ?? null,
      operations: [],
      sinks: [],
      stateStores: new Map(),
      parent: options.parent ?? this,
      initialNodeId: options.initialNodeId
    });
  }

  _appendOperation(type, fn, options = {}) {
    const operationId = uuidv4();
    const operation = { id: operationId, type, fn, options };
    this.operations.push(operation);
    const name = Named.from(options.named, `${type}-${operationId.slice(0, 6)}`);
    this._topology.addNode({ id: operationId, name, type: 'processor', metadata: { operation: type, options: this._sanitizeTopologyOptions(options) } });
    this._topology.connect(this._lastNodeId, operationId);
    this._lastNodeId = operationId;
    return operation;
  }

  _sanitizeTopologyOptions(options) {
    if (!options) {
      return {};
    }
    const sanitized = { ...options };
    if (sanitized.fn || sanitized.predicate) {
      sanitized.fn = '[function]';
      sanitized.predicate = '[function]';
    }
    if (sanitized.streams) {
      sanitized.streams = sanitized.streams.map(stream => stream.id ?? stream);
    }
    return sanitized;
  }

  _resolveMaterialized(materializedOrOptions = {}) {
    if (materializedOrOptions instanceof Materialized) {
      return materializedOrOptions.resolve();
    }
    if (materializedOrOptions.materialized instanceof Materialized) {
      return materializedOrOptions.materialized.resolve(materializedOrOptions);
    }
    return materializedOrOptions;
  }

  _coerceStoreBuilder({ storeBuilder, store, storeName, changelogConfig, logging = true }) {
    if (storeBuilder instanceof StoreBuilder) {
      return storeBuilder;
    }
    if (store instanceof StoreBuilder) {
      return store;
    }
    const supplier = typeof store === 'function' ? store : () => new MemoryStateStore(storeName);
    return new StoreBuilder({
      name: storeName,
      type: 'keyValue',
      supplier,
      loggingEnabled: logging,
      changelogConfig
    });
  }

  _registerStateStore({ name, storeBuilder, keySerde, valueSerde }) {
    if (!name) {
      throw new Error('State store definition must include a name');
    }
    const definition = {
      name,
      builder: storeBuilder,
      keySerde,
      valueSerde,
      describe: () => ({
        name,
        type: storeBuilder.type,
        keySerde: Boolean(keySerde),
        valueSerde: Boolean(valueSerde),
        builder: storeBuilder.describe()
      })
    };
    this.stateStores.set(name, definition);
    return definition;
  }

  map(mapper, options = {}) {
    this._appendOperation('map', mapper, options);
    return this;
  }

  mapValues(mapper, options = {}) {
    this._appendOperation('mapValues', mapper, options);
    return this;
  }

  mapKeys(mapper, options = {}) {
    this._appendOperation('mapKeys', mapper, options);
    return this;
  }

  selectKey(selector, options = {}) {
    this._appendOperation('selectKey', selector, options);
    return this;
  }

  filter(predicate, options = {}) {
    this._appendOperation('filter', predicate, options);
    return this;
  }

  filterNot(predicate, options = {}) {
    this._appendOperation('filterNot', predicate, options);
    return this;
  }

  flatMap(mapper, options = {}) {
    this._appendOperation('flatMap', mapper, options);
    return this;
  }

  flatMapValues(mapper, options = {}) {
    this._appendOperation('flatMapValues', mapper, options);
    return this;
  }

  peek(sideEffect, options = {}) {
    this._appendOperation('peek', sideEffect, options);
    return this;
  }

  foreach(sideEffect, options = {}) {
    this._appendOperation('foreach', sideEffect, options);
    return this;
  }

  groupByKey(options = {}) {
    this._appendOperation('groupBy', (value, record) => record.key ?? value?.key, options);
    return this;
  }

  groupBy(selector, options = {}) {
    if (typeof selector !== 'function') {
      throw new Error('groupBy expects a selector function');
    }
    this._appendOperation('groupBy', selector, options);
    return this;
  }

  aggregate(initializer, aggregator, materializedOrOptions = {}) {
    if (typeof initializer !== 'function') {
      throw new Error('aggregate expects an initializer function');
    }
    if (typeof aggregator !== 'function') {
      throw new Error('aggregate expects an aggregator function');
    }

    const resolved = this._resolveMaterialized(materializedOrOptions);
    const storeName = resolved.storeName ?? `agg-${uuidv4()}`;
    const storeBuilder = this._coerceStoreBuilder({
      storeBuilder: resolved.storeBuilder,
      store: resolved.store ?? (() => new MemoryStateStore(storeName)),
      storeName,
      logging: resolved.logging ?? true,
      changelogConfig: resolved.changelogConfig
    });

    this._registerStateStore({
      name: storeName,
      storeBuilder,
      keySerde: resolved.keySerde ?? this.keySerde,
      valueSerde: resolved.valueSerde ?? this.valueSerde
    });

    this._appendOperation('aggregate', aggregator, {
      storeName,
      initializer,
      emitOnUpdate: resolved.emitOnUpdate ?? true,
      named: resolved.named
    });

    return this;
  }

  count(materializedOrOptions = {}) {
    return this.aggregate(
      () => 0,
      (aggregate) => (aggregate ?? 0) + 1,
      materializedOrOptions instanceof Materialized
        ? materializedOrOptions.withValueSerde(materializedOrOptions.valueSerde ?? null)
        : { ...materializedOrOptions, valueSerde: materializedOrOptions.valueSerde ?? null }
    );
  }

  reduce(reducer, materializedOrOptions = {}) {
    if (typeof reducer !== 'function') {
      throw new Error('reduce expects a reducer function');
    }

    return this.aggregate(
      () => undefined,
      (aggregate, value, record) => {
        if (aggregate === undefined) {
          return value;
        }
        return reducer(aggregate, value, record);
      },
      materializedOrOptions
    );
  }

  through(topic, options = {}) {
    if (!topic) {
      throw new Error('through requires an intermediate topic');
    }
    this._appendOperation('through', {
      topic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      partitioner: options.partitioner
    }, options);
    return this;
  }

  to(topic, options = {}) {
    if (!topic) {
      throw new Error('Output topic must be provided when calling to()');
    }
    this.sinks.push({
      type: 'topic',
      topic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      partitioner: options.partitioner,
      named: options.named
    });
    return this;
  }

  branch(...predicates) {
    if (!predicates.length) {
      throw new Error('branch requires at least one predicate');
    }

    const branchOperation = this._appendOperation('branch', null, { predicates: predicates.length });
    const branchStreams = predicates.map((predicate, index) => {
      let fn = predicate;
      let named;
      if (typeof predicate === 'object' && predicate !== null) {
        fn = predicate.predicate ?? predicate.fn;
        named = predicate.named;
      }
      if (typeof fn !== 'function') {
        throw new Error('branch predicates must be functions');
      }
      const branchStream = this.cloneWith({
        origin: { type: 'branch', parent: this.id, index, name: Named.from(named, `branch-${index}`) },
        initialNodeId: branchOperation.id
      });
      this._builder._registerDerivedStream(branchStream, { parentOperationId: branchOperation.id });
      return { stream: branchStream, fn };
    });

    branchOperation.branches = branchStreams.map(({ stream, fn }, index) => ({
      streamId: stream.id,
      predicate: fn,
      index
    }));

    return branchStreams.map(({ stream }) => stream);
  }

  repartition(options = {}) {
    const topic = options.topic ?? `repartition-${this.id}`;
    const namedTopic = Named.from(options.named, topic);
    const repartitionOperation = this._appendOperation('repartition', {
      topic: namedTopic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      partitioner: options.partitioner
    }, options);

    const repartitionedStream = this.cloneWith({
      origin: { type: 'repartition', parent: this.id, topic: namedTopic },
      sourceTopic: namedTopic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      initialNodeId: repartitionOperation.id
    });

    this._builder._registerDerivedStream(repartitionedStream, { parentOperationId: repartitionOperation.id });
    repartitionOperation.targetStreamId = repartitionedStream.id;
    return repartitionedStream;
  }

  join(otherStream, joiner, options = {}) {
    return this._registerJoin('inner', otherStream, joiner, options);
  }

  leftJoin(otherStream, joiner, options = {}) {
    return this._registerJoin('left', otherStream, joiner, options);
  }

  outerJoin(otherStream, joiner, options = {}) {
    return this._registerJoin('outer', otherStream, joiner, options);
  }

  _registerJoin(type, otherStream, joiner, options = {}) {
    if (!(otherStream instanceof KStream)) {
      throw new Error('Join operations require another KStream');
    }
    if (typeof joiner !== 'function') {
      throw new Error('Join operations require a joiner function');
    }

    const joinOperation = this._appendOperation('join', joiner, {
      joinType: type,
      otherStreamId: otherStream.id,
      window: options.window,
      materialized: options.materialized,
      named: options.named
    });
    joinOperation.targetStreamId = otherStream.id;
    return this;
  }

  describe() {
    return {
      id: this.id,
      sourceTopic: this.sourceTopic,
      fromBeginning: this.fromBeginning,
      keySerde: this.keySerde,
      valueSerde: this.valueSerde,
      operations: this.operations.map(operation => ({
        id: operation.id,
        type: operation.type,
        fn: operation.fn,
        options: operation.options,
        branches: operation.branches,
        targetStreamId: operation.targetStreamId,
        metadata: this._sanitizeTopologyOptions(operation.options)
      })),
      sinks: this.sinks,
      stateStores: Array.from(this.stateStores.values()).map(store => ({
        name: store.name,
        keySerde: store.keySerde,
        valueSerde: store.valueSerde,
        builder: store.builder,
        builderMetadata: store.builder.describe()
      })),
      isSource: this.isSource,
      origin: this.origin
    };
  }
}

module.exports = {
  KStream
};
