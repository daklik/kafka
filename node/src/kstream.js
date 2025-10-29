'use strict';

const { v4: uuidv4 } = require('uuid');
const { MemoryStateStore } = require('./state/memory-store');
const { StoreBuilder } = require('./state/store-builder');
const { Materialized } = require('./materialized');
const { Named } = require('./named');
const { Joined } = require('./joined');
const { ValueJoiner } = require('./value-joiner');
const { KGroupedStream } = require('./kgrouped-stream');
const { MemoryWindowStore } = require('./state/memory-window-store');
const { JoinWindows } = require('./windows/join-windows');
const { SlidingWindows } = require('./windows/sliding-windows');
const { TimeWindows } = require('./windows/time-windows');
const { SessionWindows } = require('./windows/session-windows');
const { UnlimitedWindows } = require('./windows/unlimited-windows');
const { Suppressed } = require('./suppressed');
const { describeSerde, sanitizeValue } = require('./topology/utils');

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
    this._joinBuffers = [];
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
    const fallbackName = `${type}-${operationId.slice(0, 6)}`;
    const name = Named.from(options.named, fallbackName) ?? fallbackName;
    this._topology.addNode({
      id: operationId,
      name,
      type: 'processor',
      metadata: {
        operation: type,
        options: this._sanitizeTopologyOptions(options)
      }
    });
    this._topology.connect(this._lastNodeId, operationId, { type: 'processor', operation: type });
    this._lastNodeId = operationId;
    return operation;
  }

  _sanitizeTopologyOptions(options) {
    if (!options) {
      return {};
    }
    const sanitized = sanitizeValue(options);
    if (Array.isArray(sanitized.streams)) {
      sanitized.streams = sanitized.streams.map(stream => stream?.id ?? stream);
    }
    return sanitized;
  }

  _resolveMaterialized(materializedOrOptions = {}, defaults = {}) {
    if (materializedOrOptions instanceof Materialized) {
      return materializedOrOptions.resolve(defaults);
    }
    if (materializedOrOptions.materialized instanceof Materialized) {
      return materializedOrOptions.materialized.resolve({ ...defaults, ...materializedOrOptions });
    }
    return { ...defaults, ...(materializedOrOptions ?? {}) };
  }

  _resolveJoined(joinedOrOptions = {}, otherStream) {
    const defaults = {
      keySerde: this.keySerde ?? null,
      valueSerde: this.valueSerde ?? null,
      otherValueSerde: otherStream?.valueSerde ?? null,
      named: null
    };

    if (!joinedOrOptions) {
      return { ...defaults };
    }

    if (joinedOrOptions instanceof Joined) {
      return joinedOrOptions.resolve(defaults);
    }

    if (joinedOrOptions.joined instanceof Joined) {
      const resolved = joinedOrOptions.joined.resolve(defaults);
      return {
        keySerde: joinedOrOptions.keySerde ?? resolved.keySerde,
        valueSerde: joinedOrOptions.valueSerde ?? resolved.valueSerde,
        otherValueSerde: joinedOrOptions.otherValueSerde ?? resolved.otherValueSerde,
        named: joinedOrOptions.named ?? resolved.named
      };
    }

    return {
      keySerde: joinedOrOptions.keySerde ?? defaults.keySerde,
      valueSerde: joinedOrOptions.valueSerde ?? defaults.valueSerde,
      otherValueSerde: joinedOrOptions.otherValueSerde ?? defaults.otherValueSerde,
      named: joinedOrOptions.named ?? defaults.named
    };
  }

  _resolveSuppressed(suppressedOrOptions = {}) {
    if (suppressedOrOptions instanceof Suppressed) {
      return suppressedOrOptions.describe();
    }

    if (suppressedOrOptions?.suppressed instanceof Suppressed) {
      return suppressedOrOptions.suppressed.describe();
    }

    const strategy = suppressedOrOptions.strategy ?? suppressedOrOptions.type ?? 'untilWindowCloses';
    const bufferConfig = Suppressed.BufferConfig.from(suppressedOrOptions.bufferConfig ?? suppressedOrOptions);

    return {
      strategy,
      bufferConfig: bufferConfig.describe()
    };
  }

  _resolveWindowGrace(aggregateOperation) {
    if (!aggregateOperation) {
      return 0;
    }

    const metadataGrace = aggregateOperation.options?.window?.graceMs;
    if (metadataGrace !== undefined && metadataGrace !== null) {
      return metadataGrace;
    }

    const instance = aggregateOperation.windowInstance;
    if (instance) {
      if (typeof instance.gracePeriod === 'function') {
        const grace = instance.gracePeriod();
        if (grace !== undefined && grace !== null) {
          return grace;
        }
      }
      if (typeof instance.graceMs === 'number') {
        return instance.graceMs;
      }
    }

    return 0;
  }

  _coerceStoreBuilder({
    storeBuilder,
    store,
    storeName,
    changelogConfig,
    logging = true,
    caching = false,
    storeType = 'keyValue',
    retention,
    windowSize,
    strategy
  }) {
    if (storeBuilder instanceof StoreBuilder) {
      return storeBuilder;
    }
    if (store instanceof StoreBuilder) {
      return store;
    }
    let supplier = store;
    if (typeof supplier !== 'function') {
      if (storeType === 'window') {
        supplier = () => new MemoryWindowStore(storeName, { retention, windowSize, strategy, loggingEnabled: logging });
      } else {
        supplier = () => new MemoryStateStore(storeName);
      }
    }
    return new StoreBuilder({
      name: storeName,
      type: storeType,
      supplier,
      loggingEnabled: logging,
      cachingEnabled: caching,
      changelogConfig
    });
  }

  _normalizeAggregationWindow(windowDefinition) {
    if (!windowDefinition) {
      return null;
    }
    if (windowDefinition instanceof TimeWindows) {
      return {
        type: 'time',
        instance: windowDefinition,
        retentionMs: windowDefinition.retentionPeriod(),
        windowSizeMs: windowDefinition.windowSize(),
        metadata: windowDefinition.describe()
      };
    }
    if (windowDefinition instanceof SlidingWindows) {
      return {
        type: 'sliding',
        instance: windowDefinition,
        retentionMs: windowDefinition.retentionPeriod(),
        windowSizeMs: windowDefinition.windowSize(),
        metadata: windowDefinition.describe()
      };
    }
    if (windowDefinition instanceof SessionWindows) {
      return {
        type: 'session',
        instance: windowDefinition,
        retentionMs: windowDefinition.retentionPeriod(),
        metadata: windowDefinition.describe(),
        gapMs: windowDefinition.gap()
      };
    }
    if (windowDefinition instanceof UnlimitedWindows) {
      return {
        type: 'unlimited',
        instance: windowDefinition,
        retentionMs: windowDefinition.retentionPeriod(),
        metadata: windowDefinition.describe()
      };
    }
    throw new Error('Unsupported window definition provided for aggregation');
  }

  _registerAggregation({
    initializer,
    aggregator,
    materializedOrOptions = {},
    windowSpec = null,
    defaultSessionMerger = null,
    valueSerde
  }) {
    if (typeof initializer !== 'function') {
      throw new Error('aggregate expects an initializer function');
    }
    if (typeof aggregator !== 'function') {
      throw new Error('aggregate expects an aggregator function');
    }

    const resolved = this._resolveMaterialized(materializedOrOptions, {
      storeType: windowSpec ? 'window' : 'keyValue',
      retention: windowSpec?.retentionMs,
      windowSize: windowSpec?.windowSizeMs,
      strategy: windowSpec ? 'aggregate' : undefined,
      keySerde: this.keySerde,
      valueSerde: valueSerde ?? this.valueSerde,
      emitOnUpdate: materializedOrOptions.emitOnUpdate,
      named: materializedOrOptions.named
    });

    const storeName = resolved.storeName ?? `${windowSpec ? 'window' : 'agg'}-${uuidv4()}`;
    const baseBuilder = this._coerceStoreBuilder({
      storeBuilder: resolved.storeBuilder,
      store: resolved.store,
      storeName,
      changelogConfig: resolved.changelogConfig,
      logging: resolved.logging ?? true,
      caching: resolved.caching ?? false,
      storeType: resolved.storeType ?? (windowSpec ? 'window' : 'keyValue'),
      retention: resolved.retention ?? windowSpec?.retentionMs,
      windowSize: resolved.windowSize ?? windowSpec?.windowSizeMs,
      strategy: resolved.strategy ?? (windowSpec ? 'aggregate' : undefined)
    });

    let configuredBuilder = baseBuilder;
    if (resolved.logging === false) {
      configuredBuilder = configuredBuilder.withLoggingDisabled();
    }
    if (resolved.caching) {
      configuredBuilder = configuredBuilder.withCachingEnabled();
    }

    const storeMetadata = windowSpec
      ? {
        type: 'windowed-aggregation-store',
        window: windowSpec.metadata,
        retentionMs: resolved.retention ?? windowSpec.retentionMs,
        windowSizeMs: resolved.windowSize ?? windowSpec.windowSizeMs
      }
      : { type: 'aggregation-store' };

    this._registerStateStore({
      name: storeName,
      storeBuilder: configuredBuilder,
      keySerde: resolved.keySerde ?? this.keySerde,
      valueSerde: resolved.valueSerde ?? this.valueSerde,
      metadata: storeMetadata
    });

    const options = {
      storeName,
      initializer,
      emitOnUpdate: resolved.emitOnUpdate ?? true,
      named: resolved.named,
      window: windowSpec?.metadata ?? null,
      windowType: windowSpec?.type ?? null,
      windowRetentionMs: windowSpec?.retentionMs ?? null
    };

    if (windowSpec?.type === 'session') {
      const merger = materializedOrOptions.sessionMerger ?? materializedOrOptions.merger ?? defaultSessionMerger;
      if (typeof merger !== 'function') {
        throw new Error('Session window aggregations require a sessionMerger function');
      }
      options.sessionMerger = merger;
      options.sessionGap = windowSpec.gapMs;
    }

    const operation = this._appendOperation('aggregate', aggregator, options);
    if (windowSpec) {
      operation.windowInstance = windowSpec.instance;
    }

    const builderMetadata = configuredBuilder.describe();
    const descriptor = {
      name: storeName,
      type: builderMetadata.type,
      keySerde: describeSerde(resolved.keySerde ?? this.keySerde),
      valueSerde: describeSerde(resolved.valueSerde ?? this.valueSerde),
      changelogTopic: builderMetadata.changelog?.topic ?? resolved.changelogTopic ?? null,
      loggingEnabled: builderMetadata.loggingEnabled,
      cachingEnabled: builderMetadata.cachingEnabled,
      partitioning: 'by-key',
      retentionMs: storeMetadata.retentionMs ?? null,
      windowSizeMs: storeMetadata.windowSizeMs ?? null,
      windowType: windowSpec?.type ?? null,
      scope: storeMetadata.type,
      metadata: storeMetadata
    };

    this._topology.attachStateStore(operation.id, descriptor);

    return this;
  }

  _registerStateStore({ name, storeBuilder, keySerde, valueSerde, metadata = {} }) {
    if (!name) {
      throw new Error('State store definition must include a name');
    }
    const definition = {
      name,
      builder: storeBuilder,
      keySerde,
      valueSerde,
      metadata,
      restoreListeners: new Set(),
      builderMetadata: storeBuilder.describe(),
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

  _registerJoinBuffer({ joinId, partnerStreamId, storeName, retentionMs, windowMetadata }) {
    if (!joinId || !storeName) {
      throw new Error('Join buffer registration requires joinId and storeName');
    }
    if (this._joinBuffers.some(buffer => buffer.joinId === joinId && buffer.storeName === storeName)) {
      return;
    }
    this._joinBuffers.push({
      joinId,
      partnerStreamId,
      storeName,
      retentionMs,
      window: windowMetadata
    });
  }

  _normalizeJoinWindow(window) {
    if (!window) {
      throw new Error('Stream-stream joins require a window specification');
    }
    if (!(window instanceof JoinWindows) && !(window instanceof SlidingWindows)) {
      throw new Error('Stream-stream joins require JoinWindows or SlidingWindows definitions');
    }

    const retentionMs = typeof window.retentionPeriod === 'function' ? window.retentionPeriod() : null;
    const windowSizeMs = typeof window.windowSize === 'function' ? window.windowSize() : null;

    return {
      instance: window,
      retentionMs,
      windowSizeMs,
      metadata: window.describe?.() ?? null
    };
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
    return new KGroupedStream(this);
  }

  groupBy(selector, options = {}) {
    if (typeof selector !== 'function') {
      throw new Error('groupBy expects a selector function');
    }
    this._appendOperation('groupBy', selector, options);
    return new KGroupedStream(this);
  }

  aggregate(initializer, aggregator, materializedOrOptions = {}) {
    return this._registerAggregation({ initializer, aggregator, materializedOrOptions });
  }

  count(materializedOrOptions = {}) {
    const materialized = materializedOrOptions instanceof Materialized
      ? materializedOrOptions.withValueSerde(materializedOrOptions.valueSerde ?? null)
      : { ...materializedOrOptions, valueSerde: materializedOrOptions.valueSerde ?? null };

    return this._registerAggregation({
      initializer: () => 0,
      aggregator: (aggregate) => (aggregate ?? 0) + 1,
      materializedOrOptions: materialized,
      defaultSessionMerger: (left, right) => (left ?? 0) + (right ?? 0)
    });
  }

  reduce(reducer, materializedOrOptions = {}) {
    if (typeof reducer !== 'function') {
      throw new Error('reduce expects a reducer function');
    }

    return this._registerAggregation({
      initializer: () => undefined,
      aggregator: (aggregate, value, record) => {
        if (aggregate === undefined) {
          return value;
        }
        return reducer(aggregate, value, record);
      },
      materializedOrOptions,
      defaultSessionMerger: reducer
    });
  }

  suppress(suppressedOrOptions = {}) {
    const config = this._resolveSuppressed(suppressedOrOptions);
    const aggregateOperation = [...this.operations].reverse().find(operation => operation.type === 'aggregate');

    if (!aggregateOperation) {
      throw new Error('suppress requires a preceding aggregation');
    }

    if (!aggregateOperation.options?.windowType) {
      throw new Error('suppress currently supports windowed aggregations');
    }

    const operation = this._appendOperation('suppress', null, {
      strategy: config.strategy,
      bufferConfig: config.bufferConfig,
      windowType: aggregateOperation.options.windowType,
      graceMs: this._resolveWindowGrace(aggregateOperation),
      aggregateOperationId: aggregateOperation.id,
      windowMetadata: aggregateOperation.options.window ?? null
    });

    operation.windowInstance = aggregateOperation.windowInstance;
    operation._buffer = new Map();
    operation._streamTime = Number.NEGATIVE_INFINITY;

    return this;
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
    const sinkId = uuidv4();
    const fallbackName = `sink-${topic}`;
    const sinkName = Named.from(options.named, fallbackName) ?? fallbackName;
    const partitioner = typeof options.partitioner === 'function' ? '[function]' : options.partitioner ?? null;
    this._topology.addNode({
      id: sinkId,
      name: sinkName,
      type: 'sink',
      metadata: {
        topic,
        keySerde: describeSerde(options.keySerde ?? this.keySerde),
        valueSerde: describeSerde(options.valueSerde ?? this.valueSerde),
        partitioner
      }
    });
    this._topology.connect(this._lastNodeId, sinkId, {
      type: 'sink',
      topic,
      partitioner
    });
    this.sinks.push({
      type: 'topic',
      topic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde,
      partitioner: options.partitioner,
      named: options.named,
      topologyNodeId: sinkId
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
    const isTableLike = Boolean(otherStream?.isTable);
    const isStreamLike = otherStream instanceof KStream && !isTableLike;

    if (!isTableLike && !isStreamLike) {
      throw new Error('Join operations require a KStream or KTable');
    }

    let joinFn = joiner;
    if (joiner instanceof ValueJoiner) {
      joinFn = joiner.join.bind(joiner);
    }

    if (typeof joinFn !== 'function') {
      throw new Error('Join operations require a joiner function');
    }

    const joinedOptions = this._resolveJoined(options.joined ?? options, otherStream);
    const otherStreamType = isTableLike ? (otherStream.isGlobalKTable ? 'global-table' : 'table') : 'stream';

    let streamJoinConfig = null;
    let storeName = options.storeName ?? otherStream.materialized?.storeName;

    if (otherStreamType !== 'stream' && !storeName) {
      throw new Error('Stream-table joins require the table to be materialized');
    }

    if (otherStreamType === 'stream') {
      const windowSpec = this._normalizeJoinWindow(options.window);
      const joinId = options.joinId ?? uuidv4();
      const baseName = Named.from(options.named ?? joinedOptions.named, `join-${this.id.slice(0, 6)}-${otherStream.id.slice(0, 6)}`);
      const thisStoreName = options.thisStoreName ?? `${baseName}-this`;
      const otherStoreName = options.otherStoreName ?? `${baseName}-other`;
      const retentionMs = windowSpec.retentionMs ?? windowSpec.windowSizeMs ?? 0;

      const thisStoreBuilder = new StoreBuilder({
        name: thisStoreName,
        type: 'window',
        supplier: () => new MemoryWindowStore(thisStoreName, { retention: retentionMs, windowSize: windowSpec.windowSizeMs }),
        loggingEnabled: options.logging ?? false,
        cachingEnabled: options.caching ?? false,
        changelogConfig: options.changelogConfig
      });

      const otherStoreBuilder = new StoreBuilder({
        name: otherStoreName,
        type: 'window',
        supplier: () => new MemoryWindowStore(otherStoreName, { retention: retentionMs, windowSize: windowSpec.windowSizeMs }),
        loggingEnabled: options.logging ?? false,
        cachingEnabled: options.caching ?? false,
        changelogConfig: options.changelogConfig
      });

      this._registerStateStore({
        name: thisStoreName,
        storeBuilder: thisStoreBuilder,
        keySerde: joinedOptions.keySerde ?? this.keySerde,
        valueSerde: this.valueSerde,
        metadata: {
          type: 'join-buffer-store',
          joinId,
          role: 'this-stream',
          partnerStreamId: otherStream.id,
          retentionMs,
          window: windowSpec.metadata
        }
      });
      otherStream._registerStateStore({
        name: otherStoreName,
        storeBuilder: otherStoreBuilder,
        keySerde: joinedOptions.keySerde ?? otherStream.keySerde,
        valueSerde: otherStream.valueSerde,
        metadata: {
          type: 'join-buffer-store',
          joinId,
          role: 'other-stream',
          partnerStreamId: this.id,
          retentionMs,
          window: windowSpec.metadata
        }
      });
      otherStream._registerJoinBuffer({
        joinId,
        partnerStreamId: this.id,
        storeName: otherStoreName,
        retentionMs,
        windowMetadata: windowSpec.metadata
      });

      streamJoinConfig = {
        joinId,
        thisStoreName,
        otherStoreName,
        retentionMs,
        window: windowSpec
      };
    }

    const joinOperation = this._appendOperation('join', joinFn, {
      joinType: type,
      otherStreamId: otherStream.id,
      otherStreamType,
      storeName,
      thisStoreName: streamJoinConfig?.thisStoreName,
      otherStoreName: streamJoinConfig?.otherStoreName,
      window: streamJoinConfig?.window.metadata ?? null,
      windowRetentionMs: streamJoinConfig?.retentionMs ?? null,
      keySerde: joinedOptions.keySerde,
      valueSerde: joinedOptions.valueSerde,
      otherValueSerde: joinedOptions.otherValueSerde,
      named: options.named ?? joinedOptions.named,
      materialized: options.materialized,
      joined: joinedOptions
    });
    if (streamJoinConfig) {
      joinOperation.windowInstance = streamJoinConfig.window.instance;
    }
    joinOperation.targetStreamId = otherStream.id;

    if (streamJoinConfig) {
      const thisStore = this.stateStores.get(streamJoinConfig.thisStoreName);
      if (thisStore) {
        const builderMetadata = thisStore.builderMetadata ?? thisStore.builder.describe();
        this._topology.attachStateStore(joinOperation.id, {
          name: thisStore.name,
          type: builderMetadata.type,
          keySerde: describeSerde(thisStore.keySerde ?? this.keySerde),
          valueSerde: describeSerde(thisStore.valueSerde ?? this.valueSerde),
          changelogTopic: builderMetadata.changelog?.topic ?? null,
          loggingEnabled: builderMetadata.loggingEnabled,
          cachingEnabled: builderMetadata.cachingEnabled,
          partitioning: 'by-key',
          retentionMs: streamJoinConfig.retentionMs,
          windowSizeMs: streamJoinConfig.window.windowSizeMs,
          windowType: streamJoinConfig.window.metadata?.type ?? null,
          scope: 'stream-stream-join',
          metadata: this._sanitizeTopologyOptions(thisStore.metadata)
        });
      }

      this._topology.annotateNode(joinOperation.id, {
        join: {
          type,
          partnerStreamId: otherStream.id,
          window: streamJoinConfig.window.metadata,
          retentionMs: streamJoinConfig.retentionMs
        }
      });
    } else if (isTableLike) {
      const tableStore = otherStream?.stateStores?.get?.(storeName);
      if (tableStore) {
        const builderMetadata = tableStore.builderMetadata ?? tableStore.builder.describe();
        this._topology.attachStateStore(joinOperation.id, {
          name: tableStore.name,
          type: builderMetadata.type,
          keySerde: describeSerde(tableStore.keySerde ?? otherStream.keySerde),
          valueSerde: describeSerde(tableStore.valueSerde ?? otherStream.valueSerde),
          changelogTopic: builderMetadata.changelog?.topic ?? otherStream.materialized?.changelogTopic ?? null,
          loggingEnabled: builderMetadata.loggingEnabled,
          cachingEnabled: builderMetadata.cachingEnabled,
          partitioning: otherStream.isGlobalKTable ? 'global' : 'by-key',
          scope: otherStream.isGlobalKTable ? 'global-table-lookup' : 'table-lookup',
          metadata: this._sanitizeTopologyOptions({
            tableSource: otherStream.sourceTopic,
            tableType: otherStream.isGlobalKTable ? 'global' : 'table'
          })
        });
      }

      this._topology.annotateNode(joinOperation.id, {
        join: {
          type,
          partnerStreamId: otherStream.id,
          lookupStore: storeName,
          partnerType: otherStreamType
        }
      });
    }

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
        windowInstance: operation.windowInstance,
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
        builderMetadata: store.builder.describe(),
        metadata: store.metadata ?? {},
        restoreListeners: Array.from(store.restoreListeners ?? [])
      })),
      joinBuffers: this._joinBuffers.map(buffer => ({ ...buffer })),
      isSource: this.isSource,
      origin: this.origin
    };
  }
}

module.exports = {
  KStream
};
