'use strict';

const { v4: uuidv4 } = require('uuid');
const { KStream } = require('./kstream');
const { KTable, GlobalKTable } = require('./ktable');
const { TopologyDescription } = require('./topology/topology-description');
const { Named } = require('./named');
const { ProcessorSupplier } = require('./processor');
const { describeSerde, sanitizeValue } = require('./topology/utils');

class StreamsBuilder {
  constructor() {
    this._streams = new Map();
    this._topology = new TopologyDescription();
    this._nodesById = new Map();
    this._nodeIdsByName = new Map();
    this._stateStores = new Map();
  }

  _registerStream(stream) {
    this._streams.set(stream.id, stream);
    if (stream?.nodeName) {
      this._registerNode({
        id: stream.id,
        name: stream.nodeName,
        type: stream.isSource ? 'source' : stream.isTable ? 'table' : 'processor',
        metadata: {
          topic: stream.sourceTopic ?? null,
          fromBeginning: stream.fromBeginning ?? false,
          keySerde: describeSerde(stream.keySerde),
          valueSerde: describeSerde(stream.valueSerde)
        }
      });
    }
  }

  _registerDerivedStream(stream, { parentOperationId } = {}) {
    if (!this._streams.has(stream.id)) {
      this._streams.set(stream.id, stream);
      this._topology.addNode({
        id: stream.id,
        name: stream.nodeName,
        type: 'processor',
        metadata: {
          origin: stream.origin,
          keySerde: describeSerde(stream.keySerde),
          valueSerde: describeSerde(stream.valueSerde)
        }
      });
      if (parentOperationId) {
        this._topology.connect(parentOperationId, stream.id, this._edgeMetadataForStream(stream));
        this._connectNodes(parentOperationId, stream.id);
      } else if (stream.parent) {
        this._topology.connect(stream.parent.id, stream.id, this._edgeMetadataForStream(stream));
        this._connectNodes(stream.parent.id, stream.id);
      }
    }
  }

  _registerNode({ id, name, type = 'processor', metadata = {}, parents = [] }) {
    if (!id) {
      return null;
    }

    const existing = this._nodesById.get(id) ?? {
      id,
      name: name ?? id,
      type,
      metadata: {},
      parents: new Set(),
      children: new Set(),
      stores: new Map(),
      runtime: {}
    };

    if (name) {
      existing.name = name;
      if (!this._nodeIdsByName.has(name)) {
        this._nodeIdsByName.set(name, id);
      }
    }

    existing.type = type ?? existing.type ?? 'processor';
    if (metadata && Object.keys(metadata).length) {
      existing.metadata = { ...existing.metadata, ...sanitizeValue(metadata) };
    }

    for (const parentId of parents) {
      if (!parentId) {
        continue;
      }
      existing.parents.add(parentId);
      const parentDescriptor = this._nodesById.get(parentId);
      if (parentDescriptor) {
        parentDescriptor.children.add(id);
      }
    }

    this._nodesById.set(id, existing);
    return existing;
  }

  _connectNodes(parentId, childId) {
    if (!parentId || !childId) {
      return;
    }
    const parent = this._nodesById.get(parentId);
    const child = this._nodesById.get(childId) ?? this._registerNode({ id: childId });
    if (!parent || !child) {
      return;
    }
    parent.children.add(childId);
    child.parents.add(parentId);
  }

  _registerStateStoreDescriptor({
    name,
    builder = null,
    builderMetadata = null,
    keySerde = null,
    valueSerde = null,
    changelogTopic = null,
    loggingEnabled = null,
    cachingEnabled = null,
    scope = null,
    metadata = {}
  }) {
    if (!name) {
      throw new Error('State store registration requires a store name.');
    }

    const existing = this._stateStores.get(name) ?? {
      name,
      processors: new Set(),
      metadata: {},
      runtime: {}
    };

    if (builder && !existing.runtime.builder) {
      existing.runtime.builder = builder;
    }

    if (builderMetadata) {
      existing.builderMetadata = builderMetadata;
    } else if (!existing.builderMetadata && builder?.describe) {
      existing.builderMetadata = builder.describe();
    }

    if (keySerde !== undefined && keySerde !== null) {
      existing.keySerde = describeSerde(keySerde);
    }
    if (valueSerde !== undefined && valueSerde !== null) {
      existing.valueSerde = describeSerde(valueSerde);
    }
    if (changelogTopic !== undefined) {
      existing.changelogTopic = changelogTopic;
    }
    if (loggingEnabled !== undefined && loggingEnabled !== null) {
      existing.loggingEnabled = loggingEnabled;
    }
    if (cachingEnabled !== undefined && cachingEnabled !== null) {
      existing.cachingEnabled = cachingEnabled;
    }
    if (scope !== undefined && scope !== null) {
      existing.scope = scope;
    }

    if (metadata && Object.keys(metadata).length) {
      existing.metadata = { ...existing.metadata, ...sanitizeValue(metadata) };
    }

    this._stateStores.set(name, existing);
    return existing;
  }

  _attachStoreToNode({ nodeId, storeName, descriptor = null }) {
    if (!nodeId || !storeName) {
      return;
    }
    const node = this._nodesById.get(nodeId) ?? this._registerNode({ id: nodeId });
    const store = descriptor ?? this._stateStores.get(storeName);
    if (!node || !store) {
      return;
    }

    if (!node.stores.has(store.name)) {
      node.stores.set(store.name, {
        name: store.name,
        keySerde: store.keySerde ?? null,
        valueSerde: store.valueSerde ?? null,
        changelogTopic: store.changelogTopic ?? store.builderMetadata?.changelog?.topic ?? null,
        loggingEnabled: store.loggingEnabled ?? store.builderMetadata?.loggingEnabled ?? null,
        cachingEnabled: store.cachingEnabled ?? store.builderMetadata?.cachingEnabled ?? null,
        scope: store.scope ?? null,
        metadata: store.metadata ?? {}
      });
    }
    store.processors.add(node.name ?? node.id);
  }

  _getStateStoreDescriptor(name) {
    return this._stateStores.get(name) ?? null;
  }

  _topologyStoreMetadata(store) {
    if (!store) {
      return null;
    }

    return {
      name: store.name,
      type: store.builderMetadata?.type ?? null,
      keySerde: store.keySerde ?? null,
      valueSerde: store.valueSerde ?? null,
      changelogTopic: store.changelogTopic ?? store.builderMetadata?.changelog?.topic ?? null,
      loggingEnabled: store.loggingEnabled ?? store.builderMetadata?.loggingEnabled ?? null,
      cachingEnabled: store.cachingEnabled ?? store.builderMetadata?.cachingEnabled ?? null,
      partitioning: store.metadata?.partitioning ?? null,
      retentionMs: store.metadata?.retentionMs ?? null,
      windowSizeMs: store.metadata?.windowSizeMs ?? null,
      windowType: store.metadata?.windowType ?? null,
      scope: store.scope ?? store.metadata?.type ?? null,
      metadata: store.metadata ?? {}
    };
  }

  _describeProcessorTopology() {
    return {
      nodes: Array.from(this._nodesById.values()).map(node => ({
        id: node.id,
        name: node.name,
        type: node.type,
        metadata: sanitizeValue(node.metadata ?? {}),
        parents: Array.from(node.parents),
        children: Array.from(node.children),
        stores: Array.from(node.stores.values()).map(store => sanitizeValue(store))
      })),
      stateStores: Array.from(this._stateStores.values()).map(store => ({
        name: store.name,
        builder: sanitizeValue(store.builderMetadata ?? null),
        keySerde: store.keySerde ?? null,
        valueSerde: store.valueSerde ?? null,
        changelogTopic: store.changelogTopic ?? null,
        loggingEnabled: store.loggingEnabled ?? null,
        cachingEnabled: store.cachingEnabled ?? null,
        scope: store.scope ?? null,
        metadata: sanitizeValue(store.metadata ?? {}),
        processors: Array.from(store.processors)
      }))
    };
  }

  addSource(name, optionsOrTopic, ...restTopics) {
    if (!name || typeof name !== 'string') {
      throw new TypeError('StreamsBuilder#addSource requires a source name.');
    }

    if (this._nodeIdsByName.has(name)) {
      return name;
    }

    let options = {};
    let topics = [];

    if (typeof optionsOrTopic === 'string') {
      topics = [optionsOrTopic, ...restTopics];
    } else if (Array.isArray(optionsOrTopic)) {
      topics = [...optionsOrTopic, ...restTopics];
    } else if (optionsOrTopic && typeof optionsOrTopic === 'object') {
      options = { ...optionsOrTopic };
      const configuredTopics = [];
      if (Array.isArray(options.topics)) {
        configuredTopics.push(...options.topics);
      }
      if (typeof options.topic === 'string') {
        configuredTopics.push(options.topic);
      }
      topics = [...configuredTopics, ...restTopics];
    } else if (optionsOrTopic != null) {
      throw new TypeError('StreamsBuilder#addSource second argument must be a topic name, array, or options object.');
    }

    if (!topics.length) {
      throw new Error('StreamsBuilder#addSource requires at least one topic.');
    }

    const nodeId = uuidv4();
    const keySerde = options.keySerde ?? null;
    const valueSerde = options.valueSerde ?? null;
    const fromBeginning = options.fromBeginning ?? false;

    this._topology.addNode({
      id: nodeId,
      name,
      type: 'source',
      metadata: {
        topics: topics.slice(),
        fromBeginning,
        keySerde: describeSerde(keySerde),
        valueSerde: describeSerde(valueSerde),
        timestampExtractor: options.timestampExtractor ? options.timestampExtractor.name ?? '[function]' : null
      }
    });

    this._registerNode({
      id: nodeId,
      name,
      type: 'source',
      metadata: {
        topics: topics.slice(),
        fromBeginning,
        keySerde: describeSerde(keySerde),
        valueSerde: describeSerde(valueSerde),
        timestampExtractor: options.timestampExtractor ? options.timestampExtractor.name ?? '[function]' : null
      }
    });

    return name;
  }

  addProcessor(name, processorSupplier, ...parentNames) {
    if (!name || typeof name !== 'string') {
      throw new TypeError('StreamsBuilder#addProcessor requires a processor name.');
    }

    if (this._nodeIdsByName.has(name)) {
      return name;
    }

    const parents = parentNames.flat().filter(Boolean).map(parentName => {
      const id = this._nodeIdsByName.get(parentName);
      if (!id) {
        throw new Error(`Unknown parent node ${parentName} when registering processor ${name}.`);
      }
      return id;
    });

    const supplier = ProcessorSupplier.from(processorSupplier);
    const nodeId = uuidv4();
    const supplierDescription = supplier?._factory?.name ?? supplier?.constructor?.name ?? 'ProcessorSupplier';

    this._topology.addNode({
      id: nodeId,
      name,
      type: 'processor',
      metadata: {
        supplier: supplierDescription
      }
    });

    for (const parentId of parents) {
      this._topology.connect(parentId, nodeId, { type: 'processor', child: name });
    }

    const descriptor = this._registerNode({
      id: nodeId,
      name,
      type: 'processor',
      metadata: { supplier: supplierDescription },
      parents
    });
    descriptor.runtime = descriptor.runtime ?? {};
    descriptor.runtime.supplier = supplier;

    return name;
  }

  addSink(name, topic, parentNames = [], options = {}) {
    if (!name || typeof name !== 'string') {
      throw new TypeError('StreamsBuilder#addSink requires a sink name.');
    }

    if (this._nodeIdsByName.has(name)) {
      return name;
    }

    if (topic && typeof topic === 'object' && !Array.isArray(topic)) {
      options = { ...topic };
      topic = options.topic;
      parentNames = options.parents ?? parentNames;
    }

    if (!topic) {
      throw new Error('StreamsBuilder#addSink requires a topic name.');
    }

    const parents = Array.isArray(parentNames) ? parentNames.flat().filter(Boolean) : [parentNames].filter(Boolean);
    const parentIds = parents.map(parentName => {
      const id = this._nodeIdsByName.get(parentName);
      if (!id) {
        throw new Error(`Unknown parent node ${parentName} when registering sink ${name}.`);
      }
      return id;
    });

    const keySerde = options.keySerde ?? null;
    const valueSerde = options.valueSerde ?? null;
    const partitioner = options.partitioner ? '[function]' : null;
    const nodeId = uuidv4();

    this._topology.addNode({
      id: nodeId,
      name,
      type: 'sink',
      metadata: {
        topic,
        keySerde: describeSerde(keySerde),
        valueSerde: describeSerde(valueSerde),
        partitioner
      }
    });

    for (const parentId of parentIds) {
      this._topology.connect(parentId, nodeId, { type: 'sink', topic });
    }

    this._registerNode({
      id: nodeId,
      name,
      type: 'sink',
      metadata: {
        topic,
        keySerde: describeSerde(keySerde),
        valueSerde: describeSerde(valueSerde),
        partitioner
      },
      parents: parentIds
    });

    return name;
  }

  addStateStore(storeBuilder, ...processorNames) {
    if (!storeBuilder || typeof storeBuilder.build !== 'function') {
      throw new TypeError('StreamsBuilder#addStateStore expects a StoreBuilder instance.');
    }

    const builderMetadata = storeBuilder.describe?.() ?? null;
    const descriptor = this._registerStateStoreDescriptor({
      name: storeBuilder.name,
      builder: storeBuilder,
      builderMetadata,
      loggingEnabled: builderMetadata?.loggingEnabled ?? storeBuilder.loggingEnabled,
      cachingEnabled: builderMetadata?.cachingEnabled ?? storeBuilder.cachingEnabled,
      metadata: builderMetadata ?? {}
    });

    if (!descriptor.runtime.builder) {
      descriptor.runtime.builder = storeBuilder;
    }

    if (processorNames?.length) {
      const flattened = processorNames.flat().filter(Boolean);
      for (const processorName of flattened) {
        this.connectProcessorAndStateStores(processorName, descriptor.name);
      }
    }

    return descriptor.name;
  }

  connectProcessorAndStateStores(processorName, ...storeNames) {
    const processors = Array.isArray(processorName) ? processorName.flat().filter(Boolean) : [processorName].filter(Boolean);
    const stores = storeNames.flat().filter(Boolean);

    if (!processors.length || !stores.length) {
      return;
    }

    for (const name of processors) {
      const nodeId = this._nodeIdsByName.get(name);
      if (!nodeId) {
        throw new Error(`Unknown processor ${name} when connecting state stores.`);
      }
      for (const storeName of stores) {
        const descriptor = this._stateStores.get(storeName) ?? this._registerStateStoreDescriptor({ name: storeName });
        this._attachStoreToNode({ nodeId, storeName, descriptor });
        const metadata = this._topologyStoreMetadata(descriptor);
        if (metadata) {
          this._topology.attachStateStore(nodeId, metadata);
        }
      }
    }
  }

  _edgeMetadataForStream(stream) {
    if (!stream?.origin) {
      return { type: 'forward' };
    }

    if (stream.origin.type === 'branch') {
      return { type: 'branch', branchIndex: stream.origin.index, branchName: stream.origin.name };
    }

    if (stream.origin.type === 'repartition') {
      return { type: 'repartition', topic: stream.origin.topic };
    }

    if (stream.origin.type === 'join') {
      return { type: 'join', partnerStreamId: stream.origin.partner };
    }

    if (stream.origin.type === 'table' || stream.origin.type === 'globalTable') {
      return { type: stream.origin.type };
    }

    return { type: stream.origin.type };
  }

  stream(topic, options = {}) {
    if (!topic) {
      throw new Error('Topic name must be provided when creating a stream');
    }

    const streamId = uuidv4();
    const nodeName = Named.from(options.named, `source-${topic}`) ?? `source-${topic}`;
    const stream = new KStream({
      id: streamId,
      builder: this,
      sourceTopic: topic,
      fromBeginning: options.fromBeginning ?? false,
      keySerde: options.keySerde,
      valueSerde: options.valueSerde,
      topology: this._topology,
      isSource: true,
      origin: { type: 'source', topic, name: nodeName }
    });

    this._registerStream(stream);
    this._topology.addNode({
      id: stream.id,
      name: nodeName,
      type: 'source',
      metadata: {
        topic,
        fromBeginning: options.fromBeginning ?? false,
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde)
      }
    });
    this._registerNode({
      id: stream.id,
      name: nodeName,
      type: 'source',
      metadata: {
        topics: [topic],
        fromBeginning: options.fromBeginning ?? false,
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde)
      }
    });
    return stream;
  }

  table(topic, options = {}) {
    if (!topic) {
      throw new Error('Topic name must be provided when creating a table');
    }

    const tableId = uuidv4();
    const nodeName = Named.from(options.named, `table-${topic}`) ?? `table-${topic}`;
    const materializedConfig = this._materializedOptions(topic, options);
    const table = new KTable({
      id: tableId,
      builder: this,
      sourceTopic: topic,
      fromBeginning: options.fromBeginning ?? true,
      keySerde: options.keySerde,
      valueSerde: options.valueSerde,
      topology: this._topology,
      materialized: materializedConfig,
      nodeName,
      origin: { type: 'table', topic, name: nodeName }
    });

    this._registerStream(table);
    this._topology.addNode({
      id: table.id,
      name: nodeName,
      type: 'table',
      metadata: {
        topic,
        tableType: 'table',
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde),
        materialized: {
          storeName: table.materialized.storeName,
          changelogTopic: table.materialized.changelogTopic,
          logging: table.materialized.logging,
          caching: table.materialized.caching
        }
      }
    });
    this._registerNode({
      id: table.id,
      name: nodeName,
      type: 'table',
      metadata: {
        topic,
        tableType: 'table',
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde)
      }
    });
    const tableStore = table.stateStores.get(table.materialized.storeName);
    if (tableStore) {
      const builderMetadata = tableStore.builderMetadata ?? tableStore.builder.describe();
      this._registerStateStoreDescriptor({
        name: tableStore.name,
        builder: tableStore.builder,
        builderMetadata,
        keySerde: tableStore.keySerde ?? options.keySerde,
        valueSerde: tableStore.valueSerde ?? options.valueSerde,
        changelogTopic: table.materialized.changelogTopic,
        loggingEnabled: builderMetadata.loggingEnabled,
        cachingEnabled: builderMetadata.cachingEnabled,
        scope: 'table-materialization',
        metadata: tableStore.metadata ?? {}
      });
      this._topology.attachStateStore(table.id, {
        name: tableStore.name,
        type: builderMetadata.type,
        keySerde: describeSerde(tableStore.keySerde ?? options.keySerde),
        valueSerde: describeSerde(tableStore.valueSerde ?? options.valueSerde),
        changelogTopic: table.materialized.changelogTopic,
        loggingEnabled: builderMetadata.loggingEnabled,
        cachingEnabled: builderMetadata.cachingEnabled,
        partitioning: 'by-key',
        scope: 'table-materialization',
        metadata: tableStore.metadata ?? {}
      });
      this._attachStoreToNode({ nodeId: table.id, storeName: tableStore.name });
    }
    return table;
  }

  globalTable(topic, options = {}) {
    if (!topic) {
      throw new Error('Topic name must be provided when creating a global table');
    }

    const tableId = uuidv4();
    const nodeName = Named.from(options.named, `global-table-${topic}`) ?? `global-table-${topic}`;
    const materializedConfig = this._materializedOptions(topic, options);
    const table = new GlobalKTable({
      id: tableId,
      builder: this,
      sourceTopic: topic,
      keySerde: options.keySerde,
      valueSerde: options.valueSerde,
      topology: this._topology,
      materialized: materializedConfig,
      nodeName,
      origin: { type: 'globalTable', topic, name: nodeName }
    });

    this._registerStream(table);
    this._topology.addNode({
      id: table.id,
      name: nodeName,
      type: 'global-table',
      metadata: {
        topic,
        tableType: 'global',
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde),
        materialized: {
          storeName: table.materialized.storeName,
          changelogTopic: table.materialized.changelogTopic,
          logging: table.materialized.logging,
          caching: table.materialized.caching
        }
      }
    });
    this._registerNode({
      id: table.id,
      name: nodeName,
      type: 'global-table',
      metadata: {
        topic,
        tableType: 'global',
        keySerde: describeSerde(options.keySerde),
        valueSerde: describeSerde(options.valueSerde)
      }
    });
    const globalStore = table.stateStores.get(table.materialized.storeName);
    if (globalStore) {
      const builderMetadata = globalStore.builderMetadata ?? globalStore.builder.describe();
      this._registerStateStoreDescriptor({
        name: globalStore.name,
        builder: globalStore.builder,
        builderMetadata,
        keySerde: globalStore.keySerde ?? options.keySerde,
        valueSerde: globalStore.valueSerde ?? options.valueSerde,
        changelogTopic: table.materialized.changelogTopic,
        loggingEnabled: builderMetadata.loggingEnabled,
        cachingEnabled: builderMetadata.cachingEnabled,
        scope: 'global-table-materialization',
        metadata: globalStore.metadata ?? {}
      });
      this._topology.attachStateStore(table.id, {
        name: globalStore.name,
        type: builderMetadata.type,
        keySerde: describeSerde(globalStore.keySerde ?? options.keySerde),
        valueSerde: describeSerde(globalStore.valueSerde ?? options.valueSerde),
        changelogTopic: table.materialized.changelogTopic,
        loggingEnabled: builderMetadata.loggingEnabled,
        cachingEnabled: builderMetadata.cachingEnabled,
        partitioning: 'global',
        scope: 'global-table-materialization',
        metadata: globalStore.metadata ?? {}
      });
      this._attachStoreToNode({ nodeId: table.id, storeName: globalStore.name });
    }
    return table;
  }

  _materializedOptions(topic, options) {
    if (options.materialized) {
      return {
        materialized: options.materialized,
        storeBuilder: options.storeBuilder,
        store: options.store,
        storeName: options.storeName,
        keySerde: options.keySerde,
        valueSerde: options.valueSerde,
        logging: options.logging,
        caching: options.caching,
        changelogConfig: options.changelogConfig,
        changelogTopic: options.changelogTopic ?? topic
      };
    }

    return {
      storeBuilder: options.storeBuilder ?? options.store,
      store: options.store,
      storeName: options.storeName,
      keySerde: options.keySerde,
      valueSerde: options.valueSerde,
      logging: options.logging,
      caching: options.caching,
      changelogConfig: options.changelogConfig,
      changelogTopic: options.changelogTopic ?? topic
    };
  }

  build() {
    return {
      streams: Array.from(this._streams.values()).map(stream => stream.describe()),
      topology: this._topology.describe(),
      processorTopology: this._describeProcessorTopology()
    };
  }
}

module.exports = {
  StreamsBuilder
};
