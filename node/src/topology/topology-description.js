'use strict';

const { sanitizeValue } = require('./utils');

class TopologyDescription {
  constructor() {
    this._nodes = new Map();
    this._edges = new Map();
  }

  addNode({ id, name, type, metadata = {} }) {
    if (!id) {
      throw new Error('Topology nodes require an id');
    }

    const existing = this._nodes.get(id);
    const sanitizedMetadata = sanitizeValue(metadata);

    if (existing) {
      existing.name = name ?? existing.name;
      existing.type = type ?? existing.type;
      existing.metadata = { ...existing.metadata, ...sanitizedMetadata };
      return existing;
    }

    const node = {
      id,
      name: name ?? id,
      type: type ?? 'processor',
      metadata: sanitizedMetadata,
      predecessors: new Set(),
      successors: new Set(),
      stores: new Map()
    };

    this._nodes.set(id, node);
    return node;
  }

  annotateNode(id, metadata = {}) {
    const node = this._nodes.get(id);
    if (!node) {
      return;
    }

    const sanitizedMetadata = sanitizeValue(metadata);
    node.metadata = { ...node.metadata, ...sanitizedMetadata };
  }

  attachStateStore(nodeId, storeMetadata = {}) {
    const node = this._nodes.get(nodeId);
    if (!node || !storeMetadata?.name) {
      return;
    }

    const descriptor = sanitizeValue({
      name: storeMetadata.name,
      type: storeMetadata.type ?? null,
      keySerde: storeMetadata.keySerde ?? null,
      valueSerde: storeMetadata.valueSerde ?? null,
      partitioning: storeMetadata.partitioning ?? null,
      changelogTopic: storeMetadata.changelogTopic ?? null,
      loggingEnabled: storeMetadata.loggingEnabled ?? null,
      cachingEnabled: storeMetadata.cachingEnabled ?? null,
      retentionMs: storeMetadata.retentionMs ?? null,
      windowSizeMs: storeMetadata.windowSizeMs ?? null,
      windowType: storeMetadata.windowType ?? null,
      scope: storeMetadata.scope ?? null,
      metadata: storeMetadata.metadata ?? null
    });

    node.stores.set(descriptor.name, descriptor);
  }

  connect(from, to, metadata = {}) {
    if (!from || !to) {
      return;
    }

    const sanitizedMetadata = sanitizeValue(metadata);
    const edgeKey = `${from}->${to}`;
    const edge = {
      from,
      to,
      metadata: {
        ...(this._edges.get(edgeKey)?.metadata ?? {}),
        ...sanitizedMetadata
      }
    };

    this._edges.set(edgeKey, edge);

    const fromNode = this._nodes.get(from);
    const toNode = this._nodes.get(to);

    if (fromNode) {
      fromNode.successors.add(to);
    }
    if (toNode) {
      toNode.predecessors.add(from);
    }
  }

  describe() {
    return {
      nodes: Array.from(this._nodes.values()).map(node => ({
        id: node.id,
        name: node.name,
        type: node.type,
        metadata: sanitizeValue(node.metadata),
        predecessors: Array.from(node.predecessors),
        successors: Array.from(node.successors),
        stores: Array.from(node.stores.values()).map(store => sanitizeValue(store))
      })),
      edges: Array.from(this._edges.values()).map(edge => ({
        from: edge.from,
        to: edge.to,
        metadata: sanitizeValue(edge.metadata)
      }))
    };
  }
}

module.exports = {
  TopologyDescription
};
