'use strict';

class TopologyDescription {
  constructor() {
    this._nodes = new Map();
    this._edges = [];
  }

  addNode({ id, name, type, metadata = {} }) {
    this._nodes.set(id, { id, name, type, metadata });
  }

  connect(from, to) {
    this._edges.push({ from, to });
  }

  describe() {
    return {
      nodes: Array.from(this._nodes.values()),
      edges: this._edges.slice()
    };
  }
}

module.exports = {
  TopologyDescription
};
