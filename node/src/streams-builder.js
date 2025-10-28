'use strict';

const { v4: uuidv4 } = require('uuid');
const { KStream } = require('./kstream');
const { TopologyDescription } = require('./topology/topology-description');
const { Named } = require('./named');

class StreamsBuilder {
  constructor() {
    this._streams = new Map();
    this._topology = new TopologyDescription();
  }

  _registerStream(stream) {
    this._streams.set(stream.id, stream);
  }

  _registerDerivedStream(stream, { parentOperationId } = {}) {
    if (!this._streams.has(stream.id)) {
      this._streams.set(stream.id, stream);
      this._topology.addNode({
        id: stream.id,
        name: stream.nodeName,
        type: 'processor',
        metadata: { origin: stream.origin }
      });
      if (parentOperationId) {
        this._topology.connect(parentOperationId, stream.id);
      } else if (stream.parent) {
        this._topology.connect(stream.parent.id, stream.id);
      }
    }
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
      metadata: { topic, fromBeginning: options.fromBeginning ?? false }
    });
    return stream;
  }

  build() {
    return {
      streams: Array.from(this._streams.values()).map(stream => stream.describe()),
      topology: this._topology.describe()
    };
  }
}

module.exports = {
  StreamsBuilder
};
