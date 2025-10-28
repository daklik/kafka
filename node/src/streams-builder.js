'use strict';

const { v4: uuidv4 } = require('uuid');
const { KStream } = require('./kstream');
const { KTable, GlobalKTable } = require('./ktable');
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
        materialized: {
          storeName: table.materialized.storeName,
          changelogTopic: table.materialized.changelogTopic,
          logging: table.materialized.logging,
          caching: table.materialized.caching
        }
      }
    });
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
        materialized: {
          storeName: table.materialized.storeName,
          changelogTopic: table.materialized.changelogTopic,
          logging: table.materialized.logging,
          caching: table.materialized.caching
        }
      }
    });
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
      topology: this._topology.describe()
    };
  }
}

module.exports = {
  StreamsBuilder
};
