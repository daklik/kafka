'use strict';

const { v4: uuidv4 } = require('uuid');
const { MemoryStateStore } = require('./state/memory-store');

class StreamsBuilder {
  constructor() {
    this._streams = new Map();
  }

  stream(topic, options = {}) {
    if (!topic) {
      throw new Error('Topic name must be provided when creating a stream');
    }

    const stream = new KStream({
      id: uuidv4(),
      builder: this,
      sourceTopic: topic,
      fromBeginning: options.fromBeginning ?? false,
      keySerde: options.keySerde,
      valueSerde: options.valueSerde
    });

    this._streams.set(stream.id, stream);
    return stream;
  }

  build() {
    return {
      streams: Array.from(this._streams.values()).map(stream => stream.describe())
    };
  }
}

class KStream {
  constructor({ id, builder, sourceTopic, fromBeginning, keySerde, valueSerde, operations = [], sinks = [], stateStores = new Map() }) {
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
  }

  _appendOperation(type, fn, options = {}) {
    this.operations.push({ type, fn, options });
    return this;
  }

  _registerStateStore({ name, supplier, changelogTopic, keySerde, valueSerde }) {
    if (!name) {
      throw new Error('State store definition must include a name');
    }
    const definition = {
      name,
      supplier: supplier ?? (() => new MemoryStateStore()),
      changelogTopic,
      keySerde,
      valueSerde
    };
    this.stateStores.set(name, definition);
    return definition;
  }

  map(mapper) {
    return this._appendOperation('map', mapper);
  }

  mapValues(mapper) {
    return this._appendOperation('mapValues', mapper);
  }

  mapKeys(mapper) {
    return this._appendOperation('mapKeys', mapper);
  }

  selectKey(selector) {
    return this._appendOperation('selectKey', selector);
  }

  filter(predicate) {
    return this._appendOperation('filter', predicate);
  }

  filterNot(predicate) {
    return this._appendOperation('filterNot', predicate);
  }

  flatMap(mapper) {
    return this._appendOperation('flatMap', mapper);
  }

  flatMapValues(mapper) {
    return this._appendOperation('flatMapValues', mapper);
  }

  peek(sideEffect) {
    return this._appendOperation('peek', sideEffect);
  }

  foreach(sideEffect) {
    return this._appendOperation('foreach', sideEffect);
  }

  groupByKey() {
    return this._appendOperation('groupBy', (value, record) => record.key ?? value?.key);
  }

  groupBy(selector) {
    if (typeof selector !== 'function') {
      throw new Error('groupBy expects a selector function');
    }
    return this._appendOperation('groupBy', selector);
  }

  aggregate(initializer, aggregator, options = {}) {
    if (typeof initializer !== 'function') {
      throw new Error('aggregate expects an initializer function');
    }
    if (typeof aggregator !== 'function') {
      throw new Error('aggregate expects an aggregator function');
    }

    const storeName = options.storeName ?? `agg-${uuidv4()}`;
    const storeSupplier = options.store ?? (() => new MemoryStateStore());
    this._registerStateStore({
      name: storeName,
      supplier: typeof storeSupplier === 'function' ? storeSupplier : () => storeSupplier,
      changelogTopic: options.changelogTopic,
      keySerde: options.keySerde ?? this.keySerde,
      valueSerde: options.valueSerde ?? this.valueSerde
    });

    return this._appendOperation('aggregate', aggregator, {
      storeName,
      initializer,
      emitOnUpdate: options.emitOnUpdate ?? true
    });
  }

  count(options = {}) {
    return this.aggregate(
      () => 0,
      (aggregate) => (aggregate ?? 0) + 1,
      { ...options, valueSerde: options.valueSerde ?? null }
    );
  }

  reduce(reducer, options = {}) {
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
      options
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
    });
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
      partitioner: options.partitioner
    });
    return this;
  }

  describe() {
    return {
      id: this.id,
      sourceTopic: this.sourceTopic,
      fromBeginning: this.fromBeginning,
      keySerde: this.keySerde,
      valueSerde: this.valueSerde,
      operations: this.operations,
      sinks: this.sinks,
      stateStores: Array.from(this.stateStores.values())
    };
  }
}

module.exports = {
  StreamsBuilder,
  KStream
};
