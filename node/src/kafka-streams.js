'use strict';

const { Kafka } = require('@confluentinc/kafka-javascript');
const EventEmitter = require('events');
const { StreamsBuilder } = require('./streams-builder');
const { TaskManager } = require('./runtime/task-manager');

class KafkaStreams extends EventEmitter {
  constructor(builderOrTopology, config = {}) {
    super();
    if (builderOrTopology instanceof StreamsBuilder) {
      this.topology = builderOrTopology.build();
    } else if (builderOrTopology && builderOrTopology.streams) {
      this.topology = builderOrTopology;
    } else {
      throw new Error('KafkaStreams expects a StreamsBuilder instance or a topology description');
    }

    this.config = config;
    this._consumers = [];
    this._producers = [];
    this._running = false;
    this._kafka = null;
    this._stateStores = new Map();
    this._streamIndex = new Map(this.topology.streams.map(stream => [stream.id, stream]));
    this._taskManager = new TaskManager({ topology: this.topology });
    this._taskManager.registerTopology(this.topology);
  }

  async start() {
    if (this._running) {
      return;
    }

    const clientConfig = this.config.client ?? {};
    this._kafka = this.config.kafka ?? new Kafka(clientConfig);

    const sourceStreams = this.topology.streams.filter(stream => stream.isSource);
    const startPromises = sourceStreams.map(stream => this._startStream(stream));
    await Promise.all(startPromises);
    this._running = true;
    this.emit('started');
  }

  async stop() {
    const stops = [];
    for (const consumer of this._consumers) {
      stops.push(consumer.stop?.().catch(err => this.emit('error', err)));
      stops.push(consumer.disconnect?.().catch(err => this.emit('error', err)));
    }
    for (const producer of this._producers) {
      stops.push(producer.disconnect?.().catch(err => this.emit('error', err)));
    }
    await Promise.all(stops);
    for (const stores of this._stateStores.values()) {
      for (const store of stores.values()) {
        await store.close?.().catch(err => this.emit('error', err));
      }
    }

    this._consumers = [];
    this._producers = [];
    this._kafka = null;
    this._stateStores.clear();
    this._running = false;
    this.emit('stopped');
  }

  async _startStream(stream) {
    const consumerFactory = this.config.consumerFactory;
    const producerFactory = this.config.producerFactory;
    const groupId = this._resolveGroupId(stream);

    const consumer = consumerFactory
      ? await consumerFactory(stream, groupId)
      : this._kafka.consumer({ groupId, allowAutoTopicCreation: false });
    const producer = producerFactory
      ? await producerFactory(stream)
      : this._kafka.producer();

    this._consumers.push(consumer);
    this._producers.push(producer);

    await consumer.connect();
    await producer.connect();

    const stateStores = await this._ensureStateStores(stream);

    await consumer.subscribe({ topic: stream.sourceTopic, fromBeginning: stream.fromBeginning });

    this._taskManager.registerConsumer(stream.id, consumer);

    await consumer.run({
      eachMessage: async payload => {
        try {
          await this._processMessage(stream, payload, producer, stateStores);
          this._taskManager.recordProcessed(stream.id, payload.partition, payload.message?.offset);
        } catch (err) {
          this.emit('error', err);
        }
      }
    });
  }

  _resolveGroupId(stream) {
    if (typeof this.config.groupId === 'function') {
      return this.config.groupId(stream);
    }
    if (typeof this.config.groupId === 'string') {
      return this.config.groupId;
    }
    const prefix = this.config.groupIdPrefix ?? 'kafka-streams-node';
    return `${prefix}-${stream.id}`;
  }

  async _processMessage(stream, payload, producer, stores = this._stateStores.get(stream.id) ?? new Map()) {
    const { message, topic, partition } = payload;
    const keyBuffer = message.key;
    const valueBuffer = message.value;
    const headers = message.headers ?? {};
    const timestamp = message.timestamp ? Number(message.timestamp) : Date.now();

    const record = {
      topic,
      partition,
      headers,
      timestamp,
      key: stream.keySerde ? stream.keySerde.deserialize(keyBuffer) : this._decodeBuffer(keyBuffer),
      value: stream.valueSerde ? stream.valueSerde.deserialize(valueBuffer) : this._decodeBuffer(valueBuffer)
    };

    const results = await this._runOperations(stream, [record], producer, stores, headers, timestamp);
    await this._emitToSinks(stream, results, producer, headers, timestamp);
    return results;
  }

  async _runOperations(stream, inputRecords, producer, stores, headers, timestamp) {
    let records = inputRecords;

    for (const operation of stream.operations) {
      if (!records.length) {
        break;
      }

      switch (operation.type) {
        case 'map':
          records = await Promise.all(records.map(async r => this._normalizeRecord(await operation.fn(r))));
          break;
        case 'mapValues':
          records = await Promise.all(records.map(async r => ({ ...r, value: await operation.fn(r.value, r) })));
          break;
        case 'mapKeys':
          records = await Promise.all(records.map(async r => ({ ...r, key: await operation.fn(r.key, r) })));
          break;
        case 'selectKey':
          records = await Promise.all(records.map(async r => ({ ...r, key: await operation.fn(r.value, r) })));
          break;
        case 'filter':
          records = (await Promise.all(records.map(async r => [r, await operation.fn(r.value, r)])))
            .filter(([, keep]) => Boolean(keep))
            .map(([r]) => r);
          break;
        case 'filterNot':
          records = (await Promise.all(records.map(async r => [r, await operation.fn(r.value, r)])))
            .filter(([, keep]) => !keep)
            .map(([r]) => r);
          break;
        case 'flatMap':
          records = (await Promise.all(records.map(async r => {
            const produced = await operation.fn(r.value, r);
            if (!Array.isArray(produced)) {
              throw new Error('flatMap operation must return an array of records');
            }
            return produced.map(item => this._normalizeRecord(item));
          }))).flat();
          break;
        case 'flatMapValues':
          records = (await Promise.all(records.map(async r => {
            const produced = await operation.fn(r.value, r);
            if (!Array.isArray(produced)) {
              throw new Error('flatMapValues operation must return an array of values');
            }
            return produced.map(value => ({ ...r, value }));
          }))).flat();
          break;
        case 'peek':
        case 'foreach':
          await Promise.all(records.map(async r => operation.fn(r.value, r)));
          break;
        case 'groupBy':
          records = await Promise.all(records.map(async r => ({ ...r, key: await operation.fn(r.value, r) })));
          break;
        case 'aggregate':
          records = await this._applyAggregate(operation, records, stores);
          break;
        case 'through':
          records = await this._applyThrough(operation, records, producer, headers, timestamp);
          break;
        case 'branch':
          await this._applyBranch(stream, operation, records, producer, headers, timestamp);
          records = [];
          break;
        case 'repartition':
          records = await this._applyRepartition(operation, records, producer, headers, timestamp);
          break;
        case 'join':
          throw new Error('Join operations are not yet implemented in the Node.js port');
        default:
          throw new Error(`Unsupported operation type: ${operation.type}`);
      }
    }

    return records;
  }

  async _emitToSinks(stream, records, producer, headers, timestamp) {
    for (const sink of stream.sinks) {
      if (sink.type !== 'topic') {
        continue;
      }

      for (const outRecord of records) {
        const { key, value } = outRecord;
        const keyBufferOut = sink.keySerde ? sink.keySerde.serialize(key) : this._encodeValue(key);
        const valueBufferOut = sink.valueSerde ? sink.valueSerde.serialize(value) : this._encodeValue(value);
        await producer.produce({
          topic: sink.topic,
          message: {
            key: keyBufferOut,
            value: valueBufferOut,
            headers: outRecord.headers ?? headers,
            timestamp: String(outRecord.timestamp ?? timestamp)
          },
          partition: sink.partitioner ? sink.partitioner(outRecord) : undefined
        });
      }
    }
  }

  _normalizeRecord(record) {
    if (!record || typeof record !== 'object') {
      throw new Error('map and flatMap operations must return an object with key/value pairs');
    }
    if (!('value' in record)) {
      throw new Error('Record must have a value property');
    }
    return {
      topic: record.topic,
      partition: record.partition,
      headers: record.headers ?? {},
      timestamp: record.timestamp ?? Date.now(),
      key: 'key' in record ? record.key : undefined,
      value: record.value
    };
  }

  _decodeBuffer(buffer) {
    if (buffer == null) {
      return null;
    }
    if (Buffer.isBuffer(buffer)) {
      return buffer;
    }
    return Buffer.from(buffer);
  }

  _encodeValue(value) {
    if (value == null) {
      return undefined;
    }
    if (Buffer.isBuffer(value)) {
      return value;
    }
    if (typeof value === 'string') {
      return Buffer.from(value);
    }
    return Buffer.from(JSON.stringify(value));
  }

  async _applyAggregate(operation, records, stores) {
    if (!operation.options?.storeName) {
      throw new Error('Aggregate operation missing state store');
    }
    const store = stores.get(operation.options.storeName);
    if (!store) {
      throw new Error(`State store ${operation.options.storeName} was not initialised`);
    }

    const results = [];

    for (const record of records) {
      const key = record.key;
      if (key == null) {
        continue;
      }

      let current = await store.get(key);
      if (current === undefined) {
        current = await operation.options.initializer(key, record);
      }

      const updated = await operation.fn(current, record.value, record);

      if (updated === null || updated === undefined) {
        await store.delete(key);
        if (operation.options.emitOnUpdate) {
          results.push({ ...record, value: null });
        }
      } else {
        await store.put(key, updated);
        if (operation.options.emitOnUpdate) {
          results.push({ ...record, value: updated });
        }
      }
    }

    return results;
  }

  async _applyThrough(operation, records, producer, headers, timestamp) {
    const { topic, keySerde, valueSerde, partitioner } = operation.fn;

    for (const outRecord of records) {
      const keyBuffer = keySerde ? keySerde.serialize(outRecord.key) : this._encodeValue(outRecord.key);
      const valueBuffer = valueSerde ? valueSerde.serialize(outRecord.value) : this._encodeValue(outRecord.value);
      await producer.produce({
        topic,
        message: {
          key: keyBuffer,
          value: valueBuffer,
          headers: outRecord.headers ?? headers,
          timestamp: String(outRecord.timestamp ?? timestamp)
        },
        partition: partitioner ? partitioner(outRecord) : undefined
      });
    }

    return records.map(record => ({ ...record, topic }));
  }

  async _applyBranch(stream, operation, records, producer, headers, timestamp) {
    for (const record of records) {
      for (const branch of operation.branches ?? []) {
        const predicateResult = await branch.predicate(record.value, record);
        if (!predicateResult) {
          continue;
        }
        const branchStream = this._streamIndex.get(branch.streamId);
        if (!branchStream) {
          continue;
        }
        const stores = await this._ensureStateStores(branchStream);
        const branchRecords = await this._runOperations(branchStream, [{ ...record }], producer, stores, headers, timestamp);
        await this._emitToSinks(branchStream, branchRecords, producer, headers, timestamp);
      }
    }
  }

  async _applyRepartition(operation, records, producer, headers, timestamp) {
    const { topic, keySerde, valueSerde, partitioner } = operation.fn;
    const repartitioned = [];

    for (const outRecord of records) {
      const keyBuffer = keySerde ? keySerde.serialize(outRecord.key) : this._encodeValue(outRecord.key);
      const valueBuffer = valueSerde ? valueSerde.serialize(outRecord.value) : this._encodeValue(outRecord.value);
      await producer.produce({
        topic,
        message: {
          key: keyBuffer,
          value: valueBuffer,
          headers: outRecord.headers ?? headers,
          timestamp: String(outRecord.timestamp ?? timestamp)
        },
        partition: partitioner ? partitioner(outRecord) : undefined
      });
      repartitioned.push({ ...outRecord, topic });
    }

    const targetStream = operation.targetStreamId ? this._streamIndex.get(operation.targetStreamId) : null;
    if (targetStream) {
      const stores = await this._ensureStateStores(targetStream);
      const results = await this._runOperations(targetStream, repartitioned, producer, stores, headers, timestamp);
      await this._emitToSinks(targetStream, results, producer, headers, timestamp);
      return results;
    }

    return repartitioned;
  }

  async _ensureStateStores(stream) {
    if (this._stateStores.has(stream.id)) {
      return this._stateStores.get(stream.id);
    }

    const storeInstances = new Map();
    if (Array.isArray(stream.stateStores)) {
      for (const definition of stream.stateStores) {
        if (!definition.builder) {
          continue;
        }
        const instance = await definition.builder.build({ stream });
        storeInstances.set(definition.name, instance);
      }
    }
    this._stateStores.set(stream.id, storeInstances);
    return storeInstances;
  }
}

module.exports = {
  KafkaStreams
};
