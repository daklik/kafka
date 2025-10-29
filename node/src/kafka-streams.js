'use strict';

const { Kafka } = require('@confluentinc/kafka-javascript');
const EventEmitter = require('events');
const { StreamsBuilder } = require('./streams-builder');
const { TaskManager } = require('./runtime/task-manager');
const { StateStoreManager } = require('./runtime/state-store-manager');
const { StreamThread } = require('./runtime/stream-thread');
const { TransactionManager } = require('./runtime/transaction-manager');
const { StreamsConfig } = require('./config/streams-config');
const { HandlerAction } = require('./errors');
const { QueryMetadataManager, InteractiveQueryService } = require('./query');
const { ProcessorContext, RecordContext, callLifecycle } = require('./processor');

const SKIP_RECORD = Symbol.for('kafka-streams-skip-record');

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

    this.config = config instanceof StreamsConfig ? config : new StreamsConfig(config);
    this._consumers = [];
    this._producers = [];
    this._running = false;
    this._kafka = null;
    this._stateStores = new Map();
    this._streamIndex = new Map(this.topology.streams.map(stream => [stream.id, stream]));
    this._taskManager = new TaskManager({ topology: this.topology });
    this._taskManager.registerTopology(this.topology);
    this._transactionManager = new TransactionManager({
      guarantee: this.config.processingGuarantee,
      logger: {
        error: (message, context) => this.emit('transaction.error', { message, context })
      }
    });
    this._streamThreads = new Map();
    this._metrics = this.config.getMetricsRegistry();
    this._stateStoreManager = new StateStoreManager({ metrics: this._metrics, config: this.config });
    this._stateStoreManager.on('restore:start', event => this.emit('state.restore.start', event));
    this._stateStoreManager.on('restore:batch', event => this.emit('state.restore.batch', event));
    this._stateStoreManager.on('restore:end', event => this.emit('state.restore.end', event));
    this._taskManager.on('task.transition', event => this.emit('task.transition', event));
    this._taskManager.on('task.lag', event => this.emit('task.lag', event));
    this._errorHandlers = this.config.getErrorHandlers();
    const interactiveConfig = this.config.getInteractiveQueryConfig?.() ?? {};
    this._queryMetadata = new QueryMetadataManager({
      applicationId: this.config.applicationId,
      hostInfo: this.config.getApplicationServer?.(),
      rpcClient: interactiveConfig.rpcClient
    });
    if (interactiveConfig.rpcClient) {
      this._queryMetadata.setRpcClient(interactiveConfig.rpcClient);
    }
    this._interactiveQueryService = new InteractiveQueryService({
      metadataManager: this._queryMetadata
    });
    this._processorInstances = new Map();
  }

  async start() {
    if (this._running) {
      return;
    }

    const clientConfig = this.config.getClientConfig();
    this._kafka = this.config.getKafkaClient() ?? new Kafka(clientConfig);

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
    const processorCloses = [];
    for (const perStream of this._processorInstances.values()) {
      for (const runtime of perStream.values()) {
        for (const instance of runtime.instances.values()) {
          processorCloses.push(callLifecycle(instance.processor, 'close').catch(error => this.emit('error', error)));
        }
      }
    }
    await Promise.all(processorCloses);
    await Promise.all(stops);
    for (const [streamId, stores] of this._stateStores.entries()) {
      for (const [name, store] of stores.entries()) {
        await store.close?.().catch(err => this.emit('error', err));
        this._queryMetadata?.deregisterStore(name);
      }
      this._stateStoreManager?.reset(streamId);
    }

    this._consumers = [];
    this._producers = [];
    this._kafka = null;
    this._stateStores.clear();
    this._processorInstances.clear();
    for (const thread of this._streamThreads.values()) {
      thread.removeAllListeners();
    }
    this._streamThreads.clear();
    this._running = false;
    this.emit('stopped');
  }

  async _startStream(stream) {
    const groupId = this._resolveGroupId(stream);

    const consumer = await this.config.createConsumer({ stream, kafka: this._kafka, groupId });
    const producer = await this.config.createProducer({ stream, kafka: this._kafka });

    this._consumers.push(consumer);
    this._producers.push(producer);

    await consumer.connect();
    await producer.connect();

    const stateStores = await this._ensureStateStores(stream);

    await consumer.subscribe({ topic: stream.sourceTopic, fromBeginning: stream.fromBeginning });

    this._taskManager.registerConsumer(stream.id, consumer);

    const streamThread = new StreamThread({
      stream,
      taskManager: this._taskManager,
      stateStoreManager: this._stateStoreManager,
      metrics: this._metrics,
      config: this.config,
      transactionManager: this._transactionManager
    });
    streamThread.bindConsumer(consumer);
    streamThread.bindProducer(producer);
    streamThread.on('error', error => this.emit('error', error));
    streamThread.on('commit', event => this.emit('commit', event));
    this._streamThreads.set(stream.id, streamThread);

    const handleMessage = streamThread.wrapHandler(async payload => {
      await this._processMessage(stream, payload, producer, stateStores);
      const messageTimestamp = payload.message?.timestamp != null
        ? Number(payload.message.timestamp)
        : Date.now();
      await this._taskManager.recordProcessed(
        stream.id,
        payload.partition,
        payload.message?.offset,
        messageTimestamp
      );
    });

    await consumer.run({
      eachMessage: async payload => {
        try {
          await handleMessage(payload);
        } catch (err) {
          this._metrics.record('stream.records.failed', 1, { streamId: stream.id });
          this.emit('error', err);
        }
      }
    });
  }

  _resolveGroupId(stream) {
    return this.config.resolveGroupId(stream);
  }

  async _processMessage(stream, payload, producer, stores = this._stateStores.get(stream.id) ?? new Map()) {
    const { message, topic, partition } = payload;
    const keyBuffer = message.key;
    const valueBuffer = message.value;
    const headers = message.headers ?? {};
    const timestamp = message.timestamp ? Number(message.timestamp) : Date.now();
    const offset = message.offset != null ? Number(message.offset) : null;

    this._metrics.record('stream.records.consumed', 1, { streamId: stream.id, topic });

    const key = await this._safeDeserialize({
      stream,
      payload,
      component: 'key',
      buffer: keyBuffer,
      serde: stream.keySerde
    });
    if (key === SKIP_RECORD) {
      return [];
    }

    const value = await this._safeDeserialize({
      stream,
      payload,
      component: 'value',
      buffer: valueBuffer,
      serde: stream.valueSerde
    });
    if (value === SKIP_RECORD) {
      return [];
    }

    const recordContext = new RecordContext({ topic, partition, offset, timestamp, headers });
    const record = {
      topic,
      partition,
      headers,
      timestamp,
      offset,
      key,
      value,
      recordContext
    };

    await this._bufferStreamJoins(stream, record, stores);

    const results = await this._runOperations(stream, [record], producer, stores, headers, timestamp);

    if (stream.isTable) {
      const updates = results.length ? results : [record];
      await this._updateTableState(stream, updates, stores);
      this._metrics.record('table.records.updated', updates.length, { streamId: stream.id, topic });
      return [];
    }

    if (results.length) {
      this._metrics.record('stream.records.processed', results.length, { streamId: stream.id });
    }
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
          records = await Promise.all(records.map(async r => this._normalizeRecord(await operation.fn(r), r)));
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
            return produced.map(item => this._normalizeRecord(item, r));
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
        case 'suppress':
          records = await this._applySuppress(stream, operation, records);
          break;
        case 'through':
          records = await this._applyThrough(stream, operation, records, producer, headers, timestamp);
          break;
        case 'branch':
          await this._applyBranch(stream, operation, records, producer, headers, timestamp);
          records = [];
          break;
        case 'repartition':
          records = await this._applyRepartition(stream, operation, records, producer, headers, timestamp);
          break;
        case 'join':
          records = await this._applyJoin(stream, operation, records, producer, headers, timestamp);
          break;
        case 'processor':
          records = await this._applyProcessorOperation(stream, operation, records, producer, stores);
          break;
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
        try {
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
          this._metrics.record('stream.records.produced', 1, { streamId: stream.id, topic: sink.topic });
        } catch (error) {
          const shouldContinue = await this._handleProductionError({
            error,
            stream,
            record: outRecord,
            sink,
            stage: 'sink-produce'
          });
          if (!shouldContinue) {
            throw error;
          }
        }
      }
    }
  }

  _normalizeRecord(record, sourceRecord = null) {
    if (!record || typeof record !== 'object') {
      throw new Error('map and flatMap operations must return an object with key/value pairs');
    }
    if (!('value' in record)) {
      throw new Error('Record must have a value property');
    }
    const sourceContext = sourceRecord?.recordContext instanceof RecordContext ? sourceRecord.recordContext : null;
    const providedContext = record.recordContext instanceof RecordContext ? record.recordContext : null;

    const topic = record.topic ?? providedContext?.topic() ?? sourceContext?.topic() ?? sourceRecord?.topic ?? null;
    const partition = record.partition ?? providedContext?.partition() ?? sourceContext?.partition() ?? sourceRecord?.partition ?? null;
    const timestamp = record.timestamp != null
      ? Number(record.timestamp)
      : providedContext?.timestamp() ?? sourceContext?.timestamp() ?? sourceRecord?.timestamp ?? Date.now();
    const offset = record.offset != null
      ? Number(record.offset)
      : providedContext?.offset() ?? sourceContext?.offset() ?? sourceRecord?.offset ?? null;

    const resolvedHeaders = (() => {
      if (record.headers) {
        return { ...record.headers };
      }
      if (providedContext) {
        return { ...providedContext.headers() };
      }
      if (sourceContext) {
        return { ...sourceContext.headers() };
      }
      if (sourceRecord?.headers) {
        return { ...sourceRecord.headers };
      }
      return {};
    })();

    const recordContext = providedContext ?? (sourceContext
      ? sourceContext.withUpdates({
        topic,
        partition,
        timestamp,
        offset,
        headers: resolvedHeaders
      })
      : new RecordContext({
        topic,
        partition,
        timestamp,
        offset,
        headers: resolvedHeaders
      }));

    return {
      topic,
      partition,
      headers: resolvedHeaders,
      timestamp,
      offset,
      key: 'key' in record ? record.key : undefined,
      value: record.value,
      recordContext
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

    if (operation.options.windowType) {
      return this._applyWindowAggregate(operation, records, store);
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

  async _applyWindowAggregate(operation, records, store) {
    const windowType = operation.options.windowType;
    const windowInstance = operation.windowInstance;
    if (!windowInstance) {
      throw new Error('Windowed aggregation missing window definition');
    }

    const retentionMs = operation.options.windowRetentionMs ?? (typeof windowInstance.retentionPeriod === 'function' ? windowInstance.retentionPeriod() : null);
    const results = [];

    for (const record of records) {
      const timestamp = record.timestamp ?? Date.now();
      const eventTimestamp = record.eventTimestamp ?? timestamp;
      if (retentionMs && Number.isFinite(retentionMs) && typeof store.purge === 'function') {
        await store.purge(timestamp - retentionMs);
      }

      const key = record.key;
      if (key == null) {
        continue;
      }

      if (windowType === 'session') {
        const emitted = await this._applySessionWindowAggregate({ operation, record, store, timestamp, key });
        if (emitted) {
          results.push(emitted);
        }
        continue;
      }

      const windows = this._resolveRecordWindows(windowType, windowInstance, timestamp);
      for (const window of windows) {
        const entryStart = Number.isFinite(window.start) ? window.start : Number.MIN_SAFE_INTEGER;
        const contextRecord = { ...record, window };
        const existing = typeof store.get === 'function' ? await store.get(key, entryStart) : null;
        let current = existing?.value;
        if (current === undefined) {
          current = await operation.options.initializer(key, contextRecord);
        }
        const updated = await operation.fn(current, record.value, contextRecord);
        const outputKey = { key, window };

        if (updated === null || updated === undefined) {
          if (typeof store.delete === 'function') {
            await store.delete(key, entryStart);
          }
          if (operation.options.emitOnUpdate) {
            results.push({ ...record, key: outputKey, value: null, timestamp: window.end ?? timestamp, eventTimestamp });
          }
        } else {
          if (typeof store.put === 'function') {
            await store.put(key, updated, entryStart, { end: window.end });
          }
          if (operation.options.emitOnUpdate) {
            results.push({ ...record, key: outputKey, value: updated, timestamp: window.end ?? timestamp, eventTimestamp });
          }
        }
      }
    }

    return results;
  }

  async _applySuppress(stream, operation, records) {
    const strategy = operation.options?.strategy ?? 'untilWindowCloses';
    const windowType = operation.options?.windowType;
    if (!records.length || strategy !== 'untilWindowCloses' || !windowType) {
      return records;
    }

    if (!operation._buffer) {
      operation._buffer = new Map();
    }
    if (typeof operation._streamTime !== 'number') {
      operation._streamTime = Number.NEGATIVE_INFINITY;
    }

    const buffer = operation._buffer;
    const bufferConfig = this._normalizeSuppressionBufferConfig(operation.options?.bufferConfig);
    let streamTime = operation._streamTime;
    const graceMs = Number.isFinite(operation.options?.graceMs) ? operation.options.graceMs : (operation.options?.graceMs ?? 0);

    const emitted = [];
    let suppressedCount = 0;
    let lateCount = 0;
    let overflowCount = 0;

    for (const record of records) {
      const eventTimestamp = record.eventTimestamp ?? record.timestamp ?? Date.now();
      const previousStreamTime = streamTime;
      streamTime = Math.max(streamTime, eventTimestamp);

      const windowedKey = record.key;
      const window = windowedKey?.window;
      if (!window) {
        emitted.push(record);
        continue;
      }

      const closingTime = this._resolveSuppressionClosingTime(window, graceMs);
      if (previousStreamTime >= closingTime) {
        lateCount += 1;
        continue;
      }

      const bufferKey = this._formatSuppressionKey(windowedKey);
      buffer.set(bufferKey, { record, closingTime, eventTimestamp });
      suppressedCount += 1;

      if (buffer.size > bufferConfig.maxRecords) {
        if (bufferConfig.emitEarlyWhenFull) {
          const earliestKey = this._findEarliestSuppressed(buffer);
          if (earliestKey) {
            const earliest = buffer.get(earliestKey);
            emitted.push(earliest.record);
            buffer.delete(earliestKey);
          }
        } else {
          buffer.delete(bufferKey);
          overflowCount += 1;
        }
      }
    }

    const ready = [];
    for (const [bufferKey, entry] of buffer.entries()) {
      if (streamTime >= entry.closingTime) {
        ready.push({ bufferKey, ...entry });
      }
    }

    ready.sort((a, b) => {
      if (a.closingTime === b.closingTime) {
        return (a.eventTimestamp ?? 0) - (b.eventTimestamp ?? 0);
      }
      return a.closingTime - b.closingTime;
    });

    for (const entry of ready) {
      emitted.push(entry.record);
      buffer.delete(entry.bufferKey);
    }

    operation._streamTime = streamTime;

    if (suppressedCount) {
      this._metrics.record('stream.records.suppressed', suppressedCount, { streamId: stream.id, operationId: operation.id });
    }
    if (emitted.length) {
      this._metrics.record('stream.records.suppressed.flushed', emitted.length, { streamId: stream.id, operationId: operation.id });
    }
    if (lateCount) {
      this._metrics.record('stream.records.late', lateCount, { streamId: stream.id, operationId: operation.id });
    }
    if (overflowCount) {
      this._metrics.record('stream.suppression.buffer.overflow', overflowCount, { streamId: stream.id, operationId: operation.id });
    }

    return emitted;
  }

  _normalizeSuppressionBufferConfig(config = {}) {
    if (!config) {
      return { maxRecords: Infinity, emitEarlyWhenFull: false };
    }

    const maxRecordsRaw = config.maxRecords ?? Infinity;
    const maxRecords = maxRecordsRaw === Infinity ? Infinity : Math.max(1, Math.floor(Number(maxRecordsRaw)) || 1);

    return {
      maxRecords,
      emitEarlyWhenFull: Boolean(config.emitEarlyWhenFull)
    };
  }

  _resolveSuppressionClosingTime(window, graceMs = 0) {
    if (!window) {
      return Number.POSITIVE_INFINITY;
    }
    const end = Number.isFinite(window.end) ? window.end : (window.end ?? Number.POSITIVE_INFINITY);
    if (!Number.isFinite(end)) {
      return Number.POSITIVE_INFINITY;
    }
    const grace = Number.isFinite(graceMs) ? graceMs : 0;
    return end + grace;
  }

  _formatSuppressionKey(windowedKey = {}) {
    const keyPart = this._stringifySuppressionKey(windowedKey.key);
    const window = windowedKey.window ?? {};
    const start = Number.isFinite(window.start) ? window.start : Number.MIN_SAFE_INTEGER;
    const end = Number.isFinite(window.end) ? window.end : Number.MAX_SAFE_INTEGER;
    return `${keyPart}:${start}:${end}`;
  }

  _stringifySuppressionKey(key) {
    if (key === null) {
      return 'null';
    }
    if (key === undefined) {
      return 'undefined';
    }
    if (typeof key === 'string' || typeof key === 'number' || typeof key === 'boolean') {
      return String(key);
    }
    if (Buffer.isBuffer(key)) {
      return key.toString('base64');
    }
    try {
      return JSON.stringify(key);
    } catch (err) {
      return String(key);
    }
  }

  _findEarliestSuppressed(buffer) {
    let earliestKey = null;
    let earliestClosingTime = Number.POSITIVE_INFINITY;
    for (const [key, entry] of buffer.entries()) {
      if (entry.closingTime < earliestClosingTime) {
        earliestClosingTime = entry.closingTime;
        earliestKey = key;
      }
    }
    return earliestKey;
  }

  _resolveRecordWindows(windowType, windowInstance, timestamp) {
    if (windowType === 'time') {
      return windowInstance.windowsFor(timestamp);
    }
    if (windowType === 'sliding') {
      const bounds = windowInstance.bounds(timestamp);
      return [{ start: bounds.start, end: bounds.end }];
    }
    if (windowType === 'unlimited') {
      return windowInstance.windowsFor(timestamp).map(window => ({
        start: Number.isFinite(window.start) ? window.start : Number.MIN_SAFE_INTEGER,
        end: window.end
      }));
    }
    throw new Error(`Unsupported window type ${windowType}`);
  }

  async _applySessionWindowAggregate({ operation, record, store, timestamp, key }) {
    const gapMs = operation.options.sessionGap ?? 0;
    const sessionMerger = operation.options.sessionMerger;
    if (typeof sessionMerger !== 'function') {
      throw new Error('Session window aggregations require a sessionMerger function');
    }

    const contextRecord = { ...record, window: { start: timestamp, end: timestamp } };
    let aggregate = await operation.options.initializer(key, contextRecord);
    aggregate = await operation.fn(aggregate, record.value, contextRecord);

    const overlapStart = timestamp - gapMs;
    const overlapEnd = timestamp + gapMs;
    const overlapping = typeof store.fetch === 'function' ? await store.fetch(key, overlapStart, overlapEnd) : [];

    let sessionStart = timestamp;
    let sessionEnd = timestamp;

    for (const session of overlapping) {
      const start = session.timestamp ?? session.start ?? session.window?.start ?? timestamp;
      const end = session.end ?? session.window?.end ?? session.timestamp ?? timestamp;
      sessionStart = Math.min(sessionStart, start);
      sessionEnd = Math.max(sessionEnd, end);
      aggregate = await sessionMerger(session.value, aggregate);
      if (typeof store.delete === 'function') {
        await store.delete(key, start);
      }
    }

    if (typeof store.put === 'function') {
      await store.put(key, aggregate, sessionStart, { end: sessionEnd });
    }

    if (!operation.options.emitOnUpdate) {
      return null;
    }

    return {
      ...record,
      key: { key, window: { start: sessionStart, end: sessionEnd } },
      value: aggregate,
      timestamp: sessionEnd,
      eventTimestamp: record.eventTimestamp ?? record.timestamp ?? timestamp
    };
  }

  async _applyThrough(stream, operation, records, producer, headers, timestamp) {
    const { topic, keySerde, valueSerde, partitioner } = operation.fn;

    for (const outRecord of records) {
      const keyBuffer = keySerde ? keySerde.serialize(outRecord.key) : this._encodeValue(outRecord.key);
      const valueBuffer = valueSerde ? valueSerde.serialize(outRecord.value) : this._encodeValue(outRecord.value);
      try {
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
        this._metrics.record('stream.records.produced', 1, { streamId: stream.id, topic });
      } catch (error) {
        const shouldContinue = await this._handleProductionError({
          error,
          stream,
          record: outRecord,
          stage: 'through-produce'
        });
        if (!shouldContinue) {
          throw error;
        }
      }
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

  async _applyRepartition(stream, operation, records, producer, headers, timestamp) {
    const { topic, keySerde, valueSerde, partitioner } = operation.fn;
    const repartitioned = [];

    for (const outRecord of records) {
      const keyBuffer = keySerde ? keySerde.serialize(outRecord.key) : this._encodeValue(outRecord.key);
      const valueBuffer = valueSerde ? valueSerde.serialize(outRecord.value) : this._encodeValue(outRecord.value);
      try {
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
        this._metrics.record('stream.records.produced', 1, { streamId: stream.id, topic });
        repartitioned.push({ ...outRecord, topic });
      } catch (error) {
        const shouldContinue = await this._handleProductionError({
          error,
          stream,
          record: outRecord,
          stage: 'repartition-produce'
        });
        if (!shouldContinue) {
          throw error;
        }
      }
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

  async _applyJoin(stream, operation, records) {
    const options = operation.options ?? {};
    const targetStreamId = options.otherStreamId;
    if (!targetStreamId) {
      throw new Error('Join operation missing target stream id');
    }

    const otherStream = this._streamIndex.get(targetStreamId);
    if (!otherStream) {
      throw new Error(`Join target stream ${targetStreamId} was not registered in the topology`);
    }

    if (options.otherStreamType === 'table' || options.otherStreamType === 'global-table' || otherStream.isTable) {
      return this._applyStreamTableJoin({ stream, operation, records, tableStream: otherStream });
    }

    if (options.otherStreamType === 'stream') {
      return this._applyStreamStreamJoin({ stream, operation, records, otherStream });
    }

    throw new Error('Unsupported join target type');
  }

  async _applyProcessorOperation(stream, operation, records, producer, stores) {
    if (!records.length) {
      return records;
    }

    const results = [];
    const mode = operation.processorMode ?? operation.options?.processorType ?? 'process';

    for (const record of records) {
      const partition = record.partition ?? 0;
      const instance = await this._ensureProcessorInstance({ stream, operation, partition, stores });
      instance.processing = true;
      instance.currentRecord = record;
      instance.forwardQueue.length = 0;
      instance.context.setCurrentNode(operation.name ?? null);
      if (record.recordContext instanceof RecordContext) {
        instance.context.setRecordContext(record.recordContext);
      } else {
        instance.context.setRecordContext(new RecordContext(record.recordContext ?? record));
      }

      if (mode === 'process') {
        await callLifecycle(instance.processor, 'process', [record]);
      } else if (mode === 'transform') {
        const transformed = await callLifecycle(instance.processor, 'transform', [record.key, record.value, record.headers ?? {}]);
        if (transformed !== null && transformed !== undefined) {
          results.push(this._normalizeRecord(transformed, record));
        }
      } else if (mode === 'transformValues') {
        const updatedValue = await callLifecycle(instance.processor, 'transform', [record.value]);
        results.push({ ...record, value: updatedValue });
      } else {
        throw new Error(`Unsupported processor operation mode: ${mode}`);
      }

      if (instance.forwardQueue.length) {
        results.push(...instance.forwardQueue);
        instance.forwardQueue.length = 0;
      }

      instance.processing = false;
      instance.currentRecord = null;
    }

    return results;
  }

  async _ensureProcessorInstance({ stream, operation, partition, stores }) {
    let perStream = this._processorInstances.get(stream.id);
    if (!perStream) {
      perStream = new Map();
      this._processorInstances.set(stream.id, perStream);
    }

    let runtime = perStream.get(operation.id);
    if (!runtime) {
      runtime = {
        supplier: operation.supplier ?? operation.fn,
        mode: operation.processorMode ?? operation.options?.processorType ?? 'process',
        instances: new Map()
      };
      perStream.set(operation.id, runtime);
    }

    let instance = runtime.instances.get(partition);
    if (!instance) {
      const supplier = runtime.supplier;
      if (!supplier || typeof supplier.get !== 'function') {
        throw new Error(`Processor operation ${operation.name ?? operation.id} is missing a ProcessorSupplier.`);
      }

      const processor = supplier.get();
      const forwardQueue = [];
      let contextInstance = null;
      const context = new ProcessorContext({
        applicationId: this.config.applicationId,
        taskId: `${stream.id}-${partition}`,
        forwarder: record => {
          if (contextInstance?.processing) {
            forwardQueue.push(this._normalizeRecord(record, contextInstance.currentRecord));
            return;
          }
          forwardQueue.push(this._normalizeRecord(record, null));
        },
        committer: () => this._commitProcessor(stream, partition),
        scheduler: this._taskManager.createScheduler({
          streamId: stream.id,
          partition,
          contextProvider: () => contextInstance?.context ?? null,
          nodeName: operation.name
        }),
        stateStoreRegistrar: payload => this._registerProcessorStore({ stream, payload, stores }),
        stateStoreProvider: storeName => stores.get(storeName) ?? this._stateStoreManager.getStore(stream.id, storeName)
      });

      contextInstance = {
        processor,
        context,
        forwardQueue,
        currentRecord: null,
        processing: false,
        mode: runtime.mode
      };

      runtime.instances.set(partition, contextInstance);
      await callLifecycle(processor, 'init', [context]);
      instance = contextInstance;
    }

    return instance;
  }

  async _registerProcessorStore({ stream, payload, stores }) {
    const builder = payload.storeBuilder ?? payload.builder ?? null;
    const storeName = payload.storeName ?? builder?.name ?? null;
    const store = await this._stateStoreManager.registerStore({
      stream,
      builder,
      storeName,
      keySerde: payload.keySerde ?? null,
      valueSerde: payload.valueSerde ?? null,
      metadata: payload.metadata ?? {},
      stateRestoreListener: payload.stateRestoreListener ?? null,
      restoreBatches: payload.restoreBatches ?? [],
      restoreOffsets: payload.restoreOffsets ?? {}
    });

    if (store && storeName) {
      let perStream = this._stateStores.get(stream.id);
      if (!perStream) {
        perStream = new Map();
        this._stateStores.set(stream.id, perStream);
      }
      perStream.set(storeName, store);
      stores.set(storeName, store);
    }

    return store;
  }

  async _commitProcessor(_stream, _partition) {
    return Promise.resolve();
  }

  async _applyStreamTableJoin({ stream, operation, records, tableStream }) {
    const options = operation.options ?? {};
    const storeName = options.storeName ?? tableStream.materialized?.storeName;
    if (!storeName) {
      throw new Error(`Join with table ${tableStream.id} requires a materialized state store`);
    }

    const tableStores = await this._ensureStateStores(tableStream);
    const store = tableStores.get(storeName);
    if (!store) {
      throw new Error(`State store ${storeName} was not initialised for join with table ${tableStream.id}`);
    }

    const joinType = options.joinType ?? 'inner';
    const results = [];

    for (const record of records) {
      const key = record.key;
      if (key === null || key === undefined) {
        if (joinType === 'left' || joinType === 'outer') {
          const joinedValue = await operation.fn(record.value, null, record);
          results.push({ ...record, value: joinedValue });
        }
        continue;
      }

      const tableValue = await store.get(key);
      if (tableValue === undefined) {
        if (joinType === 'left' || joinType === 'outer') {
          const joinedValue = await operation.fn(record.value, null, record);
          results.push({ ...record, value: joinedValue });
        }
        continue;
      }

      const joinedValue = await operation.fn(record.value, tableValue, record);
      results.push({ ...record, value: joinedValue });
    }

    if (results.length) {
      this._metrics.record('stream.records.joined', results.length, {
        streamId: stream.id,
        joinType,
        otherStreamId: tableStream.id
      });
    }

    return results;
  }

  async _applyStreamStreamJoin({ stream, operation, records, otherStream }) {
    const options = operation.options ?? {};
    const joinType = options.joinType ?? 'inner';
    const thisStoreName = options.thisStoreName;
    const otherStoreName = options.otherStoreName;
    const windowInstance = operation.windowInstance;

    if (!thisStoreName || !otherStoreName) {
      throw new Error('Stream-stream joins require window store definitions for both streams');
    }
    if (!windowInstance || typeof windowInstance.bounds !== 'function') {
      throw new Error('Stream-stream joins require a window specification');
    }

    const selfStores = await this._ensureStateStores(stream);
    const otherStores = await this._ensureStateStores(otherStream);
    const thisStore = selfStores.get(thisStoreName);
    const otherStore = otherStores.get(otherStoreName);

    if (!thisStore) {
      throw new Error(`State store ${thisStoreName} was not initialised for stream-stream join`);
    }
    if (!otherStore) {
      throw new Error(`State store ${otherStoreName} was not initialised for stream-stream join`);
    }

    const retentionMs = options.windowRetentionMs ?? (typeof windowInstance.retentionPeriod === 'function' ? windowInstance.retentionPeriod() : 0);
    const results = [];

    for (const record of records) {
      const timestamp = record.timestamp ?? Date.now();
      if (retentionMs && typeof thisStore.purge === 'function') {
        await thisStore.purge(timestamp - retentionMs);
      }
      if (retentionMs && typeof otherStore.purge === 'function') {
        await otherStore.purge(timestamp - retentionMs);
      }

      const key = record.key;

      if (key === null || key === undefined) {
        if (joinType === 'left' || joinType === 'outer') {
          const joinedValue = await operation.fn(record.value, null, record);
          results.push({ ...record, value: joinedValue });
        }
        await thisStore.put(key, record.value, timestamp);
        continue;
      }

      const { start, end } = windowInstance.bounds(timestamp);
      const matches = await otherStore.fetch(key, start, end);

      if (matches.length) {
        for (const match of matches) {
          const joinedValue = await operation.fn(record.value, match.value, record);
          const outTimestamp = Math.max(timestamp, match.timestamp ?? timestamp);
          results.push({ ...record, value: joinedValue, timestamp: outTimestamp });
        }
      } else if (joinType === 'left' || joinType === 'outer') {
        const joinedValue = await operation.fn(record.value, null, record);
        results.push({ ...record, value: joinedValue });
      }

      await thisStore.put(key, record.value, timestamp);
    }

    if (results.length) {
      this._metrics.record('stream.records.joined', results.length, {
        streamId: stream.id,
        joinType,
        otherStreamId: otherStream.id
      });
    }

    return results;
  }

  async _bufferStreamJoins(stream, record, stores) {
    const buffers = Array.isArray(stream.joinBuffers) ? stream.joinBuffers : [];
    if (!buffers.length) {
      return;
    }

    const timestamp = record.timestamp ?? Date.now();

    for (const buffer of buffers) {
      const store = stores.get(buffer.storeName);
      if (!store || typeof store.put !== 'function') {
        continue;
      }
      if (buffer.retentionMs && typeof store.purge === 'function') {
        await store.purge(timestamp - buffer.retentionMs);
      }
      await store.put(record.key, record.value, timestamp);
    }
  }

  async _ensureStateStores(stream) {
    if (this._stateStores.has(stream.id)) {
      return this._stateStores.get(stream.id);
    }

    const storeInstances = new Map();
    if (Array.isArray(stream.stateStores)) {
      for (const definition of stream.stateStores) {
        if (!definition?.builder) {
          continue;
        }
        const registered = this._stateStoreManager.registerDefinition(stream, definition);
        const instance = await this._stateStoreManager.buildAndRegister({
          stream,
          definition: registered,
          storeInstances
        });
        storeInstances.set(registered.name, instance);
      }
    }
    this._stateStoreManager.bindExisting(stream, storeInstances);
    this._registerPrebuiltStateStores(stream, storeInstances);
    return storeInstances;
  }

  _registerPrebuiltStateStores(stream, storeInstances) {
    if (!stream || !storeInstances) {
      return;
    }
    this._stateStores.set(stream.id, storeInstances);
    if (!this._queryMetadata) {
      return;
    }
    const definitions = Array.isArray(stream.stateStores) ? stream.stateStores : [];
    for (const definition of definitions) {
      if (!definition?.name) {
        continue;
      }
      const instance = storeInstances.get(definition.name);
      if (!instance) {
        continue;
      }
      this._queryMetadata.registerLocalStore({
        storeName: definition.name,
        store: instance,
        stream,
        metadata: definition.builderMetadata
      });
    }
  }

  store(storeName) {
    return this._interactiveQueryService?.store(storeName) ?? null;
  }

  metadataForStore(storeName) {
    return this._interactiveQueryService?.metadataForStore(storeName) ?? [];
  }

  getInteractiveQueryService() {
    return this._interactiveQueryService;
  }

  registerRemoteStoreMetadata({ storeName, hostInfo, topicPartitions }) {
    this._queryMetadata?.registerRemoteStore({ storeName, hostInfo, topicPartitions });
  }

  async _safeDeserialize({ stream, payload, component, buffer, serde }) {
    try {
      if (!serde) {
        return this._decodeBuffer(buffer);
      }
      return serde.deserialize(buffer);
    } catch (error) {
      const handler = this._errorHandlers.deserialization;
      const action = handler ? await handler.handle({
        error,
        stage: `deserialization-${component}`,
        stream,
        payload
      }) : HandlerAction.FAIL;
      this._metrics.record('stream.deserialization.errors', 1, { streamId: stream.id, component });
      if (action === HandlerAction.CONTINUE) {
        this._metrics.record('stream.records.skipped', 1, { streamId: stream.id, reason: 'deserialization' });
        return SKIP_RECORD;
      }
      throw error;
    }
  }

  async _handleProductionError({ error, stream, record, sink, stage }) {
    const handler = this._errorHandlers.production;
    this._metrics.record('stream.production.errors', 1, { streamId: stream.id, topic: sink?.topic });
    if (!handler) {
      return false;
    }
    const action = await handler.handle({
      error,
      stream,
      record,
      sink,
      stage
    });
    if (action === HandlerAction.CONTINUE) {
      this._metrics.record('stream.records.skipped', 1, { streamId: stream.id, reason: 'production' });
      return true;
    }
    return false;
  }

  async _updateTableState(stream, records, stores) {
    if (!records?.length) {
      return;
    }

    const storeName = stream.materialized?.storeName ?? stream.stateStores?.[0]?.name;
    if (!storeName) {
      throw new Error(`Table ${stream.id} is missing a materialized state store`);
    }

    const store = stores.get(storeName);
    if (!store) {
      throw new Error(`State store ${storeName} was not initialised for table ${stream.id}`);
    }

    for (const record of records) {
      if (record.key === null || record.key === undefined) {
        continue;
      }
      if (record.value === null || record.value === undefined) {
        await store.delete(record.key);
      } else {
        await store.put(record.key, record.value);
      }
    }
  }
}

module.exports = {
  KafkaStreams
};
