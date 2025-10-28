'use strict';

const { StreamsMetrics, NoopReporter } = require('../metrics');
const { createExceptionHandler, LogAndFailExceptionHandler } = require('../errors');
const { HostInfo } = require('../query/host-info');

const DEFAULT_PROCESSING_GUARANTEE = 'at_least_once';
const SUPPORTED_PROCESSING_GUARANTEES = new Set([
  DEFAULT_PROCESSING_GUARANTEE,
  'exactly_once_v2'
]);

function buildMetricsRegistry(metricsConfig = {}) {
  if (metricsConfig instanceof StreamsMetrics) {
    return metricsConfig;
  }

  const reporters = [];
  const provided = metricsConfig.reporters ?? metricsConfig.reporter;
  if (Array.isArray(provided)) {
    for (const reporter of provided) {
      const instance = instantiateReporter(reporter);
      if (instance) {
        reporters.push(instance);
      }
    }
  } else if (provided) {
    const instance = instantiateReporter(provided);
    if (instance) {
      reporters.push(instance);
    }
  }

  if (!reporters.length) {
    reporters.push(new NoopReporter());
  }

  return new StreamsMetrics({ reporters });
}

function instantiateReporter(reporter) {
  if (!reporter) {
    return null;
  }
  if (typeof reporter === 'function') {
    try {
      return new reporter();
    } catch (err) {
      return null;
    }
  }
  if (typeof reporter.record === 'function') {
    return reporter;
  }
  return null;
}

function buildErrorHandlers(raw = {}) {
  const deserialization = createExceptionHandler(raw.deserializationExceptionHandler) ?? new LogAndFailExceptionHandler();
  const production = createExceptionHandler(raw.productionExceptionHandler) ?? new LogAndFailExceptionHandler();
  return {
    deserialization,
    production
  };
}

class StreamsConfig {
  constructor(raw = {}) {
    if (raw instanceof StreamsConfig) {
      return raw;
    }
    if (!raw.applicationId) {
      throw new Error('StreamsConfig requires `applicationId` to be configured');
    }
    this.raw = { ...raw };

    this.applicationId = raw.applicationId;
    this.clientId = raw.clientId ?? `${raw.applicationId}-client`;
    this.numStreamThreads = raw.numStreamThreads ?? 1;
    this.processingGuarantee = raw.processingGuarantee ?? DEFAULT_PROCESSING_GUARANTEE;
    if (!SUPPORTED_PROCESSING_GUARANTEES.has(this.processingGuarantee)) {
      throw new Error(`Unsupported processingGuarantee: ${this.processingGuarantee}`);
    }
    this.replicationFactor = raw.replicationFactor ?? 1;
    this.commitInterval = raw.commitInterval ?? 5000;

    this.groupIdPrefix = raw.groupIdPrefix ?? raw.applicationId;

    this.clientConfig = {
      ...(raw.client ?? {}),
      clientId: (raw.client && raw.client.clientId) ? raw.client.clientId : this.clientId
    };

    this.consumerConfig = {
      allowAutoTopicCreation: false,
      ...(raw.consumer ?? {})
    };
    this.consumerFactory = raw.consumerFactory;

    const resolvedGroupId = raw.groupId ?? this.consumerConfig.groupId ?? raw.applicationId;
    this.consumerConfig.groupId = resolvedGroupId;

    this.producerConfig = {
      ...(raw.producer ?? {})
    };
    this.producerFactory = raw.producerFactory;

    if (this.processingGuarantee.startsWith('exactly_once')) {
      this.producerConfig.idempotent = this.producerConfig.idempotent ?? true;
      this.producerConfig.transactionalId = this.producerConfig.transactionalId ?? `${raw.applicationId}-tx`;
    }

    this.kafka = raw.kafka;

    this.applicationServer = HostInfo.from(
      raw.applicationServer ?? raw.applicationServerHostInfo ?? raw['application.server']
    );
    this.metadataRefreshInterval = raw.metadataRefreshInterval ?? 30000;
    this.interactiveQueryConfig = { ...(raw.interactiveQueries ?? {}) };

    this.metricsRegistry = buildMetricsRegistry(raw.metrics ?? {});
    this.errorHandlers = buildErrorHandlers(raw);
  }

  getKafkaClient() {
    return this.kafka;
  }

  getClientConfig() {
    return { ...this.clientConfig };
  }

  getConsumerConfig(overrides = {}) {
    return { ...this.consumerConfig, ...overrides };
  }

  getProducerConfig(overrides = {}) {
    return { ...this.producerConfig, ...overrides };
  }

  getMetricsRegistry() {
    return this.metricsRegistry;
  }

  getErrorHandlers() {
    return this.errorHandlers;
  }

  getApplicationServer() {
    return this.applicationServer;
  }

  getMetadataRefreshInterval() {
    return this.metadataRefreshInterval;
  }

  getInteractiveQueryConfig() {
    return { ...this.interactiveQueryConfig };
  }

  async createConsumer({ stream, kafka, groupId }) {
    if (this.consumerFactory) {
      if (this.consumerFactory.length > 1) {
        return this.consumerFactory(stream, groupId, this, kafka);
      }
      const options = this._factoryOptions({ stream, kafka, groupId });
      return this.consumerFactory(options);
    }
    return kafka.consumer(this.getConsumerConfig({ groupId }));
  }

  async createProducer({ stream, kafka }) {
    if (this.producerFactory) {
      const options = this._factoryOptions({ stream, kafka });
      return this.producerFactory(options);
    }
    return kafka.producer(this.getProducerConfig());
  }

  resolveGroupId(stream) {
    if (typeof this.raw.groupId === 'function') {
      return this.raw.groupId(stream);
    }
    if (typeof this.raw.groupId === 'string') {
      return this.raw.groupId;
    }
    if (stream?.isGlobalKTable) {
      const topic = stream.sourceTopic ?? stream.materialized?.storeName ?? stream.id;
      return `${this.applicationId}-global-${topic}`;
    }
    return `${this.groupIdPrefix}-${stream.id}`;
  }

  _factoryOptions({ stream, kafka, groupId }) {
    const options = {
      stream,
      kafka,
      groupId,
      config: this
    };
    if (stream && typeof stream === 'object') {
      Object.setPrototypeOf(options, stream);
    }
    return options;
  }
}

module.exports = {
  StreamsConfig,
  DEFAULT_PROCESSING_GUARANTEE,
  SUPPORTED_PROCESSING_GUARANTEES
};
