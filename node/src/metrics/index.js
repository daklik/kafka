'use strict';

class MetricsReporter {
  // eslint-disable-next-line class-methods-use-this
  record() {
    throw new Error('MetricsReporter#record must be implemented by subclasses');
  }
}

class NoopReporter extends MetricsReporter {
  // eslint-disable-next-line class-methods-use-this
  record() {}
}

class ConsoleMetricsReporter extends MetricsReporter {
  constructor(logger = console) {
    super();
    this.logger = logger;
  }

  record(sample) {
    if (!sample) {
      return;
    }
    if (typeof this.logger.debug === 'function') {
      this.logger.debug('[KafkaStreams][metrics]', sample);
    } else if (typeof this.logger.log === 'function') {
      this.logger.log('[KafkaStreams][metrics]', sample);
    }
  }
}

class Sensor {
  constructor(name, tags, reporters) {
    this.name = name;
    this.tags = tags;
    this.reporters = reporters;
  }

  record(value = 1, metadata = {}) {
    const sample = {
      name: this.name,
      value,
      tags: this.tags,
      timestamp: metadata.timestamp ?? Date.now(),
      ...metadata
    };

    for (const reporter of this.reporters) {
      reporter.record(sample);
    }
  }
}

class StreamsMetrics {
  constructor({ reporters = [] } = {}) {
    this.reporters = reporters;
    this.sensors = new Map();
  }

  sensor(name, tags = {}) {
    const key = this._sensorKey(name, tags);
    if (!this.sensors.has(key)) {
      this.sensors.set(key, new Sensor(name, tags, this.reporters));
    }
    return this.sensors.get(key);
  }

  record(name, value = 1, tags = {}, metadata = {}) {
    this.sensor(name, tags).record(value, metadata);
  }

  _sensorKey(name, tags) {
    const sortedEntries = Object.entries(tags).sort(([a], [b]) => a.localeCompare(b));
    return `${name}:${JSON.stringify(sortedEntries)}`;
  }
}

module.exports = {
  StreamsMetrics,
  MetricsReporter,
  ConsoleMetricsReporter,
  NoopReporter
};
