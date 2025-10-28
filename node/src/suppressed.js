'use strict';

class BufferConfig {
  constructor({ maxRecords = Infinity, emitEarlyWhenFull = false } = {}) {
    if (maxRecords !== Infinity && (typeof maxRecords !== 'number' || maxRecords <= 0)) {
      throw new Error('Suppressed buffer maxRecords must be a positive number or Infinity');
    }
    this.maxRecords = maxRecords;
    this.emitEarlyWhenFull = Boolean(emitEarlyWhenFull);
  }

  static unbounded() {
    return new BufferConfig({ maxRecords: Infinity, emitEarlyWhenFull: false });
  }

  static bounded(maxRecords, options = {}) {
    return new BufferConfig({
      maxRecords,
      emitEarlyWhenFull: options.emitEarlyWhenFull ?? false
    });
  }

  static from(config = {}) {
    if (config instanceof BufferConfig) {
      return config;
    }
    if (config.bufferConfig instanceof BufferConfig) {
      return config.bufferConfig;
    }
    if (config instanceof Suppressed) {
      return BufferConfig.from(config.bufferConfig);
    }
    if (config.bufferConfig instanceof Suppressed) {
      return BufferConfig.from(config.bufferConfig);
    }
    if (config === undefined || config === null) {
      return BufferConfig.unbounded();
    }
    if (typeof config === 'object' && 'maxRecords' in config) {
      return new BufferConfig({
        maxRecords: config.maxRecords,
        emitEarlyWhenFull: config.emitEarlyWhenFull
      });
    }
    return BufferConfig.unbounded();
  }

  describe() {
    return {
      maxRecords: this.maxRecords,
      emitEarlyWhenFull: this.emitEarlyWhenFull
    };
  }
}

class Suppressed {
  constructor({ strategy, bufferConfig }) {
    if (!strategy) {
      throw new Error('Suppressed requires a strategy');
    }
    this.strategy = strategy;
    this.bufferConfig = BufferConfig.from(bufferConfig);
  }

  static untilWindowCloses(bufferConfig = BufferConfig.unbounded()) {
    return new Suppressed({ strategy: 'untilWindowCloses', bufferConfig });
  }

  withBufferConfig(bufferConfig) {
    return new Suppressed({ strategy: this.strategy, bufferConfig });
  }

  describe() {
    return {
      strategy: this.strategy,
      bufferConfig: this.bufferConfig.describe()
    };
  }
}

Suppressed.BufferConfig = BufferConfig;

module.exports = {
  Suppressed,
  BufferConfig
};
