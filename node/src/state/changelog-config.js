'use strict';

class ChangelogConfig {
  constructor({ topic, replicationFactor, retentionMs, additionalConfig = {} } = {}) {
    this.topic = topic;
    this.replicationFactor = replicationFactor;
    this.retentionMs = retentionMs;
    this.additionalConfig = additionalConfig;
  }

  withTopic(topic) {
    return new ChangelogConfig({ ...this, topic });
  }

  withReplicationFactor(replicationFactor) {
    return new ChangelogConfig({ ...this, replicationFactor });
  }

  withRetentionMs(retentionMs) {
    return new ChangelogConfig({ ...this, retentionMs });
  }

  withConfig(config) {
    return new ChangelogConfig({ ...this, additionalConfig: { ...this.additionalConfig, ...config } });
  }

  toKafkaConfig(storeName) {
    return {
      topic: this.topic ?? `${storeName}-changelog`,
      replicationFactor: this.replicationFactor ?? 3,
      retentionMs: this.retentionMs,
      config: this.additionalConfig
    };
  }
}

module.exports = {
  ChangelogConfig
};
