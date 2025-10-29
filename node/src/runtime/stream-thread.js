'use strict';

const EventEmitter = require('events');
const { TaskState } = require('./task-manager');
const { TransactionManager } = require('./transaction-manager');

const DEFAULT_CLOCK = {
  now: () => Date.now()
};

function normalizeClock(clock) {
  if (!clock || typeof clock.now !== 'function') {
    return DEFAULT_CLOCK;
  }
  return {
    now: clock.now.bind(clock)
  };
}

function toCommitOffset(offset) {
  if (offset == null) {
    return null;
  }
  const numeric = Number(offset);
  if (Number.isNaN(numeric)) {
    return null;
  }
  return String(numeric + 1);
}

class StreamThread extends EventEmitter {
  constructor({
    stream,
    taskManager,
    stateStoreManager,
    metrics,
    config,
    transactionManager,
    clock
  } = {}) {
    super();
    if (!stream?.id) {
      throw new TypeError('StreamThread requires a stream with an id.');
    }
    this.stream = stream;
    this._taskManager = taskManager ?? null;
    this._stateStoreManager = stateStoreManager ?? null;
    this._metrics = metrics ?? null;
    this._config = config ?? null;
    this._transactionManager = transactionManager ?? new TransactionManager({
      guarantee: config?.processingGuarantee
    });
    this._clock = normalizeClock(clock);
    this._consumer = null;
    this._producer = null;
    this._partitionState = new Map();
    this._commitInterval = config?.commitInterval ?? 5000;

    if (this._taskManager?.on) {
      this._taskManager.on('task.transition', event => {
        if (event?.streamId === this.stream.id) {
          this.emit('task.transition', event);
        }
      });
    }
  }

  bindConsumer(consumer) {
    this._consumer = consumer;
    return this;
  }

  bindProducer(producer) {
    this._producer = producer;
    return this;
  }

  isExactlyOnce() {
    return Boolean(this._transactionManager?.isEnabled?.());
  }

  wrapHandler(handler) {
    if (typeof handler !== 'function') {
      throw new TypeError('StreamThread.wrapHandler requires a handler function.');
    }
    return async payload => {
      const partition = payload?.partition ?? 0;
      const topic = payload?.topic ?? this.stream.sourceTopic;
      const message = payload?.message ?? {};
      const offset = message.offset != null ? Number(message.offset) : null;
      const commitMetadata = {
        topic,
        partition,
        offset: toCommitOffset(offset)
      };

      this._ensurePartitionState(partition);

      if (this._taskManager?.updateState) {
        try {
          this._taskManager.updateState(this.stream.id, partition, TaskState.RUNNING, {
            reason: 'record-received'
          });
        } catch (error) {
          this.emit('error', error);
        }
      }

      const processRecord = async () => {
        await handler(payload);
        await this._afterProcess({ partition, commitMetadata });
      };

      if (this.isExactlyOnce()) {
        await this._transactionManager.withTransaction({
          producer: this._producer,
          consumer: this._consumer,
          offsets: commitMetadata,
          handler: processRecord,
          streamId: this.stream.id,
          partition
        });
      } else {
        await processRecord();
      }
    };
  }

  async restoreTask({ partition, storeName, restoreBatches = [], restoreOffsets = {} } = {}) {
    if (!this._stateStoreManager?.restoreStore) {
      throw new Error('StreamThread requires a StateStoreManager with restoreStore() support.');
    }
    if (storeName == null) {
      throw new TypeError('restoreTask requires a storeName.');
    }
    await this._stateStoreManager.restoreStore({
      stream: this.stream,
      storeName,
      restoreBatches,
      restoreOffsets
    });
    if (this._taskManager?.updateState) {
      this._taskManager.updateState(this.stream.id, partition, TaskState.RUNNING, {
        reason: 'restore-complete',
        metadata: { storeName }
      });
    }
  }

  async _afterProcess({ partition, commitMetadata }) {
    if (!commitMetadata?.offset) {
      return;
    }
    const partitionState = this._ensurePartitionState(partition);
    partitionState.lastOffset = commitMetadata.offset;
    const now = this._clock.now();

    if (!this.isExactlyOnce() && typeof this._consumer?.commitOffsets === 'function') {
      const shouldCommit = partitionState.lastCommitTime == null
        || (now - partitionState.lastCommitTime) >= this._commitInterval;
      if (shouldCommit) {
        await this._consumer.commitOffsets([
          {
            topic: commitMetadata.topic,
            partition: commitMetadata.partition,
            offset: commitMetadata.offset
          }
        ]);
        partitionState.lastCommitTime = now;
        this.emit('commit', {
          streamId: this.stream.id,
          partition,
          offset: commitMetadata.offset
        });
      }
    }
  }

  _ensurePartitionState(partition) {
    let state = this._partitionState.get(partition);
    if (!state) {
      state = {
        lastOffset: null,
        lastCommitTime: null
      };
      this._partitionState.set(partition, state);
    }
    return state;
  }
}

module.exports = {
  StreamThread
};
