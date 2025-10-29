'use strict';

class TransactionManager {
  constructor({ guarantee, logger } = {}) {
    this._enabled = guarantee === 'exactly_once_v2';
    this._logger = logger ?? null;
  }

  isEnabled() {
    return this._enabled;
  }

  async withTransaction({ producer, consumer, offsets, handler, streamId, partition } = {}) {
    if (!this._enabled || typeof handler !== 'function' || !producer) {
      return handler?.();
    }

    const offsetRecords = this._normalizeOffsets(offsets);

    if (typeof producer.transaction === 'function') {
      return producer.transaction(async transactional => {
        const result = await handler();
        await this._forwardOffsets({ transactional, producer, consumer, offsets: offsetRecords });
        return result;
      });
    }

    let began = false;
    try {
      if (typeof producer.beginTransaction === 'function') {
        await producer.beginTransaction();
        began = true;
      }
      const result = await handler();
      await this._forwardOffsets({ producer, consumer, offsets: offsetRecords });
      if (typeof producer.commitTransaction === 'function') {
        await producer.commitTransaction();
      }
      return result;
    } catch (error) {
      if (began && typeof producer.abortTransaction === 'function') {
        try {
          await producer.abortTransaction();
        } catch (abortError) {
          this._logger?.error?.('Failed to abort transaction', {
            error: abortError,
            streamId,
            partition
          });
        }
      }
      throw error;
    }
  }

  async _forwardOffsets({ transactional, producer, consumer, offsets }) {
    if (!offsets?.length) {
      return;
    }
    if (transactional && typeof transactional.sendOffsets === 'function') {
      await transactional.sendOffsets(offsets, consumer);
      return;
    }
    if (typeof producer?.sendOffsetsToTransaction === 'function') {
      await producer.sendOffsetsToTransaction(offsets, consumer);
    }
  }

  _normalizeOffsets(offsets) {
    if (!offsets) {
      return [];
    }
    const entries = Array.isArray(offsets) ? offsets : [offsets];
    const normalized = [];
    for (const entry of entries) {
      if (!entry) {
        continue;
      }
      const { topic, partition, offset } = entry;
      if (topic == null || partition == null || offset == null) {
        continue;
      }
      normalized.push({
        topic,
        partition,
        offset: String(offset)
      });
    }
    return normalized;
  }
}

module.exports = {
  TransactionManager
};
