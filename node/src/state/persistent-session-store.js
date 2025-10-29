'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');

const { SessionStore } = require('./store');
const { serializeDatum, deserializeDatum, compareSerializedValues } = require('./serialization');

function loadRocksDBBinding(customBinding = null) {
  if (customBinding) {
    return customBinding;
  }
  try {
    // eslint-disable-next-line global-require
    return require('rocksdb-native');
  } catch (error) {
    const message = 'PersistentSessionStore requires the `rocksdb-native` package. ' +
      'Install it and ensure native dependencies (e.g. librocksdb, libatomic) are available.';
    const augmented = new Error(message);
    augmented.cause = error;
    throw augmented;
  }
}

function toNumber(value, fallback = null) {
  if (value === null || value === undefined) {
    return fallback;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? fallback : numeric;
}

function normalizeTimestamp(timestamp, fallback = Date.now()) {
  const numeric = toNumber(timestamp, null);
  if (numeric === null) {
    return Math.trunc(fallback);
  }
  return Math.trunc(numeric);
}

function serializeSessionKey(key, start, end) {
  const keyBuffer = serializeDatum(key);
  const buffer = Buffer.allocUnsafe(keyBuffer.length + 16);
  keyBuffer.copy(buffer, 0);
  buffer.writeBigInt64BE(BigInt(start), keyBuffer.length);
  buffer.writeBigInt64BE(BigInt(end), keyBuffer.length + 8);
  return buffer;
}

function deserializeSessionKey(buffer) {
  const { value: key, bytesRead } = deserializeDatum(buffer, 0);
  const start = Number(buffer.readBigInt64BE(bytesRead));
  const end = Number(buffer.readBigInt64BE(bytesRead + 8));
  return { key, start, end };
}

function compareKeyRange(candidate, fromKey = null, toKey = null) {
  if (fromKey != null && compareSerializedValues(candidate, fromKey) < 0) {
    return false;
  }
  if (toKey != null && compareSerializedValues(candidate, toKey) > 0) {
    return false;
  }
  return true;
}

class PersistentSessionStore extends SessionStore {
  constructor(name, options = {}) {
    super(name, {
      persistent: true,
      loggingEnabled: options.loggingEnabled ?? true,
      cachingEnabled: options.cachingEnabled ?? false,
      changelogConfig: options.changelogConfig ?? null,
      retention: options.retentionMs ?? options.retention ?? null
    });
    this._rocksdbFactory = options.rocksdb ?? null;
    this._dbOptions = options.dbOptions ?? {};
    this._directoryOverride = options.directory ?? null;
    this._retentionMs = toNumber(options.retentionMs ?? options.retention, null);
    this._graceMs = toNumber(options.gracePeriodMs ?? options.graceMs, 0);
    this._db = null;
    this._directory = null;
  }

  async init(context = {}) {
    super.init(context);
    this._directory = await this._resolveDirectory(context);
    await fs.mkdir(this._directory, { recursive: true });
    const RocksDB = loadRocksDBBinding(this._rocksdbFactory);
    this._db = new RocksDB(this._directory, this._dbOptions);
    if (typeof this._db.ready === 'function') {
      await this._db.ready();
    }
  }

  async put(sessionKey, value) {
    if (!sessionKey || sessionKey.key === null || sessionKey.key === undefined) {
      return;
    }
    const start = normalizeTimestamp(sessionKey.start, null);
    const end = normalizeTimestamp(sessionKey.end, start);
    if (start == null || end == null) {
      return;
    }
    const normalized = await this._mergeOverlapping({
      key: sessionKey.key,
      start,
      end
    });

    if (value === null || value === undefined) {
      for (const entry of normalized.removed) {
        await this._db.delete(entry.compositeKey);
      }
      await this._db.delete(serializeSessionKey(normalized.session.key, normalized.session.start, normalized.session.end));
      return;
    }

    const payload = serializeDatum(value);
    for (const entry of normalized.removed) {
      await this._db.delete(entry.compositeKey);
    }
    const compositeKey = serializeSessionKey(normalized.session.key, normalized.session.start, normalized.session.end);
    await this._db.put(compositeKey, payload);
    await this._enforceRetention(normalized.session.end);
  }

  async delete(sessionKey) {
    if (!sessionKey || sessionKey.key === null || sessionKey.key === undefined) {
      return;
    }
    const start = normalizeTimestamp(sessionKey.start, null);
    const end = normalizeTimestamp(sessionKey.end, start);
    if (start == null || end == null) {
      return;
    }
    const compositeKey = serializeSessionKey(sessionKey.key, start, end);
    await this._db.delete(compositeKey);
  }

  async findSessions(...args) {
    if (!args.length) {
      return [];
    }
    if (args.length === 3) {
      const [key, earliestEnd, latestStart] = args;
      return this._findSessionsForKey(key, earliestEnd, latestStart);
    }
    if (args.length === 4) {
      const [keyFrom, keyTo, earliestEnd, latestStart] = args;
      return this._findSessionsInRange(keyFrom, keyTo, earliestEnd, latestStart);
    }
    throw new Error('PersistentSessionStore#findSessions expects (key, earliestEnd, latestStart) or (keyFrom, keyTo, earliestEnd, latestStart).');
  }

  async entries() {
    const results = [];
    for await (const entry of this._iterate()) {
      results.push([entry.key, { start: entry.start, end: entry.end, value: entry.value }]);
    }
    return results;
  }

  async purge(beforeTimestamp) {
    if (beforeTimestamp == null) {
      return;
    }
    const cutoff = normalizeTimestamp(beforeTimestamp);
    const batch = this._db.write ? this._db.write({ autoDestroy: true }) : null;
    try {
      for await (const entry of this._iterate()) {
        if (entry.end < cutoff) {
          if (batch) {
            batch.tryDelete(entry.compositeKey);
          } else {
            await this._db.delete(entry.compositeKey);
          }
        }
      }
      if (batch) {
        await batch.flush();
      }
    } finally {
      batch?.destroy?.();
    }
  }

  async flush() {
    if (this._db?.flush) {
      await this._db.flush();
    }
  }

  async close() {
    if (!this._db) {
      return;
    }
    await this.flush();
    await this._db.close?.({ force: true });
    this._db = null;
  }

  async _findSessionsForKey(key, earliestEnd, latestStart) {
    if (key === null || key === undefined) {
      return [];
    }
    const earliest = normalizeTimestamp(earliestEnd, Number.MIN_SAFE_INTEGER);
    const latest = normalizeTimestamp(latestStart, Number.MAX_SAFE_INTEGER);
    const matches = [];
    for await (const entry of this._iterate()) {
      if (compareSerializedValues(entry.key, key) !== 0) {
        continue;
      }
      if (entry.end < earliest || entry.start > latest) {
        continue;
      }
      matches.push({ key: entry.key, start: entry.start, end: entry.end, value: entry.value });
    }
    matches.sort((a, b) => a.start - b.start);
    return matches;
  }

  async _findSessionsInRange(keyFrom, keyTo, earliestEnd, latestStart) {
    const earliest = normalizeTimestamp(earliestEnd, Number.MIN_SAFE_INTEGER);
    const latest = normalizeTimestamp(latestStart, Number.MAX_SAFE_INTEGER);
    const matches = [];
    for await (const entry of this._iterate()) {
      if (!compareKeyRange(entry.key, keyFrom, keyTo)) {
        continue;
      }
      if (entry.end < earliest || entry.start > latest) {
        continue;
      }
      matches.push({ key: entry.key, start: entry.start, end: entry.end, value: entry.value });
    }
    matches.sort((a, b) => {
      const cmp = compareSerializedValues(a.key, b.key);
      if (cmp !== 0) {
        return cmp;
      }
      return a.start - b.start;
    });
    return matches;
  }

  async _mergeOverlapping(session) {
    const overlaps = [];
    for await (const entry of this._iterate()) {
      if (compareSerializedValues(entry.key, session.key) !== 0) {
        continue;
      }
      const overlapsWithExisting = entry.start <= session.end && entry.end >= session.start;
      if (overlapsWithExisting) {
        overlaps.push(entry);
      }
    }

    if (!overlaps.length) {
      return { session, removed: [] };
    }

    let start = session.start;
    let end = session.end;
    const removed = [];
    for (const entry of overlaps) {
      start = Math.min(start, entry.start);
      end = Math.max(end, entry.end);
      removed.push(entry);
    }
    return { session: { key: session.key, start, end }, removed };
  }

  async _enforceRetention(referenceEndTime) {
    if (this._retentionMs == null) {
      return;
    }
    const grace = this._graceMs ?? 0;
    const cutoff = referenceEndTime - this._retentionMs - grace;
    if (!Number.isFinite(cutoff)) {
      return;
    }
    await this.purge(cutoff);
  }

  async *_iterate() {
    if (!this._db?.iterator) {
      return;
    }
    const iterator = this._db.iterator();
    try {
      for await (const { key, value } of iterator) {
        const compositeKey = key;
        const { key: entryKey, start, end } = deserializeSessionKey(compositeKey);
        const { value: deserialized } = deserializeDatum(value, 0);
        yield {
          compositeKey,
          key: entryKey,
          start,
          end,
          value: deserialized
        };
      }
    } finally {
      iterator?.destroy?.();
    }
  }

  async _resolveDirectory(context) {
    if (this._directoryOverride) {
      return path.resolve(this._directoryOverride);
    }
    if (context.storeDirectory) {
      return path.resolve(context.storeDirectory);
    }
    if (context.config?.resolveStateStoreDirectory) {
      return path.resolve(
        context.config.resolveStateStoreDirectory({
          stream: context.stream,
          storeName: context.storeName ?? this.name
        })
      );
    }
    if (context.stateDir) {
      const streamComponent = context.stream?.id ?? 'global';
      return path.join(context.stateDir, streamComponent, context.storeName ?? this.name);
    }
    throw new Error('PersistentSessionStore requires a state directory. Provide StreamsConfig.stateDir or a directory override.');
  }
}

module.exports = {
  PersistentSessionStore
};
