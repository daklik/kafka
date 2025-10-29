'use strict';

class ForwardingDisabledException extends Error {
  constructor(message = 'Forwarding is disabled for the current processor node.') {
    super(message);
    this.name = 'ForwardingDisabledException';
  }
}

class RecordContext {
  constructor({ topic = null, partition = null, offset = null, timestamp = null, headers = {} } = {}) {
    this._topic = topic;
    this._partition = partition;
    this._offset = offset != null ? Number(offset) : null;
    this._timestamp = timestamp != null ? Number(timestamp) : null;
    this._headers = freezeHeaders(headers);
  }

  topic() {
    return this._topic;
  }

  partition() {
    return this._partition;
  }

  offset() {
    return this._offset;
  }

  timestamp() {
    return this._timestamp;
  }

  headers() {
    return this._headers;
  }

  withUpdates(updates = {}) {
    return new RecordContext({
      topic: updates.topic ?? this._topic,
      partition: updates.partition ?? this._partition,
      offset: updates.offset ?? this._offset,
      timestamp: updates.timestamp ?? this._timestamp,
      headers: updates.headers ?? this._headers
    });
  }

  describe() {
    return {
      topic: this._topic,
      partition: this._partition,
      offset: this._offset,
      timestamp: this._timestamp,
      headers: { ...this._headers }
    };
  }
}

class To {
  constructor(options = {}) {
    this.child = options.child ?? null;
    this.children = options.children ?? null;
    this.topic = options.topic ?? null;
    this.allChildren = options.allChildren ?? false;
    this.headers = freezeHeaders(options.headers ?? {});
  }

  static child(name) {
    if (!name) {
      throw new TypeError('To.child requires a downstream processor name.');
    }
    return new To({ child: name });
  }

  static children(names) {
    if (!Array.isArray(names) || !names.length) {
      throw new TypeError('To.children requires a non-empty array of downstream processor names.');
    }
    return new To({ children: [...names] });
  }

  static all() {
    return new To({ allChildren: true });
  }

  static topic(topic) {
    if (!topic) {
      throw new TypeError('To.topic requires a sink topic name.');
    }
    return new To({ topic });
  }

  withHeaders(headers = {}) {
    return new To({
      child: this.child,
      children: this.children ? [...this.children] : null,
      topic: this.topic,
      allChildren: this.allChildren,
      headers
    });
  }

  describe() {
    return {
      child: this.child,
      children: this.children ? [...this.children] : null,
      topic: this.topic,
      allChildren: this.allChildren,
      headers: { ...this.headers }
    };
  }
}

function freezeHeaders(headers = {}) {
  const cloned = { ...headers };
  for (const key of Object.keys(cloned)) {
    const value = cloned[key];
    if (Buffer.isBuffer(value)) {
      cloned[key] = Buffer.from(value);
    } else if (Array.isArray(value)) {
      cloned[key] = value.map(entry => Buffer.isBuffer(entry) ? Buffer.from(entry) : entry);
    }
  }
  return Object.freeze(cloned);
}

function normalizeForwardRecord(recordOrKey, maybeValue, context) {
  if (recordOrKey && typeof recordOrKey === 'object' && !Buffer.isBuffer(recordOrKey)) {
    const record = { ...recordOrKey };
    if (!('recordContext' in record) && context?.recordContext()) {
      record.recordContext = context.recordContext();
    }
    if (!('headers' in record) && context) {
      record.headers = context.headers();
    }
    return record;
  }

  return {
    key: recordOrKey,
    value: maybeValue,
    headers: context?.headers() ?? {},
    recordContext: context?.recordContext() ?? null
  };
}

class ProcessorContext {
  constructor({
    applicationId = null,
    taskId = null,
    forwarder = null,
    committer = null,
    scheduler = null,
    currentNode = null,
    recordContext = null,
    forwardingEnabled = true
  } = {}) {
    this.applicationId = applicationId;
    this.taskId = taskId;
    this._forwarder = forwarder;
    this._committer = committer;
    this._scheduler = scheduler;
    this._currentNode = currentNode;
    this._recordContext = recordContext;
    this._forwardingEnabled = forwardingEnabled;
  }

  setCurrentNode(node) {
    this._currentNode = node;
  }

  currentNode() {
    return this._currentNode;
  }

  setRecordContext(context) {
    this._recordContext = context instanceof RecordContext ? context : new RecordContext(context);
  }

  recordContext() {
    return this._recordContext ?? null;
  }

  headers() {
    return this._recordContext?.headers() ?? {};
  }

  forward(recordOrKey, maybeValue, maybeTo) {
    if (!this._forwardingEnabled) {
      throw new ForwardingDisabledException();
    }
    if (typeof this._forwarder !== 'function') {
      throw new Error('ProcessorContext was constructed without a forwarder.');
    }

    let record;
    let to;
    if (maybeValue instanceof To || maybeValue == null) {
      record = normalizeForwardRecord(recordOrKey, undefined, this);
      to = maybeValue ?? To.all();
    } else if (maybeTo instanceof To || maybeTo === undefined) {
      record = normalizeForwardRecord(recordOrKey, maybeValue, this);
      to = maybeTo ?? To.all();
    } else {
      throw new TypeError('ProcessorContext#forward expects To options when forwarding key/value pairs.');
    }

    return Promise.resolve(this._forwarder(record, to));
  }

  disableForwarding() {
    this._forwardingEnabled = false;
  }

  enableForwarding() {
    this._forwardingEnabled = true;
  }

  commit() {
    if (typeof this._committer !== 'function') {
      return Promise.resolve();
    }
    return Promise.resolve(this._committer());
  }

  schedule(intervalMs, punctuator, options = {}) {
    if (typeof this._scheduler !== 'function') {
      throw new Error('ProcessorContext was constructed without a scheduler.');
    }
    if (typeof intervalMs !== 'number' || intervalMs < 0) {
      throw new TypeError('ProcessorContext#schedule requires a non-negative interval in milliseconds.');
    }
    if (typeof punctuator !== 'function') {
      throw new TypeError('ProcessorContext#schedule requires a punctuator callback.');
    }
    return this._scheduler(intervalMs, punctuator, options);
  }
}

module.exports = {
  ForwardingDisabledException,
  ProcessorContext,
  RecordContext,
  To
};
