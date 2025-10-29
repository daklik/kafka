'use strict';

function isPromiseLike(value) {
  return value !== null && typeof value === 'object' && typeof value.then === 'function';
}

function toPromise(result) {
  if (isPromiseLike(result)) {
    return result;
  }
  return Promise.resolve(result);
}

function isProcessorLike(candidate) {
  if (candidate === null || (typeof candidate !== 'object' && typeof candidate !== 'function')) {
    return false;
  }

  return (
    typeof candidate.process === 'function' ||
    typeof candidate.transform === 'function'
  );
}

function ensureProcessorLike(candidate) {
  if (!isProcessorLike(candidate)) {
    throw new TypeError('Processor suppliers must create objects implementing process() or transform().');
  }
  return candidate;
}

function callLifecycle(target, methodName, args = []) {
  if (!target) {
    return Promise.reject(new TypeError('Processor instance is required to invoke lifecycle methods.'));
  }

  const handler = target[methodName];
  if (typeof handler !== 'function') {
    // init and close are optional lifecycle hooks. Treat missing handlers as resolved operations.
    if (methodName === 'init' || methodName === 'close') {
      return Promise.resolve();
    }

    return Promise.reject(new TypeError(`Processor ${target.constructor?.name ?? '<anonymous>'} must implement ${methodName}().`));
  }

  try {
    return toPromise(handler.apply(target, args));
  } catch (error) {
    return Promise.reject(error);
  }
}

class Processor {
  init(context) {
    return Promise.resolve();
  }

  process(record) {
    return Promise.resolve();
  }

  close() {
    return Promise.resolve();
  }
}

class Transformer {
  init(context) {
    return Promise.resolve();
  }

  transform(key, value, headers) {
    return Promise.resolve();
  }

  close() {
    return Promise.resolve();
  }
}

class ValueTransformer {
  init(context) {
    return Promise.resolve();
  }

  transform(value) {
    return Promise.resolve();
  }

  close() {
    return Promise.resolve();
  }
}

function isConstructor(candidate) {
  return typeof candidate === 'function' && candidate.prototype && (
    typeof candidate.prototype.process === 'function' ||
    typeof candidate.prototype.transform === 'function' ||
    typeof candidate.prototype.init === 'function'
  );
}

class ProcessorSupplier {
  constructor(factory) {
    if (typeof factory !== 'function') {
      throw new TypeError('ProcessorSupplier factory must be a function.');
    }
    this._factory = factory;
  }

  get() {
    const instance = this._factory();
    if (isPromiseLike(instance)) {
      throw new TypeError('ProcessorSupplier factory must return a processor instance, not a Promise.');
    }

    return ensureProcessorLike(instance);
  }

  static from(supplier, ...args) {
    if (supplier instanceof ProcessorSupplier) {
      if (args.length > 0) {
        return new ProcessorSupplier(() => supplier.get(...args));
      }
      return supplier;
    }

    if (isConstructor(supplier)) {
      return ProcessorSupplier.fromClass(supplier, ...args);
    }

    if (typeof supplier === 'function') {
      if (args.length > 0) {
        return new ProcessorSupplier(() => supplier(...args));
      }
      return new ProcessorSupplier(supplier);
    }

    throw new TypeError('Unsupported processor supplier. Provide a factory function, class, or ProcessorSupplier.');
  }

  static fromClass(ProcessorClass, ...args) {
    if (!isConstructor(ProcessorClass)) {
      throw new TypeError('ProcessorSupplier.fromClass expects a constructor with lifecycle methods.');
    }

    return new ProcessorSupplier(() => new ProcessorClass(...args));
  }
}

const {
  ForwardingDisabledException,
  ProcessorContext,
  RecordContext,
  To
} = require('./context');

module.exports = {
  Processor,
  ProcessorSupplier,
  Transformer,
  ValueTransformer,
  callLifecycle,
  ensureProcessorLike,
  isProcessorLike,
  ProcessorContext,
  RecordContext,
  ForwardingDisabledException,
  To
};
