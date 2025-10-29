'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  processor: {
    Processor,
    ProcessorSupplier,
    callLifecycle,
    ensureProcessorLike
  }
} = require('../src');

test('callLifecycle resolves synchronous lifecycle handlers', async () => {
  class SyncProcessor extends Processor {
    init() {
      this.initialized = true;
    }

    process(record) {
      this.lastRecord = record;
    }

    close() {
      this.closed = true;
    }
  }

  const processor = new SyncProcessor();
  await callLifecycle(processor, 'init', [{ test: true }]);
  await callLifecycle(processor, 'process', ['record']);
  await callLifecycle(processor, 'close');

  assert.equal(processor.initialized, true);
  assert.equal(processor.lastRecord, 'record');
  assert.equal(processor.closed, true);
});

test('callLifecycle awaits asynchronous lifecycle handlers', async () => {
  class AsyncProcessor extends Processor {
    async process(record) {
      this.values = (this.values ?? []).concat(record);
    }
  }

  const processor = new AsyncProcessor();
  await callLifecycle(processor, 'process', [1]);
  await callLifecycle(processor, 'process', [2]);

  assert.deepEqual(processor.values, [1, 2]);
});

test('ProcessorSupplier.from adapts factory functions', () => {
  class Example extends Processor {}
  const supplier = ProcessorSupplier.from(() => new Example());
  const instance = supplier.get();

  assert.ok(instance instanceof Example);
  assert.equal(typeof instance.process, 'function');
});

test('ProcessorSupplier.fromClass instantiates constructors with arguments', () => {
  class CustomProcessor extends Processor {
    constructor(id) {
      super();
      this.id = id;
    }
  }

  const supplier = ProcessorSupplier.fromClass(CustomProcessor, 'processor-1');
  const instance = supplier.get();

  assert.ok(instance instanceof CustomProcessor);
  assert.equal(instance.id, 'processor-1');
});

test('ensureProcessorLike rejects invalid processor objects', () => {
  assert.throws(() => ensureProcessorLike({}), /process\(\) or transform\(\)/);
});
