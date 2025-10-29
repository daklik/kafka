'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  TaskManager,
  processor: {
    ProcessorContext,
    RecordContext,
    PunctuationType
  }
} = require('../src');

class FakeClock {
  constructor() {
    this._now = 0;
    this._nextId = 1;
    this._timers = new Map();
  }

  now() {
    return this._now;
  }

  setTimeout(fn, delay) {
    const id = this._nextId++;
    this._timers.set(id, { fn, time: this._now + delay });
    return id;
  }

  clearTimeout(id) {
    this._timers.delete(id);
  }

  async advance(ms) {
    this._now += ms;
    await this._processDue();
  }

  async _processDue() {
    while (true) {
      const due = Array.from(this._timers.entries())
        .filter(([, timer]) => timer.time <= this._now)
        .sort((a, b) => a[1].time - b[1].time);
      if (!due.length) {
        break;
      }
      for (const [id, timer] of due) {
        this._timers.delete(id);
        await Promise.resolve(timer.fn());
      }
    }
  }
}

test('stream-time punctuators fire as stream time advances', async () => {
  const clock = new FakeClock();
  const manager = new TaskManager({ clock });
  const streamId = 'stream-time';
  const partition = 0;
  let context;
  const scheduler = manager.createScheduler({
    streamId,
    partition,
    contextProvider: () => context,
    nodeName: 'processor-A'
  });
  context = new ProcessorContext({ scheduler });

  const fired = [];
  context.setRecordContext(new RecordContext({ timestamp: 1_000 }));
  const handle = context.schedule(50, (timestamp, ctx) => {
    fired.push({ timestamp, ctx });
  }, { type: PunctuationType.STREAM_TIME });

  context.setRecordContext(new RecordContext({ timestamp: 1_020 }));
  await manager.recordProcessed(streamId, partition, 0, 1_020);
  assert.equal(fired.length, 0);

  context.setRecordContext(new RecordContext({ timestamp: 1_060 }));
  await manager.recordProcessed(streamId, partition, 1, 1_060);
  assert.equal(fired.length, 1);
  assert.equal(fired[0].timestamp, 1_050);
  assert.equal(fired[0].ctx, context);

  context.setRecordContext(new RecordContext({ timestamp: 1_110 }));
  await manager.recordProcessed(streamId, partition, 2, 1_110);
  assert.equal(fired.length, 2);
  assert.equal(fired[1].timestamp, 1_100);

  handle.cancel();
  context.setRecordContext(new RecordContext({ timestamp: 1_200 }));
  await manager.recordProcessed(streamId, partition, 3, 1_200);
  assert.equal(fired.length, 2);
});

test('wall-clock punctuators respect the injected clock and deduplicate schedules', async () => {
  const clock = new FakeClock();
  const manager = new TaskManager({ clock });
  const streamId = 'wall-clock';
  const partition = 1;
  let context;
  const scheduler = manager.createScheduler({
    streamId,
    partition,
    contextProvider: () => context
  });
  context = new ProcessorContext({ scheduler });

  const fired = [];
  const punctuator = timestamp => {
    fired.push(timestamp);
  };

  const handleA = context.schedule(100, punctuator, { type: PunctuationType.WALL_CLOCK_TIME });
  const handleB = context.schedule(100, punctuator, { type: PunctuationType.WALL_CLOCK_TIME });
  assert.strictEqual(handleA, handleB);

  await clock.advance(90);
  assert.equal(fired.length, 0);

  await clock.advance(10);
  assert.equal(fired.length, 1);
  assert.equal(fired[0], 100);

  await clock.advance(100);
  assert.equal(fired.length, 2);

  handleA.cancel();
  await clock.advance(200);
  assert.equal(fired.length, 2);
});
