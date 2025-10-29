'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { TaskManager, TaskState } = require('../src');

function createClock() {
  let now = 0;
  return {
    now: () => now,
    advance: ms => {
      now += ms;
    }
  };
}

test('task manager assigns, transitions, and snapshots task state', async () => {
  const clock = createClock();
  const manager = new TaskManager({ clock });
  const transitions = [];
  manager.on('task.transition', event => transitions.push(event));

  const snapshot = manager.assign('stream-A', [
    { partition: 0, state: TaskState.RESTORING },
    { partition: 1 }
  ]);
  assert.equal(snapshot.length, 2);
  assert.equal(snapshot[0].state, TaskState.RESTORING);
  assert.equal(snapshot[1].state, TaskState.ASSIGNED);

  clock.advance(5);
  const updated = manager.updateState('stream-A', 0, TaskState.RUNNING, { reason: 'restore-complete' });
  assert.equal(updated.state, TaskState.RUNNING);

  clock.advance(5);
  await manager.recordProcessed('stream-A', 0, '10', 20);
  const runningTask = manager.snapshot('stream-A').find(task => task.partition === 0);
  assert.equal(runningTask.lastOffset, 10);
  assert.equal(runningTask.streamTime, 20);
  assert.equal(runningTask.state, TaskState.RUNNING);

  manager.assign('stream-A', [{ partition: 1, state: TaskState.RESTORING }]);
  const revokedTask = manager.snapshot('stream-A').find(task => task.partition === 0);
  assert.equal(revokedTask.state, TaskState.REVOKED);

  assert.ok(transitions.length >= 4);
  const reasons = transitions.map(t => t.metadata?.reason).filter(Boolean);
  assert.ok(reasons.includes('assignment'));
  assert.ok(reasons.includes('restore-complete'));
});

test('invalid task state transitions throw descriptive errors', () => {
  const manager = new TaskManager();
  manager.assign('stream-B', [{ partition: 0, state: TaskState.RUNNING }]);
  assert.throws(
    () => manager.updateState('stream-B', 0, TaskState.CREATED),
    /Invalid task state transition/
  );
});

test('standby lag metrics are tracked per task', () => {
  const manager = new TaskManager();
  manager.assign('stream-C', [{ partition: 0, type: 'standby' }]);
  const lag = manager.markStandbyLag('stream-C', 0, { total: 42, perStore: { storeOne: 21 } });
  assert.equal(lag.total, 42);
  assert.equal(lag.perStore.storeOne, 21);
});
