'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { assignment } = require('../src');

function materialize(plan) {
  const result = {};
  for (const [memberId, assignments] of plan.entries()) {
    result[memberId] = {
      active: Array.from(assignments.active),
      standby: Array.from(assignments.standby)
    };
  }
  return result;
}

test('assignment coordinator balances active tasks and retains ownership', () => {
  const plan = assignment.planAssignments({
    members: [{ memberId: 'member-1' }, { memberId: 'member-2' }],
    activeTasks: ['task-0', 'task-1', 'task-2'],
    standbyTasks: ['task-0', 'task-1', 'task-2'],
    previousAssignments: {
      'member-1': { active: ['task-0'] },
      'member-2': { active: ['task-1'] }
    }
  });
  const view = materialize(plan);
  assert.equal(view['member-1'].active.length, 2);
  assert.equal(view['member-2'].active.length, 1);
  for (const task of ['task-0', 'task-1', 'task-2']) {
    const owners = Object.entries(view).filter(([, value]) => value.active.includes(task));
    assert.equal(owners.length, 1);
  }
});

test('standby assignments avoid active owners when possible', () => {
  const plan = assignment.planAssignments({
    members: [{ memberId: 'member-1' }, { memberId: 'member-2' }],
    activeTasks: ['task-0', 'task-1'],
    standbyTasks: ['task-0', 'task-1'],
    previousAssignments: {}
  });
  const view = materialize(plan);
  for (const task of ['task-0', 'task-1']) {
    const activeOwner = Object.entries(view).find(([, value]) => value.active.includes(task))[0];
    const standbyOwner = Object.entries(view).find(([, value]) => value.standby.includes(task))[0];
    assert.notEqual(activeOwner, standbyOwner);
  }
});
