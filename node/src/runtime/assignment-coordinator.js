'use strict';

function toMap(input) {
  if (input instanceof Map) {
    return new Map(input);
  }
  const map = new Map();
  for (const entry of Object.entries(input ?? {})) {
    const { active = [], standby = [] } = entry[1] ?? {};
    map.set(entry[0], {
      active: new Set(active),
      standby: new Set(standby)
    });
  }
  return map;
}

function ensureMember(plan, memberId) {
  if (!plan.has(memberId)) {
    plan.set(memberId, {
      active: new Set(),
      standby: new Set()
    });
  }
  return plan.get(memberId);
}

function pickLeastLoaded(plan, members, type) {
  let selected = null;
  let smallest = Number.POSITIVE_INFINITY;
  for (const member of members) {
    const entry = ensureMember(plan, member.memberId ?? member);
    const size = entry[type].size;
    if (size < smallest) {
      smallest = size;
      selected = member.memberId ?? member;
    }
  }
  return selected;
}

function planAssignments({
  members = [],
  activeTasks = [],
  standbyTasks = [],
  previousAssignments = new Map()
} = {}) {
  if (!Array.isArray(members) || !members.length) {
    return new Map();
  }

  const normalizedPrevious = toMap(previousAssignments);
  const plan = new Map();

  const activeOwnership = new Map();
  for (const [memberId, assignments] of normalizedPrevious.entries()) {
    for (const task of assignments.active) {
      activeOwnership.set(task, memberId);
    }
  }

  for (const task of activeTasks) {
    const owner = activeOwnership.get(task);
    let target = owner && members.find(member => (member.memberId ?? member) === owner) ? owner : null;
    if (!target) {
      target = pickLeastLoaded(plan, members, 'active');
    }
    ensureMember(plan, target).active.add(task);
    activeOwnership.set(task, target);
  }

  const standbyOwnership = new Map();
  for (const [memberId, assignments] of normalizedPrevious.entries()) {
    for (const task of assignments.standby) {
      standbyOwnership.set(task, memberId);
    }
  }

  for (const task of standbyTasks) {
    const activeOwner = activeOwnership.get(task);
    let target = null;
    const preferred = standbyOwnership.get(task);
    if (preferred && preferred !== activeOwner && members.find(member => (member.memberId ?? member) === preferred)) {
      target = preferred;
    }
    if (!target) {
      const eligibleMembers = members.filter(member => (member.memberId ?? member) !== activeOwner);
      target = pickLeastLoaded(plan, eligibleMembers.length ? eligibleMembers : members, 'standby');
    }
    ensureMember(plan, target).standby.add(task);
  }

  return plan;
}

module.exports = {
  planAssignments
};
