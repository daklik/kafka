'use strict';

const EventEmitter = require('events');

class TaskManager extends EventEmitter {
  constructor({ topology } = {}) {
    super();
    this.topology = topology;
    this._tasks = new Map();
    this._consumers = new Map();
  }

  registerTopology(topology) {
    this.topology = topology;
  }

  registerConsumer(streamId, consumer) {
    this._consumers.set(streamId, consumer);
    const rebalanceEvents = ['rebalance', 'consumer.rebalance', 'group_join', 'consumer.group_join'];
    for (const eventName of rebalanceEvents) {
      consumer?.on?.(eventName, event => this._handleRebalance(streamId, eventName, event));
    }
  }

  _handleRebalance(streamId, eventName, event) {
    const tasks = this._tasks.get(streamId) ?? new Map();
    if (event?.assignedPartitions) {
      for (const partition of event.assignedPartitions) {
        const task = this._ensureTask(streamId, partition);
        task.state = 'assigned';
        task.assignmentEpoch = (task.assignmentEpoch ?? 0) + 1;
      }
    }
    if (event?.revokedPartitions) {
      for (const partition of event.revokedPartitions) {
        const task = tasks.get(partition);
        if (task) {
          task.state = 'revoked';
        }
      }
    }
    this.emit('rebalance', { streamId, eventName, event, tasks: this.snapshot(streamId) });
  }

  recordProcessed(streamId, partition, offset) {
    const task = this._ensureTask(streamId, partition);
    task.lastOffset = offset != null ? Number(offset) : null;
    task.lastUpdate = Date.now();
    task.state = task.state ?? 'running';
  }

  _ensureTask(streamId, partition) {
    let tasks = this._tasks.get(streamId);
    if (!tasks) {
      tasks = new Map();
      this._tasks.set(streamId, tasks);
    }
    let task = tasks.get(partition);
    if (!task) {
      task = {
        streamId,
        partition,
        state: 'created',
        assignmentEpoch: 0,
        lastOffset: null,
        lastUpdate: Date.now()
      };
      tasks.set(partition, task);
    }
    return task;
  }

  snapshot(streamId) {
    if (streamId) {
      return Array.from((this._tasks.get(streamId) ?? new Map()).values()).map(task => ({ ...task }));
    }
    const snapshot = {};
    for (const [id, tasks] of this._tasks.entries()) {
      snapshot[id] = Array.from(tasks.values()).map(task => ({ ...task }));
    }
    return snapshot;
  }
}

module.exports = {
  TaskManager
};
