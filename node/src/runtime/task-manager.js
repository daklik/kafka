'use strict';

const EventEmitter = require('events');
const { PunctuationType } = require('../processor/context');

const DEFAULT_CLOCK = {
  now: () => Date.now(),
  setTimeout: (fn, delay) => setTimeout(fn, delay),
  clearTimeout: timer => clearTimeout(timer)
};

const TaskState = Object.freeze({
  CREATED: 'CREATED',
  ASSIGNED: 'ASSIGNED',
  RESTORING: 'RESTORING',
  RUNNING: 'RUNNING',
  SUSPENDED: 'SUSPENDED',
  REVOKED: 'REVOKED',
  FAILED: 'FAILED'
});

const VALID_TRANSITIONS = Object.freeze({
  [TaskState.CREATED]: new Set([TaskState.ASSIGNED, TaskState.RESTORING, TaskState.RUNNING, TaskState.REVOKED]),
  [TaskState.ASSIGNED]: new Set([TaskState.RESTORING, TaskState.RUNNING, TaskState.SUSPENDED, TaskState.REVOKED]),
  [TaskState.RESTORING]: new Set([TaskState.RUNNING, TaskState.SUSPENDED, TaskState.REVOKED, TaskState.FAILED]),
  [TaskState.RUNNING]: new Set([TaskState.RUNNING, TaskState.SUSPENDED, TaskState.REVOKED, TaskState.FAILED]),
  [TaskState.SUSPENDED]: new Set([TaskState.RESTORING, TaskState.RUNNING, TaskState.REVOKED, TaskState.FAILED]),
  [TaskState.REVOKED]: new Set([TaskState.ASSIGNED, TaskState.RESTORING, TaskState.RUNNING]),
  [TaskState.FAILED]: new Set([TaskState.RESTORING, TaskState.REVOKED])
});

function normalizeClock(clock = {}) {
  if (!clock) {
    return DEFAULT_CLOCK;
  }

  const now = typeof clock.now === 'function' ? clock.now.bind(clock) : DEFAULT_CLOCK.now;
  const setTimeoutFn = typeof clock.setTimeout === 'function'
    ? clock.setTimeout.bind(clock)
    : DEFAULT_CLOCK.setTimeout;
  const clearTimeoutFn = typeof clock.clearTimeout === 'function'
    ? clock.clearTimeout.bind(clock)
    : DEFAULT_CLOCK.clearTimeout;

  return {
    now,
    setTimeout: setTimeoutFn,
    clearTimeout: clearTimeoutFn
  };
}

function toTimestamp(value) {
  if (value == null) {
    return null;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? null : numeric;
}

class TaskManager extends EventEmitter {
  constructor({ topology, clock } = {}) {
    super();
    this.topology = topology;
    this._tasks = new Map();
    this._consumers = new Map();
    this._punctuators = new Map();
    this._clock = normalizeClock(clock);
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
        task.assignmentEpoch = (task.assignmentEpoch ?? 0) + 1;
        this._transitionTask(task, TaskState.ASSIGNED, { reason: 'rebalance-assignment', eventName });
      }
    }
    if (event?.revokedPartitions) {
      for (const partition of event.revokedPartitions) {
        const task = tasks.get(partition);
        if (task) {
          this._transitionTask(task, TaskState.REVOKED, { reason: 'rebalance-revocation', eventName });
          this._cancelTaskPunctuators(streamId, partition);
        }
      }
    }
    this.emit('rebalance', { streamId, eventName, event, tasks: this.snapshot(streamId) });
  }

  async recordProcessed(streamId, partition, offset, timestamp) {
    const task = this._ensureTask(streamId, partition);
    task.lastOffset = offset != null ? Number(offset) : null;
    task.lastUpdate = this._clock.now();
    this._transitionTask(task, TaskState.RUNNING, { reason: 'record-processed' });

    const numericTimestamp = toTimestamp(timestamp);
    if (numericTimestamp != null) {
      const previous = task.streamTime ?? Number.NEGATIVE_INFINITY;
      task.streamTime = Math.max(previous, numericTimestamp);
      await this._runStreamTimePunctuators(streamId, partition, task.streamTime);
    }
  }

  createScheduler({ streamId, partition, contextProvider = () => null, nodeName = null } = {}) {
    if (!streamId) {
      throw new TypeError('TaskManager#createScheduler requires a streamId.');
    }
    return (intervalMs, punctuator, options = {}) => this._schedulePunctuator({
      streamId,
      partition,
      intervalMs,
      punctuator,
      options,
      contextProvider,
      nodeName
    });
  }

  _schedulePunctuator({
    streamId,
    partition,
    intervalMs,
    punctuator,
    options = {},
    contextProvider,
    nodeName
  }) {
    if (typeof intervalMs !== 'number' || Number.isNaN(intervalMs) || intervalMs <= 0) {
      throw new TypeError('Punctuators require a positive interval.');
    }
    if (typeof punctuator !== 'function') {
      throw new TypeError('Punctuator must be a function.');
    }

    const type = options.type ?? PunctuationType.WALL_CLOCK_TIME;
    const task = this._ensureTask(streamId, partition);

    let perStream = this._punctuators.get(streamId);
    if (!perStream) {
      perStream = new Map();
      this._punctuators.set(streamId, perStream);
    }
    let handles = perStream.get(partition);
    if (!handles) {
      handles = new Set();
      perStream.set(partition, handles);
    }

    for (const existing of handles) {
      if (!existing.cancelled && existing.type === type && existing.intervalMs === intervalMs && existing.punctuator === punctuator && existing.nodeName === (nodeName ?? null)) {
        return existing.publicHandle;
      }
    }

    const handle = {
      streamId,
      partition,
      intervalMs,
      punctuator,
      type,
      nodeName: nodeName ?? null,
      contextProvider,
      cancelled: false,
      timer: null,
      nextTimestamp: null,
      publicHandle: null
    };

    handle.cancel = () => {
      if (handle.cancelled) {
        return;
      }
      handle.cancelled = true;
      if (handle.timer != null) {
        this._clock.clearTimeout(handle.timer);
        handle.timer = null;
      }
      handles.delete(handle);
      if (handles.size === 0) {
        perStream.delete(partition);
        if (perStream.size === 0) {
          this._punctuators.delete(streamId);
        }
      }
    };

    handle.publicHandle = {
      cancel: () => handle.cancel()
    };

    handles.add(handle);

    if (type === PunctuationType.WALL_CLOCK_TIME) {
      this._scheduleWallClock(handle, options.initialDelay ?? intervalMs);
    } else if (type === PunctuationType.STREAM_TIME) {
      this._initializeStreamTimeHandle(handle, task);
    } else {
      throw new TypeError(`Unsupported punctuation type: ${type}`);
    }

    return handle.publicHandle;
  }

  _initializeStreamTimeHandle(handle, task) {
    const context = typeof handle.contextProvider === 'function' ? handle.contextProvider() : null;
    const contextTimestamp = context?.recordContext?.()?.timestamp?.();
    const baseline = toTimestamp(contextTimestamp) ?? toTimestamp(task.streamTime);
    if (baseline != null) {
      handle.nextTimestamp = baseline + handle.intervalMs;
    } else {
      handle.nextTimestamp = null;
    }
  }

  _scheduleWallClock(handle, initialDelay) {
    const delay = initialDelay ?? handle.intervalMs;
    const run = async () => {
      if (handle.cancelled) {
        return;
      }
      try {
        await this._invokePunctuator(handle, this._clock.now());
      } catch (error) {
        // Already emitted via _invokePunctuator; swallow to keep interval running.
      }
      if (!handle.cancelled) {
        handle.timer = this._clock.setTimeout(run, handle.intervalMs);
      }
    };

    handle.timer = this._clock.setTimeout(run, delay);
  }

  async _runStreamTimePunctuators(streamId, partition, currentTimestamp) {
    const perStream = this._punctuators.get(streamId);
    const handles = perStream?.get(partition);
    if (!handles || handles.size === 0) {
      return;
    }

    const ordered = Array.from(handles)
      .filter(handle => !handle.cancelled && handle.type === PunctuationType.STREAM_TIME)
      .sort((a, b) => {
        const aTime = a.nextTimestamp ?? Number.POSITIVE_INFINITY;
        const bTime = b.nextTimestamp ?? Number.POSITIVE_INFINITY;
        return aTime - bTime;
      });

    for (const handle of ordered) {
      if (handle.nextTimestamp == null) {
        handle.nextTimestamp = currentTimestamp + handle.intervalMs;
      }
      while (!handle.cancelled && handle.nextTimestamp != null && currentTimestamp >= handle.nextTimestamp) {
        await this._invokePunctuator(handle, handle.nextTimestamp);
        if (handle.cancelled) {
          break;
        }
        handle.nextTimestamp += handle.intervalMs;
      }
    }
  }

  async _invokePunctuator(handle, timestamp) {
    const context = typeof handle.contextProvider === 'function' ? handle.contextProvider() : null;
    try {
      await Promise.resolve(handle.punctuator(timestamp, context));
    } catch (error) {
      this.emit('punctuator.error', {
        error,
        streamId: handle.streamId,
        partition: handle.partition,
        type: handle.type,
        nodeName: handle.nodeName,
        timestamp
      });
      throw error;
    }
  }

  _cancelTaskPunctuators(streamId, partition) {
    const perStream = this._punctuators.get(streamId);
    if (!perStream) {
      return;
    }
    const handles = perStream.get(partition);
    if (!handles) {
      return;
    }
    for (const handle of Array.from(handles)) {
      handle.cancel();
    }
  }

  assign(streamId, assignments = []) {
    if (!streamId) {
      throw new TypeError('assign requires a streamId.');
    }
    const assigned = new Set();
    for (const assignment of assignments) {
      const partition = assignment?.partition;
      if (partition == null) {
        continue;
      }
      const task = this._ensureTask(streamId, partition);
      task.type = assignment.type ?? task.type ?? 'active';
      task.standby = task.type === 'standby';
      task.assignmentEpoch = (assignment.epoch ?? task.assignmentEpoch ?? 0) + 1;
      task.changelogOffsets = assignment.changelogOffsets ?? task.changelogOffsets ?? null;
      const state = assignment.state ?? (assignment.restoring ? TaskState.RESTORING : TaskState.ASSIGNED);
      this._transitionTask(task, state, { reason: 'assignment', metadata: assignment.metadata });
      assigned.add(partition);
    }

    const tasks = this._tasks.get(streamId);
    if (tasks) {
      for (const [partition, task] of tasks.entries()) {
        if (!assigned.has(partition) && task.state !== TaskState.REVOKED) {
          this._transitionTask(task, TaskState.REVOKED, { reason: 'assignment-revocation' });
          this._cancelTaskPunctuators(streamId, partition);
        }
      }
    }
    return this.snapshot(streamId);
  }

  updateState(streamId, partition, newState, metadata = {}) {
    const task = this._ensureTask(streamId, partition);
    this._transitionTask(task, newState, metadata);
    return { ...task };
  }

  markStandbyLag(streamId, partition, lagMetrics = {}) {
    const task = this._ensureTask(streamId, partition);
    task.standbyLag = {
      total: lagMetrics.total != null ? Number(lagMetrics.total) : task.standbyLag?.total ?? null,
      perStore: { ...(task.standbyLag?.perStore ?? {}), ...(lagMetrics.perStore ?? {}) }
    };
    task.lastUpdate = this._clock.now();
    this.emit('task.lag', { streamId, partition, lag: task.standbyLag });
    return { ...task.standbyLag };
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
        state: TaskState.CREATED,
        assignmentEpoch: 0,
        lastOffset: null,
        lastUpdate: this._clock.now(),
        streamTime: null,
        type: 'active',
        standby: false,
        metadata: {}
      };
      tasks.set(partition, task);
    }
    return task;
  }

  _transitionTask(task, newState, metadata = {}) {
    if (!task) {
      return;
    }
    const currentState = task.state ?? TaskState.CREATED;
    const requestedState = newState ?? currentState;
    const allowed = VALID_TRANSITIONS[currentState];
    if (requestedState !== currentState && !(allowed && allowed.has(requestedState))) {
      const error = new Error(`Invalid task state transition from ${currentState} to ${requestedState}`);
      error.code = 'ERR_INVALID_TASK_STATE';
      throw error;
    }
    task.state = requestedState;
    task.lastUpdate = this._clock.now();
    if (metadata) {
      const nextMetadata = { ...(task.metadata ?? {}) };
      if (metadata.metadata && typeof metadata.metadata === 'object') {
        Object.assign(nextMetadata, metadata.metadata);
      }
      if (metadata.reason) {
        nextMetadata.lastTransitionReason = metadata.reason;
      }
      task.metadata = nextMetadata;
    }
    this.emit('task.transition', {
      streamId: task.streamId,
      partition: task.partition,
      state: task.state,
      previousState: currentState,
      metadata,
      task: { ...task }
    });
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
  TaskManager,
  TaskState
};
