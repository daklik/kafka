'use strict';

const DAY_IN_MS = 24 * 60 * 60 * 1000;

class JoinWindows {
  constructor({ beforeMs, afterMs, graceMs }) {
    if (beforeMs == null || beforeMs < 0) {
      throw new Error('JoinWindows requires a non-negative beforeMs interval');
    }
    if (afterMs == null || afterMs < 0) {
      throw new Error('JoinWindows requires a non-negative afterMs interval');
    }
    if (graceMs == null || graceMs < 0) {
      throw new Error('JoinWindows requires a non-negative graceMs interval');
    }

    this.beforeMs = beforeMs;
    this.afterMs = afterMs;
    this.graceMs = graceMs;
  }

  static of(timeDifferenceMs) {
    if (timeDifferenceMs == null || timeDifferenceMs < 0) {
      throw new Error('JoinWindows.of requires a non-negative time difference');
    }
    return new JoinWindows({ beforeMs: timeDifferenceMs, afterMs: timeDifferenceMs, graceMs: DAY_IN_MS });
  }

  static ofTimeDifferences({ beforeMs, afterMs }) {
    if (beforeMs == null || afterMs == null) {
      throw new Error('JoinWindows.ofTimeDifferences requires beforeMs and afterMs');
    }
    if (beforeMs < 0 || afterMs < 0) {
      throw new Error('JoinWindows.ofTimeDifferences requires non-negative intervals');
    }
    return new JoinWindows({ beforeMs, afterMs, graceMs: DAY_IN_MS });
  }

  before(timeDifferenceMs) {
    if (timeDifferenceMs == null || timeDifferenceMs < 0) {
      throw new Error('JoinWindows.before requires a non-negative interval');
    }
    return new JoinWindows({ beforeMs: timeDifferenceMs, afterMs: this.afterMs, graceMs: this.graceMs });
  }

  after(timeDifferenceMs) {
    if (timeDifferenceMs == null || timeDifferenceMs < 0) {
      throw new Error('JoinWindows.after requires a non-negative interval');
    }
    return new JoinWindows({ beforeMs: this.beforeMs, afterMs: timeDifferenceMs, graceMs: this.graceMs });
  }

  grace(graceMs) {
    if (graceMs == null || graceMs < 0) {
      throw new Error('JoinWindows.grace requires a non-negative interval');
    }
    return new JoinWindows({ beforeMs: this.beforeMs, afterMs: this.afterMs, graceMs });
  }

  bounds(timestamp) {
    const ts = Number(timestamp);
    return {
      start: ts - this.beforeMs,
      end: ts + this.afterMs
    };
  }

  retentionPeriod() {
    return this.beforeMs + this.afterMs + this.graceMs;
  }

  windowSize() {
    return this.beforeMs + this.afterMs;
  }

  describe() {
    return {
      type: 'join',
      beforeMs: this.beforeMs,
      afterMs: this.afterMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionPeriod()
    };
  }
}

module.exports = {
  JoinWindows
};
