'use strict';

const DAY_IN_MS = 24 * 60 * 60 * 1000;

class SlidingWindows {
  constructor({ timeDifferenceMs, graceMs }) {
    if (timeDifferenceMs == null || timeDifferenceMs <= 0) {
      throw new Error('SlidingWindows requires a positive time difference');
    }
    if (graceMs == null || graceMs < 0) {
      throw new Error('SlidingWindows requires a non-negative grace interval');
    }

    this.timeDifferenceMs = timeDifferenceMs;
    this.graceMs = graceMs;
  }

  static ofTimeDifferenceAndGrace(timeDifferenceMs, graceMs = DAY_IN_MS) {
    return new SlidingWindows({ timeDifferenceMs, graceMs });
  }

  static of(timeDifferenceMs) {
    return new SlidingWindows({ timeDifferenceMs, graceMs: DAY_IN_MS });
  }

  grace(graceMs) {
    return new SlidingWindows({ timeDifferenceMs: this.timeDifferenceMs, graceMs });
  }

  bounds(timestamp) {
    const ts = Number(timestamp);
    return {
      start: ts - this.timeDifferenceMs,
      end: ts + this.timeDifferenceMs
    };
  }

  retentionPeriod() {
    return this.windowSize() + this.graceMs;
  }

  windowSize() {
    return this.timeDifferenceMs * 2;
  }

  describe() {
    return {
      type: 'sliding',
      timeDifferenceMs: this.timeDifferenceMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionPeriod()
    };
  }
}

module.exports = {
  SlidingWindows
};
