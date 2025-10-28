'use strict';

const { requireNonNegativeDuration } = require('./utils');

const DEFAULT_UNLIMITED_GRACE_MS = 24 * 60 * 60 * 1000;

class UnlimitedWindows {
  constructor({ graceMs = DEFAULT_UNLIMITED_GRACE_MS }) {
    this.graceMs = requireNonNegativeDuration('Unlimited window grace', graceMs);
  }

  static of() {
    return new UnlimitedWindows({});
  }

  grace(graceMs) {
    return new UnlimitedWindows({ graceMs });
  }

  windowsFor(timestamp) {
    const ts = Number(timestamp);
    return [{ start: Number.NEGATIVE_INFINITY, end: ts }];
  }

  retentionPeriod() {
    return Number.POSITIVE_INFINITY;
  }

  describe() {
    return {
      type: 'unlimited',
      graceMs: this.graceMs,
      retentionMs: this.retentionPeriod()
    };
  }
}

module.exports = {
  UnlimitedWindows
};
