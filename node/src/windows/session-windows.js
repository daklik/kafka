'use strict';

const { requirePositiveDuration, requireNonNegativeDuration } = require('./utils');

const DEFAULT_SESSION_GRACE_MS = 24 * 60 * 60 * 1000;

class SessionWindows {
  constructor({ inactivityGapMs, graceMs = DEFAULT_SESSION_GRACE_MS, retentionMs = null }) {
    this.inactivityGapMs = requirePositiveDuration('Session inactivity gap', inactivityGapMs);
    this.graceMs = requireNonNegativeDuration('Session grace', graceMs);
    this.retentionMs = retentionMs != null ? requirePositiveDuration('Session retention', retentionMs) : null;
  }

  static with(inactivityGapMs) {
    return new SessionWindows({ inactivityGapMs });
  }

  grace(graceMs) {
    return new SessionWindows({
      inactivityGapMs: this.inactivityGapMs,
      graceMs,
      retentionMs: this.retentionMs
    });
  }

  until(retentionMs) {
    return new SessionWindows({
      inactivityGapMs: this.inactivityGapMs,
      graceMs: this.graceMs,
      retentionMs
    });
  }

  gap() {
    return this.inactivityGapMs;
  }

  retentionPeriod() {
    if (this.retentionMs != null) {
      return this.retentionMs;
    }
    return this.inactivityGapMs * 2 + this.graceMs;
  }

  describe() {
    return {
      type: 'session',
      inactivityGapMs: this.inactivityGapMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionPeriod()
    };
  }
}

module.exports = {
  SessionWindows
};
