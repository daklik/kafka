'use strict';

const { requirePositiveDuration, requireNonNegativeDuration } = require('./utils');

const DEFAULT_GRACE_MS = 24 * 60 * 60 * 1000;

class TimeWindows {
  constructor({ sizeMs, advanceMs, graceMs = DEFAULT_GRACE_MS, retentionMs = null, startOffsetMs = 0 }) {
    this.sizeMs = requirePositiveDuration('Time window size', sizeMs);
    this.advanceMs = requirePositiveDuration('Time window advance', advanceMs ?? sizeMs);
    this.graceMs = requireNonNegativeDuration('Time window grace', graceMs);
    this.retentionMs = retentionMs != null ? requirePositiveDuration('Time window retention', retentionMs) : null;
    this.startOffsetMs = Number(startOffsetMs ?? 0);
  }

  static of(sizeMs) {
    return new TimeWindows({ sizeMs, advanceMs: sizeMs });
  }

  advanceBy(advanceMs) {
    return new TimeWindows({
      sizeMs: this.sizeMs,
      advanceMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionMs,
      startOffsetMs: this.startOffsetMs
    });
  }

  grace(graceMs) {
    return new TimeWindows({
      sizeMs: this.sizeMs,
      advanceMs: this.advanceMs,
      graceMs,
      retentionMs: this.retentionMs,
      startOffsetMs: this.startOffsetMs
    });
  }

  until(retentionMs) {
    return new TimeWindows({
      sizeMs: this.sizeMs,
      advanceMs: this.advanceMs,
      graceMs: this.graceMs,
      retentionMs,
      startOffsetMs: this.startOffsetMs
    });
  }

  startingFrom(startOffsetMs) {
    return new TimeWindows({
      sizeMs: this.sizeMs,
      advanceMs: this.advanceMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionMs,
      startOffsetMs
    });
  }

  windowsFor(timestamp) {
    const ts = Number(timestamp);
    const latestStart = Math.floor((ts - this.startOffsetMs) / this.advanceMs) * this.advanceMs + this.startOffsetMs;
    const windows = [];
    for (let start = latestStart; start > ts - this.sizeMs; start -= this.advanceMs) {
      const end = start + this.sizeMs;
      if (ts >= start && ts < end) {
        windows.push({ start, end });
      } else if (end <= ts) {
        break;
      }
    }
    return windows;
  }

  retentionPeriod() {
    if (this.retentionMs != null) {
      return this.retentionMs;
    }
    return this.sizeMs + this.graceMs;
  }

  windowSize() {
    return this.sizeMs;
  }

  describe() {
    return {
      type: 'time',
      sizeMs: this.sizeMs,
      advanceMs: this.advanceMs,
      graceMs: this.graceMs,
      retentionMs: this.retentionPeriod(),
      startOffsetMs: this.startOffsetMs
    };
  }
}

module.exports = {
  TimeWindows
};
