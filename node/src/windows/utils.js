'use strict';

function requirePositiveDuration(name, value) {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new Error(`${name} must be a positive duration in milliseconds`);
  }
  return ms;
}

function requireNonNegativeDuration(name, value) {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms < 0) {
    throw new Error(`${name} must be a non-negative duration in milliseconds`);
  }
  return ms;
}

module.exports = {
  requirePositiveDuration,
  requireNonNegativeDuration
};
