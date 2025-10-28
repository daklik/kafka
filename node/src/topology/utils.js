'use strict';

function describeSerde(serde) {
  if (!serde) {
    return null;
  }

  if (typeof serde === 'string') {
    return serde;
  }

  if (typeof serde === 'function') {
    return serde.name || '[function]';
  }

  if (typeof serde === 'object') {
    if (serde.name && typeof serde.name === 'string') {
      return serde.name;
    }
    if (serde.constructor && serde.constructor !== Object) {
      return serde.constructor.name;
    }
  }

  return typeof serde;
}

function sanitizeValue(value) {
  if (typeof value === 'function') {
    return '[function]';
  }

  if (Array.isArray(value)) {
    return value.map(entry => sanitizeValue(entry));
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value).map(([key, val]) => [key, sanitizeValue(val)]);
    return Object.fromEntries(entries);
  }

  return value;
}

module.exports = {
  describeSerde,
  sanitizeValue
};
