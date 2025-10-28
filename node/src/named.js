'use strict';

class Named {
  constructor(name) {
    if (!name || typeof name !== 'string') {
      throw new Error('Named requires a non-empty string name');
    }
    this.name = name;
  }

  static as(name) {
    return new Named(name);
  }

  static from(namedOrOptions, fallback) {
    if (!namedOrOptions) {
      return fallback;
    }
    if (namedOrOptions instanceof Named) {
      return namedOrOptions.name;
    }
    if (typeof namedOrOptions === 'string') {
      return namedOrOptions;
    }
    if (typeof namedOrOptions.named === 'string') {
      return namedOrOptions.named;
    }
    return fallback;
  }
}

module.exports = {
  Named
};
