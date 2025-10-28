'use strict';

class ValueJoiner {
  constructor(joiner) {
    if (typeof joiner !== 'function') {
      throw new Error('ValueJoiner requires a function');
    }
    this._joiner = joiner;
  }

  static with(joiner) {
    return new ValueJoiner(joiner);
  }

  join(leftValue, rightValue, record) {
    return this._joiner(leftValue, rightValue, record);
  }
}

module.exports = {
  ValueJoiner
};
