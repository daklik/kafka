'use strict';

class KGroupedStream {
  constructor(stream, { windowSpec = null } = {}) {
    if (!stream) {
      throw new Error('KGroupedStream requires a backing KStream');
    }
    this._stream = stream;
    this._windowSpec = windowSpec;
  }

  windowedBy(windowDefinition) {
    const windowSpec = this._stream._normalizeAggregationWindow(windowDefinition);
    return new KGroupedStream(this._stream, { windowSpec });
  }

  aggregate(initializer, aggregator, materializedOrOptions = {}) {
    return this._stream._registerAggregation({
      initializer,
      aggregator,
      materializedOrOptions,
      windowSpec: this._windowSpec
    });
  }

  count(materializedOrOptions = {}) {
    return this._stream._registerAggregation({
      initializer: () => 0,
      aggregator: aggregate => (aggregate ?? 0) + 1,
      materializedOrOptions,
      windowSpec: this._windowSpec,
      valueSerde: materializedOrOptions?.valueSerde ?? null,
      defaultSessionMerger: (left, right) => (left ?? 0) + (right ?? 0)
    });
  }

  reduce(reducer, materializedOrOptions = {}) {
    if (typeof reducer !== 'function') {
      throw new Error('reduce expects a reducer function');
    }

    const aggregator = (aggregate, value, record) => {
      if (aggregate === undefined) {
        return value;
      }
      return reducer(aggregate, value, record);
    };

    return this._stream._registerAggregation({
      initializer: () => undefined,
      aggregator,
      materializedOrOptions,
      windowSpec: this._windowSpec,
      defaultSessionMerger: (left, right) => reducer(left, right)
    });
  }
}

module.exports = {
  KGroupedStream
};
