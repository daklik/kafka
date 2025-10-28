'use strict';

class Joined {
  constructor({ keySerde = null, valueSerde = null, otherValueSerde = null, name = null } = {}) {
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.otherValueSerde = otherValueSerde;
    this.name = name;
  }

  static with(keySerde, valueSerde, otherValueSerde = null) {
    return new Joined({ keySerde, valueSerde, otherValueSerde });
  }

  static withKeyValueSerde(keySerde, valueSerde) {
    return new Joined({ keySerde, valueSerde });
  }

  static withKeySerde(keySerde) {
    return new Joined({ keySerde });
  }

  static as(name) {
    return new Joined({ name });
  }

  withKeySerde(keySerde) {
    this.keySerde = keySerde;
    return this;
  }

  withValueSerde(valueSerde) {
    this.valueSerde = valueSerde;
    return this;
  }

  withOtherValueSerde(otherValueSerde) {
    this.otherValueSerde = otherValueSerde;
    return this;
  }

  withName(name) {
    this.name = name;
    return this;
  }

  resolve(defaults = {}) {
    return {
      keySerde: this.keySerde ?? defaults.keySerde ?? null,
      valueSerde: this.valueSerde ?? defaults.valueSerde ?? null,
      otherValueSerde: this.otherValueSerde ?? defaults.otherValueSerde ?? null,
      named: this.name ?? defaults.named ?? null
    };
  }
}

module.exports = {
  Joined
};
