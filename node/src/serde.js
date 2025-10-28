'use strict';

class Serde {
  constructor({ serialize, deserialize }) {
    this.serialize = serialize;
    this.deserialize = deserialize;
  }

  static string() {
    return new Serde({
      serialize: value => value == null ? undefined : Buffer.from(String(value)),
      deserialize: buffer => buffer == null ? null : buffer.toString()
    });
  }

  static json() {
    return new Serde({
      serialize: value => value == null ? undefined : Buffer.from(JSON.stringify(value)),
      deserialize: buffer => buffer == null ? null : JSON.parse(buffer.toString())
    });
  }
}

module.exports = {
  Serde
};
