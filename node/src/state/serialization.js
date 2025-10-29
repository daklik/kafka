'use strict';

const TYPE_NULL = 0;
const TYPE_BUFFER = 1;
const TYPE_STRING = 2;
const TYPE_NUMBER = 3;
const TYPE_BOOLEAN = 4;
const TYPE_BIGINT = 5;
const TYPE_JSON = 6;

function serializeDatum(value) {
  if (value === null || value === undefined) {
    return Buffer.from([TYPE_NULL]);
  }
  if (Buffer.isBuffer(value)) {
    const header = Buffer.allocUnsafe(1 + 4);
    header.writeUInt8(TYPE_BUFFER, 0);
    header.writeUInt32BE(value.length, 1);
    return Buffer.concat([header, value]);
  }
  if (typeof value === 'string') {
    const payload = Buffer.from(value, 'utf8');
    const header = Buffer.allocUnsafe(1 + 4);
    header.writeUInt8(TYPE_STRING, 0);
    header.writeUInt32BE(payload.length, 1);
    return Buffer.concat([header, payload]);
  }
  if (typeof value === 'number') {
    const buffer = Buffer.allocUnsafe(1 + 8);
    buffer.writeUInt8(TYPE_NUMBER, 0);
    buffer.writeDoubleBE(value, 1);
    return buffer;
  }
  if (typeof value === 'boolean') {
    const buffer = Buffer.allocUnsafe(1 + 1);
    buffer.writeUInt8(TYPE_BOOLEAN, 0);
    buffer.writeUInt8(value ? 1 : 0, 1);
    return buffer;
  }
  if (typeof value === 'bigint') {
    const buffer = Buffer.allocUnsafe(1 + 8);
    buffer.writeUInt8(TYPE_BIGINT, 0);
    buffer.writeBigInt64BE(value, 1);
    return buffer;
  }
  const json = Buffer.from(JSON.stringify(value), 'utf8');
  const header = Buffer.allocUnsafe(1 + 4);
  header.writeUInt8(TYPE_JSON, 0);
  header.writeUInt32BE(json.length, 1);
  return Buffer.concat([header, json]);
}

function deserializeDatum(buffer, offset = 0) {
  if (!Buffer.isBuffer(buffer)) {
    throw new TypeError('deserializeDatum expects a Buffer.');
  }
  if (offset >= buffer.length) {
    throw new RangeError('deserializeDatum offset out of bounds.');
  }
  const type = buffer.readUInt8(offset);
  let cursor = offset + 1;
  switch (type) {
    case TYPE_NULL:
      return { value: null, bytesRead: cursor - offset };
    case TYPE_BUFFER: {
      const length = buffer.readUInt32BE(cursor);
      cursor += 4;
      const end = cursor + length;
      return { value: buffer.slice(cursor, end), bytesRead: end - offset };
    }
    case TYPE_STRING: {
      const length = buffer.readUInt32BE(cursor);
      cursor += 4;
      const end = cursor + length;
      return { value: buffer.toString('utf8', cursor, end), bytesRead: end - offset };
    }
    case TYPE_NUMBER: {
      const value = buffer.readDoubleBE(cursor);
      cursor += 8;
      return { value, bytesRead: cursor - offset };
    }
    case TYPE_BOOLEAN: {
      const value = buffer.readUInt8(cursor) === 1;
      cursor += 1;
      return { value, bytesRead: cursor - offset };
    }
    case TYPE_BIGINT: {
      const value = buffer.readBigInt64BE(cursor);
      cursor += 8;
      return { value, bytesRead: cursor - offset };
    }
    case TYPE_JSON: {
      const length = buffer.readUInt32BE(cursor);
      cursor += 4;
      const end = cursor + length;
      const json = buffer.toString('utf8', cursor, end);
      return { value: JSON.parse(json), bytesRead: end - offset };
    }
    default:
      throw new Error(`Unknown serialized datum type: ${type}`);
  }
}

function compareSerializedValues(a, b) {
  const left = serializeDatum(a);
  const right = serializeDatum(b);
  return Buffer.compare(left, right);
}

module.exports = {
  serializeDatum,
  deserializeDatum,
  compareSerializedValues
};
