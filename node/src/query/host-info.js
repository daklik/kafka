'use strict';

class HostInfo {
  constructor(host, port, protocol = 'http') {
    if (!host) {
      throw new Error('HostInfo requires a host');
    }
    if (port === undefined || Number.isNaN(Number(port))) {
      throw new Error('HostInfo requires a numeric port');
    }
    this.host = host;
    this.port = Number(port);
    this.protocol = protocol ?? 'http';
  }

  static from(value) {
    if (!value) {
      return null;
    }
    if (value instanceof HostInfo) {
      return value;
    }
    if (typeof value === 'string') {
      if (value.includes('://')) {
        const url = new URL(value);
        if (!url.port) {
          throw new Error('Host string must include an explicit port');
        }
        return new HostInfo(url.hostname, url.port, url.protocol.replace(':', '') || 'http');
      }
      const [host, port] = value.split(':');
      if (!host || port === undefined) {
        throw new Error('Host string must include host and port (host:port)');
      }
      return new HostInfo(host, port);
    }
    if (typeof value === 'object') {
      const { host, port, protocol } = value;
      if (!host || port === undefined) {
        throw new Error('HostInfo object must include host and port');
      }
      return new HostInfo(host, port, protocol);
    }
    throw new Error('Unable to construct HostInfo from value');
  }

  equals(other) {
    if (!other) {
      return false;
    }
    return this.host === other.host && this.port === other.port && this.protocol === (other.protocol ?? 'http');
  }

  toString() {
    return `${this.protocol ?? 'http'}://${this.host}:${this.port}`;
  }

  toJSON() {
    return { host: this.host, port: this.port, protocol: this.protocol };
  }
}

module.exports = { HostInfo };
