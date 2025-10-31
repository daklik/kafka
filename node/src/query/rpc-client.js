'use strict';

const { HostInfo } = require('./host-info');

const DEFAULT_ENDPOINT = '/interactive-query/v1';

class QueryRpcClient {
  constructor({
    fetchImpl = typeof fetch === 'function' ? fetch.bind(globalThis) : null,
    basePath = DEFAULT_ENDPOINT,
    requestTimeoutMs = 5000,
    maxRetries = 2,
    retryBackoffMs = 200
  } = {}) {
    if (!fetchImpl) {
      throw new Error('QueryRpcClient requires a fetch implementation');
    }
    this.fetchImpl = fetchImpl;
    this.basePath = basePath.endsWith('/') ? basePath.slice(0, -1) : basePath;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxRetries = maxRetries;
    this.retryBackoffMs = retryBackoffMs;
  }

  async fetchKeyValue({ hostInfo, storeName, key, options = {} } = {}) {
    if (!storeName) {
      throw new TypeError('fetchKeyValue requires a storeName');
    }
    const host = HostInfo.from(hostInfo);
    const encodedStore = encodeURIComponent(storeName);
    const encodedKey = encodeURIComponent(String(key));
    const url = new URL(host.toString());
    url.pathname = `${this.basePath}/kv/${encodedStore}/${encodedKey}`;
    if (options.query && typeof options.query === 'object') {
      for (const [name, value] of Object.entries(options.query)) {
        if (value !== undefined && value !== null) {
          url.searchParams.set(name, String(value));
        }
      }
    }
    return this._requestJson(url, { method: 'GET', headers: options.headers });
  }

  async fetch(url, init = {}) {
    const target = typeof url === 'string' ? new URL(url) : url;
    return this._requestJson(target, init);
  }

  async _requestJson(url, init = {}) {
    const headers = new Headers(init.headers ?? {});
    if (!headers.has('accept')) {
      headers.set('accept', 'application/json');
    }
    const config = { ...init, headers };

    const response = await this._fetchWithRetry(url, config);
    const contentType = response.headers.get('content-type') ?? '';
    if (!contentType.includes('application/json')) {
      throw new Error(`Unexpected content type from query endpoint: ${contentType}`);
    }
    return response.json();
  }

  async _fetchWithRetry(url, init) {
    let attempt = 0;
    let lastError = null;
    while (attempt <= this.maxRetries) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
      try {
        const response = await this.fetchImpl(url, { ...init, signal: controller.signal });
        if (!response.ok) {
          if (response.status >= 500 && response.status < 600 && attempt < this.maxRetries) {
            await this._backoff(attempt);
            attempt += 1;
            continue;
          }
          const error = new Error(`Query RPC request failed with status ${response.status}`);
          error.status = response.status;
          throw error;
        }
        return response;
      } catch (error) {
        lastError = error;
        if (error.name === 'AbortError') {
          lastError = new Error('Query RPC request timed out');
        }
        if (attempt >= this.maxRetries) {
          throw lastError;
        }
        await this._backoff(attempt);
        attempt += 1;
      } finally {
        clearTimeout(timeout);
      }
    }
    throw lastError ?? new Error('Query RPC request failed');
  }

  async _backoff(attempt) {
    const delay = Math.max(this.retryBackoffMs, 0) * Math.pow(2, attempt);
    if (!delay) {
      return;
    }
    await new Promise(resolve => setTimeout(resolve, delay));
  }
}

module.exports = { QueryRpcClient };
