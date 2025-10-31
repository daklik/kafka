'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { query } = require('../src');

function createResponse({ status, body }) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: {
      get(name) {
        if (name.toLowerCase() === 'content-type') {
          return 'application/json';
        }
        return null;
      }
    },
    async json() {
      return body;
    }
  };
}

test('query rpc client retries transient failures', async () => {
  const invocations = [];
  const responses = [
    createResponse({ status: 503, body: { error: 'unavailable' } }),
    createResponse({ status: 200, body: { key: 'alpha', value: 'value' } })
  ];

  const fetchImpl = async (url, init) => {
    invocations.push({ url: url.toString(), init });
    return responses.shift();
  };

  const client = new query.QueryRpcClient({
    fetchImpl,
    retryBackoffMs: 1,
    maxRetries: 1,
    requestTimeoutMs: 50
  });

  const result = await client.fetchKeyValue({
    hostInfo: { host: 'remote-host', port: 8443 },
    storeName: 'store-A',
    key: 'alpha'
  });

  assert.deepEqual(result, { key: 'alpha', value: 'value' });
  assert.equal(invocations.length, 2);
  assert(invocations[0].url.endsWith('/interactive-query/v1/kv/store-A/alpha'));
  assert.equal(invocations[0].init.method, 'GET');
});
