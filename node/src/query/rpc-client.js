'use strict';

class QueryRpcClient {
  async fetchKeyValue() {
    throw new Error('Query RPC client not configured');
  }

  async fetch() {
    throw new Error('Query RPC client not configured');
  }
}

module.exports = { QueryRpcClient };
