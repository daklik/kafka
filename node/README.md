# Kafka Streams Node

A production-ready Node.js implementation of the Kafka Streams Java library, built on top of [`@confluentinc/kafka-javascript`](https://www.npmjs.com/package/@confluentinc/kafka-javascript).

## Installation

```bash
npm install kafka-streams-node
```

## Usage

```js
const { StreamsBuilder, KafkaStreams, Serde } = require('kafka-streams-node');

const builder = new StreamsBuilder();
const stream = builder
  .stream('input-topic', {
    valueSerde: Serde.json()
  })
  .mapValues(value => ({ ...value, processedAt: Date.now() }))
  .filter((value) => value.active)
  .peek((value) => console.log('Processed value', value))
  .to('output-topic', {
    valueSerde: Serde.json()
  });

const kafkaStreams = new KafkaStreams(builder, {
  applicationId: 'example-stream-app',
  client: {
    brokers: ['localhost:9092']
  }
});

kafkaStreams.on('error', console.error);

(async () => {
  await kafkaStreams.start();
})();
```

## API Overview

### StreamsBuilder

- `stream(topic, options)` &rarr; creates a new `KStream` for the provided topic.
- `build()` &rarr; returns the topology description used by `KafkaStreams`.

### KStream

Transformation and branching primitives mirror the Java DSL:

- `map(mapper)` / `mapValues(mapper)` / `mapKeys(mapper)` / `selectKey(selector)`
- `filter(predicate)` / `filterNot(predicate)`
- `flatMap(mapper)` / `flatMapValues(mapper)`
- `peek(sideEffect)` / `foreach(sideEffect)`
- `groupBy(selector)` / `groupByKey()`
- `aggregate(initializer, aggregator, options)` / `count(options)` / `reduce(reducer, options)`
- `branch(...predicates)`
- `repartition(options)`
- `through(topic, options)`
- `to(topic, options)`
- `join/leftJoin/outerJoin` (API scaffolded – execution semantics coming in a later phase)

### KafkaStreams

- `new KafkaStreams(builderOrTopology, config)`
- `start()` / `stop()`
- Access to state stores registered via aggregations
- Emits `started`, `stopped`, and `error` events
- Interactive query surface via `store()`/`metadataForStore()` and the `InteractiveQueryService`

### Interactive Queries

Phase 1 now exposes the foundations for Java-style interactive queries. Configure an application server endpoint and optionally
an RPC client to route remote lookups:

```js
const { KafkaStreams, StreamsBuilder, Serde, query } = require('kafka-streams-node');

const builder = new StreamsBuilder();
builder
  .stream('input', { keySerde: Serde.string(), valueSerde: Serde.json() })
  .groupBy(value => value.category)
  .count({ storeName: 'category-counts' });

const rpcClient = new class extends query.QueryRpcClient {
  async fetchKeyValue({ hostInfo, storeName, key }) {
    // invoke your HTTP/gRPC endpoint here
    return fetch(`http://${hostInfo.host}:${hostInfo.port}/stores/${storeName}/${key}`).then(res => res.json());
  }
}();

const streams = new KafkaStreams(builder, {
  applicationId: 'interactive-app',
  applicationServer: { host: 'app-host', port: 8080 },
  interactiveQueries: { rpcClient }
});

const queryService = streams.getInteractiveQueryService();
const localStore = queryService.store('category-counts');
```

Metadata for registered stores is available via `metadataForStore(storeName)` mirroring Java's `KafkaStreams#metadataForLocalSto
res`.

### Serdes

- `Serde.string()`
- `Serde.json()`
- Create a custom serde with `new Serde({ serialize, deserialize })`

### State Stores

Aggregations materialize into local state stores. By default an in-memory key value store (`MemoryStateStore`) is used, but custom stores can be provided per aggregation via the `store` option.

Phase 1 introduces parity helpers inspired by the Java API:

- `Materialized` for fluent materialization options (`Materialized.as('name').withKeySerde(...).withStoreBuilder(...)`).
- `Named` for node naming parity.
- `Stores` registry and `StoreBuilder` abstractions for building reusable state store suppliers.

State store metadata includes changelog configuration hooks and caching/logging toggles, laying the groundwork for persistent stores in later phases.

### Runtime & Tasks

A lightweight `TaskManager` tracks stream-to-partition assignments, rebalance events, and offset progression. The runtime surfaces this information for future cooperative rebalancing and fault tolerance work.

## Configuration

`KafkaStreams` now consumes a first-class `StreamsConfig` surface that mirrors the Java `StreamsConfig` defaults. An
`applicationId` is required and drives the derived `clientId`, consumer `group.id`, and changelog topic prefixes.

```js
const { KafkaStreams, StreamsConfig } = require('kafka-streams-node');

const config = new StreamsConfig({
  applicationId: 'inventory-app',
  client: {
    clientId: 'inventory-client',
    brokers: ['kafka:9092'],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: 'user',
      password: 'password'
    }
  },
  processingGuarantee: 'exactly_once_v2'
});

const kafkaStreams = new KafkaStreams(builder, config);
```

Optional factories can be provided to override consumer and producer creation. Factories receive an options object with the
stream metadata, derived group id, and resolved config:

```js
const kafkaStreams = new KafkaStreams(builder, {
  applicationId: 'custom-factories',
  consumerFactory: async ({ stream, groupId, kafka }) => {
    return kafka.consumer({ groupId, allowAutoTopicCreation: false });
  },
  producerFactory: async ({ kafka }) => kafka.producer()
});
```

### Metrics and Exception Handlers

Phase 1 P1 introduces a lightweight metrics registry and Java-parity exception handler interfaces. Custom reporters can be
registered and will receive samples for consumption, production, and error counters:

```js
const { metrics, errors } = require('kafka-streams-node');

class ConsoleReporter extends metrics.MetricsReporter {
  record(sample) {
    console.log(sample);
  }
}

const kafkaStreams = new KafkaStreams(builder, {
  applicationId: 'instrumented-app',
  metrics: { reporters: [new ConsoleReporter()] },
  deserializationExceptionHandler: new errors.LogAndContinueExceptionHandler(),
  productionExceptionHandler: new errors.LogAndFailExceptionHandler()
});
```

## Development

```bash
npm install
npm test
```

The test suite now includes a compatibility harness (`node/test/compatibility-harness.test.js`) that executes shared fixtures ag
ainst the Node.js runtime and validates parity with Java word-count expectations.

## Documentation

- [Roadmap](docs/roadmap.md)
- [Phase 1 Gap Analysis](docs/phase1-gap-analysis.md)

## License

[Apache License 2.0](../LICENSE)
