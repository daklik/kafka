# Kafka Streams Node

A production-ready Node.js implementation of the Kafka Streams Java library, built on top of [`@confluentinc/kafka-javascript`](https://www.npmjs.com/package/@confluentinc/kafka-javascript).

## Installation

```bash
npm install kafka-streams-node
```

## Usage

```js
const { StreamsBuilder, KafkaStreams, Serde, Materialized } = require('kafka-streams-node');

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

const usersTable = builder.table('users-topic', {
  keySerde: Serde.string(),
  valueSerde: Serde.json(),
  materialized: Materialized.as('users-store')
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
- `table(topic, options)` &rarr; materializes a changelog topic as a queryable `KTable`.
- `globalTable(topic, options)` &rarr; consumes a topic on every instance as a `GlobalKTable`.
- `build()` &rarr; returns the topology description used by `KafkaStreams`.

### KStream

Transformation and branching primitives mirror the Java DSL:

- `map(mapper)` / `mapValues(mapper)` / `mapKeys(mapper)` / `selectKey(selector)`
- `filter(predicate)` / `filterNot(predicate)`
- `flatMap(mapper)` / `flatMapValues(mapper)`
- `peek(sideEffect)` / `foreach(sideEffect)`
- `groupBy(selector)` / `groupByKey()` returning a `KGroupedStream` for windowed aggregations
- `aggregate(initializer, aggregator, options)` / `count(options)` / `reduce(reducer, options)`
- `branch(...predicates)`
- `repartition(options)`
- `through(topic, options)`
- `to(topic, options)`
- `join/leftJoin/outerJoin` for stream-table and stream-stream joins with configurable serdes and window definitions

### KTable & GlobalKTable

- `builder.table(topic, options)` materializes changelog topics into local state stores. Combine with `Materialized`/`Named` for Java-parity naming and serde configuration.
- `builder.globalTable(topic, options)` mirrors Java's `globalTable`, consuming the full topic on each instance using a shared consumer group derived from the application id.
- Materialized stores are registered with the interactive query service, enabling `KafkaStreams#store()` and `#metadataForStore()` to return table handles consistent with the Java API.

#### Stream-Table Joins

Materialized tables can be joined back to streams using the familiar Java DSL operators. Configure serdes with the `Joined` helper and optional `ValueJoiner` wrapper when you want a reusable join function:

```js
const { StreamsBuilder, Serde, Materialized, Joined, ValueJoiner } = require('kafka-streams-node');

const builder = new StreamsBuilder();
const orders = builder.stream('orders-topic', { keySerde: Serde.string(), valueSerde: Serde.json() });
const customers = builder.table('customers-topic', {
  keySerde: Serde.string(),
  valueSerde: Serde.json(),
  materialized: Materialized.as('customers-store')
});

orders
  .leftJoin(
    customers,
    ValueJoiner.with((order, customer) => ({
      orderId: order.id,
      customerName: customer?.name ?? null
    })),
    { joined: Joined.withKeyValueSerde(Serde.string(), Serde.json()) }
  )
  .to('enriched-orders', { valueSerde: Serde.json() });
```

#### Stream-Stream Joins and Windows

Stream-stream joins mirror the Java DSL by buffering each side in a windowed state store and emitting matches based on window boundaries. Use the `windows.JoinWindows` helper for symmetric before/after windows or `windows.SlidingWindows` for time-difference joins. The join output is driven by the stream that declares the join (mirroring how Java's `KStream#join` returns a new stream derived from the caller).

```js
const { StreamsBuilder, Serde, Joined, ValueJoiner, windows } = require('kafka-streams-node');

const builder = new StreamsBuilder();
const purchases = builder.stream('purchases-topic', { keySerde: Serde.string(), valueSerde: Serde.json() });
const clicks = builder.stream('clicks-topic', { keySerde: Serde.string(), valueSerde: Serde.json() });

purchases
  .join(
    clicks,
    ValueJoiner.with((purchase, click) => ({
      purchaseId: purchase.id,
      clickUrl: click?.url ?? null
    })),
    {
      window: windows.JoinWindows.of(5_000),
      joined: Joined.withKeyValueSerde(Serde.string(), Serde.json())
    }
  )
  .to('purchases-with-clicks', { valueSerde: Serde.json() });
```

By default the window grace period matches Java's 24 hour default; override it with `.grace(ms)` on the window specification. The DSL now exposes `TimeWindows`, `SessionWindows`, `UnlimitedWindows`, and `SlidingWindows` for aggregation and join use cases.

#### Windowed Aggregations

Grouping operations return a `KGroupedStream` so you can apply windowed aggregations with Java-parity semantics. Tumbling and hopping windows use `windows.TimeWindows`, sliding windows use `windows.SlidingWindows`, and session windows rely on `windows.SessionWindows` (with default session mergers for `count` and `reduce`). Unlimited windows provide open-ended aggregates that emit on each update.

```js
const { StreamsBuilder, Serde, windows } = require('kafka-streams-node');

const builder = new StreamsBuilder();

builder
  .stream('orders-topic', { keySerde: Serde.string(), valueSerde: Serde.json() })
  .groupBy(order => order.category)
  .windowedBy(windows.TimeWindows.of(60_000).advanceBy(30_000))
  .count({ keySerde: Serde.string(), storeName: 'category-counts' })
  .to('category-counts-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });

builder
  .stream('sessions-topic', { keySerde: Serde.string(), valueSerde: Serde.json() })
  .groupBy(value => value.userId)
  .windowedBy(windows.SessionWindows.with(5_000))
  .reduce((left, right) => ({ ...left, ...right }), { keySerde: Serde.string(), storeName: 'user-sessions' })
  .to('user-sessions-topic', { keySerde: Serde.json(), valueSerde: Serde.json() });
```

Windowed stores are registered with the interactive query service and include metadata describing the window type, retention, and changelog configuration so remote instances can route queries to the correct host.

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

Aggregations and table sources materialize into local state stores. By default an in-memory key value store (`MemoryStateStore`) is used, but custom stores can be provided per aggregation or table via the `store` option.

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
