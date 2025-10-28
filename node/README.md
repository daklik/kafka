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
- `through(topic, options)`
- `to(topic, options)`

### KafkaStreams

- `new KafkaStreams(builderOrTopology, config)`
- `start()` / `stop()`
- Access to state stores registered via aggregations
- Emits `started`, `stopped`, and `error` events

### Serdes

- `Serde.string()`
- `Serde.json()`
- Create a custom serde with `new Serde({ serialize, deserialize })`

### State Stores

Aggregations materialize into local state stores. By default an in-memory key value store (`MemoryStateStore`) is used, but custom stores can be provided per aggregation via the `store` option.

## Configuration

The Kafka client configuration mirrors the options expected by `@confluentinc/kafka-javascript`:

```js
const kafkaStreams = new KafkaStreams(builder, {
  client: {
    clientId: 'my-app',
    brokers: ['kafka:9092'],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: 'user',
      password: 'password'
    }
  },
  groupIdPrefix: 'my-streams'
});
```

Optional factories can be provided to override consumer and producer creation:

```js
const kafkaStreams = new KafkaStreams(builder, {
  kafka: existingKafkaClient,
  consumerFactory: async (stream, groupId) => myKafka.createConsumer({ groupId }),
  producerFactory: async () => myKafka.createProducer()
});
```

## Development

```bash
npm install
npm test
```

## Documentation

- [Roadmap](docs/roadmap.md)
- [Phase 1 Gap Analysis](docs/phase1-gap-analysis.md)

## License

[Apache License 2.0](../LICENSE)
