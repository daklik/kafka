# Processor API Guide

Node Kafka Streams now exposes the Processor API primitives familiar to Java users. This guide walks through wiring processors, transformers, state stores, and punctuators using the new DSL helpers.

## Transforming Records with `transformValues`

Use `KStream#transformValues` when you want to reuse `ValueTransformer` implementations that depend on `ProcessorContext` metadata or state stores:

```js
const { StreamsBuilder, KafkaStreams, Serde, Stores } = require('kafka-streams-node');

const builder = new StreamsBuilder();
const storeBuilder = Stores.inMemoryKeyValueStore('value-counts');

builder
  .stream('input-topic', { keySerde: Serde.string(), valueSerde: Serde.string() })
  .transformValues(() => ({
    async init(context) {
      await context.register(storeBuilder);
      this.store = context.getStateStore('value-counts');
    },
    async transform(value) {
      const key = value.toLowerCase();
      const current = (await this.store.get(key)) ?? 0;
      const next = current + 1;
      await this.store.put(key, next);
      return `${key}:${next}`;
    }
  }), { stateStore: storeBuilder })
  .to('output-topic', { valueSerde: Serde.string() });

const kafkaStreams = new KafkaStreams(builder, { applicationId: 'transform-values-example' });
await kafkaStreams.start();
```

The `stateStore` option automatically wires the store into the topology so `context.register` and `context.getStateStore` resolve the same instance.

## Custom Processors with Forwarding and Scheduling

`KStream#process` creates traditional `Processor` nodes that can maintain state, forward downstream, and schedule punctuators:

```js
const { StreamsBuilder, KafkaStreams, Serde, Stores, processor: { PunctuationType } } = require('kafka-streams-node');

const builder = new StreamsBuilder();
const storeBuilder = Stores.inMemoryKeyValueStore('account-totals');

builder
  .stream('transactions', { keySerde: Serde.string(), valueSerde: Serde.json() })
  .process(() => ({
    async init(context) {
      this.context = context;
      await context.register(storeBuilder);
      this.store = context.getStateStore('account-totals');
      context.schedule(60_000, async () => {
        const entries = await this.store.entries();
        console.log('Periodic totals', entries);
      }, { type: PunctuationType.WALL_CLOCK_TIME });
    },
    async process(record) {
      if (record.key == null) {
        return;
      }
      const current = (await this.store.get(record.key)) ?? 0;
      const next = current + Number(record.value?.amount ?? 0);
      await this.store.put(record.key, next);
      await this.context.forward({ key: record.key, value: next });
    }
  }), { stateStore: storeBuilder })
  .to('account-updates', { valueSerde: Serde.json() });

const kafkaStreams = new KafkaStreams(builder, { applicationId: 'processor-example' });
await kafkaStreams.start();
```

Processors now share the same scheduling infrastructure as the Java runtime via `ProcessorContext#schedule`. Forwarded records continue through the remaining DSL steps before being written to sinks.

## Compatibility Harness Fixtures

`node/test/compatibility-harness.test.js` exercises both the DSL and Processor API pathways against fixtures that mirror the Java reference suite. Add new fixtures under `node/test/fixtures` and extend the harness to compare Node output with Java baselines.

## Sample Application

The repository includes `node/examples/processor-pipeline.js`, showcasing a combined `transformValues` and `process` topology with scheduled reporting. Run it with:

```bash
node node/examples/processor-pipeline.js
```

The sample script logs state store updates and demonstrates how to bootstrap stores outside of test environments.

## Further Reading

- [Phase 3 Gap Analysis](phase3-gap-analysis.md) – detailed roadmap for Processor API parity.
- [Processor Context tests](../test/processor-context.test.js) – examples covering `ProcessorContext` helpers, scheduling, and state restoration.
