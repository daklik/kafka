'use strict';

const {
  StreamsBuilder,
  KafkaStreams,
  Serde,
  Stores,
  processor: { PunctuationType }
} = require('../src');

const builder = new StreamsBuilder();
const normalizationStore = Stores.inMemoryKeyValueStore('normalization-counts');
const totalsStore = Stores.inMemoryKeyValueStore('totals');

builder
  .stream('orders', { keySerde: Serde.string(), valueSerde: Serde.json() })
  .transformValues(() => ({
    async init(context) {
      await context.register(normalizationStore);
      this.store = context.getStateStore('normalization-counts');
    },
    async transform(order) {
      if (!order?.id) {
        return order;
      }
      const sku = order.sku?.toLowerCase() ?? 'unknown';
      const current = (await this.store.get(sku)) ?? 0;
      await this.store.put(sku, current + 1);
      return { ...order, sku };
    }
  }), { stateStore: normalizationStore })
  .process(() => ({
    async init(context) {
      this.context = context;
      await context.register(totalsStore);
      this.store = context.getStateStore('totals');
      context.schedule(30_000, async () => {
        const entries = await this.store.entries();
        console.log('[punctuate] Current order totals:', entries);
      }, { type: PunctuationType.WALL_CLOCK_TIME });
    },
    async process(record) {
      if (record.key == null || !record.value) {
        return;
      }
      const amount = Number(record.value.amount ?? 0);
      const current = (await this.store.get(record.key)) ?? 0;
      const next = current + amount;
      await this.store.put(record.key, next);
      await this.context.forward({ key: record.key, value: { ...record.value, runningTotal: next } });
    }
  }), { stateStore: totalsStore })
  .to('order-updates', { valueSerde: Serde.json() });

const kafkaStreams = new KafkaStreams(builder, {
  applicationId: 'processor-pipeline-sample',
  client: {
    brokers: ['localhost:9092']
  }
});

kafkaStreams.on('error', error => console.error('[streams error]', error));

async function main() {
  await kafkaStreams.start();
  console.log('Processor pipeline started. Send records to the "orders" topic to observe transformations.');
}

main().catch(error => {
  console.error('Failed to start processor pipeline', error);
  process.exitCode = 1;
});
