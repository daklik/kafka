# Phase 3 Gap Analysis – Processor API & Topology Infrastructure

## Overview
Phase 3 builds on the DSL parity delivered in Phase 2 and focuses on porting the Java Kafka Streams Processor API and supporting topology infrastructure to Node.js. The goal is to unlock custom processors, punctuators, and advanced topology rewrites so that Java blueprints can execute unchanged against the Node runtime.

## 1. Goals and Success Criteria
- **Processor API Surface**: Provide `Processor`, `ProcessorSupplier`, `Transformer`, and `ValueTransformer` equivalents with async-friendly lifecycles that match Java semantics.
- **Context & Forwarding Semantics**: Mirror `ProcessorContext`, `RecordContext`, `To`, and `ForwardingDisabledException` behaviors, including record metadata access, headers, and downstream forwarding controls.
- **Scheduling & Punctuation**: Support wall-clock and stream-time `Punctuator`s with `schedule()` APIs that integrate with the runtime task loop.
- **State Store Registration**: Allow custom processors to register state stores, restore changelogged state, and receive callbacks via `StateRestoreListener` analogs.
- **Topology Builder Enhancements**: Extend `StreamsBuilder`/`Topology` so processor nodes, source/sink optimizations, and named topologies align with Java `TopologyBuilder` outputs.
- **Testing & Documentation**: Add unit/integration coverage that exercises custom processors, punctuation, and store restoration; document APIs for early adopters.

## 2. Current Node.js Baseline
- `StreamsBuilder`, `Topology`, and runtime task manager exist under `node/src/streams-builder.js`, `node/src/topology`, and `node/src/runtime`, delivering DSL parity and topology description.
- `StreamsConfig`, metrics, exception handlers, and query services from Phases 1–2 provide configuration, observability, and interactive query hooks.
- State store abstractions under `node/src/state` support in-memory KV/window/session stores, changelog metadata, and builder registration.
- DSL operators (`kstream`, `ktable`, `materialized`, `suppressed`, `windows`) already register state stores, enabling reuse for Processor API store integration.

## 3. Step-by-Step Plan

### Step 1 – Define Processor API Types (P0)
**Status:** ✅ Complete.
1. Port `Processor`, `ProcessorSupplier`, `Transformer`, and `ValueTransformer` interfaces into a new `node/src/processor` package.
2. Implement lifecycle hooks (`init`, `process`, `close`) returning Promises to accommodate async work while preserving sync compatibility.
3. Add compatibility shims for legacy `ProcessorSupplier` signatures (`() => Processor`) and Node-friendly class-based processors.

### Step 2 – Implement Processor Context & Record Metadata (P0)
**Status:** ✅ Complete.
1. Added a promise-friendly `ProcessorContext` that surfaces `forward`, `commit`, `schedule`, `currentNode`, `recordContext`, and `headers` accessors with forwarding guard rails.
2. Introduced immutable `RecordContext` metadata (topic, partition, offset, timestamp, headers) and threaded it through the runtime message pipeline.
3. Published a `To` routing helper and `ForwardingDisabledException` that mirror Java semantics for downstream targeting and suppression safety.
4. Connected context propagation to the existing DSL runtime so future Processor API nodes can share forwarding and metadata infrastructure.

### Step 3 – Scheduling & Punctuation (P1)
**Status:** ✅ Complete.
1. Extended `node/src/runtime/task-manager.js` with a unified scheduler that registers wall-clock and stream-time punctuators per task, deduplicating repeated schedules and cancelling them during rebalances.
2. Ported Java `PunctuationType` semantics, exposing a shared enum through the processor module and validating intervals and types inside `ProcessorContext#schedule`.
3. Threaded timestamp-aware bookkeeping through `TaskManager#recordProcessed`, allowing stream-time punctuators to fire deterministically as KafkaStreams advances record metadata.
4. Added unit tests with a fake clock to cover firing order, stream-time advancement, deduplication, and cancellation across wall-clock and stream-time scenarios.

### Step 4 – State Store Registration & Restoration (P1)
**Status:** ✅ Complete.
1. Added `ProcessorContext#register`/`getStateStore` helpers that wire StoreBuilder instances into task state, normalise listeners, and surface runtime store handles.
2. Introduced a shared `StateStoreManager` that records changelog metadata, instantiates stores through the existing builders, and exposes instances for interactive queries.
3. Emitted state-restore lifecycle events (`state.restore.start/batch/end`) with metrics hooks and listener callbacks mirroring Java’s `StateRestoreListener` contract.
4. Exercised processor-managed store registration with unit tests covering changelog offsets, lifecycle events, and metrics emission.

### Step 5 – Topology Builder Enhancements (P1)
**Status:** ✅ Complete.
1. Added `StreamsBuilder#addSource`, `addProcessor`, `addSink`, `addStateStore`, and `connectProcessorAndStateStores` APIs so custom processor topologies can be assembled without the DSL and reuse the same metadata pipeline as DSL graphs.
2. Centralised node/state-store registration so topology metadata deduplicates sources, processors, and changelog-backed stores while preserving parent/child relationships.
3. Enriched topology descriptions with processor node details (supplier hints, edge metadata, attached stores) via a shared `processorTopology` export for debugging parity.
4. Hooked DSL builders into the shared node/state-store registry to ensure both DSL and Processor API paths rely on the same topology internals going forward.

### Step 6 – Testing, Samples, and Documentation (P2)
**Status:** ✅ Complete.
1. Added `processor-transformers.test.js` with end-to-end coverage for `transformValues` and `process` operations, validating state store registration and forwarded outputs alongside existing punctuator and context suites.
2. Extended the compatibility harness with a Processor API fixture that mirrors Java `transformValues` behaviour so Node runs now assert against shared expectations.
3. Published a dedicated Processor API guide, README pointers, and roadmap updates detailing how to register stores, schedule punctuators, and chain processors.
4. Introduced a runnable sample under `node/examples` illustrating processor chaining and scheduled reporting to demonstrate ergonomics in practice.

## 4. Dependencies and Risks
| Area | Status | Notes |
| --- | --- | --- |
| Runtime scheduling | ✅ Validated | Wall-clock and stream-time punctuators are orchestrated through the `TaskManager` with fake-clock coverage to ensure deterministic firing and cancellation semantics. |
| Kafka client integration | ✅ Mitigated | Existing consumer/producer factories already expose hooks for commit and metadata retrieval; Processor API work reuses these surfaces. |
| State restoration | ✅ Validated | The `StateStoreManager` emits restore lifecycle events, registers listener hooks, and records metrics; unit tests cover offsets, batches, and interactive store access. |
| Backpressure & forwarding | ⚠️ Tracking | `ProcessorContext` now guards `forward()` with `ForwardingDisabledException`, but async-heavy processors may still need cooperative yielding guidance in later phases. |

## 5. Deliverables
- Processor API modules under `node/src/processor` with parity interfaces and lifecycle handling.
- Enhanced runtime (`node/src/runtime`) supporting context creation, scheduling, and restore callbacks.
- Updated topology builder and description to accommodate processor nodes and store attachments.
- Comprehensive tests and compatibility fixtures validating Processor API semantics.
- Updated documentation and samples guiding users through Processor API adoption.

## 6. Exit Criteria
Phase 3 is complete when:
- Custom processors built with the new API can be registered alongside DSL topologies and execute end-to-end with forwarding, punctuators, and state stores.
- Topology descriptions enumerate processor nodes, connected stores, and scheduling metadata comparable to Java outputs.
- State restoration for processor-managed stores succeeds with metrics emitted for progress.
- Test suites covering Processor API scenarios pass locally and in CI, with cross-language fixtures matching Java reference results.
- Documentation directs users through Processor API setup, punctuation, and store registration without referencing Java source.

## 7. Exit Criteria Validation
- [x] **Processor execution parity** – `KStream#process`/`transform`/`transformValues` register processor suppliers, attach state stores, and the runtime instantiates `ProcessorContext` instances that manage forwarding queues, commits, and scheduling hooks.【F:node/src/kstream.js†L516-L546】【F:node/src/kafka-streams.js†L932-L995】
- [x] **Topology metadata** – The shared builder registry exposes processor nodes, parent/child links, and attached state stores for both manual and DSL pipelines, exercised by targeted metadata tests.【F:node/test/processor-topology-builder.test.js†L1-L72】
- [x] **State restoration lifecycle** – Processor-managed stores trigger restore events, listener callbacks, and metrics emissions validated by unit tests covering offsets and batch markers.【F:node/src/runtime/state-store-manager.js†L1-L168】【F:node/test/processor-state-store.test.js†L1-L92】
- [x] **Scheduling fidelity** – Wall-clock and stream-time punctuators fire deterministically under fake-clock control and deduplicate registrations, matching Java semantics.【F:node/src/runtime/task-manager.js†L1-L140】【F:node/test/processor-scheduling.test.js†L1-L88】
- [x] **Cross-language fixtures & docs** – Compatibility harness fixtures confirm Processor API outputs align with Java expectations, while documentation and examples guide adoption without relying on Java source code.【F:node/test/compatibility-harness.test.js†L1-L109】【F:node/docs/processor-api-guide.md†L1-L95】【F:node/examples/processor-pipeline.js†L1-L72】
