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
**Status:** ⏳ Pending.
1. Port `Processor`, `ProcessorSupplier`, `Transformer`, and `ValueTransformer` interfaces into a new `node/src/processor` package.
2. Implement lifecycle hooks (`init`, `process`, `close`) returning Promises to accommodate async work while preserving sync compatibility.
3. Add compatibility shims for legacy `ProcessorSupplier` signatures (`() => Processor`) and Node-friendly class-based processors.

### Step 2 – Implement Processor Context & Record Metadata (P0)
**Status:** ⏳ Pending.
1. Create `ProcessorContext` with APIs mirroring Java (`forward`, `commit`, `schedule`, `currentNode`, `recordContext`, `headers`).
2. Introduce a `RecordContext` struct capturing topic, partition, offset, timestamp, and headers. Bind it to the runtime via task execution.
3. Provide `To` helper for downstream routing (e.g., to named child, to all children) and enforce `ForwardingDisabledException` parity when forwarding is not allowed (e.g., during suppress emission).
4. Wire context creation through `StreamsBuilder#addProcessor` and `Topology#build` so DSL processors and new custom processors share infrastructure.

### Step 3 – Scheduling & Punctuation (P1)
**Status:** ⏳ Pending.
1. Extend the runtime task loop in `node/src/runtime/task-manager.js` with a scheduler capable of registering wall-clock and stream-time punctuators.
2. Port Java scheduling semantics (`PunctuationType.STREAM_TIME`, `PunctuationType.WALL_CLOCK_TIME`) including coalescing duplicate schedules and honoring `cancel()` semantics.
3. Expose scheduling via `ProcessorContext#schedule`, returning handles with `cancel()`; ensure compatibility with async task execution and Node timers.
4. Add tests covering punctual firing order, stream-time advancement, and cancellation across multiple processors.

### Step 4 – State Store Registration & Restoration (P1)
**Status:** ⏳ Pending.
1. Implement `ProcessorContext#getStateStore` and `register` semantics to attach custom stores defined via `Stores.storeBuilder`.
2. Integrate with existing changelog metadata so registered stores automatically enlist for restoration and logging.
3. Provide `StateRestoreListener` equivalents in the runtime to emit `onRestoreStart`, `onBatchRestored`, and `onRestoreEnd` events, reusing metrics hooks from Phase 1.
4. Expand compatibility harness fixtures to cover processor-managed stores and changelog replay scenarios.

### Step 5 – Topology Builder Enhancements (P1)
**Status:** ⏳ Pending.
1. Add `StreamsBuilder#addSource`, `addProcessor`, `addSink`, `addStateStore`, and `connectProcessorAndStateStores` entry points so custom topologies can be assembled without the DSL.
2. Port optimization passes from Java (`TopologyMetadata`, `OptimizableRepartitionNode`, source reuse) to deduplicate nodes where possible.
3. Update topology description to include processor node details (e.g., suppliers, punctuation schedules, connected stores) for debugging parity.
4. Ensure DSL-generated topologies leverage the new internals to avoid divergence between DSL and Processor API paths.

### Step 6 – Testing, Samples, and Documentation (P2)
**Status:** ⏳ Pending.
1. Add unit tests for processors, transformers, punctuators, and state store registration under `node/test` mirroring Java `ProcessorTopologyTest` scenarios.
2. Extend the compatibility harness to execute Java Processor API reference topologies (e.g., `transformValues`, punctuator-based sessionization) against shared fixtures.
3. Publish documentation updates in `node/README.md` and new guides describing how to build custom processors, register stores, and interact with punctuators.
4. Provide sample applications demonstrating processor chaining and schedule usage to validate ergonomics.

## 4. Dependencies and Risks
| Area | Status | Notes |
| --- | --- | --- |
| Runtime scheduling | ⚠️ Risk | Node's single-threaded event loop requires careful handling of long-running punctuators; may need worker threads or cooperative yielding to avoid blocking record processing. |
| Kafka client integration | ✅ Mitigated | Existing consumer/producer factories already expose hooks for commit and metadata retrieval; Processor API work reuses these surfaces. |
| State restoration | ⚠️ Risk | Changelog replay for custom stores may surface performance issues; plan incremental roll-out with metrics to monitor restore durations. |
| Backpressure & forwarding | ⚠️ Risk | Processor chains with heavy async work could overwhelm downstream nodes; need guardrails in `forward` to detect unbounded buffering. |

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
