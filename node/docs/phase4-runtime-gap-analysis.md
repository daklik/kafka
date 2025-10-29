# Phase 4 Gap Analysis – Runtime, Tasks, and Exactly-Once Semantics

## Overview
Phase 4 turns the Node.js Kafka Streams port into a production-ready runtime by mirroring the Java task model, thread orchestration, cooperative assignment, exactly-once-v2 semantics, and state restoration flows. The work builds on the Processor API infrastructure delivered in Phase 3 and focuses on long-running runtime behaviours such as rebalances, commit management, transactions, and changelog replay.

## 1. Goals and Success Criteria
- **Thread & Task Model**: Provide a `StreamThread` runtime that tracks per-partition task lifecycle (`CREATED → RESTORING → RUNNING → REVOKED`) and surfaces transitions for observability.
- **Assignment & Rebalance**: Offer an assignment coordinator capable of distributing active and standby tasks across members while preserving prior ownership when possible, emulating `StreamsPartitionAssignor` strategies.
- **Exactly-Once Processing**: Support `processing.guarantee=exactly_once_v2` via a dedicated `TransactionManager` that wraps transactional producers, forwards offsets, and gracefully aborts on failures.
- **Commit Management**: Honor `commit.interval.ms` for at-least-once streams, issuing consumer commits on schedule without starving busy partitions.
- **State Restoration**: Replay changelog batches through the `StateStoreManager`, emit restore lifecycle events, and transition tasks from `RESTORING` to `RUNNING` once replay completes.
- **Task Metrics**: Track standby lag and task metadata to fuel future metrics surfaces and compatibility checks.

## 2. Current Node.js Baseline
- `KafkaStreams` orchestrated consumers and producers per source stream but lacked explicit thread abstractions, transactional handling, or cooperative assignment helpers.
- `TaskManager` tracked offsets and punctuators but did not enforce task-state transitions or standby metadata.
- State store lifecycle hooks existed, yet changelog replay and restore listeners were only invoked during initial builder registration.
- Exactly-once guarantees and transactional APIs were absent, so `processing.guarantee` values beyond `at_least_once` were ignored.

## 3. Step-by-Step Plan

### Step 1 – Model Task Lifecycle & Standby Metadata (P0)
**Status:** ✅ Complete.
1. Extend `TaskManager` with explicit states (`CREATED`, `ASSIGNED`, `RESTORING`, `RUNNING`, `SUSPENDED`, `REVOKED`, `FAILED`) and guard invalid transitions.
2. Emit `task.transition` and `task.lag` events to expose lifecycle changes and standby lag updates.
3. Support `assign()`, `updateState()`, `markStandbyLag()`, and enhanced `snapshot()` outputs for downstream monitoring and cooperative assignors.

### Step 2 – Introduce StreamThread Runtime (P0)
**Status:** ✅ Complete.
1. Add `runtime/stream-thread.js` encapsulating per-stream thread behaviour, wrapping `consumer.run()` handlers.
2. Coordinate commit cadence via injected clocks, honouring `commit.interval.ms` for at-least-once workloads while delegating EOS commits to the transaction layer.
3. Re-emit task transition events, forward commit notifications, and surface runtime errors back to `KafkaStreams`.

### Step 3 – Cooperative Assignment Utilities (P1)
**Status:** ✅ Complete.
1. Ship `runtime/assignment-coordinator.js` with a `planAssignments()` helper that balances active/standby tasks across members.
2. Retain sticky ownership when previous assignments are provided, mirroring Java's cooperative assignor heuristics.
3. Ensure standby placement avoids active owners where possible to maximise redundancy.

### Step 4 – Transaction & Exactly-Once Layer (P0)
**Status:** ✅ Complete.
1. Implement `TransactionManager` to wrap callback-based or imperative transactional producers.
2. Forward offsets via `sendOffsetsToTransaction` or transactional callbacks, aborting on handler failures and emitting diagnostics.
3. Integrate the manager within `StreamThread` and `KafkaStreams` so EOS handling is automatic when `processing.guarantee=exactly_once_v2`.

### Step 5 – State Restoration Lifecycle (P1)
**Status:** ✅ Complete.
1. Extend `StateStoreManager` with `getDefinition()` and `restoreStore()` to replay changelog batches outside of initial store construction.
2. Wire `StreamThread#restoreTask()` to call into the manager, propagate restore events, and advance task state to `RUNNING` upon completion.
3. Emit metrics-compatible restore payloads and maintain backwards compatibility with existing store registration flows.

### Step 6 – Runtime Integration & Testing (P0)
**Status:** ✅ Complete.
1. Update `KafkaStreams` to instantiate `StreamThread` per source stream, register transaction and task events, and delegate message processing through the new runtime.
2. Add comprehensive Node.js test coverage for task transitions, commit cadence, EOS commit/abort flows, assignment planning, and restore lifecycle callbacks.
3. Export new runtime utilities through `node/src/index.js` for downstream consumers and future integration tests.

## 4. Dependencies and Risks
| Area | Status | Notes |
| --- | --- | --- |
| Kafka client transactional APIs | ⚠️ Mitigated | `TransactionManager` gracefully degrades when producers lack transactional methods, but real cluster validation is required once integration tests run. |
| Cooperative rebalance fidelity | ⚠️ Pending validation | Assignment helper mirrors core heuristics but needs cross-language verification with Java compatibility fixtures in later phases. |
| Standby lag metrics | ✅ Captured | `TaskManager#markStandbyLag` retains per-store lag data for future metrics reporters. |
| Restore replay order | ✅ Covered | Restore batches route through `StateStoreManager` lifecycle hooks, aligning with Java `StateRestoreListener` semantics. |

## 5. Deliverables
- Enhanced `TaskManager` with lifecycle transitions, standby metadata, and assignment APIs.
- New `StreamThread`, `TransactionManager`, and assignment coordinator modules exported for runtime orchestration.
- Updated `KafkaStreams` runtime integrating commit cadence, EOS transactions, and task/restore events.
- Extended `StateStoreManager` capable of replaying changelog batches on demand.
- Node.js test suites covering task management, stream threads, assignment planning, and transactional behaviour.

## 6. Exit Criteria
Phase 4 is complete when:
- Tasks transition through lifecycle states with validation, and standby lag is observable programmatically.
- At-least-once streams commit on schedule without interfering with EOS streams, which rely on transactions instead of consumer commits.
- Restoring tasks invoke changelog replay, emit lifecycle callbacks, and enter `RUNNING` once complete.
- Assignment planning balances tasks and preserves sticky ownership, ready for future member metadata integration.
- Test suites cover runtime scenarios, including transactional success and failure paths.

## 7. Exit Criteria Validation
- [x] **Task lifecycle instrumentation** – `TaskManager` enforces transitions, emits events, and snapshots metadata for observability.
- [x] **Runtime threading & commits** – `StreamThread` wraps message handling, honours commit intervals, and surfaces commit notifications.
- [x] **Exactly-once transactions** – `TransactionManager` coordinates begin/commit/abort flows with offset forwarding for EOS workloads.
- [x] **State restoration** – `restoreStore()` replays changelog batches, triggering restore listeners and transitioning tasks to `RUNNING`.
- [x] **Assignment planning** – `planAssignments()` balances active/standby tasks while preserving stickiness, covering cooperative rebalance requirements.
- [x] **Comprehensive tests** – New runtime-focused unit tests validate lifecycle transitions, commit cadence, transactions, restoration, and assignment balancing.
