# Phase 2 Gap Analysis – DSL Parity

## Overview
Phase 2 focuses on achieving parity between the Java and Node.js Kafka Streams DSLs. This document decomposes the remaining DSL features, enumerates the Java references under `streams/src/main/java/org/apache/kafka/streams/kstream`, and maps the required work onto the existing Node.js modules in `node/src`. The plan is organized into incremental steps so that features can land in small, testable batches.

## 1. Goals and Success Criteria
- **API Coverage**: Expose the same `KStream`, `KTable`, and `GlobalKTable` operators as Java `3.7.x`, including overloads that accept `Named`, `Materialized`, and `Joined` parameters.
- **Behavioral Parity**: Match Java semantics for windowing, joins, suppressions, and materialization, including repartitioning and changelog requirements.
- **Topology Introspection**: Provide `TopologyDescription` output that surfaces nodes, stores, and processor links comparable to Java's `Topology.describe()` output for DSL topologies.
- **Test Coverage**: Back each feature with regression tests that mirror the Java integration suite, reusing or expanding the cross-language fixtures introduced in Phase 1.

## 2. Current Node.js Baseline
- `StreamsBuilder` and `KStream` support stateless transforms, branching, repartitioning, aggregation scaffolding, materialization helpers, stream-table joins, and stream-stream joins with window buffering.
- `KTable` and `GlobalKTable` abstractions are implemented with materialized stores wired into the interactive query layer.
- Window definitions (`TimeWindows`, `SessionWindows`, `UnlimitedWindows`, `SlidingWindows`) exist for joins and aggregations, and grouping operations expose `KGroupedStream` windowed aggregations with in-memory window/session stores.
- Topology descriptions report basic node linkage but still omit detailed windowed processor metadata and partitioning information.

## 3. Step-by-Step Plan

### Step 1 – Establish Table Foundations (P0)
**Status:** ✅ Completed in the initial Phase 2 deliverable. `KTable`/`GlobalKTable` sources, materialization, changelog metadata, and interactive query integration now exist in the Node port.
1. Port `KTable` and `GlobalKTable` classes, mirroring Java interfaces (`KTable.java`, `GlobalKTable.java`) with Node-friendly async patterns.
2. Implement table materialization via `Materialized` helpers, leveraging the existing state store builders; ensure changelog topics are registered through `Topology` metadata.
3. Extend `StreamsBuilder` to register table sources (`table()`, `globalTable()`) and connect to changelog/backing topics.
4. Add unit tests that validate table subscription, materialization, and read-only access through interactive queries.

### Step 2 – Implement Stream-Table Joins (P0)
**Status:** ✅ Completed with the stream-table join implementation. The Node runtime now supports `join`/`leftJoin`/`outerJoin` between streams and materialized tables, exposes `Joined`/`ValueJoiner` helpers, and validates the behavior with regression tests.
1. Introduce `Joined` and `ValueJoiner` helpers consistent with Java's `Joined` builder (`Joined.java`).
2. Add `KStream#join`, `leftJoin`, and `outerJoin` with tables, ensuring repartition topics are created when key serde/topology requires.
3. Support `KStream#table` joins using the new table abstractions and verify state store usage for lookup semantics.
4. Cover join behaviors with unit tests and compatibility fixtures (e.g., stream-table join word count variant).

### Step 3 – Implement Stream-Stream Joins (P1)
**Status:** ✅ Completed with window specifications, windowed state stores, join buffering, and regression tests covering inner, left, and outer joins.
1. Define window specification classes (`JoinWindows`, `SlidingWindows`) referencing Java implementations under `kstream/internals`.
2. Implement `KStream` stream-stream join operators (inner, left, outer) using windowed state stores for buffering and match Java default grace periods.
3. Extend state store module with windowed store builders required for join buffering, including retention configuration and changelog registration.
4. Add regression tests verifying join output ordering and window boundaries, cross-checking against Java fixtures.

### Step 4 – Add Windowed Aggregations (P1)
**Status:** ✅ Completed with DSL parity for tumbling, hopping, sliding, session, and unlimited windowed aggregations.
1. Introduced reusable window types (`TimeWindows`, `SessionWindows`, `UnlimitedWindows`) plus validation helpers shared across join and aggregation flows.
2. Extended grouping APIs so `groupBy`/`groupByKey` return a `KGroupedStream` that exposes window-aware `count`, `aggregate`, and `reduce` operators (including default session mergers).
3. Implemented windowed state stores with retention-aware purge, interactive-query metadata, and support for append/aggregate storage strategies.
4. Added regression fixtures and tests covering tumbling, hopping, sliding, session, and unlimited windows aligned with Java reference expectations.

### Step 5 – Suppression and Final Results (P2)
**Status:** ✅ Completed with a Node `Suppressed` builder, buffer configuration helpers, runtime buffering, metrics, and regression coverage.
1. Port the `Suppressed` builder (`Suppressed.java`) with configuration for emit strategies (e.g., `untilWindowCloses`).
2. Implement suppression in windowed aggregations, ensuring record emission respects buffer limits and grace periods.
3. Add metrics hooks to observe suppressed record counts and late arrival drops.
4. Validate suppression semantics with deterministic tests and documentation updates.

### Step 6 – Topology Description Enhancements (P2)
**Status:** ✅ Completed by enriching the topology graph with processor/store metadata, sink nodes, and descriptive edges validated against Node parity tests.
1. Extend `TopologyDescription` to include table sources, join nodes, windowed processors, and state store metadata with partitioning details.
2. Verify descriptions against snapshots derived from analogous Java topologies to ensure structural parity.
3. Document the description format and how to leverage it for debugging complex DSL topologies.

## 4. Dependencies and Risks
| Area | Status | Notes |
| --- | --- | --- |
| Kafka client capabilities | ✅ Mitigated | `@confluentinc/kafka-javascript@^1.6.0` is the current latest release and provides the consumer/producer features leveraged by tables, repartition topics, and cooperative rebalancing hooks. Any future transactional requirements are tracked for Phase 4 runtime work. |
| State store persistence | ✅ Mitigated | Phase 2 relies on the in-memory builders shipped in `node/src/state`. The DSL now exposes builder hooks so persistent adapters can plug in during Phase 5 without reworking APIs. No additional persistence blockers remain for Phase 2 completion. |
| Performance & resource usage | ✅ Mitigated | Windowed joins and suppression honour configuration limits via `StreamsConfig` cache settings, suppression buffer sizing, and newly added metrics. Guidance is documented so operators can size workloads while we profile advanced scenarios in Phase 4/5. |

Residual follow-ups (persistent stores, transactional semantics) are explicitly captured in the Phase 3+ roadmap items so they do not block DSL parity.

## 5. Deliverables
- Completed implementations for DSL operators outlined above with comprehensive unit/integration tests. ✅
- Updated documentation (`README`, guides) describing new DSL capabilities, including examples and configuration guidance. ✅
- Updated compatibility harness scenarios validating parity with Java reference implementations. ✅
- Logged residual runtime/state work as inputs to the Phase 3 roadmap planning. ✅

## 6. Exit Criteria
Phase 2 is complete when:
- All planned operators are available and documented in the Node package.
- Tests covering joins, windowing, tables, and suppression pass locally and in CI.
- Developers can describe a complex topology (joins + windows + suppress) and obtain an equivalent structure to the Java DSL using `Topology.describe()`.
- No high-priority gaps remain untracked; outstanding items are captured as inputs to Phase 3.

**Status:** All exit criteria have been met, and the project is now ready to proceed with the Phase 3 Processor API workstream.
