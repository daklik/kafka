# Phase 2 Gap Analysis – DSL Parity

## Overview
Phase 2 focuses on achieving parity between the Java and Node.js Kafka Streams DSLs. This document decomposes the remaining DSL features, enumerates the Java references under `streams/src/main/java/org/apache/kafka/streams/kstream`, and maps the required work onto the existing Node.js modules in `node/src`. The plan is organized into incremental steps so that features can land in small, testable batches.

## 1. Goals and Success Criteria
- **API Coverage**: Expose the same `KStream`, `KTable`, and `GlobalKTable` operators as Java `3.7.x`, including overloads that accept `Named`, `Materialized`, and `Joined` parameters.
- **Behavioral Parity**: Match Java semantics for windowing, joins, suppressions, and materialization, including repartitioning and changelog requirements.
- **Topology Introspection**: Provide `TopologyDescription` output that surfaces nodes, stores, and processor links comparable to Java's `Topology.describe()` output for DSL topologies.
- **Test Coverage**: Back each feature with regression tests that mirror the Java integration suite, reusing or expanding the cross-language fixtures introduced in Phase 1.

## 2. Current Node.js Baseline
- `StreamsBuilder` and `KStream` support stateless transforms, branching, repartitioning, aggregation scaffolding, and materialization helpers.
- `KTable` and `GlobalKTable` abstractions are not implemented; joins and suppress operators are stubs.
- Window definitions (`TimeWindows`, `SessionWindows`, `UnlimitedWindows`) are absent, and aggregation flows assume unwindowed processing.
- Topology descriptions report basic node linkage but omit windowed processors, joins, and table metadata.

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
1. Introduce window types (`TimeWindows`, `SessionWindows`, `UnlimitedWindows`) and shared validation utilities.
2. Extend `KStream#groupByKey` and `KGroupedStream` aggregations (`count`, `aggregate`, `reduce`) to accept window definitions.
3. Implement windowed state stores with segment retention, caching, and query integration via interactive query API.
4. Provide fixtures covering tumbling, hopping, sliding, and session windows with expected results sourced from the Java suite.

### Step 5 – Suppression and Final Results (P2)
1. Port the `Suppressed` builder (`Suppressed.java`) with configuration for emit strategies (e.g., `untilWindowCloses`).
2. Implement suppression in windowed aggregations, ensuring record emission respects buffer limits and grace periods.
3. Add metrics hooks to observe suppressed record counts and late arrival drops.
4. Validate suppression semantics with deterministic tests and documentation updates.

### Step 6 – Topology Description Enhancements (P2)
1. Extend `TopologyDescription` to include table sources, join nodes, windowed processors, and state store metadata with partitioning details.
2. Verify descriptions against snapshots derived from analogous Java topologies to ensure structural parity.
3. Document the description format and how to leverage it for debugging complex DSL topologies.

## 4. Dependencies and Risks
- **Kafka Client Features**: Ensure `@confluentinc/kafka-javascript` exposes APIs required for cooperative rebalancing, transactions (for future phases), and partition assignment metadata used in join repartitioning.
- **State Store Persistence**: Windowed and join state stores may require persistent backing (e.g., RocksDB). Plan for pluggable storage adapters or leverage existing Node-native databases.
- **Performance Considerations**: Windowed joins and suppressions are memory-intensive. Introduce configuration-driven cache limits and metrics to monitor resource usage.

## 5. Deliverables
- Completed implementations for DSL operators outlined above with comprehensive unit/integration tests.
- Updated documentation (`README`, guides) describing new DSL capabilities, including examples and configuration guidance.
- Updated compatibility harness scenarios validating parity with Java reference implementations.
- Tracking issues or tickets for any follow-on work discovered during implementation (fed into Phase 3 planning).

## 6. Exit Criteria
Phase 2 is complete when:
- All planned operators are available and documented in the Node package.
- Tests covering joins, windowing, tables, and suppression pass locally and in CI.
- Developers can describe a complex topology (joins + windows + suppress) and obtain an equivalent structure to the Java DSL using `Topology.describe()`.
- No high-priority gaps remain untracked; outstanding items are captured as inputs to Phase 3.
