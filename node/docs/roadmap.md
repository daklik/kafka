# Kafka Streams Node.js Port Roadmap

## Vision and Goals
- Deliver a feature-complete Node.js implementation of Apache Kafka Streams that mirrors the Java library under `streams/src/main/java/org/apache/kafka/streams`.
- Maintain API compatibility with the Java DSL (`kstream`, `table`, `globalKTable`) and Processor API while embracing Node.js conventions (Promises, async iteration).
- Provide production-grade runtime capabilities: fault tolerance, state management, exactly-once processing, interactive queries, and observability.

## Current Node.js Baseline
- Lightweight DSL implemented via `StreamsBuilder`, `KafkaStreams`, serde helpers, and in-memory state stores located in `node/src`.
- Core stream transformations (`map`, `filter`, `flatMap`, grouping/aggregation) and unit tests exist, but advanced features, runtime services, and integration with Kafka clusters are incomplete.

## Phase 1 – Comprehensive Gap Analysis
1. **Catalogue Java APIs**: Enumerate public classes and methods across `kstream`, `processor`, `state`, and `query` packages to establish coverage targets.
2. **Topology & Runtime Semantics**: Document task assignment, thread model, checkpointing, and rebalance behavior from `processor.internals` and `processor.assignment`.
3. **State & Query Capabilities**: Review state store abstractions (`KeyValueStore`, `WindowStore`, `SessionStore`) and interactive query APIs in `streams/state` and `streams/query`.
4. **Cross-Cutting Concerns**: Inventory configuration options, metrics reporters, and error handling paths defined under `streams/internals` and `streams/errors`.

_Deliverable_: Gap analysis report with prioritized backlog mapped to Node.js modules (see [Phase 1 Gap Analysis](phase1-gap-analysis.md)). **Status: Completed – includes interactive query metadata scaffolding and cross-language compatibility harness.**

## Phase 2 – DSL Parity
1. **KStream Operations**: Implement missing transformations (flatMap, branch, repartition, joins, windowed aggregations) guided by `kstream/KStream.java` and `kstream/internals` implementations.
2. **KTable & GlobalKTable**: Port table semantics, table-table/table-stream joins, suppress, and materialization behavior.
3. **Windowing Model**: Introduce tumbling, hopping, sliding, and session windows mirroring `TimeWindows`, `SessionWindows`, and windowed stores.
4. **Topology Description**: Ensure `Topology.describe()` style output for debugging parity.

_Deliverable_: Expanded `StreamsBuilder`/DSL API with parity tests referencing Java behavior. Detailed milestones are captured in the [Phase 2 Gap Analysis](phase2-gap-analysis.md). **Status:** Steps 1–6 are complete, dependencies and risks are mitigated, and the DSL stack is ready for Phase 3 Processor API work.

## Phase 3 – Processor API & Topology Infrastructure
1. **Processor Contexts**: Recreate `ProcessorContext`, `To`, and `RecordMetadata` behaviors for custom processors.
2. **Punctuators & Schedulers**: Implement wall-clock and stream-time punctuation similar to `Punctuator` handling in `processor.internals`.
3. **State Store Registration**: Support attaching custom stores, logging, and changelog topics with recovery callbacks.
4. **Topology Optimizations**: Port graph optimizations (e.g., KTable/Source node reuse) and named processors.

_Deliverable_: Processor API surface and topology builder capable of loading Java parity blueprints.
**Status:** Step 1–2 complete (processor types and contexts); scheduling, state store wiring, and topology builder work remain – see [Phase 3 Gap Analysis](phase3-gap-analysis.md) for detailed steps and dependencies.

## Phase 4 – Runtime, Tasks, and Exactly-Once Semantics
1. **Threading & Task Model**: Model stream threads, standby tasks, task transitions (`CREATED`, `RESTORING`, `RUNNING`) following `KafkaStreams` and `StreamThread` logic.
2. **Assignment & Rebalance**: Implement `StreamsPartitionAssignor` equivalent and cooperative rebalancing workflows.
3. **Record Delivery Guarantees**: Support at-least-once and exactly-once-v2 semantics using transactions, following `streams/config` and `internals` implementations.
4. **Restore Logic**: Implement state restoration from changelog topics with progress listeners.

_Deliverable_: Production-grade runtime managing tasks, rebalances, and EOS modes.

## Phase 5 – State Stores & Interactive Queries
1. **Persistent Stores**: Port RocksDB-like storage via Node bindings or pluggable adapters; support changelogging and caching layers.
2. **Windowed & Session Stores**: Implement window and session stores mirroring retention, segment, and flush semantics.
3. **Queryable State**: Expose interactive queries (`KafkaStreams#store`) with host info metadata akin to `StreamsMetadataState`.
4. **State Store Metrics**: Replicate cache hit ratios, restore metrics, and store-level gauges.

_Deliverable_: Robust state store module with materialization and query parity.

## Phase 6 – SerDes, Schema Integration, and Type Safety
1. **Built-in Serdes**: Provide Avro, JSON Schema, Protobuf serdes consistent with `Serdes` utilities, leveraging Confluent Schema Registry clients.
2. **Custom Serdes API**: Match Java interfaces (`Serde`, `Serializer`, `Deserializer`) with Node patterns and async support.
3. **Error Handling**: Align with `ProductionExceptionHandler`, `DeserializationExceptionHandler`, and tolerances for corrupt data.

_Deliverable_: Serde ecosystem supporting schema evolution and resilient error handling.

## Phase 7 – Observability, Metrics, and Tooling
1. **Metrics Registry**: Port metrics reporters (`StreamsMetricsImpl`) and expose Prometheus-friendly endpoints.
2. **Logging & Tracing**: Integrate structured logging and tracing hooks aligned with Java SLF4J usage.
3. **Health & Liveness**: Provide readiness checks, lag monitoring, and lag-based alerts.

_Deliverable_: Operational toolkit for monitoring and managing Node.js Kafka Streams deployments.

## Phase 8 – Testing, Compatibility, and Release Engineering
1. **Compliance Test Suite**: Mirror Java topology integration tests under `streams` by running both Java and Node topologies against shared fixtures.
2. **Benchmarking**: Create benchmarks comparable to Java JMH harness to validate throughput/latency.
3. **Release Pipeline**: Automate npm packaging, versioning, and compatibility validation with Confluent Platform.
4. **Documentation & Samples**: Publish migration guides and examples covering DSL and Processor API parity.

_Deliverable_: Certified release candidate with documentation, automated tests, and performance baselines.

## Governance and Timeline
- Establish cross-language review board including Java maintainers and Node.js contributors.
- Prioritize delivery based on user demand and feasibility; early milestones focus on DSL parity and runtime stability.
- Iterate with phased releases (alpha -> beta -> GA) to gather feedback and harden features.

