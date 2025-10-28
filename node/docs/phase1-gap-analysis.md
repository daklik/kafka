# Phase 1 Gap Analysis – Kafka Streams Node.js Port

## Overview
This document records the findings of Phase 1 of the roadmap outlined in `node/docs/roadmap.md`. It catalogs the Java Kafka Streams APIs, captures the runtime and state semantics that must be mirrored, and produces a prioritized backlog mapped to the current Node.js modules under `node/src`.

## 1. Java API Catalogue
| Package | Representative APIs & Classes | Node Coverage (Apr 2024) | Notes |
| --- | --- | --- | --- |
| `org.apache.kafka.streams` | `KafkaStreams`, `Topology`, `StreamsConfig`, `StreamsBuilder` | **Partial** – `KafkaStreams`, `StreamsBuilder` ported with limited config support | Need topology description, config parsing, lifecycle hooks.
| `org.apache.kafka.streams.kstream` | `KStream`, `KTable`, `GlobalKTable`, joins, windowing (`TimeWindows`, `SessionWindows`), suppress, materialized stores | **Limited** – only `KStream` subset (basic transforms, aggregations) | Missing tables, joins, window definitions, suppression, materialization options.
| `org.apache.kafka.streams.processor` | `Processor`, `ProcessorSupplier`, `ProcessorContext`, punctuators, state restoration callbacks | **None** | Requires processor API surface and scheduling semantics.
| `org.apache.kafka.streams.state` | `KeyValueStore`, `WindowStore`, `SessionStore`, persistent stores, store builders | **Minimal** – in-memory KV store only | Need persistent stores, window/session stores, changelog integration.
| `org.apache.kafka.streams.query` | Interactive queries, `StateQueryRequest`, metadata | **None** | Requires metadata tracking and RPC/HTTP layer for queries.
| `org.apache.kafka.streams.errors` | Exception handlers, tolerances, dead letter queue semantics | **None** | Need configurable handlers for production readiness.
| `org.apache.kafka.streams.processor.internals` | `StreamThread`, `TaskManager`, partition assignor, record queueing | **None** | Critical for runtime parity and fault tolerance.
| `org.apache.kafka.streams.metrics` | `StreamsMetricsImpl`, sensors, reporters | **None** | Observability not yet addressed.

## 2. Topology & Runtime Semantics
- **Task Model**: Java divides topologies into stream tasks managed by `StreamThread`. Each task handles assigned partitions, maintains state stores, and participates in cooperative rebalancing. Node runtime currently executes topology operations inline on a single async loop – no task abstraction or partition awareness.
- **Threading**: Java uses dedicated stream threads plus global state threads. Node implementation must model concurrency, even if using worker pools or async queues instead of OS threads.
- **State Restoration**: Java replays changelog topics during startup using `StoreChangelogReader`. Node lacks restoration logic; aggregations only exist in-memory without persistence guarantees.
- **Rebalances & Assignment**: Java relies on `StreamsPartitionAssignor` and `TaskAssignor`. Node runtime currently consumes with a single group ID and no rebalance handling beyond basic consumer re-joins.
- **Fault Handling**: Java exposes `UncaughtExceptionHandler` and allows automatic shutdown or replacement of threads. Node runtime only emits generic `error` events.

## 3. State & Query Capabilities
- **Store Abstractions**: Need parity with `KeyValueStore`, `WindowStore`, `SessionStore`, and builder APIs (`Stores.keyValueStoreBuilder`, etc.). Node must provide pluggable adapters and caching layers for performance.
- **Materialization & Changelogging**: Java supports persistent stores with changelog topics, logging enabled by default for fault tolerance. Node lacks materialization strategies beyond in-memory maps.
- **Interactive Queries**: Java exposes `KafkaStreams#store` with host metadata via `StreamsMetadataState`. Node requires metadata registry, discovery protocol (e.g., HTTP server), and query routing.
- **Queryable State Consistency**: Must define read-after-write behavior, standby replicas, and metadata refresh intervals equivalent to Java defaults.

## 4. Cross-Cutting Concerns
- **Configuration**: Java `StreamsConfig` resolves hundreds of options (thread counts, buffering, EOS, metrics). Node currently forwards Kafka client options only; needs structured config parsing with validation and defaults.
- **Metrics & Observability**: Java reports metrics via JMX and pluggable reporters. Node must expose metrics hooks (Prometheus, OpenTelemetry) and align naming.
- **Error Handling & Tolerances**: Java supports deserialization and production exception handlers to skip or fail records. Node must provide configurable strategies.
- **Testing & Compatibility**: Java suite includes unit, integration, and system tests. Node requires analogous coverage, including compatibility harnesses to compare outputs across languages.

## 5. Prioritized Backlog (Node Modules)
| Priority | Node Module(s) | Workstream | Key Deliverables |
| --- | --- | --- | --- |
| P0 | `src/streams-builder.js`, `src/kstream.js` | DSL parity foundations | **Completed** – topology description, materialization helpers, branch/repartition operators, and join scaffolding exported. |
| P0 | `src/runtime/task-manager.js`, `src/kafka-streams.js` | Task & assignment model | **Completed** – task manager tracks partition assignments and offsets with rebalance hooks. |
| P0 | `src/state` package | State store interfaces | **Completed** – abstract store contracts, builder registry, and changelog configuration delivered. |
| P1 | `src/config/streams-config.js` (new) | Configuration system | **Completed** – introduced `StreamsConfig` with derived IDs, EOS validation, and consumer/producer factories. |
| P1 | `src/metrics` (new) | Metrics framework | **Completed** – delivered `StreamsMetrics` registry, reporters, and runtime instrumentation for consumption/production/error counters. |
| P1 | `src/errors` (new) | Error handling | **Completed** – added Java-style exception handler interfaces wired into deserialization and production paths. |
| P2 | `src/query` (new) | Interactive queries | **Completed** – metadata manager, host info registry, interactive query service, and RPC client stubs wired through `KafkaStreams`. |
| P2 | `test/` | Compatibility harness | **Completed** – cross-language fixture harness with word-count regression referencing Java outputs. |

### Notes on Prioritization
- **P0** items unblock Phase 2 (DSL parity) and Phase 3 (processor API) by ensuring the foundation mirrors Java constructs.
- **P1** items enhance production readiness and align configuration/error semantics before advanced runtime work.
- **P2** items can begin after foundational runtime pieces exist but should be planned early to avoid architectural rework.

## 6. Next Steps
Phase 1 deliverables are complete. Follow-up work is now tracked under the dedicated [Phase 2 Gap Analysis](phase2-gap-analysis.md), which enumerates the DSL parity milestones and supporting tasks that build on this foundation.
