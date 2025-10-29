# Phase 5 Gap Analysis – State Stores & Interactive Queries

## Overview
Phase 5 focuses on delivering production-ready state stores and interactive query capabilities for the Node.js Kafka Streams port. The Java implementation under `streams/src/main/java/org/apache/kafka/streams/state` offers persistent RocksDB-backed stores, window/session semantics, changelogging, caching, and interactive query metadata via `StreamsMetadataState`. The Node.js codebase currently provides only in-memory store variants and a lightweight metadata manager, leaving major functionality gaps for durability, query routing, and observability. This document outlines the goals, current state, and step-by-step plan required to close those gaps.

## 1. Goals and Success Criteria
- **Durable Key-Value Stores**: Provide a persistent store implementation with configurable caching and changelogging semantics aligned with `RocksDBStore` and `StoreBuilder` behaviour in Java.
- **Windowed & Session Stores**: Support retention-aware window and session stores, including segment management, backward-compatible aggregations, and record expiration consistent with `WindowStore`/`SessionStore` contracts.
- **Interactive Queries & Metadata**: Expose `KafkaStreams#store` and RPC-backed query routing that mirrors `StreamsMetadataState`, including metadata refresh, host mapping, and remote fetch delegation.
- **State Store Metrics**: Surface cache, restore, and hit/miss metrics compatible with Java's `StreamsMetricsImpl` gauges to enable parity dashboards and alerts.
- **Robust Testing & Docs**: Validate persistence, failover, and query behaviours through unit/integration tests and update developer guides for new capabilities.

## 2. Current Node.js Baseline
- `node/src/state/memory-store.js` and `memory-window-store.js` supply non-persistent in-memory stores without caching, segment retention, or changelog integration.
- `node/src/state/store.js` defines abstract `StateStore`, `WindowStore`, and `SessionStore` types but lacks concrete persistent, windowed, or session implementations.
- `node/src/state/store-builder.js` only wires memory stores and does not emit changelog configuration or caching metadata needed by downstream runtime components.
- Interactive query scaffolding exists via `node/src/query/metadata-manager.js` and `interactive-query-service.js`, yet metadata refresh, standby awareness, and RPC client integrations are minimal.
- Runtime components from Phase 4 (`state/store-manager`, `runtime/assignment-coordinator`) recognise logging flags but do not manage changelog topics, restoration progress, or query registration lifecycles for persistent stores.
- Tests cover in-memory store semantics but omit persistence, query routing, or cross-instance metadata validation.

## 3. Step-by-Step Plan

### Step 1 – Persistent Key-Value Store Adapter (P0)
**Status:** ✅ Completed.
1. Introduced `node/src/state/persistent-key-value-store.js` backed by pluggable RocksDB bindings (defaulting to the maintained `rocksdb-native`) with batched put/delete, iterator support, and graceful dependency errors.
2. Extended `StoreBuilder`/`Stores` to surface persistent store metadata via `Stores.persistentKeyValueStore`, propagating changelog configs, caching flags, retention hints, and cache byte limits to the runtime.
3. Implemented a write-behind cache that honours `cache.max.bytes.buffering` thresholds from `StreamsConfig`, flushing pending entries on pressure, explicit flushes, and store closure.
4. Updated `StreamsConfig` and `StateStoreManager` to resolve per-store state directories for RocksDB, wiring defaults for state directories and cache byte limits.

> **Note:** Earlier drafts referenced a non-existent `@rocksdb-community/rocksdb` package. Because the [`rocksdb`](https://www.npmjs.com/package/rocksdb) bindings have since been deprecated, our primary adapter target is [`rocksdb-native`](https://www.npmjs.com/package/rocksdb-native); we will document API gaps, platform coverage, and build requirements we uncover. We will also investigate any other maintained RocksDB bindings that emerge while prototyping so we can fall back if `rocksdb-native` proves unsuitable.

### Step 2 – Changelogging & Restoration Hooks (P0)
**Status:** ✅ Completed.
1. Wrapped logging-enabled key-value stores with a change-logging adapter that routes mutations through `StateStoreManager`, binds stream-scoped producers from `KafkaStreams`, and serializes entries using configured serdes and headers.
2. Integrated restoration flows so changelog batches hydrate underlying stores without re-emitting change-log records, emitting lifecycle metrics/events while ensuring flush order.
3. Persisted per-store checkpoints after writes and restores to track last processed offsets and power warm restarts without duplicate replay.

### Step 3 – Window & Session Store Implementations (P1)
**Status:** ✅ Completed.
1. Added `persistent-window-store.js` to back windowed stores with RocksDB, including composite key serialization, retention-aware purging, range fetches, and iterator helpers that surface timestamp boundaries alongside stored values.
2. Introduced `persistent-session-store.js` with overlapping-session merges, tombstone-aware deletes, retention enforcement, and key-range lookups compatible with `RocksDBSessionStore` semantics.
3. Extended `Stores` with `persistentWindowStore`/`persistentSessionStore` builders that describe window metadata, and added unit tests covering persistence, purge behaviour, and key range queries.

### Step 4 – Interactive Query Metadata & RPC (P1)
**Status:** ⏳ Planned.
1. Enhance `QueryMetadataManager` with metadata refresh APIs that ingest `stateDirectory` snapshots and assignment events, matching `StreamsMetadataState#onChange` semantics.
2. Implement a pluggable RPC layer (HTTP/gRPC) for remote fetches with retry/backoff policies, integrating with `InteractiveQueryService#get` and remote store lookups.
3. Support standby awareness by tracking active vs. standby replicas and falling back to warm standbys when active hosts are unavailable.

### Step 5 – State Store Metrics & Observability (P1)
**Status:** ⏳ Planned.
1. Instrument store operations (get/put/delete/fetch) with latency/hit/miss counters exposed via the existing metrics registry under `state-metrics` namespace.
2. Publish restore progress metrics (restored bytes, records, lag) to align with Java's `restore-rate` and `restoration-info` gauges.
3. Emit interactive query request metrics (local vs. remote hits, latency) for parity with Java's query monitoring surfaces.

### Step 6 – Testing, Docs, and Samples (P0)
**Status:** ⏳ Planned.
1. Add unit tests for persistent stores, window/session retention, changelog replay, and query routing using embedded RocksDB and mocked RPC clients.
2. Create integration tests that spin up multiple Node.js instances, exchange metadata, and validate interactive queries across hosts.
3. Update `node/docs/processor-api-guide.md` and add a new state store cookbook demonstrating persistent stores, interactive queries, and operational guidance.

## 4. Dependencies and Risks
| Area | Status | Notes |
| --- | --- | --- |
| RocksDB Node bindings | ⚠️ Risk | Requires native dependencies; with the legacy `rocksdb` package deprecated we must validate `rocksdb-native` (and any other maintained forks) for API parity, verify platform support, gate the dependency behind optional installation, and document build prerequisites plus CI validation.
| Changelog topic management | ⚠️ Risk | Needs coordination with Kafka admin clients; ensure `KafkaStreams` runtime can auto-create topics or rely on provisioning scripts.
| Cross-instance metadata | ⚠️ Risk | Accurate metadata requires coordination with cooperative rebalance events; integration with Phase 4 assignment signals must be validated.
| RPC transport | ✅ Contained | Design allows pluggable transports; default HTTP client can leverage existing Node fetch implementations.

## 5. Deliverables
- Persistent key-value, window, and session store implementations with caching and changelog support.
- Extended store builders and runtime wiring for persistence, restoration, and metadata registration.
- Interactive query metadata refresh loop, RPC transport, and standby-aware routing.
- Metrics instrumentation covering store operations, restoration, and interactive queries.
- Comprehensive automated tests and updated documentation/samples for Phase 5 capabilities.

## 6. Exit Criteria
Phase 5 is complete when:
- Persistent stores survive process restarts with changelog replay and pass integration tests for recovery scenarios.
- Window and session stores honour retention/grace configurations and expose iterator APIs compatible with Java semantics.
- Interactive queries resolve to active or standby hosts, with remote fetch fallbacks validated by multi-instance tests.
- Metrics emit cache hit/miss, restore progress, and query latency information consumable by the metrics subsystem.
- Documentation and samples describe how to configure persistent stores, interactive queries, and monitoring hooks.

## 7. Exit Criteria Validation
- [ ] **Persistent store durability** – Restart integration tests confirm data survives restarts and changelog replay restores state.
- [ ] **Window/session correctness** – Unit tests cover retention, range, and session merge behaviour with persistent backing stores.
- [ ] **Interactive query routing** – Multi-instance tests verify metadata refresh, remote fetches, and standby fallback flows.
- [ ] **Metrics coverage** – Metrics assertions confirm state store, restoration, and query metrics are published.
- [ ] **Docs & samples** – Updated guides and examples are published and referenced from the roadmap.
