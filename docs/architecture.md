# Pulso Architecture

This document is the source of truth for Pulso's design decisions. Every non-trivial contribution should be consistent with what it says. When reality forces a divergence, update this file in the same change.

## What Pulso is

A headless, open-source observability backend: unified logs, metrics, and traces access, with built-in alerting and a native Model Context Protocol (MCP) interface, built on Elixir/OTP with a Rust hot path for columnar data.

Pulso ships no UI. It exists to be talked to by humans through their own dashboards and, first-class, by AI agents through MCP.

## Core bets

Four bets shape everything below. Deviating from any of them is a redesign, not a refactor.

1. **Headless nodes, shared-nothing at the write path.** No shared database, no leader election, no consensus service, no cluster-visible mutable state. Nodes coordinate only through object storage. Kill a node, launch another; nothing is lost.
2. **Object storage is the source of truth.** S3 (or any S3-compatible: R2, GCS, Azure Blob, MinIO) holds every acknowledged record, forever. Local disk is a warm cache and nothing more.
3. **BEAM for orchestration, Rust for bytes.** Elixir/OTP owns concurrency, supervision, per-tenant isolation, backpressure, and the MCP surface. Rust owns Parquet encode/decode, DataFusion query execution, and the S3 object store client, called via Rustler NIFs. Nothing crosses the boundary that doesn't need to.
4. **MCP is first-class, not bolted on.** The primary read surface is MCP tools. HTTP APIs exist for compatibility with agents (Alloy, OTel collectors, Prometheus remote_write clients) that expect them, not as the recommended read path.

## Reference architecture

Cursor's [Git at Any Scale](https://cursor.com/blog/git-at-any-scale) writeup is the closest reference implementation of what Pulso's coordination layer should look like. Reread it if any of the bets above become unclear.

## Storage model

### Signals and formats

All three signals are stored in **Apache Parquet** files in S3. Same substrate as Tempo (VParquet blocks), OpenObserve, and increasingly common across the modern observability stack.

For each signal Pulso stores:

- **Segment files** (`s3://<bucket>/tenant=X/signal=logs/date=Y/hour=Z/segment-<node>-<seq>.parquet`): immutable Parquet objects containing the records themselves.
- **Sidecar index files** (`.bloom`, `.postings`, `.stats`), written alongside the segment at flush time, immutable, live in S3.
- **Per-tenant, per-signal manifest** (`s3://<bucket>/tenant=X/signal=logs/manifest.json`): the list of segments that currently exist for this (tenant, signal), each with its time range, row count, and any tiny summary metadata the query planner needs to decide whether to open it.

The manifest is the only file for a tenant that ever gets rewritten. Segments and sidecar indexes are write-once.

### Segment lifecycle

1. **Open**: records accumulate in an in-memory Arrow buffer on the ingester assigned to this (tenant, signal). The Elixir process holds the buffer; no on-disk WAL, nothing else exists yet.
2. **Close** (every ~1 second, or when the buffer hits a size threshold): the Arrow buffer is encoded to Parquet by the Rust NIF, the sidecar indexes are computed, both are `PUT` to S3, and the manifest is updated via a conditional PUT (S3 CAS on ETag).
3. **Acknowledge**: only after both the segment PUT and the manifest CAS succeed does the ingester ack the batch to the client. This is the load-bearing durability point.
4. **Compact** (background): a separate compactor role reads N small recent segments, merges them into one larger segment with fresh sidecar indexes, uploads the compacted segment, CASs the manifest to reference the compacted segment and mark the small ones as superseded, and after a grace period deletes the small segments.

### Compression, sorting, and encoding

- Sort each row group by `(service, timestamp)` for logs and traces, by `(series_id, timestamp)` for metrics. Tight per-row-group min/max stats make predicate pushdown cheap.
- Dictionary encoding for low-cardinality columns (`level`, `service`, `status`).
- Delta encoding for timestamps.
- Zstd compression on column chunks.
- Parquet 2.0 bloom filters on high-cardinality columns (`trace_id`, `span_id`, `user_id`).

## Coordination

### The rules

- **No shared database.** No Postgres, no shared SQLite, no RocksDB with Raft replication.
- **No consensus service.** No etcd, no ZooKeeper, no Consul.
- **No cluster metadata store.** Cluster membership comes from `libcluster`'s DNS strategy (or equivalent); rendezvous hashing computes ownership from the current node list on demand.
- **S3 is the only shared state.**

### Rendezvous hashing for tenant ownership

Each `(tenant, signal)` has a rendezvous-hashed owner among the current live nodes. Any node can compute the owner from the tenant id and the current member list — no lookup table, no assignment protocol. When the cluster grows or shrinks, roughly `1/N` of ownerships move; nothing else does.

Ownership is an *optimization*, not a correctness property. It determines which node is expected to buffer records for a (tenant, signal) and thus have the warmest cache for it. Any node can serve any query; any node can accept any ingest and forward if it isn't the owner.

### S3 CAS for manifest updates

Manifest updates use S3 conditional PUT (`If-Match: <etag>` on writes; `If-None-Match: *` for first-time creation). All major S3-compatible providers support this as of 2024.

Under normal operation there is exactly one writer per manifest (the current rendezvous owner), so CAS conflicts do not happen. During failover or a cluster resize, two nodes may briefly race; the loser retries with the new etag. This is the failover mechanism — no explicit election.

### Freshness via conditional GET

Queriers cache manifests locally, keyed by ETag. Before serving a query they issue a conditional GET (`If-None-Match: <cached-etag>`). 304 → cache is fresh; 200 → refresh the manifest before using it. This is the same mechanism Cursor uses on its WAL index.

### Optimistic UDP gossip for cache invalidation

When an ingester publishes a new manifest version it broadcasts `("manifest-updated", tenant, signal, new-etag)` over UDP to peers. Peers use this to shortcut the polling interval on the next query. UDP loss is fine — the conditional GET is the correctness backstop.

## Ingest path

### Wire protocols accepted

Pulso is designed to receive telemetry from **existing agents unchanged**. Priority order:

1. **OTLP/HTTP** (protobuf, gzip): the universal path. Covers logs, metrics, traces.
2. **Prometheus `remote_write`** (Snappy protobuf): the metrics on-ramp the existing Alloy install base already uses.
3. **Loki push API** (`/loki/api/v1/push`, JSON and Snappy variants): the logs on-ramp the existing Alloy install base already uses.
4. **OTLP/gRPC** (protobuf over HTTP/2): follow-up. Same signals as OTLP/HTTP, lower overhead.

### Per-record flow

1. Request lands on any node.
2. Node identifies the tenant and the rendezvous-hashed owner for `(tenant, signal)`.
3. If this is the owner: append the decoded records to the in-memory Arrow buffer. Otherwise forward internally to the owner (one hop).
4. On the owner: the buffer flushes on cadence (`~1s`) or size (`~10 MiB`), producing one Parquet segment plus its sidecar indexes.
5. Rust NIF writes segment + sidecars to S3.
6. Manifest CAS-PUT appends the segment to the tenant manifest.
7. Owner acks the batch to the requester.
8. Owner broadcasts UDP gossip to peers.

Ack latency ≈ flush cadence + S3 PUT latency ≈ **~1 second**. This is deliberate; observability clients batch at the source anyway.

## Query path

### Per-query flow

1. Query lands on any node (or MCP request, or alert-evaluator internal call — same code path).
2. Node fetches the tenant manifest (conditional GET; usually 304 from local cache).
3. Manifest lists candidate segments; time-range and label-based pruning reduces the set. Sidecar indexes (bloom filters, posting lists) prune further.
4. For each remaining segment, DataFusion opens the Parquet file (from local NVMe cache if present, otherwise from S3 via range GETs), pushes down predicates via Parquet row-group stats, and executes the query.
5. Results merge and return.

Query fan-out is entirely within one node. Nothing crosses node boundaries at query time except S3 I/O.

### Query languages

- **PromQL** subset for metrics.
- **LogQL** subset for logs.
- **TraceQL** subset for traces.

Full grammar coverage is a long tail; ship the useful subset first. Each language is parsed in Elixir; the resulting plan compiles to a DataFusion query executed in the Rust NIF.

### Local cache

Each node maintains an LRU cache of recent Parquet segments and sidecar indexes on local NVMe. Cache eviction is best-effort; a miss just triggers a range GET to S3. Cache warming happens organically via queries; there is no proactive prefetch.

## Alerting

### Rule storage

Alert rules are JSON objects in S3 (`s3://<bucket>/tenant=X/alerts/rule=Y.json`). Rule updates use S3 CAS the same way manifests do.

### Rule evaluators

Each rule has a rendezvous-hashed evaluator among the current live nodes. The evaluator is a supervised Elixir process that queries the same S3 data any other query would touch and evaluates the rule expression on the returned series.

### Firing

Fires are recorded as an immutable, append-only WAL per rule (`s3://<bucket>/tenant=X/alerts/rule=Y/fires/<seq>.json`). Firing = CAS-PUT with `If-None-Match: *` on the next sequence.

This gives exactly-once firing without Raft, without Horde-with-guardrails, without any of the coordination primitives I earlier considered. Two evaluators racing: only one CAS wins. The loser reads back the winner's fire record and knows the fire is handled.

### Notification routing

A separate process subscribes to the per-tenant fires log (via S3 polling or event notifications where available) and dispatches to configured channels (webhook, Slack, PagerDuty, email). Notification-side dedup is trivial because fires are already exactly-once.

## MCP interface

### The read/write boundary is load-bearing

| Tier | Where it lives | Blast radius |
|---|---|---|
| Read-only diagnosis | This repo (`Pulso.MCP.Tools`) | None; queries are read-only against S3 |
| Write within Pulso | This repo (silence/ack alerts, add annotations) | Contained; no external side effects |
| Infrastructure remediation | **Separate MCP server, not this repo** | Unbounded without policy |

**Do not add remediation tools to `Pulso.MCP.Tools`.** They live in a separate, narrowly-scoped MCP server that policy-gates each action. This is a design invariant, not a preference.

### Rollout tiers for agent integration

1. **Diagnose-only**: alert fires → agent (read-only MCP access) pulls context, correlates signals, posts a root-cause summary. Most of the value, minimal risk.
2. **Suggested remediation, human-approved**: agent proposes a fix; the agent runtime's task-suspend/resume pauses for approval and resumes once granted.
3. **Narrow autonomous remediation**: a fixed allowlist of safe, reversible actions via the separate infra-remediation MCP server, fully audited — agent actions themselves logged back into Pulso as spans and events, closing the loop.

## Elixir / Rust boundary

The rule of thumb: **Elixir owns the write path's control flow; Rust owns anything that touches columnar bytes in bulk.**

### Elixir

- HTTP endpoints (Phoenix): OTLP receivers, remote_write receiver, Loki push receiver, MCP JSON-RPC.
- Per-tenant supervision, backpressure via Broadway/GenStage.
- Arrow buffer accumulation.
- Manifest read/write logic, ETag caching, conditional GET orchestration.
- Rendezvous hashing.
- UDP gossip.
- Alert rule scheduling, fire CAS, notification dispatch.
- MCP tool registry and dispatch.
- Query language parsing (PromQL/LogQL/TraceQL subsets).

### Rust (via Rustler NIF)

- Arrow → Parquet encode at flush time.
- Sidecar index computation (bloom filters, posting lists, stats).
- Parquet decode and columnar scan at query time.
- DataFusion query plan execution.
- `object_store` crate for S3 GET/PUT/CAS.
- Decompression and wire-format decoding for high-volume ingest protocols (Loki push protobuf today), returning terms whose strings are sub-binaries of the request buffer rather than copies.
- SIMD-heavy predicate evaluation.

## What each node holds

All local state is disposable. Everything is reconstructible from S3 in bounded time.

- **In-memory Arrow buffer** for the currently-accumulating batch (transient, ≤ flush cadence).
- **NVMe LRU cache** of hot Parquet segments and sidecar indexes.
- **In-memory manifest cache** with ETag, invalidated by UDP gossip or refreshed by conditional GET.
- **In-memory rendezvous-hash table** of current cluster members (from libcluster's DNS strategy).

## What Pulso deliberately does not have

- No shared database.
- No local WAL — S3 is the WAL.
- No SQLite, no RocksDB, no Postgres.
- No etcd, no ZooKeeper, no Consul.
- No leader election, no Raft, no Horde-managed singletons.
- No routing tables.
- No cluster-visible mutable state outside S3.

If you feel the urge to add one of these, revisit "Core bets" first.

## Dependencies with hard requirements

- **S3 or S3-compatible storage with conditional PUT** (`If-Match`, `If-None-Match: *`). Supported by AWS S3 (since 2024), Cloudflare R2, GCS, Azure Blob, MinIO. This is non-negotiable.
- **Erlang/OTP and Elixir** as pinned in `mise.toml`.
- **Rust toolchain** for the NIF (added when the NIF lands).

## Signal-specific notes

### Logs

- LogQL subset. Label filter + substring/regex match on message.
- Sort row groups by `(service, ts)`.
- Sidecar: label→segment posting list; optional token inverted index for `|=`/`!=` filters.

### Metrics

- PromQL subset. Range vector selection, `rate`, `increase`, aggregations, `histogram_quantile`.
- Sort row groups by `(series_id, ts)`. `series_id` is a fingerprint of the label set.
- Sidecar: label→series posting list (TSDB-style).
- Caveat: very high active-series cardinality (100M+) may eventually justify a specialized TSDB block layout beside Parquet. Not v1.

### Traces

- TraceQL subset. Span attribute filters, trace-id point lookup.
- Sort row groups by `(trace_id, span_id)` for lookup efficiency, or by `(service, ts)` for search.
- Sidecar: per-segment bloom filter over `trace_id`.

## Non-goals

- Serving as a UI. There is no dashboard, and there will not be one in-repo.
- Replacing every backend for every workload. Pulso targets the common case; hyperscale metrics with 1B+ active series may need a specialized system.
- Being a general-purpose log-and-metric database. The design is optimized for observability access patterns (recent-heavy, time-and-label-filtered, append-only).

## Open questions

- Compactor role: dedicated compactor nodes, or ingesters run compaction in the background?
- Multi-region: single region only in v1; multi-region S3 replication and query routing is a separate design.
- Tenant tiering: are hot tenants sharded to dedicated nodes, or homogeneous?
- Retention: TTL enforcement is a background job that rewrites manifests and deletes segments past the retention window; specifics TBD.

## When this document must be updated

- Adding a new signal type.
- Changing wire protocols accepted.
- Changing the coordination model or ownership assignment.
- Changing what lives in S3 vs on-disk vs in-memory.
- Adding or removing an MCP tool tier.
- Adding a dependency that maintains state (a database, a queue, a consensus service).

If the change touches any of the above, update this document in the same PR. Otherwise no.
