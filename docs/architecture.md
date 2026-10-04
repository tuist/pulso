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

- **Segment files** (`s3://<bucket>/tenants/<tenant>/v4/signal=<s>/date=<Y-m-d>/hour=<H>/<min_ts>-<max_ts>-<suffix>.parquet`): immutable Parquet objects containing the records themselves. The zero-padded 20-digit `[min_ts, max_ts]` tail is what the query path prunes against at LIST time; the `date=`/`hour=` partitions are derived from `min_ts` as UTC and let Athena-style callers prune without opening the manifest.
- **Sidecar index files** (`.bloom`, `.postings`, `.stats`), written alongside the segment at flush time, immutable, live in S3. **Not yet implemented**; the first signal to need them is metrics (label→series posting list).
- **Per-tenant, per-signal manifest** (`s3://<bucket>/tenants/<tenant>/v4/signal=<s>/manifest.json`): the list of segments that currently exist for this (tenant, signal), each with its time range, row count, and any tiny summary metadata the query planner needs to decide whether to open it.

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

Implemented ownership currently covers background metrics merge and cleanup.
`ManifestOwner` is a node-local request coalescer, not a cluster-wide owner;
conditional object-storage writes arbitrate concurrent ingest from multiple
nodes. Ingest forwarding, buffered ingest ownership, and alert evaluation remain
future features and must reuse `Pulso.Rendezvous` with their own eligible roles.
Query caches and query admission limits deliberately remain local to each node.

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

### Implemented receiver budgets

The current OTLP JSON logs, Loki JSON/Snappy logs, and remote-write receivers
validate per-request record counts and attribute budgets before appending.
Defaults are 10,000 supplied records; 128 entries per attribute/label set;
256-byte keys; 16 KiB values; and 64 KiB aggregate attribute bytes. JSON trees
also have 16-level depth and 1,024-node budgets, including structured OTLP bodies.
Separate container counts bound empty-stream/series/resource/scope floods.
Over-budget requests return HTTP 413 and append nothing, rather than truncating
or reporting partial success. Protobuf preflight runs after bounded decompression
and before record/sample collection allocation and Erlang record expansion; JSON preflight
runs before AnyValue conversion and record expansion. Authentication precedes
semantic validation, but JSON parsing/gzip inflation remains pre-authentication
under the existing 4 MiB wire and 16 MiB expanded-body caps. These are local
request safety limits, not ingest admission control or rate limiting.
See [ingest limits](ingest-limits.md) for exact counting and configuration.

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

- **[Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/)** subset for metrics: float selectors, `rate`, `increase`, `irate`, `delta`, `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, `count_over_time`, and nested `sum`/`avg`/`min`/`max`/`count` with `by` or `without` grouping. Positive selector offsets are supported.
- **LogQL** subset for logs.
- **TraceQL** subset for traces.

Full grammar coverage is a long tail; ship the useful subset first. Each language is parsed in Elixir; the intended query plan compiles to DataFusion in Rust. Today label and time filters run in the Rust Parquet decoder, while log and metric aggregations run in Elixir. Metrics fetch the selected interval once per selector for all evaluation steps, without a sample limit that could silently truncate aggregations. The initial metrics evaluator enforces independent sample, work, scan, result, heap, and concurrency limits with explicit errors; columnar aggregation remains a follow-up.

The read-only `query_promql` tool accepts a tenant, expression, and optional evaluation time or range with step. Compatibility routes `GET|POST /api/v1/query` and `/api/v1/query_range` share the evaluator and tenant authorization. Responses use Prometheus vector/matrix envelopes with numeric timestamps in seconds and Prometheus decimal/exponent value formatting. Evaluation retains Pulso nanosecond precision rather than truncating to Prometheus milliseconds. Selectors use a left-open five-minute lookback; range functions use left-open windows, counter-reset correction, and Prometheus boundary extrapolation. Unsupported syntax is rejected explicitly: scalar/binary expressions, subqueries, `@`, negative offsets, histograms, and functions outside the listed subset. Both storage adapters use the same anchored native regular-expression engine, including missing-label and ASCII character-class semantics. Unsupported regular-expression constructs, including Rust class-set extensions and nested classes, are rejected; full Prometheus syntax compatibility is not claimed. Quoted regular-expression literals (`\Q...\E`), octal regular-expression escapes, literal unescaped brackets in classes, and Unicode-heavy repeats exceeding the native program budget currently return errors. Non-numeric sample values are rejected; stale-marker handling is not implemented yet. Decimal timestamps are parsed without floating-point conversion and bounded to signed 64-bit nanoseconds; selector durations are limited to one year. Endpoint `timeout` is accepted and clamped to the ten-second server maximum; other unsupported parameter overrides are rejected.

Expressions are limited to 16,384 bytes, 64 matchers per selector, and 1,024 bytes per regular expression. Native regular-expression programs and their deterministic-automaton caches each have a one-mebibyte limit. Parsing and evaluation run under a supervised task with a ten-second timeout and a 16-million-word heap limit. A node admits at most four concurrent query tasks, returning a retryable capacity error (status 429 on compatibility endpoints) when occupied. Each tenant can hold at most two of the four slots. Default budgets are 1,024 candidate objects, 128 mebibytes of object data, one million manifest-reported rows, 100,000 selected samples, five million units of window work, 100,000 result points, and 11,000 evaluation steps. Sample decoding stops at the budget, while sliding windows avoid rescanning every sample at every step. Resource exhaustion returns an execution error without partial results. These are independent ceilings, not guaranteed query capacity: label-heavy data can hit the heap limit before the sample limit. Operators can tune `max_heap_words` in `config :pulso, Pulso.PromQL.Evaluator`; `max_samples`, `max_work`, `max_result_points`, `max_scan_segments`, `max_scan_bytes`, and `max_scan_rows` are also configurable. Unexpected task crashes are reported as server execution failures. Scans preflight manifest counts and known sizes, check actual downloaded bytes, and check deadlines between objects. An in-flight native call may finish after the request deadline; admitted task slots remain occupied until that call exits. Conflicting values at the same timestamp resolve to the largest value deterministically and add a response warning. This policy preserves repeatable results across segment ordering; it does not claim ingest-time duplicate rejection.

### Local cache

Each node maintains an LRU cache of recent Parquet segments and sidecar indexes on local NVMe. Cache eviction is best-effort; a miss just triggers a range GET to S3. Cache warming happens organically via queries; there is no proactive prefetch.

## Operational self-monitoring

`GET /metrics` exports Prometheus-text self-monitoring from supervised,
node-local ETS counters and live registry/VM gauges. It never reads or writes
Pulso storage, and reporting has no mailbox. Labels have a finite vocabulary;
tenants, expressions, object keys, and raw errors are not retained. Counters
reset when the metrics process or node restarts. Manifest owners publish batch
queue depths in their existing registry entries, so scrapes do not wait for
owners blocked in native I/O; registry cleanup removes terminated owners.

Receiver records count acknowledged deliveries after successful append and
known decoder/append rejections, not unique stored rows. Pre-decode failures
count rejected requests without inventing record counts. Public HTTP queries
and MCP tool results expose failures, including tool errors transported as
HTTP 200. Object operations report logical calls, CAS outcomes, and successful
body bytes, not all provider-billed retries or transfer. Compaction and cleanup
report completed attempts and confirmed segment counts. A separate background
worker deadline counter exposes timeouts even before stalled native work finishes. See
[self-monitoring](self-monitoring.md) for the full contract and limitations.
Scrape every node into an independent monitor during rollout. The endpoint is
not tenant-authenticated and shares the ingest listener. Keep the whole listener
private, or deny/separately authenticate router-equivalent metrics paths at every
public ingress proxy; port-level network policy cannot isolate this path from
ingest. It adds no cluster state or admission policy.

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

Pulso implements only the stateless [MCP `2026-07-28`](https://modelcontextprotocol.io/specification/2026-07-28)
revision over Streamable HTTP at `POST /mcp`. There is no `initialize` handshake and
no protocol session, so any node can answer any request and nothing about a client
lives in memory between requests, matching the rest of the system. Every request
carries `io.modelcontextprotocol/protocolVersion` and `clientCapabilities` in
`params._meta`; missing metadata is invalid params (`-32602`), and unsupported
versions return `-32022` listing the supported ones. Requests must mirror the version,
method, and (for `tools/call`) tool name in `MCP-Protocol-Version`, `Mcp-Method`, and
`Mcp-Name` headers; a missing, repeated, or mismatched header is `-32020`. Protocol
errors use HTTP `400`, unknown methods `404`, and tool failures remain `200` results
with `isError: true`. Each POST holds one message: batches are rejected, notifications
are acknowledged with `202` and never executed, and GET or DELETE return `405`.
`server/discover` and `tools/list` are cacheable for 60 seconds with private scope.
Pulso emits no change notifications, so `subscriptions/listen` acknowledges an empty
filter and closes the stream with a completion result. A present `Origin` header must
match `PULSO_MCP_ALLOWED_ORIGINS` (`403` otherwise) and POST bodies must be
`application/json` (`415` otherwise); both checks run before the body is parsed, on
the percent-decoded path the router matches. Requests without an `Origin` header are
accepted. Legacy initialize-based clients, including Atlas's proxy at
the time of writing, need a client-side migration rather than a compatibility shim.

All four query tools advertise read-only, non-destructive, idempotent, closed-world
annotations as defined by the [Model Context Protocol tool schema](https://modelcontextprotocol.io/specification/2026-07-28/schema#tool).
Calls validate the schema vocabulary used by the registry before evaluation:
required fields, types, numeric bounds, string lengths, and enum values, including
nested label matchers. Optional null fields retain the same defaults as omitted
fields, and integer-valued decimal numbers are normalized to integers before
validation. Unknown fields remain allowed. Time bounds must fit signed 64-bit
nanoseconds and must not be reversed; Prometheus range queries require both
bounds and a positive step. Both language tools cap millisecond steps so conversion
fits signed nanoseconds. Log-based metric queries share the Prometheus evaluator's
11,000-step ceiling and reject excessive ranges before constructing a timeline.
Window scans clamp their lower bound to the earliest stored timestamp without
changing the logical window. Raw metric matcher patterns are checked with the
native regular-expression compiler, with a 1,024-byte pattern limit, before
storage is read. A valid tenant is authorized before the remaining arguments,
expressions, or patterns are validated. Annotations describe behavior and do not
replace authorization.

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

- HTTP endpoints (Phoenix): OTLP receivers, remote_write receiver, Loki push receiver, stateless MCP Streamable HTTP.
- Per-tenant supervision, backpressure via Broadway/GenStage.
- Arrow buffer accumulation.
- Manifest read/write logic, ETag caching, conditional GET orchestration.
- Rendezvous hashing.
- UDP gossip.
- Alert rule scheduling, fire CAS, notification dispatch.
- MCP tool registry and dispatch.
- Query language parsing (PromQL/LogQL/TraceQL subsets).

### Rust (via Rustler NIF)

- Arrow → Parquet log segment encode at flush time (arrow-rs and parquet-rs), with rows sorted by `(service, timestamp_ns)`, dictionary encoding on `service`/`severity_text`/`severity_number`, delta-binary-packed on `timestamp_ns`/`observed_timestamp_ns`, and zstd column compression.
- Sidecar index computation (bloom filters, posting lists, stats).
- Parquet decode and columnar scan at query time: row-group `timestamp_ns` min/max stats prune whole row groups before any column page is read, then per-row time and service filters run in Rust and only surviving rows materialise as Erlang terms. One Erlang binary per string column per batch (the "arena") backs zero-copy sub-binary strings — a batch of N rows costs 7 fresh binaries per column, not 7 × N.
- DataFusion query plan execution.
- `object_store` crate for S3 GET/PUT/CAS.
- Decompression and wire-format decoding for high-volume ingest protocols (Loki push protobuf today), returning terms whose strings are sub-binaries of the request buffer rather than copies.
- JSON encode/decode for HTTP bodies and responses. The `body`, `attributes`, and `resource` log fields are stored as JSON-encoded strings inside the Parquet segment's Utf8 columns and JSON-decoded on read; the shared `Pulso.JSON` fast path defers to Elixir's `JSON` on any case it cannot guarantee to encode identically.
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

- Ingest wire protocol: **Prometheus `remote_write` v1** (Snappy-compressed protobuf), at `POST /api/v1/write`. Full receiver contract: `Content-Type: application/x-protobuf`, `Content-Encoding: snappy` (strict — unlike Loki push, an absent header is rejected), `X-Prometheus-Remote-Write-Version: 0.*` (0.1.0 in practice), `X-Scope-OrgID` for tenant (defaults to `"default"`), `Idempotency-Key` propagated to storage. `204` on success, `400` for invalid snappy/protobuf, `415` on wrong content-type/encoding, `429` on `:owner_overloaded` backpressure, `5xx` on storage transients. OTLP/HTTP metrics (`/v1/metrics`) is a follow-up PR.
- Sort row groups by `(series_id, timestamp_ns)`.
- `series_id` is Pulso's port of Prometheus's `labels.StableHash` — xxhash64 over `name<0xff>value<0xff>…` across labels sorted by name. Byte-exact compatibility with the Go reference is pinned by `native/pulso_codec/src/stable_hash.rs` and its oracle test. Treat `series_id` as an accelerator only — the canonical labels are the identity, and readers must compare them, not the hash.
- Parquet schema: `series_id Int64`, `timestamp_ns Int64`, `value Float64`, `metric_name Utf8` (dictionary-encoded), `labels_canonical Utf8` (the bytes `StableHash` consumes), `labels_json Utf8` (for materialisation on read). Delta-binary-packed on `timestamp_ns`, zstd column compression.
- Sidecar: **not yet built.** Manifests now optionally carry a complete metric-name set (shared `names` dictionary and segment `ni` reference on the wire; legacy inline `n` is still read), capped at 128 names of at most 256 bytes each. Exceeding either bound omits the summary entirely rather than truncating it. The dictionary has a 65,536-byte budget per manifest; additional sets remain unknown. Exact `__name__` matchers prune known irrelevant segments before download; absent or malformed summaries remain eligible. Pruning benefits exact-name queries against small or repeated name sets; varied or large batches frequently lose their summary under these budgets. This is an additive manifest field, so existing segments need no migration. Top-label summaries and label postings remain follow-ups.
- Native histograms and exemplars are deliberately out of v1. If OTLP metrics ships before they do, the receiver must reject unsupported histogram/summary types with partial success rather than silently coercing.
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

## Metrics compaction implementation

`Pulso.Storage.S3.CompactionWorker` runs independently from ingest and manifest
owners. It is disabled by default. Upgrade every writer before enabling it with
`PULSO_METRICS_COMPACTION_ENABLED=true` or `compaction_enabled: true` in storage
configuration. Existing writers cannot preserve the new retirement metadata, so
mixed-version writing is unsupported once compaction has begun.

Every 60 seconds, with scheduling jitter, each worker discovers tenant directories
with delimiter listing under `tenants/`. Provider pagination returns immediate
tenant prefixes without materializing descendant segment keys. This durable
discovery includes tenants first observed by other instances and idle tenants
after all caches or nodes restart. Owners load only their metrics manifests;
missing manifests never adopt orphan segments. Discovery memory grows with tenant
count rather than the segment backlog. Discovery errors skip the pass and retry
on the next interval without falling back to an incomplete local cache.

Enabled workers join an [Erlang process group](https://www.erlang.org/doc/apps/kernel/pg.html)
scoped by storage endpoint, region, and bucket. Existing domain name discovery
connects nodes; process groups advertise only live compaction workers, excluding
disabled nodes and removing failed workers even when their node stays connected.
Deployments must use identical store identities and connect the eligible nodes
in a full mesh. A standalone node owns every tenant. The process-group scope and
worker share a supervision tree that restores registration after scope failure.

The shared `Pulso.Rendezvous` algorithm accepts binary key fields for tenant work
and future rule evaluation. For each `(tenant, signal)`, rendezvous hashing chooses the highest digest of
length-prefixed purpose (`compaction`), tenant, signal, and node-name fields, with node name breaking
score ties. Membership order and duplicate announcements do not affect ownership.
Only the current local owner merges or cleans up; ownership is checked again
before cleanup. Joins and departures change the next pass without transferring
state. Views can disagree during propagation or partitions, and a merge already
in progress can finish after ownership moves. Ownership reduces duplicate work;
it is not a lock. Conditional publication, durable retirement revisions, and
grace periods remain the correctness backstop for overlapping workers.

`MetricsCompactor.compact/3` and `cleanup/3` remain explicit maintenance operations
that bypass background ownership. `compaction_interval_ms`, `compaction_options`,
and `compaction_cleanup_options` tune cadence and limits. Each operation runs under a dedicated supervised task with a 30-second deadline
(`compaction_timeout_ms`). Errors and exceptions remain isolated; a merge failure
does not prevent cleanup or other tenants. A timeout stops waiting and withdraws
the worker from eligibility, allowing peers to take over. The task remains
supervised until it finishes: killing a process inside a native call can report
termination while native work is still running. The worker skips further passes
until the task actually finishes, then rejoins on its next interval. A timed-out
tenant is deferred locally for five minutes (`compaction_retry_ms`), allowing
other tenants to progress instead of repeatedly rotating a slow tenant among
nodes. Discovery has its own 60-second deadline (`compaction_discovery_timeout_ms`).
These limits bound admitted work during a stall. A late task may still publish or clean up;
conditional publication and durable retirement safeguards remain mandatory.

A merge selects at least two segments from the same hour partition, up to 32
segments, 8 MiB of encoded input, and 100,000 samples. Each candidate must be at
most 1 MiB and have known byte and row counts. Native metric codecs preserve every
sample, including duplicates, without aggregation or resampling. Query results
use sample value as the final tie breaker for identical labels and timestamps,
keeping limited queries stable when conflicting values are compacted. This first
implementation materializes bounded samples in Elixir between native decode and
encode. Segments recovered by listing without summaries are skipped until
summary enrichment is implemented.

Replacements use the existing partitioned keys with a `compact` suffix. The
compactor reloads the manifest and conditionally publishes the replacement with
retirement metadata. Concurrent appends are preserved on retry. A competing
compactor that definitively loses publication or exhausts conflicts deletes its
unreferenced upload after a fresh manifest confirms it was never published;
a lost publication response is recognized as success if the unique replacement
key is active or already retired by subsequent compaction. Ambiguous storage
errors retain uploads until a future orphan-reclamation policy can prove them
safe to delete.

Manifest version 2 records each retired key's deletion deadline, revision, and
completion status, plus a cleanup cursor. Legacy version 1 manifests remain
readable; unsupported future versions fail closed. The default grace period is
one hour. Nodes must keep their wall clocks synchronized; grace must exceed
expected cross-node clock skew and reader lifetime. Ownership changes do not
reset durable deletion deadlines. Cleanup attempts at most 128 deletions per pass, continues past failed
keys, and persists progress through conditional writes. The cursor rotates past
permanent failures. Completed objects are not repeatedly deleted. A delayed
ingest retry increments the retired key's revision and schedules its re-upload
for deletion, preventing overlapping cleanup from incorrectly marking it done.
Tombstones remain permanently to prevent duplicate ingest. Bounding their
metadata requires an explicit ingest retry horizon in a later version. An ingest
retry that crashes between re-upload and manifest registration can leave an
unreferenced retired object; reclaiming abandoned uploads remains a follow-up.

The manifest becomes indispensable after compaction. Prefix reconstruction
cannot distinguish published replacements from orphan uploads or retired sources.
If a manifest is missing and compacted objects exist, reads and ingest return
`:compacted_manifest_missing` rather than reconstructing incomplete or duplicate
data. Recovery requires restoring the manifest from object-store version history
or a backup. Legacy prefixes without compaction retain their listing-based
migration path.

Queries that lose a segment to cleanup restart their entire scan once against a
fresh manifest. A second missing object returns an error rather than silently
omitting samples. The retry uses its own snapshot without overwriting the shared
cache, so it cannot hide a later acknowledged append. This protects stale readers
even after extended refresh failures; the grace period alone cannot bound every
snapshot's lifetime.

The next metrics work is label posting indexes, followed by OpenTelemetry metrics ingestion and then alert evaluation.
