# Pulso Architecture

This document is the source of truth for Pulso's design decisions. Every non-trivial contribution should be consistent with what it says. When reality forces a divergence, update this file in the same change.

## What Pulso is

A headless, open-source observability backend: unified logs, metrics, and traces access, with built-in alerting and a native Model Context Protocol (MCP) interface, built on Elixir/OTP with a Rust hot path for columnar data.

Pulso ships no UI. It exists to be talked to by humans through their own dashboards and, first-class, by AI agents through MCP.

## Core bets

Four bets shape everything below. Deviating from any of them is a redesign, not a refactor.

1. **Headless nodes, shared-nothing at the write path.** No shared database, no leader election, no consensus service, no cluster-visible mutable state. Nodes coordinate only through object storage. Kill a node, launch another; nothing is lost.
2. **Object storage is the source of truth.** S3 (or any S3-compatible: R2, GCS, Azure Blob, MinIO) holds every acknowledged record until an explicitly configured retention policy expires it (indefinitely by default). Local disk is a warm cache and nothing more.
3. **BEAM for orchestration, Rust for bytes.** Elixir/OTP owns concurrency, supervision, per-tenant isolation, backpressure, and the MCP surface. Rust owns Parquet encode/decode, DataFusion query execution, and the S3 object store client, called via Rustler NIFs. Object-store calls never block a BEAM scheduler: the NIF starts the request on its tokio runtime and replies to the caller's mailbox, so in-flight provider requests are not capped by the VM's dirty I/O schedulers. Nothing crosses the boundary that doesn't need to.
4. **MCP is first-class, not bolted on.** The primary read surface is MCP tools. HTTP APIs exist for compatibility with agents (Alloy, OTel collectors, Prometheus remote_write clients) that expect them, not as the recommended read path.

## Reference architecture

Cursor's [Git at Any Scale](https://cursor.com/blog/git-at-any-scale) writeup is the closest reference implementation of what Pulso's coordination layer should look like. Reread it if any of the bets above become unclear.

For protocol engineering, pinned [GreptimeDB/OpenObserve source notes](../plans/backend-protocol-references.md)
provide naming/default/oneof comparisons, not architecture authority or semantic
acceptance. Follow the actual OTLP deviations from generic ProtoJSON rather than
blindly importing another receiver's leniency; S3/CAS remains Pulso's authority.

## Storage model

### Signals and formats

All three signals are stored in **Apache Parquet** files in S3. Same substrate as Tempo (VParquet blocks), OpenObserve, and increasingly common across the modern observability stack.

For each signal Pulso stores:

- **Segment files** (`s3://<bucket>/tenants/<tenant>/v4/signal=<s>/date=<Y-m-d>/hour=<H>/<min_ts>-<max_ts>-<suffix>.parquet`): immutable Parquet objects containing the records themselves. The zero-padded 20-digit `[min_ts, max_ts]` tail is what the query path prunes against at LIST time; the `date=`/`hour=` partitions are derived from `min_ts` as UTC and let Athena-style callers prune without opening the manifest.
- **Sidecar index files** (`.bloom`, `.postings`, `.stats`), written alongside the segment at flush time, immutable, live in S3. **External sidecars are not yet implemented.** Metrics now have bounded label→canonical-series→row-group postings inside their existing Parquet footer, reducing native decode work. Opt-in footer-first metric reads can also avoid full-object GETs when no group survives, under one strong-ETag snapshot. External label postings and selected-column/group range reads remain follow-ups.
- **Per-tenant, per-signal manifest** (`s3://<bucket>/tenants/<tenant>/v4/signal=<s>/manifest.json`): the list of segments that currently exist for this (tenant, signal), each with its time range, row count, and any tiny summary metadata the query planner needs to decide whether to open it. Formats 1 and 2 are flat lists. Format 3, written only when retention is enforced, is a bounded root over immutable time-bucket pages; see [Retention implementation](#retention-implementation).

The manifest root is the only file for a tenant that ever gets rewritten. Segments, sidecar indexes and format-3 metadata pages are write-once.

### Target segment lifecycle

The numbered flow below is the intended buffered/forwarded shape, not the
current ingest implementation. Today every authorized instance writes directly
to shared S3; optional unkeyed buffers coalesce only on that instance. There is
no ingest forwarding, rendezvous-owned ingest buffer or external sidecar index.
Acknowledgement already requires the segment PUT and manifest publication.

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
Optional unkeyed append buffers are node-local, not rendezvous-owned. They can
coalesce concurrent requests within a configured 1–1000 ms window before encoding
one segment. Keyed requests stay on the original direct path. Admission counters
reserve at most 128 callers, 100,000 rows and 10 MiB of estimated input terms per
buffer, including queued and executing work; overflow is processed unbuffered.
Reservations are released by the buffer only after publication finishes, never by
caller timeouts. Both segment PUT and manifest CAS precede every acknowledgment. If the latest
floor rejects a combined upload, multiple callers receive retryable
`retention_floor_advanced`, not a permanent error inherited from an expired
sibling. Never split after a potentially ambiguous publication. Lost legacy
CAS responses succeed only when a fresh, ETag-bearing root proves all pending
keys visible, never from unpublished LIST reconstruction.
No local durability or new cluster state is introduced; unkeyed lost-response
retries still have at-least-once semantics. Idle buffers terminate after 30 seconds.
`ManifestOwner` is a node-local request coalescer, not a cluster-wide owner;
conditional object-storage writes arbitrate concurrent ingest from multiple
nodes. Ingest forwarding and buffered ingest ownership remain future features and
must reuse `Pulso.Rendezvous` with their own eligible roles. Opt-in native alert
evaluation already uses eligible-worker rendezvous ownership and rule-head CAS.
Query caches and query admission limits deliberately remain local to each node.

### S3 CAS for manifest updates

Manifest updates use S3 conditional PUT (`If-Match: <etag>` on writes; `If-None-Match: *` for first-time creation). All major S3-compatible providers support this as of 2024.

Initial ingest into a prefix without a manifest reconstructs legacy segments by
listing, then publishes the rebuilt entries and new segment metadata in one
conditional creation. There is no intermediate bootstrap manifest PUT. The
unpublished snapshot does not enter the query cache. Read-first legacy migration
still publishes its reconstruction before returning. Prefixes carrying the
retention `.managed` marker never rebuild by listing, and compacted prefixes
without a manifest still fail closed.

Each instance has its own manifest coalescer. Independent instances can write
the same manifest continuously, so CAS conflicts are normal, not limited to
failover. Maintenance publications can also overlap local ingest. Losers reload
and retry against fresh authority; no leader or election is involved. Adding
instances increases aggregate CAS/provider pressure even when local batching
reduces each instance's request rate.

### Immutable keyed uploads

All signal appends with a nonempty idempotency key use conditional object
creation before manifest registration. A failed create, including an ambiguous
response, verifies the existing GET snapshot against the exact locally encoded
bytes in Rust. Verification checks declared length, streamed bytes and EOF;
no second full-object binary, Elixir record expansion or semantic/hash inference.
At most two create/verify rounds handle a precondition followed by missing GET.
Each verification has a 15s async timeout, shortened by any internal append
`deadline_ms`; this does not bound encoding/PUT/CAS or preempt native work.
Provider SDK retries are additional, and every instance contributes GET pressure.
Consumed-body metrics are not billed wire bytes. Unkeyed/random uploads and fresh
compaction replacement keys retain their existing PUT path.

Exact bytes prove the size and summaries registered through the existing CAS;
different bytes never register the incoming candidate or overwrite the object.
They fail permanently as HTTP409 `keyed_content_mismatch` (OTLP code6), counted
by the label-free `pulso_keyed_publication_conflicts_total`. Semantic equivalence
between encoders is not guessed. Retired keys still only rearm cleanup, and
latest floors fence publication; failed registration can leave orphans under the
existing bounded retention-sweep/flat-catalog limitations. No parallel receipts,
new mutable authority, locks or single-instance assumptions are introduced.
Local object-store ABI is `{retention1,keyed1,snapshot1,async1}`; old unconditional writers must
all be upgraded/drained, including recording evaluators. Local startup cannot
fence another pool, and stopping old nodes does not repair past damage. See
[keyed ingestion](keyed-ingestion.md) for operator recovery and limitations.

### Freshness via conditional GET

Queriers cache manifests locally, keyed by ETag. Ordinary queries reuse entries
for `refresh_stale_ms` (one second by default), then issue a conditional GET.
304 validates the snapshot; 200 supplies a new snapshot. Legacy flat roots may
retain their prior snapshot after a transport failure, but a successfully fetched
undecodable/unsupported authority fails closed. Managed-root refresh failures
also fail closed. Required-fresh alert queries validate authority on every lookup
and never reconstruct it by LIST. A required-fresh fetched decode error or
missing root also invalidates the ordinary cache, even when its freshness
interval has not elapsed; an ensuing ordinary query cannot reuse authority just
proven malformed, unsupported or absent. Managed prefixes never reconstruct a
missing root by LIST. Transport failures remain distinct from those authoritative
results.

Read installation compares the cache generation captured before I/O, including
cold reads. Publications compare both their predecessor ETag and their captured
pre-CAS generation, fencing repeated ETag/ABA observations as well as cold
invalidation. An incomparable cache entry is invalidated rather than ordered by
opaque tags. Invalidation leaves a
disposable generation fence so delayed reads/publications cannot resurrect an
older root into a newly cold cache. A cold publication may install only against
the exact empty generation observed before its CAS. A wholly rejected attempt
that performs no CAS/fresh read never republishes its private root into cache.
This state is local cache coordination, not durable or cluster-visible authority. A query can keep its own
admitted snapshot even when its cache installation loses a race.

### Per-tenant metric metadata object

Remote-write metric metadata (type, help, unit) lives in one mutable object per
tenant, `tenants/<t>/v4/metadata/metrics.json`, outside the signal manifests.
Writers merge into it with conditional PUTs (`If-Match`, or `If-None-Match: *`
to create) and jittered retries, so concurrent instances never drop each
other's variants; a push that changes nothing costs one GET and no write.
Readers GET it directly, so there is no cross-instance staleness. Merges apply
expiry and size bounds (`Pulso.MetricMetadata`), keeping the object bounded
without a separate retention job, and retention sweeps never touch it.

### Planned optimistic UDP gossip for cache invalidation

This optimization is not implemented; current visibility uses manifest refresh.
When an ingester publishes a new manifest version it broadcasts `("manifest-updated", tenant, signal, new-etag)` over UDP to peers. Peers use this to shortcut the polling interval on the next query. UDP loss is fine — the conditional GET is the correctness backstop.

## Ingest path

### Wire protocols accepted

Pulso is designed to receive telemetry from **existing agents unchanged**. Priority order:

1. **OTLP/HTTP** (protobuf, gzip): the universal path. Covers logs, metrics, traces.
2. **Prometheus `remote_write`** (Snappy protobuf): the metrics on-ramp the existing Alloy install base already uses.
3. **Loki push API** (`/loki/api/v1/push`, JSON and Snappy variants): the logs on-ramp the existing Alloy install base already uses.
4. **OTLP/gRPC** (protobuf over HTTP/2): an opt-in tonic listener in the `pulso_grpc` NIF owns HTTP/2, decompression, message limits and in-flight bounds. It never decodes OTLP: each raw message reaches the owning BEAM process as a binary backed by the request buffer, with a reply handle, and runs through the same `Pulso.OTLP.Ingest` path as OTLP/HTTP. A dropped handle completes the call as `UNAVAILABLE`.

### Implemented receiver budgets

The current OTLP JSON/protobuf logs and traces, Loki JSON/Snappy logs, and remote-write receivers
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
Logs additionally enforce `max_otlp_nodes` (262,144 default), a 16 MiB
normalized full-record ceiling before append and before Parquet row allocation,
and effective logical depth at most 32 to keep typed envelopes readable. Raw
MCP log result JSON is capped at 16 MiB before native binary allocation. Logs
share the pre-parser heap guard with traces. Native artifact capability checks
fail at startup rather than acknowledging metadata through stale codecs.
Shared OTLP wire capability 3 additionally fences recursive singular-message
merging, protobuf oneof selection, the bounded native merge-plan stage and
schema-scoped raw JSON integer lexemes and receiver-bound duplicate field checks.
The JSON transport passes a static signal root through its decoder MFA, never
infers scope from the payload or changes the general JSON library. Supported
schema fields get disjoint per-object u64 bits (including Span field16); repeated
keys reject regardless of null/value equality or escaped spelling. Every object
starts a fresh mask, while unknown objects/foreign signal roots stay opaque.
Native failure falls back to an independent Elixir token/object-pair reference,
not an ordinary JSON map that would already have discarded duplicate keys. Its
seen sets contain only known fields; transient pair/token trees remain proportional
to the bounded input and may add pressure before semantic admission. No total
native/BEAM memory or node-wide capacity claim follows. JSON parse errors reject
whole before any append, including a valid earlier resource group.
Raw preflight charges all fragments, losing branches and unknown occurrences;
plans retain borrowed slices and supplied owner charges before any BEAM term
expansion. Logical owner counts/bytes/nodes accumulate across fragments, including
overwritten AnyValues. Exact quoted decimal/exponent integers normalize in Elixir;
unquoted integral fields parse natively without float rounding or exponent-sized
bignums, matching an independent Elixir schema/token-callback reference. Invalid
fractions use an unforgeable scalar atom until semantic rejection, retaining the
numeric node/eight-byte admission cost instead of cheaper string accounting.
Unknown objects/floating fields keep ordinary JSON numeric semantics; escaped
keys still select their real schema fields. The OTLP-only zero-mantissa adapter
also preserves `-0e0` against integer-mantissa normalization in Elixir's decoder.
No storage format or existing
signal capability tuple changes; every serving ingress app/native pair must
upgrade. See [shared wire contract](otlp-wire.md).
Traces additionally enforce a configurable request-node preflight (default
262,144, `PULSO_INGEST_MAX_TRACE_NODES`, distinct `too_many_trace_nodes` errors) before native
intermediate-term allocation/JSON normalization, and a cumulative 16 MiB normalized
payload budget while encoding Parquet. Repeated resource/scope metadata counts
per row. A pre-parser OTLP log/trace heap guard caps 16 million words including shared
binaries and restores the previous flag before sending the response so HTTP/1
connection reuse does not change other endpoints' limits. Non-finite trace
attribute doubles become their canonical OTLP JSON spellings and retain the
doubleValue tag, including nested values and resource/scope/event/link owners. Malformed UTF-8 wire strings and all safety-budget
violations still reject the whole request. No truncation or node-wide ingest
admission is introduced. See [ingest limits](ingest-limits.md) for exact counting
and configuration.

### Target per-record flow

This is the forwarding roadmap. Current receivers use direct S3 publication or
optional node-local unkeyed coalescing, as described above; no forwarding or UDP
gossip is implemented.

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

### Target per-query flow

DataFusion, external sidecars and an NVMe segment cache in this flow are planned,
not current query machinery. Today bounded S3 downloads feed native Parquet
filtering/projection/trace aggregation; PromQL/LogQL orchestration and remaining
aggregation use Elixir under admission/heap/deadline limits. Optional metric
footer-first reads can avoid proven-empty object bodies, not fetch arbitrary
selected columns. Ordinary manifest freshness is described above.

1. Query lands on any node (or MCP request, or alert-evaluator internal call — same code path).
2. Node fetches the tenant manifest (conditional GET; usually 304 from local cache).
3. Manifest lists candidate segments; time-range and label-based pruning reduces the set. Sidecar indexes (bloom filters, posting lists) prune further.
4. For each remaining segment, DataFusion opens the Parquet file (from local NVMe cache if present, otherwise from S3 via range GETs), pushes down predicates via Parquet row-group stats, and executes the query.
5. Results merge and return.

Query fan-out is entirely within one node. Nothing crosses node boundaries at query time except S3 I/O.

### Query languages

- **[Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/)** subset for metrics: float selectors and range functions, grouped aggregations, scalar/vector arithmetic and comparisons, set operators, explicit vector matching, classic histogram quantiles, label replacement, clamps, ranking, sorting, rounding, and time functions. Positive selector offsets are supported. See [querying](querying.md) for the exact function set.
- **LogQL** subset for logs. Unwrapped range aggregations support suffix grouping,
  drop the extracted value label from identity and interpolate quantiles over
  grouped observations. Ordinary empty windows remain missing; absence is a
  separate whole-window operation. Binary evaluation reuses typed Prometheus
  matching/set/IEEE operations rather than inferring scalars from empty labels.
  Log metric queries reuse shared sample/work/point counters and charge repeated
  window visits. Raw log queries still use scan/heap limits without a selected
  record cap. Parser byte/nesting/operator and AST bounds precede evaluation.
  Unsupported counter/at/post-unwrap semantics reject explicitly. This is not
  complete Loki pipeline or client parity and does not enable Grafana imports;
  native graph previews are a separate, explicitly bound read-only surface.
- **Traces:** implemented exact trace/span/parent ID lookup, typed attribute search,
  duration bounds, root selection and output projection through `query_traces`;
  bounded fully anchored string resource/span regex predicates are supported;
  `search_traces` accepts bounded single-spanset resource/span string equality
  and anchored regex joined by AND. Native aggregation retains every trace in
  an explicit inclusive span-start window (including nonmatching context), then
  selects matching traces and orders window-local earliest start descending/ID
  ascending before limiting distinct traces. Legacy typed-unknown rows fail
  rather than infer byte/string identity. All-window group cap 65,536 and native
  scanned-row cap 1,000,000 complement shared whole-scan/page/deadline bounds.
  This native contract is not Tempo trace-wide metadata/most-recent semantics;
  full TraceQL, indexed planning and deployed-client parity remain planned.
  Separate default-off Tempo-shaped search aggregates all retained trace context
  before inclusive trace-envelope overlap, full root/duration/service metadata,
  using candidate-first original-string matching or legacy uncertainty. S3 exact
  resource.service.name equality can first omit sources with a complete excluding
  service set AND optional segment `t.k=true` proof: every stored row rich, strict
  core/header projection agreement, original string service.name represented by
  the promoted service column. Native encode/compaction derive this from the exact
  bytes; old/unknown/legacy proofs always read. Compaction reproves rather than
  inheriting flags. Old nodes dropping k only remove pruning. Duplicate original
  keys reject, not last-wins. Invalid core input never appends; projection-invalid
  compaction fails distinctly, leaving sources live, not guessed/repaired.
  Seal replays exact first-pass binaries, then ID-prefix/canonical bloom probes
  select all additional candidate context with no service/time restriction.
  Unknown summaries and a 2M native probe ceiling read conservatively. Complete
  trace context precedes window/order/limit. Read context validates all visible
  payloads; new potential IDs after seal poison the plan. Unread proven-irrelevant
  objects are not scrubbed for corruption/existence. Catalog enumeration remains
  metadata-byte/page/heap bounded (8 MiB default), hard-capped at 8192 refs rather
  than truncating; this is not a guaranteed capacity. Actual fetched sources keep
  cumulative 1024-input/32 MiB/1M-footer-row bounds across both phases even after
  input owners are freed. Retries abort native state and retain spent I/O bounds.
  Each native pass visits at most a million rows. Candidate bookkeeping adds
  roughly 4–6 MiB outside BEAM, plus <=512 KiB native hash prefixes and a temporary
  <=2 MiB BEAM ID binary; input/retained/result bounds remain independent.
  Current metadata2/export3/search4/discovery12 tuple {2,1,3,4,12} is required. Search capability4 also adds a separate bounded same-parent boolean RPN program for retained-context queries: string negation, original-int64 comparisons, status and own-span duration, with equal-precedence left-associative AND/OR and explicit groups. The old positive-string selector/discovery/window grammar remains separate. A dedicated storage callback fences old adapters; native compilation revalidates stack/types/16-leaf/8-regex bounds. Expression queries bypass service/ID planning and admit all retained sources, then replay exact captured bytes without more GETs. Candidate/context passes both charge two owners, all resource/span attrs and every instruction under an additional 20M-work ceiling before filtering; no DNF expansion or short-circuit accounting. Samples retain individually matching typed attributes; legacy/corruption and all limits fail whole. This is scoped public Tempo 2.9 engine/Parquet evidence, not full TraceQL or capacity acceptance. See [operator semantics](trace-query-expressions.md). This bounded immutable
  plan is not representative posting/index/client/capacity acceptance; old sources,
  false positives or read-all fallback can still exhaust limits late in context.
  It preserves
  most-recent ordering and sampled spansets; actual public Tempo Parquet/engine
  fixtures establish only its explicit selector subset. Exact-ID v1/v2 export
  uses complete stored assembly and faithful typed serialization. Unfiltered
  resource/span/instrumentation/event/link discovery scans retained rows directly into bounded name/typed
  value sets, without trace grouping or BEAM span expansion. Names preserve all
  keys; value discovery supports original string/int64/bool/float values (nonfinite classes included) and rejects
  targeted unsupported types. Every visible legacy row fails whole discovery;
  value filters accept bounded same-span original-string equality/regex AND;
  every visible row validates before filtering and raw scan work stays charged.
  Requested-scope owners and their attribute entries additionally spend an
  independent 2,000,000-work ceiling per scan attempt before filters, even empty events/links.
  This intentionally lowers flat-scope capacity to floor(2M / (1 + attribute count))
  parents, with explicit trace_discovery_work_limit, and recharges repeated
  instrumentation owners per span. Retried provider/byte/row work stays cumulative.
  Nested attribute sets never flatten into span attributes or invent intrinsics;
  filters still target resource/span strings on one parent, not nested predicates.
  Filtered adapters must implement a separate explicit optional callback;
  legacy unfiltered callbacks cannot silently discard filters. Missing support
  uses the bounded reference scan instead. Public invalid filters reject before
  private NIF dispatch; raw native constructor errors are not HTTP statuses.
  Upstream same-target equality can return unproven query literals, so Pulso
  explicitly rejects that shortcut. Malformed queries never become unfiltered.
  Filtered names use the same parent predicates, returning complete original
  matching-owner keys plus fixed resource/span promotion hints from all visible
  context (including required Tempo service.name with no original attr). This is
  explicitly not matching-key presence. Unlike upstream name iterators, keys
  seen on earlier nonmatches cannot disappear; 64/75 oracle cases are exact,
  11 are deliberate complete supersets, established independently from actual
  engine-matched IDs and original producer attrs. Unfiltered names/value results
  never use this shortcut. Dynamic dedicated-column parity is not implemented.
  Name adapters need a distinct optional callback, never value-only support.
  Default/none names additionally combine all five namespaces in one native pass
  with 20 intrinsic suggestions (v2); v1 flattens/dedups excluding intrinsics.
  A separate default-name adapter callback fences old scoped callbacks. Default
  service.name is a required-column hint whenever a visible parent exists even
  without a filter/original attr. All five work costs aggregate under 2M, and
  the global 5000 scope/name-pair budget includes 20 intrinsic reservations for
  both versions. Scoped name bytes now count scope strings too. Any error fails
  whole, not partial static suggestions. Unscoped values combine resource/span/
  instrumentation only. Discovery8 adds bounded resource/span string AND filters
  on one parent, then unions its three owners, retaining their original types;
  values need not themselves match the filter literal/regex. This is a complete
  extension, not upstream's ignored-query mingled path. All three owners+attrs
  charge before matching; legacy/corrupt nonmatches fail, matching complex targets
  reject whole. Same-key equality across resource/span rejects the literal shortcut,
  while same-key original-string regex is allowed. Root aliases still reject. Finite float discovery uses
  native Ryu/BEAM-shortest TraceQL spelling, signed zero and integral .0, established
  on 273 actual IEEE cases. Seven stored intrinsic strings accept parent filters,
  charge requested owners/attrs before matching (event names add one parent), and
  fail whole on storage/legacy failures. Complete matching strings retain duplicates
  upstream pre-join distincts can omit: 30/42 exact,12 complete supersets. New
  unfiltered unscoped /3 and dynamic /4 callbacks retain old contracts. Query-aware
  unscoped /4 or bounded reference is required for filtered-none; windowed filtered
  none needs separate explicit /6, never old window /7. Missing support rejects.
  V1 is a canonical-target envelope, not legacy alias parity. Capability 9 is required. Paired positive time bounds select whole retention-visible
  source envelopes using actual start/end columns; floor excludes expired starts
  before min/max. All retained source header rows/provider costs remain charged;
  only selected context incurs typed metadata/owner work. This is block routing,
  not excluded-payload integrity scrubbing or indexed I/O. Two local Arrow passes
  share exact bytes, no second GET/cross-window dictionary cache. Results change
  with source layout/compaction. Explicit window-aware /7 callbacks fence flat
  adapters; independent reference segments retain boundaries.
  Zero/partial windows, TraceQL hints and intrinsic predicates still reject.
  Discovery9 additionally exposes event:timeSinceStart as exact **int nanoseconds**,
  not duration strings, matching public unfiltered Tempo's column path. Bounded
  resource/span original-string filters select one parent, then collect its event
  timestamps minus that parent's start. This is a complete filtered extension
  versus upstream's empty filtered path, not omission parity. Source times are
  nonnegative int64, subtraction cannot overflow, and effective event default0 can
  yield negative offsets; no event invents no value. Ingest writes default0 explicitly;
  a missing stored timestamp fails, never guessed0. Requested times validate even
  on nonmatches, while parent+all events+attrs charge before matching. Offsets are
  high-cardinality; broad dictionaries commonly exceed5000 and fail whole. Selective
  parent filters/narrow source windows can help but compaction broadens sources;
  filters do not reduce raw provider/header or requested-owner work.
  Same global discovery bounds/source-envelope windows remain, not event-time
  point filtering. Explicit event-time /4 or bounded reference and dedicated
  windowed-event-time /6 fence old string /4/window /7; missing support rejects.
  Actual public engine-selected IDs/original times establish seven cases over280
  parents, one exact unfiltered/five complete filtered supersets/one empty;14
  current native public protobuf messages were consumed. These are not deployed
  datasource/provider/physical-layout/capacity acceptance. No new format/state.
  Discovery10 adds complete-retained root name/service dictionaries under the
  same default-off gate. Canonical rootName/trace:rootName and
  rootServiceName/trace:rootService select candidate IDs from source-envelope
  parents, then recover every retained candidate span from the exact admitted
  binaries; root context is never clipped to a service/predicate/window. No
  service/ID planning: all retained sources must fit1024/32MiB/1M raw rows,
  regardless of window or matches, and replay decodes all Arrow columns with
  independent1M replay rows and header/body count agreement. Noncandidate JSON
  is not integrity-scrubbed. Resource/span/link owners+attrs spend2M work before
  parent selection or once on additional context; roots have65,536 candidate
  groups/16MiB bookkeeping and5000-entry/independent1MiB dictionary/result caps.
  Parentless spans with original child_of links are not roots. Distinct root IDs,
  contradictory same-ID root classification or root tuples fail whole; missing
  retained roots or original string root service fail409, never invented defaults.
  This deliberately rejects in-flight/retention-straddling/multi-root traces,
  often making broad/recent autocomplete unusable; no skip-incomplete or exporter
  watermark. It is an explicit fail-closed extension, not upstream default/choice
  parity or full span-ID/export validation. New root /4 and dedicated windowed
  root /6 callbacks fence old span-local string/window execution; flat reference
  fallback uses a sentinel and cannot infer source windows. All serving artifacts
  require current discovery12. Scoped actual public fixture/consumer evidence is not
  representative capacity or deployed-client/physical-layout acceptance.
  Discovery11 adds canonical trace:duration on the same bounded complete-retained
  capture/replay machinery. Parent predicates select trace IDs; min retained start
  and max retained end recover context outside filters/source windows. Dedicated
  constructor/finish and storage /3 + windowed /5 fence old root/string/window
  callbacks; flat sentinel references reject windows. Root absence/conflicts do
  not block duration: it measures only the current retained envelope, may grow
  during ingestion or shrink with retention, and proves no exporter completeness.
  Public upstream paths differ: no predicates emits type int decimal nanoseconds;
  nonempty predicates emit type duration with integer-only Go duration spelling
  (U+00B5 microseconds), both lexical. Keep static duration/span:duration/
  traceDuration empty shortcuts unchanged. Same1024/32MiB/1M raw input and replay
  bounds, no planner/second GET;65,536 groups/80B per-summary charge under16MiB
  charge ceiling, separate from native container/input/Arrow overhead.2M work is
  resource/span owners+attrs, before predicates/additional candidate context once;
  events/links are not requested owners but envelopes still validate.5000 values/
  independent1MiB dictionary/result limits reject whole; nanosecond cardinality
  commonly overflows broad queries. Legacy/corrupt nonmatches still fail. Matching
  app/native artifacts require current tuple {2,1,3,4,12}. Six public fixtures and1047 Go
  formatter cases are scoped evidence, not physical-layout/client/capacity parity.
  A separate intrinsic scope returns tiny static Tempo vocabulary: 20 names and
  seven exact v2 keyword/empty shortcuts. It uses ordinary auth/gate/raw admission
  but never storage. This is not proof of stored values or full TraceQL support;
  static-keyword filters, stored span-duration intrinsics and v1 static shortcuts reject.
  Dotted stored attributes never become keywords. Static JSON/protobuf framing
  stays in Elixir; there is no columnar work to move across the NIF boundary.
  Both compatibility slices share authoritative floors, actual-footer accounting,
  retries/admission/deadlines and disposable native state. No new S3 mutable
  authority or local durable index is introduced. Full retained-history scans,
  independent dictionary/result/row ceilings and native bookkeeping outside the
  BEAM heap cap remain explicit capacity limits. See [traces](traces.md).

New trace segment manifest entries carry versioned complete service sets and
adaptive trace-ID blooms (16–1,024 bytes in new segments, up to 8,192 in legacy summaries, four SHA-256-derived bit
positions). Unknown versions, absent/invalid summaries and legacy LIST rebuilds
always scan. Bloom false positives consume scan budgets; they never authorize
partial results. These are bounded per-segment pruning aids, not dedicated
trace-ID postings or a replacement for trace compaction.

Trace decoders maintain native top-k candidates before Erlang term construction,
using typed columns and a canonical, type-preserving metadata key shared with an
Elixir reference. Attribute predicates inspect requested JSON fields without
materializing rejected rows. Only surviving payloads get independent binaries,
so a tiny result cannot pin whole Arrow payload arenas. S3 merges with a bounded
global top-k by merging ordered streams, lazily comparing metadata only on
prefix ties, and a 16 MiB metadata-key budget measured without constructing
keys. A running inclusive kth-start floor avoids expanding older candidates,
without losing boundary ties. GC before each dirty-NIF expansion reclaims
preceding merge garbage. Each native candidate set also has independent 16 MiB
payload and sort-key ceilings. Explicit limited scans charge visited
segments/bytes/rows, recovering missing trace row counts from Parquet footers,
rather than rejecting all potential older candidates up front. Explicit result
limits do not retain every matching span. Default unbounded raw responses still error above 5,000 matches. Traces
have opt-in native compaction and use the shared format-3 event-time retention
and bounded immutable catalogs. Flat manifests remain unbounded when retention
has not been enforced. This is not a production Tempo replacement.

Full grammar coverage is a long tail; ship the useful subset first. Each language is parsed in Elixir; the intended query plan compiles to DataFusion in Rust. Today label and time filters run in the Rust Parquet decoder, while log and metric aggregations run in Elixir. Metrics fetch the selected interval once per selector for all evaluation steps, without a sample limit that could silently truncate aggregations. The initial metrics evaluator enforces independent sample, work, scan, result, heap, and concurrency limits with explicit errors; columnar aggregation remains a follow-up.

The read-only `query_promql` tool accepts a tenant, expression, and optional evaluation time or range with step. Compatibility routes `GET|POST /api/v1/query` and `/api/v1/query_range` share the evaluator and tenant authorization. Responses use Prometheus scalar/vector/matrix envelopes with numeric timestamps in seconds and Prometheus decimal/exponent value formatting. Evaluation retains Pulso nanosecond precision rather than truncating to Prometheus milliseconds. Selectors use a left-open five-minute lookback; range functions use left-open windows, counter-reset correction, and Prometheus boundary extrapolation. Unsupported syntax is rejected explicitly: omitted-resolution subqueries, `@`, negative offsets, native histograms, and functions outside the documented subset. The parser caps ASTs at 256 nodes and 64 levels. Binary operators implement Prometheus precedence, matching cardinalities, metric-name rules, and explicit duplicate-result errors. Instant sort/ranking operations preserve numeric ordering; range results retain canonical label ordering. Keyword parsing is case-insensitive; malformed nested aggregations cannot retry the generic-function branch. Sums and averages use Prometheus's compensated summation and overflow-safe incremental mean. Classic bucket parsing accepts Go float spellings, and quantiles retain IEEE NaN/infinity behavior. Both storage adapters use the same anchored native regular-expression engine, including missing-label and ASCII character-class semantics. Unsupported regular-expression constructs, including Rust class-set extensions and nested classes, are rejected; full Prometheus syntax compatibility is not claimed. Quoted regular-expression literals (`\Q...\E`), octal regular-expression escapes, literal unescaped brackets in classes, and Unicode-heavy repeats exceeding the native program budget currently return errors. Stale markers and IEEE-754 non-finite values use the closed atoms `:stale`, `:nan`, `:infinity`, and `:negative_infinity` across BEAM/native boundaries. Parquet retains its Float64 value column: stale markers map to the exact `0x7ff0000000000002` NaN payload, ordinary NaNs normalize, and infinities retain their signs. The Float64 schema is unchanged, but this is a data compatibility boundary: collectors emit stale markers automatically and new writers persist them immediately. Stop old readers/writers/compactors before starting new writers (use a `Recreate` upgrade for the supported single-node chart). An image rollback cannot read a bucket after a non-finite write; recovery must retain a compatible reader or use a separately validated migration. See the operator [upgrade procedure](deployment.md#stale-sample-storage-compatibility). Instant selectors suppress a series whose latest sample is stale, while range functions ignore markers. Out-of-order data is sorted; same-timestamp conflicts prefer staleness, then the maximum ordinary value, with NaN last. Compaction preserves the values rather than evaluating or deduplicating them. Decimal timestamps are parsed without floating-point conversion and bounded to signed 64-bit nanoseconds; selector durations are limited to one year. Endpoint `timeout` is accepted and clamped to the ten-second server maximum; other unsupported parameter overrides are rejected.

Expressions are limited to 16,384 bytes, 64 matchers per selector, and 1,024 bytes per regular expression. Native regular-expression programs and their deterministic-automaton caches each have a one-mebibyte limit. Public Prometheus Query Language, LogQL, raw tool, and Loki label-discovery queries run under supervised tasks with a ten-second timeout and a 16-million-word heap limit. A node admits at most four tasks, of which interactive requests can occupy three; each tenant can hold two. Raw tools and label discovery each have one class slot by default. Busy requests return a retryable capacity error. `Pulso.QueryRunner` configures `max_global` at startup and `max_per_tenant`, `max_interactive`, `class_limits`, `timeout_ms`, `max_heap_words`, `max_scan_segments`, `max_scan_bytes`, and `max_scan_rows` for new requests. These scan budgets default to 1,024 candidate objects, 128 mebibytes of object data, and one million manifest-reported rows. A native call still occupies its task and slots until it actually exits, even after a request timeout.

The metrics evaluator additionally bounds selected samples at 100,000, work at five million units, result points at 100,000, and evaluation steps at 11,000. Selected samples and work accumulate across selectors and operations in an expression; binary/function intermediate point allocations share the result-point budget. Object scan accounting remains per selector. Binary/function inputs are indexed by evaluation timestamp once, avoiding repeated full-series scans. Query-local native regex resources provide `label_replace` captures with the same bounded engine and ASCII class semantics as selectors, and Elixir implements Go-style replacement expansion without creating atoms. Explicit-resolution subqueries evaluate one shared epoch-aligned grid of at most 11,000 points per level. Materialized subquery samples share an independent 100,000-sample budget (`query_intermediate_limit`) rather than the output-point budget. Absence-label inference follows the observable Prometheus engine, including TSDB's intersecting-before-subtracting matcher ordering. Millisecond-aligned timestamp conversions avoid double rounding. Equal-label range series merge only when their evaluation times do not overlap. Operators can tune its `max_heap_words`, `max_samples`, `max_work`, `max_result_points`, `max_materialized_samples`, `max_scan_segments`, `max_scan_bytes`, and `max_scan_rows`; the shared runner's heap setting applies unless the evaluator overrides it. Sample decoding stops at the budget, while sliding windows avoid rescanning every sample at every step. Log queries use scan and heap budgets rather than a selected-record cap because Loki applies its result limit after matching. Raw tools cap unbounded responses at 5,000 records; an explicit result limit retains its requested semantics. Label discovery fails explicitly when more than 5,000 matching log records would be needed to produce a complete list. Narrow its time range until posting indexes become available. Resource exhaustion returns an error without partial results. These are independent ceilings, not guaranteed query capacity. Unexpected task crashes are reported as server execution failures. Scans preflight manifest counts and known sizes, check actual downloaded bytes, and check deadlines between objects. Conflicting values at the same timestamp resolve to the largest value deterministically and add a response warning. This policy preserves repeatable results across segment ordering; it does not claim ingest-time duplicate rejection.

### Optional metric footer-first reads

`PULSO_METRICS_FOOTER_PRUNING_ENABLED` defaults off. Ordinary metric source reads
probe a 64 KiB suffix, at most one expanded suffix up to 1 MiB, then use the full
decoder's time/postings group selection. Unknown/malformed optional evidence
requires a full matching body; zero surviving groups skip it. A complete suffix
is reused. Discovery, writes and maintenance stay on their existing paths.
Elixir owns staged I/O/deadlines/budgets; Rust owns bounded suffix metadata and
filtering. The tail-only ChunkReader never fabricates unavailable bytes. Full
metric and footer readers share immutable Erlang-owned input through
`OwnedEnv`/`Bytes::from_owner` instead of copying complete blobs/tails. Both use
the same required/optional Arrow-column type checks before a negative proof;
malformed optional columns cannot turn corruption into a successful empty result.

Snapshot GET validates full source size, exact returned range and strong ETag
before allocation, streams to one Erlang binary with exact EOF checks, and caps
I/O timeout at 15s shortened by caller deadline. Every subsequent read has If-Match
and must return the same tag/size; provider/protocol failures fail whole. Native
parsing can outlive deadlines. Successful body-byte metrics are not billed wire
costs. Every replica contributes independent request/memory pressure.

Each source costs max(full logical size, all successful transferred bytes), so
probe-only exclusions do not enlarge admission. Complete small suffixes cost
size once. Enabled reads additionally cap each source at 128 MiB/1M actual footer
rows under remaining query budgets. Actual group/header row counts agree before
expansion/exclusion. Missing-source retries retain reserved source/transfer/row
costs; 412 does not mix versions. Matching sources can add two GETs and duplicate
footer bytes. Excluded bodies are not integrity-scrubbed. No new authority,
format, registration or keyed encoder bytes; current metric tuple `{2,2,1,1,1}` and
object tuple `{1,1,1,1}` fence local artifacts only. Full column/page range reads,
external indexes, node-wide admission and representative capacity remain open.
See [operator contract](metric-footer-pruning.md).

### Metric discovery

`discover_metrics` and GET/form-POST Prometheus labels/label-values/series routes
share `QueryRunner` discovery admission. Selectors are ORed in one scan, with
AND matchers and empty-label matching within each branch; only single-selector
manifest pruning is reused. Native disposable state projects timestamp/canonical
labels, preserves Binary dictionaries and caches at most 1024 dictionary indices
per batch, charging every row plus distinct-key parse/matcher work. It never
expands sample values/OTLP envelopes into BEAM. Actual source footer/group row
counts are charged before native accumulation; decode counts agree with headers.
Missing-source retries abort/restart state and retain spent source budgets.

Results are sample-backed exact inclusive time presence, not TSDB block/tombstone
metadata semantics. Empty-valued labels project as absent; raw stored labels and
OTLP metadata never change. Names/values sort by bytes, series by pairwise label
name/value tuples with an order-preserving native key. Global 5000-entry/1MiB
key/encoded limits, 16MiB dictionary probes, 5M work per attempt and hard
1024-source/128MiB/1M-footer-row caps reject whole, never truncate. Metadata bounds
and BEAM heap/deadline remain independent of Arrow/container memory. Unrequested
value/OTLP columns are not integrity-scrubbed. Non-native adapters use a bounded
100,000-selected-record reference with a sentinel and equal-label run cache.
Read-only storage callback5 and metric discovery API1 extend local codec tuple
to include discovery1 without changing files (current tuple `{2,2,1,1,1}` also includes source-start retention, enriched OTLP decoding and footer planning). Upgrade serving app/native pairs, not infer
old pooled nodes are fenced. Omitted times scan retained history; no external
postings/range reads or representative client/provider capacity acceptance.
HTTP transport bounds query/forms to 32KiB each and checks raw duplicate scalars
before any silent form normalization. See [operator contract](metric-discovery.md).

### Remote Write 2.0 source starts

The experimental public protocol moved from per-series `created_timestamp`
(Prometheus 3.5.0/rc.3, series field 6) to per-point `start_timestamp` (3.8.0/rc.4,
sample field 3, histogram field 17). Both retain signed int64 milliseconds in
`MetricSample.start_timestamp_ms`; zero/missing is nil, without rescaling overflow.
A present modern field wins over the legacy fallback, even explicit zero; this
hybrid policy is ours. Starts do not enter labels, series identity, sample time,
manifest ranges or retention. There are no synthetic zero samples, start-aware
rates, local reset history or additional authority. Bounded native preflight
validates start wire fields before any metadata/sample publication; source
sample admission is unchanged. Real pinned default Prometheus receivers verify
written counts and stored-query values, not optional zero-injection behavior.

An optional nullable Parquet `Int64 start_timestamp_ms` column is emitted only
when needed, preserving all nil-start keyed encoder bytes/content hashes. A
non-null start participates in content hashes, decode duplicate sharing and
compaction. Metric metadata/start capability 2 fences both storage preservation
and v2 decoder point triples; current tuple is `{2,2,1,1,1}`. Retention activates
immediately, so require a fleet-wide maintenance pause: block all metric ingress,
drain old keyed retries, stop all old writers/readers/compactors, upgrade the
complete fleet, then resume delivery. An old compactor can erase starts; mixed
ingress can select different content-derived keys. A rolling upgrade under
traffic or disabling only compaction is unsafe. There is no rollout switch.
Never bypass exact-byte conflicts. Local checks do not
fence other pools; optional zero-injection/newer stateless validation, real
provider/separate-node/deployed-client capacity and historical recovery remain
open. See [operator contract](metrics.md#remote-write-20-start-timestamps).

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

Canonical `Pulso.SelfMetrics` families retain their published `layer` labels,
latency summaries, distinct decoder-rejected/publication-failed record counts,
and aggregate payload/progress metrics. Query instrumentation covers HTTP, MCP,
evaluators, and storage as separate layers. The `Pulso.Metrics` detailed view adds
route/tool distinctions, purpose-aware object bytes, histograms, and separate
queue/runtime gauges. Optional unkeyed append buffers expose active count and
queued/executing caller, row, and estimated input-byte reservations through
direct ETS reads, without messaging buffers blocked in storage I/O. These
estimates are not heap memory measurements, and unbuffered overflow is excluded.
Overlapping operation, duration, and compaction family
names use `pulso_detailed_` prefixes; receiver-only delivery counts use
`pulso_ingest_delivery_records_total`. The two views describe overlapping work
and must not be summed. Native operations and record publication run once.

Record counts describe acknowledged attempts, not unique stored rows. Canonical
publication failures remain separate from decoder drops; the delivery view
combines them as rejected deliveries. Pre-decode failures count requests without
inventing record counts. Tool errors remain observable even over HTTP 200.
Object metrics describe logical calls and successful body bytes, not all billed
retries or network transfers. Compaction and cleanup
report completed attempts and confirmed segment counts. A separate background
worker deadline counter exposes timeouts even before stalled native work finishes. See
[self-monitoring](self-monitoring.md) for the full contract and limitations.
Scrape every node into an independent monitor during rollout. The endpoint is
not tenant-authenticated and shares the ingest listener. Keep the whole listener
private, or deny/separately authenticate router-equivalent metrics paths at every
public ingress proxy; port-level network policy cannot isolate this path from
ingest. It adds no cluster state or admission policy.

## Alerting

Alerting has an **experimental implemented native slice**: object-backed rule
management, immutable revision audit chains, bounded per-rule transition replay,
and opt-in native Prometheus/LogQL threshold and Prometheus recording evaluation. See [alerting](alerting.md)
for the available endpoints, credentials, semantics and limits. Conditional head
writes publish configuration/audit and evaluation state; every completed tick is
fenced, including unchanged normal results. Native threshold rules have explicit
`keep_last` (default) or `ok` whole-result no-data policy; `ok` honors pending and
keep-firing transitions while health remains no-data. LogQL thresholds execute
metric-producing instant queries under reserved background admission, capped at
128 instances. Authored `expression_threshold` rules combine 1–16 explicit
native instant PromQL/metric-LogQL sources and a bounded pure math condition.
They execute under one batch worker/deadline/heap/background slot with cumulative
source counters and conservatively partitioned scans. The kernel retains its
independent fixed work/input/output/label quotas. Native execution rejects every
label-drop notice; no implicit unlabeled fallback or partial alert set. All refs
must be declared and used; no external UIDs/URLs/tenants, range frames or Grafana
wrapper enablement. Optional source `empty_value: 0` substitutes a scalar only
for a successful fully empty vector, never an error, missing authority or a
partially missing label set. Fixed kernel violations checkpoint error health;
only transient source budgets/authority failures and deadlines skip. Overflow publishes error health, never a partial set of fires.
Native alert/recording selectors require a conditional S3 root read on every
lookup: missing authority or failed freshness skips without advancing the
watermark or resolving state. Required reads never adopt LIST objects or
rebuild missing authority. Node-local bounded-reason skip counters expose
missing-authority/freshness/capacity/timeout/budget/overrun without retaining
identities; they are not durable progress. Ordinary query caching is unchanged.
Upgrade all evaluators before introducing new policy/kinds; old workers do not
apply these semantics. Audit snapshots are retained without
GC in this slice; replay floors do not authorize deleting them. Tombstones and a
single bounded history page list retain older generations' reachable data.

Native Slack delivery uses bounded per-target outboxes in the same rule authority;
lease claims and acknowledgements are conditional head writes. Queue references
outlive replay pruning. Delivery is at-least-once, without remote-order guarantees.
Native resource subscriptions are request-scoped HTTP streams polling committed
heads, with bounded admission and authorization refresh; hints are not replay.

Rule-head GETs are bounded to 1 MiB before decoding; immutable revision, event and
recording reads use their authoritative reference size as the download ceiling.
Native recordings publish immutable, bounded result intents through the rule-head
CAS before keyed metric append. A FIFO queue survives edits, deletion, and
re-creation; conditional acknowledgement follows durable metric publication.
Current series and their potential retirement each fit 31 KiB; a joint transition
is at most 62 KiB plus bounded envelope metadata. Successful disappearance emits
stale markers. Configuration retirement uses a strictly later timestamp and
fences new ticks, preserving prior answers. Ordinary publication reserves slot 64
for configuration retirement. A bounded drain plus empty-queue gating prevents
refill-neutral catch-up; there is no missed-tick backfill. Explicit metrics-floor
expiry retires an intent with a committed diagnostic event. Native publication
is not recording-group ordering, source residency or same-tick freshness.

Grafana routing/silences, recording translation/source closure and production
lifecycle execution are **not implemented yet**. Original Grafana rules are
losslessly representable but remain disabled. A bounded pure expression kernel
and read-only previews support closed math/threshold/strict-last nodes with
explicit same-tenant native Prometheus instant and metric-producing LogQL
instant/range source bindings. Loki ranges use explicit window/resolution and
sparse native frames without inferred Grafana gap filling. No datasource-frame
or full-pipeline parity, external adapter, source-residency proof, recording
dependency ordering, templates or Grafana lifecycle policy is inferred. Source queries and kernel work share one
admitted query task, deadline and heap. Native batch samples/work/materialized
and output counters are shared across languages; logs charge samples/work before
pipeline filtering. Static selector counts partition the smaller query-runner and
PromQL object scan ceilings without borrowing unused allowances. Nonempty
pipeline error labels reject metric queries before grouping. Mixed previews
consume the PromQL admission class and heap, not separate per-source class slots.
This does not change ordinary single-query per-selector scan accounting or bound
billed metadata/retry traffic. The full implementation and compatibility plan is [S3-backed alerting](../plans/alerting-implementation-plan.md).
The remaining sections constrain that planned functionality; they do not imply
that every described endpoint or coordination mechanism exists.

### Rules and durable state

Tenant-scoped rule revisions, routing, silences and lifecycle state live in a
separate `tenants/<tenant>/alerting/v1/` object namespace. IDs and tenant paths
must be validated and encoded. Rule heads use conditional writes and are the
single publication authority for configuration revision, enabled/paused state,
completed evaluation timestamp, pending/firing/keep-firing state, committed
bounded inline event tail, bounded sealed-page reference list/replay floor and per-target
notification progress. Fresh nonces scope revisions/events/cursors; retained old
generation roots and detached drain heads preserve recovery through re-creation.
Edits and disable/delete tombstones contend on the same head, fencing stale
evaluators. Durable paginated discovery must recover idle tenants and rules after
every node restarts.

Each immutable full configuration revision IS its audit record: parent reference,
change sequence, operation/request identity, principal, reason, publication time
and classified impact. The same head CAS publishes configuration and audit; no
second audit journal/index or duplicate change event. Compute authorized diffs on
read. Audit pagination follows parents from a frozen committed tip, independently
of forward lifecycle replay. Budget exhaustion continues, not a fabricated gap.
Historical snapshots survive their explicit audit horizon, including deleted
generations; publish retention floors before GC. Lifecycle-registry removal does
not authorize deleting audit snapshots: GC is per object class and protects the
cross-generation parent chain until its audit floor expires. Restore creates a
new authorized revision, never rewinds history or silently re-enables a rule. API/MCP share
history, revision and diff reads; change subscriptions are opt-in, not firing
triggers. Secret values are excluded. Snapshot and change-metadata sensitivity
remain historical even after a source is removed.

Write/audit APIs require operator-configured per-principal hashed credentials and
capabilities, not just existing tenant-shared auth. A service credential identifies
the service, not an independently verified end user. Delegated identities are
deferred; no accounts database or S3 credential registry is introduced.

### Rule evaluators

Each rule has an expected rendezvous-hashed evaluator among live eligible
workers, reusing `Pulso.Rendezvous`. Supervised, resource-bounded tasks use the
same query/storage path as interactive requests, with reserved background
admission and explicit freshness requirements. Ownership only reduces duplicate
work: nodes can disagree, and conditional object publication enforces correctness.
Pending and keep-firing timers survive restart; query failure, unavailable
freshness and skipped admission must not become a successful empty result.

Rules can contain source queries and a bounded expression graph. Explicit,
operator-created, capability-scoped read-only external source bindings may remain
necessary during migration. An existing SQL source is never an alert-state store
or coordinator. HTTP and stateless MCP share validation, authorization, preview
and management semantics; preview does not emit transitions or notifications.
Infrastructure remediation remains outside Pulso.

### Transition publication

Prepare content-addressed candidate transitions and sealed history pages, then
publish their bounded page-reference/range list, tail, replay floor and lifecycle
in the rule-head CAS with a monotonic evaluation timestamp. Logical IDs live
inside candidate objects, not in keys competing writers can reserve with different
bytes. References carry digest/size and readers validate them. Flat bounded pages
provide forward progress within the committed floor without a copy-on-write tree.
Retention is count or time, whichever limit is reached first; it is not a promised
time window under arbitrary churn. Floor advancement precedes GC.
Unreferenced losing candidates are not fires and cannot trigger delivery.
Request-bound operation identities and content digests resolve lost responses.
Exact retries reuse an identity; changed payload/precondition requires a new one.
A bounded ancestry search without a match is ambiguous, not proof of failure. Read-back is
required after any conditional failure, including conflicts: automatic client
retries can report a conflict after the first attempt already committed. Never
blindly increment a sequence or assume a failed response means no write occurred.

This deduplicates **logical committed transitions**, not external notifications.
Live outbox/payload references must remain recoverable throughout the retry
horizon; retention and orphan cleanup cannot erase outstanding work. Exact
committed history is distinct from coalescing notification snapshots and has an
explicit replay floor. Disconnected event consumers do not pin retention.

### Notification routing and delivery

Separate supervised workers materialize groups and channel intents from committed
outboxes. Consumption watermarks advance in the same CAS as the resulting group
or channel state, and source entries are pruned only after every target has
consumed, resynchronized an explicit gap, or durably retired them. Target-specific
backpressure must not freeze lifecycle evaluation or other channels. Group/channel
heads coalesce the latest membership snapshot, while source history preserves
committed transitions. Grouping/timing, fan-out, silences, resolved messages and
retry progress are durable in S3, never in a local broker or database.

Start with Slack and compatible webhooks; preserve incident integrations before
transferring paging responsibility. Destinations reference deployment secrets;
API/MCP responses and diagnostics do not disclose credentials. Notification
configuration, paging-affecting silences and explicit test sends require distinct
capabilities beyond diagnosis. V1 agents cannot create or alter silences; humans
need a separate audited silence capability. Defer approval-attestation protocols until a trusted agent
suppression workflow is explicitly designed.

Delivery is **at least once** unless a provider's idempotency contract gives a
stronger guarantee. A crash after an accepted remote send but before recording
its receipt can produce a duplicate on retry. Ownership and expiring delivery
claims fence S3 commits, not remote HTTP effects. Document this limitation and
use verified receiver deduplication rather than claiming exactly-once delivery.

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
Tool discovery emits no change notifications. Native alert resource subscriptions
use request-scoped `subscriptions/listen`; an empty filter completes immediately,
while authorized alert watches can emit resource-update hints. A present `Origin` header must
match `PULSO_MCP_ALLOWED_ORIGINS` (`403` otherwise) and POST bodies must be
`application/json` (`415` otherwise); both checks run before the body is parsed, on
the percent-decoded path the router matches. Requests without an `Origin` header are
accepted. Legacy initialize-based clients, including Atlas's proxy at
the time of writing, need a client-side migration rather than a compatibility shim.

Native alert subscriptions use standard `subscriptions/listen` resource watches,
`notifications/resources/updated`, and authorized `resources/list`/`resources/read`.
The specification requires acknowledgement-first ordering and request-scoped
stream cancellation. Notifications are hints: consumers read durable committed
alert-event pages with encrypted application cursors in tool request/response
bodies, never resource URIs/headers, and deduplicate event IDs. Keys survive the
advertised cursor lifetime; undecodable or scope-changed cursors require explicit
replay restart from retained authorized floors, with continuity uncertainty and
possible duplicates, not jumping to a current-state checkpoint. Restart dedup
uses durable event IDs, not a sequence high-water mark that would skip newly
authorized older events. Reconnection
works through any node without sessions; this revision explicitly does not support
`Last-Event-ID` stream resumption. Bound watchers, replay work and stream lifetimes,
reauthorize long-lived streams, and propagate source capabilities into event and
stored-state reads. Event classification is immutable per event, and notifier
lease changes have separate classified history from evaluator truth. The
[MCP alert events plan](../plans/alerting-mcp-events-plan.md)
defines replay/retention gaps and the external proactive-agent flow. These resource
methods and live subscriptions are implemented for native alerting; broader
Grafana notification-policy compatibility is not. See [native alert resources](alerting.md).
Agent-triggered infrastructure remediation remains in a separate server with runtime policy/HITL.

All diagnosis tools advertise read-only, non-destructive, idempotent, closed-world
annotations as defined by the [Model Context Protocol tool schema](https://modelcontextprotocol.io/specification/2026-07-28/schema#tool).
Calls validate the schema vocabulary used by the registry before evaluation:
required fields, types, numeric bounds, string lengths, and enum values, including
nested label matchers. Optional null fields retain the same defaults as omitted
fields, and integer-valued decimal numbers are normalized to integers before
validation. `get_trace`, `query_traces`, `search_traces`, `search_trace_metadata`,
`discover_trace_tags`, `discover_trace_tag_values` and `discover_metrics` close
their top-level schemas; misspelled filters reject rather than broaden a query.
Other legacy inputs remain open unless their schema says otherwise. Time bounds must fit signed 64-bit
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
| Write within Pulso | This repo (alert configuration, silence/ack alerts, add annotations) | Configuration/state; notification settings and silences affect external paging and require separate permissions |
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
- Parquet decode and columnar scan at query time: row-group `timestamp_ns` min/max stats prune whole row groups before any column page is read, then per-row time and service filters run in Rust and only surviving rows materialise as Erlang terms. One Erlang binary per string column per batch (the "arena") backs zero-copy sub-binary strings — a batch of N rows costs one arena binary per string column, not one per row.
- DataFusion query plan execution.
- `object_store` crate for S3 GET/PUT/CAS.
- Decompression and wire-format decoding for high-volume ingest protocols (Loki push protobuf today), returning terms whose strings are sub-binaries of the request buffer rather than copies.
- JSON encode/decode for HTTP bodies and responses. The `body`, `attributes`, `resource` and nullable `otlp` log fields are stored as JSON-encoded strings inside the Parquet segment's Utf8 columns and JSON-decoded on read; the shared `Pulso.JSON` fast path defers to Elixir's `JSON` on any case it cannot guarantee to encode identically.
- SIMD-heavy predicate evaluation.

## What each node holds

All local state is disposable. Durable state is recoverable from S3, but recovery
can exhaust explicit work/metadata/deadline budgets; retain-forever flat catalogs
remain unbounded. The Arrow/NVMe/gossip caches below describe the target shape,
not a claim that those optimizations already exist.

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

### Local SDK/Collector bridge evidence

An optional loopback Go SDK 1.43/core Collector 0.137 harness exercises actual
W3C HTTP propagation, gRPC→gzip HTTP, static tenant headers, pre-publication
429/503 retry, real 401/413 permanence and a malformed public-protobuf partial
batch. SDK export request-message snapshots, Collector requests and stored
records reconcile after manifest-supervision restart against an owned HTTP S3
stub. It is not a literal gRPC packet capture, real provider, node restart,
deployed Alloy/TLS, capacity or migration acceptance. Ordinary tests replay
frozen artifacts without Go/Collector/network/probes; live testing has a separate
explicit collector_bridge tag, never the normal S3 integration job.

The source SDK omits link tracestate before export. Collector batch split of
mixed resources in the tested version loses resource/scope schema URLs; a frozen
counterexample prevents claiming transparency. Passing profile disables Collector
processors and queues and keeps tiny SDK batches. No lost metadata is invented.
Queues/processors/other versions need independent acceptance, and caller success
can hide partial backend rejections/background SDK failures. No Pulso receiver,
authority, runtime dependency or native ABI changes. See [bridge contract](trace-collector-bridge.md).

### OTLP trace non-finite class fidelity

All five attribute owners and nested AnyValues accept canonical OTLP double
NaN/positive/negative infinity. BEAM projections use strings, typed envelopes
retain doubleValue, and native validation accepts exactly three non-finite strings
plus finite numeric stored doubles. NaN sign/payload/signaling state normalize;
protobuf exports canonical quiet NaN. Finite/signed-zero contracts are unchanged.
Native and reference metadata validators, export and discovery share this rule.
Original-string selectors never infer string type from those projections; raw
query exact/regex compatibility predicates explicitly inspect projections.
Service promotion now requires original stringValue (including removing prior
byte-service promotion), without rewriting stored history. Drain byte-service
keyed retries. Current trace artifacts {2,1,3,4,12} require all-participant upgrade
before new writers resume; schema envelope1/physical columns remain unchanged.
See [operator compatibility and remaining gaps](trace-otel-compatibility.md).

### Error tracking (structured logs)

Sentry SDK error envelopes (`POST /api/:project/envelope/`) and legacy plain
JSON store requests (`POST /api/:project/store/`) publish structured `Log`
records through the existing Parquet/S3 manifest path. This is a semantic log
subtype, not a new signal/codec. Operator-provisioned hashed ingest keys bind
projects to tenants/services; they never authorize reads. Both segment PUT and
manifest CAS precede HTTP success. Retries can create duplicate occurrences;
stock SDK transports can drop unacknowledged failures, so this is not an
end-to-end at-least-once capture guarantee.

`query_errors` and `list_error_issues` use tenant read authorization and the
existing bounded query runner. Immutable versioned fingerprints are carried by
events; groups/counts/first-last seen are computed from the complete filtered
window, deduplicated by project/event ID. More than 5,000 matching stored
occurrences fails explicitly in the native decoder before result limiting.
Serialization stays inside the supervised task; pages cap JSON text at 2 MiB.
Independently, native decoding charges expanded Arrow column buffers against a
32 MiB whole-scan budget, including filtered-out rows and missing-object retries.
Selected oversized uncompressed row groups fail before expansion. A low-level
ByteArray preflight sums repeated string lengths without concatenating dictionary
values into Arrow arrays; its actual value lengths also bound batch sizing.
This applies to ordinary logs in legacy/mixed segments, not just size-capped
Sentry events. Actual buffer checks still precede Erlang arena materialization. Neither compressed
object size nor small output pages bound expanded payload memory by themselves.
Encrypted expiring cursors bind tool/tenant/filters/window plus a full result
digest and offset. Each continuation freshly rescans and rejects changed results
instead of silently skipping late arrivals or retention removals. This is
bounded diagnosis, not a durable monitoring checkpoint or first-ever novelty.
No mutable issue heads, counter cache, local queue, SQL database or new event
publication authority is needed for these diagnosis tools.

New log manifest entries carry optional `el` complete error-source/project
value sets, reusing independent metric-label summary budgets. Only exact
mismatches prune; old/malformed/oversized summaries remain unknown. Candidate
pruning precedes segment/row/byte scan limits. Mixed log/error segments still
consume full candidate budgets. Service/time pruning and resource-label filtering
reuse the native log path. Errors share log retention. Both diagnosis and
projection validate the structured error subtype before grouping/compacting:
resource labels alone are not an origin discriminator. Malformed claims remain
raw logs and consume scan budgets, but are excluded from error results. The
projector marks their source segments processed instead of repeatedly crashing.

`S3.log_snapshot/2` exposes strictly fresh committed log candidate segments,
retained event-time floor and root ETag under the query scan/metadata/deadline
budgets. It never discovers by listing or falls back to stale roots. Consumers
use bounded `S3.decode_segment/5` after fetching those objects. A manifest
snapshot is not a source journal: inputs can arrive and expire between snapshots,
so projections must explicitly report coverage gaps rather than infer continuous
history from an ETag. See [error tracking](error-tracking.md) for the SDK subset,
privacy baseline, credential rotation, counts and unsupported functionality.

The separately enabled error monitoring projection is derived, never an ingest
release gate. Each `(tenant, project)` has one conditional S3 root publishing
processed segment identities, pending source coverage, compact occurrences,
deduplicated issue summaries and immutable change-page references together.
An ambiguous write reads back its exact publication; competing workers cannot
emit changes outside the winning root. Rendezvous ownership reduces work, while
the root CAS fences publication across independent nodes and partitions. No
local index, queue, affinity, SQL database or leader is required for recovery.

Segment identity prevents reprocessing, while `(tenant, project, event_id)`
deduplicates retries across segments. Conflicting occurrences choose the greatest
occurrence timestamp then canonical payload/service/issue digest. History is
explicitly bounded; bootstrap/rebaseline suppresses historical novelty and only
complete reconciliation exposes exact counts. Newness means
`new_in_retained_history`, never first ever. Occurrence timestamps and projection
discovery sequence/time remain separate. A higher raw floor beyond previous
coverage, or known unprocessed input disappearing, forces a conservative reset;
current manifests cannot prove that unseen input did not arrive and expire.
Raw retention never waits for projection progress.

The initial implementation bounds each project to 4096 source segments, 20,000
stored occurrence copies and an 8 MiB root, processing at most 32 new segments per
pass under query admission/deadline/scan budgets and an independently cumulative
32 MiB expanded-column allowance. The root references at most 64
immutable replay pages of up to 64 changes each and advances its replay floor
before deleting old pages. Authenticated tenant/project-bound cursors resume on
any node; missing retained pages or expired replay coverage require explicit
reset. Indexed source ETag/time, history floor, baseline/backlog and unknown lag
are public, not an implicit continuity promise. See
[error monitoring](error-monitoring.md) for operation, capacities and derived
object cleanup limitations. Lifecycle and native error alerts remain out of scope.

### Logs

`POST /v1/logs` accepts OTLP/HTTP JSON/protobuf and gzip. A nullable `Log.otlp`
envelope retains canonical typed body/attribute AnyValues, resource/scope
metadata, schema URLs, flags, dropped counts, event name and original times/IDs.
Primary fields remain query-friendly projections; byte/string and numeric tags
stay distinct in the envelope. Non-finite doubles use OTLP JSON spellings,
signed zero survives numeric-string/unquoted JSON and protobuf, and retained
strings are copied rather than pinning request arenas. General JSON semantics
remain unchanged: an OTLP-only spelling adapter preserves unquoted `-0` and zero-mantissa exponents such as `-0e0`.
Empty/duplicate keys reject semantically. Unsupported resource entity refs and
invalid resource/scope metadata reject whole requests; unkeyed record errors
return partial success, while keyed invalid batches are never rewritten.

Event time falls back to observed time. Both unknown remain nil during decode;
unkeyed HTTP preparation assigns receive time and records it, while keyed
unknown-time requests fail before append. No clock-derived keyed fingerprints
or new shared authority are introduced. Invalid string IDs mean unassociated
primary IDs, with their supplied representation retained. The native protobuf
HTTP transport carries the consumed adapter state through success and failures,
including bounded gzip errors, avoiding duplicate socket drains on large bodies.

New segments add a nullable JSON `otlp` column; legacy ten-column Parquet and
NDJSON decode to nil metadata. Wrong present column types fail rather than
masquerading as legacy absence. Duplicate-row sharing compares complete Arrow
bytes, not BEAM numeric equality that conflates signed zero. S3 uses versioned
codec bindings and startup checks to prevent stale artifacts discarding the
column. Stop old readers/writers and drain pending legacy keyed OTLP retries
before rich writes; the richer content changes old reduced-projection hashes.
Legacy/Loki nil-envelope fingerprints stay unchanged. See [logs](logs.md).

Live-tail admission uses unique process-owned registry slots per tenant on each
instance; concurrent joins cannot exceed that instance's configured limit, and
caller termination releases a slot. No durable or cluster-visible admission state
is added. Follower loss closes affected WebSockets with 1013, rather than silently
replacing a cursor whose continuity cannot be proved. A reconnect queries fresh
history and starts a new best-effort stream. Local supervision unavailability
before upgrade returns HTTP503. These limits do not bound aggregate deployment
polling pressure or make live tail a complete changefeed.

New segment manifest entries optionally carry `ls`, a complete set of at most
128 nonempty promoted service values, each at most 256 bytes. Exact `service`
and `service_name` matchers and the direct service filter prune known irrelevant
segments before download. A null/empty promoted service can fall back to resource
labels in native matchers, so such batches omit the summary rather than risk
false negatives. Legacy, malformed, and over-budget summaries always scan.
The additive field needs no segment migration; older writers may erase it,
which only loses the optimization. Other labels and regular expressions remain
native decoder filters.

- LogQL subset. Label filter + substring/regex match on message.
- Sort row groups by `(service, ts)`.
- Sidecar: label→segment posting list; optional token inverted index for `|=`/`!=` filters.

### Metrics

- Ingest wire protocol: **Prometheus `remote_write` 1.0 and 2.0** (Snappy-compressed protobuf; the Content-Type `proto` parameter selects the message), at `POST /api/v1/write`. Full receiver contract: `Content-Type: application/x-protobuf`, `Content-Encoding: snappy` (strict — unlike Loki push, an absent header is rejected), `X-Prometheus-Remote-Write-Version: 0.*` (0.1.0 in practice), `X-Scope-OrgID` for tenant (defaults to `"default"`), `Idempotency-Key` propagated to storage. `204` on success, `400` for invalid snappy/protobuf, `415` on wrong content-type/encoding, `429` on `:owner_overloaded` backpressure, `5xx` on storage transients. `POST /v1/metrics` also accepts OTLP/HTTP JSON/protobuf and gzip for gauges, cumulative/delta sums, explicit/exponential histograms and summaries, with bounded native wire/node preflight. Unspecified temporality is rejected. Delta points retain their producer-reported intervals without cross-point cumulative conversion or a node-local accumulator. Explicit histograms prefix-sum source buckets into classic float families; summaries project original quantiles. Every projection retains the full source point, including exact uint64 counts and optional fields. Supplied points and expanded samples independently obey the record ceiling; repeated complete metadata obeys the unchanged 16 MiB additive ceiling, so wide distributions can fail even as single points. Request-local instrument/typed-owner aliases and conflicting same-time distribution envelopes fail whole; cross-request stream identity remains the producer's obligation. There is no mutable distribution registry or delta accumulator. Supported points retain complete typed source metadata and exemplars (no dedicated exemplar query API). Require source time; do not introduce local delta authority. Explicit naming/series identity/batching/partial/keyed contracts are in [metrics](metrics.md).
- Exponential classic projection is bounded to 65,536 source counts per side and 160 populated coarsened bins, including at the minimum supported scale -10. Conversion overflow returns whole-request413, never a valid-neighbor subset. Source scales -10..20 are supported; scale is not globally restricted by OTLP. Libm boundaries retain finite exponents below1024 and rounded subnormals above-1075; equal numeric bounds coalesce at their own cumulative mass. Negative infinity is explicit, projected signed zero canonical, original threshold bits retained. Nonstale populations wholly inside their zero region reject; partial negative bins clip to its edge. Classic inclusivity/zero-region geometry and adaptive grids remain approximate, while original arrays/scale/owners persist. These are Elixir scalar-normalization policy fixes under existing native preflight and metadata proof, not bulk columnar work or a new native ABI. Metadata/Parquet formats and API tuples do not change, but affected projected keyed bytes do: upgrade all ingress apps and drain retries, never overwrite or evade409.
- Sort row groups by `(series_id, timestamp_ns)`.
- `series_id` is Pulso's port of Prometheus's `labels.StableHash` — xxhash64 over `name<0xff>value<0xff>…` across labels sorted by name. Byte-exact compatibility with the Go reference is pinned by `native/pulso_codec/src/stable_hash.rs` and its oracle test. Treat `series_id` as an accelerator only — the canonical labels are the identity, and readers must compare them, not the hash.
- Parquet schema: `series_id Int64`, `timestamp_ns Int64`, `value Float64`, `metric_name Utf8` (dictionary-encoded), `labels_canonical Binary` (the bytes `StableHash` consumes), `labels_json Utf8`, plus nullable JSON `otlp Utf8` metadata. New readers accept legacy six-column segments; a malformed present metadata column fails closed. Versioned codec bindings and startup capability checks prevent old artifacts discarding the envelope. The OTLP scalar/distribution receiver uses this envelope for resource/scope/descriptor/point metadata, including exemplars. The additive envelope column has a 16 MiB encoded-byte ceiling before row allocation; ordinary scalar/label-only writes and 100,000-row compactions do not acquire this metadata-only cap. Complete raw MCP metric result text has its own independent 16 MiB ceiling before binary allocation. Metadata participates in hashes/sort ties and duplicate sharing uses Float64 bits and complete metadata bytes. Nil-envelope legacy hashes are unchanged. Upgrade all readers/writers/compactors before metadata-bearing writes; old compactors erase that column. Rolling back requires a compatible reader or validated migration. Timestamp dictionaries with delta-binary-packed fallback, zstd column compression. Row groups stay at 8,192 rows for small batches or when a sorted series run reaches that size, preserving useful time pruning. Segments made of shorter series runs use 65,536-row groups to reduce repeated dictionaries and footers; these groups are still bounded well below the library's million-row default. The row-group choice uses `series_id` only as an optimization: collisions can select smaller groups, never alter sample identity or filtering.
- Sidecar: **not yet built.** Manifests now optionally carry a complete metric-name set (shared `names` dictionary and segment `ni` reference on the wire; legacy inline `n` is still read), capped at 128 names of at most 256 bytes each. Exceeding either bound omits the summary entirely rather than truncating it. The dictionary has a 65,536-byte budget per manifest; additional sets remain unknown. Exact `__name__` matchers prune known irrelevant segments before download; absent or malformed summaries remain eligible. Pruning benefits exact-name queries against small or repeated name sets; varied or large batches frequently lose their summary under these budgets. This is an additive manifest field, so existing segments need no migration. Generic bounded label-value summaries now augment name pruning (below); external label postings remain a follow-up.
- Exact label matchers also prune using optional complete value sets for at most 16 label names chosen from the first sample, excluding `__name__`. Each label has at most 32 values of at most 256 bytes, including `""` for samples missing that label. High-cardinality, long, or invalid values omit that label's summary rather than truncate it; invalid UTF-8 names omit the whole summary because native label JSON can normalize them. Per-segment summaries have a 4 KiB budget. A separate 64 KiB manifest `labels` dictionary deduplicates repeated summaries; segments carry `li` references. Absent, malformed, or budget-exhausted references remain scan-eligible. Compaction computes summaries from the replacement's complete sample set. Older writers may remove summaries but cannot change data. Regular expressions and negative matchers remain decoder filters.
- **Optional native footer postings** (`pulso.metric.postings.v1`) on unkeyed appends and fresh compaction outputs intersect positive equalities over complete canonical-series ordinals, then select row groups before Arrow decode. Caller-supplied/colliding series hashes never define identity. Missing labels contribute `""`. Examines `__name__` first, then byte-sorted names, at most 16 names; each included label is a complete disjoint value partition. Caps: 512 canonical series / 100,000 rows / 128 groups, 256-byte names/values, 4 KiB per canonical set / 128 KiB unique canonical dictionary / 16 MiB byte-probe work. Consecutive pointer-identical immutable label buffers reuse ordinals without touching bytes; a bounded 100,000-entry u16 vector supplies group membership without a second dictionary lookup. These are optional work ceilings, not total native memory guarantees. Complete label partitions fit a shared 32 KiB binary / 64 KiB hex footer ceiling or remain unknown. Hex avoids a new codec dependency and simplifies strict parsing, at the cost of doubled footer bytes on every read. Decode checks magic, xxhash64 accidental-corruption checksum, ordered unique strings, bitmap universe/disjoint partition coverage, and exact footer/group row counts; missing, malformed, duplicate or incompatible indexes scan. Coverage still trusts the first-party writer as Parquet statistics do; the checksum is not authentication or a cross-column semantic proof. Time/postings-pruned groups are not integrity-scrubbed. Regex/negative/unknown-name matchers remain row filters. No additional S3 object, PUT, authority, registration or cleanup; by default whole-object downloads and all source budgets are unchanged. Optional footer-first reads can avoid proven-empty bodies while retaining logical source admission. Keyed writes deliberately retain `encode_metric_segment_parquet_v2` bytes because exact-byte keyed retries must not acquire a different-sized footer and become permanent conflicts. Conditional creation now prevents overwrites, but does not guess semantic equivalence across encoders. A separate indexed native entry point handles fresh keys; local metric artifact tuple `{2,2,1,1,1}` fences the current metric APIs, not pooled old application nodes. Footer-only rolling compatibility does not relax rich metadata's all-participant upgrade rule. See [operator limits](metrics.md#bounded-native-query-acceleration). This is not external indexed planning or representative provider/cold-cache capacity evidence.
- Native histograms and exemplars are optional binary columns (`histogram`, `exemplars`) written only in segments with rows that carry them, so plain segments keep their exact bytes for keyed retries. A histogram row stores the stale marker as its float value (older readers see the series end, never an invented number); a row that only carries exemplars, whose sample came in another request, stores a reserved NaN payload that queries skip. Exemplars attach to their series' sample nearest in time. Histogram semantics follow Prometheus 3.5.0's float histograms (`Pulso.PromQL.NativeHistogram`, `Pulso.PromQL.NativeRange`), checked against Prometheus on the same data. OTLP rejects unspecified temporality; cumulative/delta exponential points retain sparse source arrays and project classic buckets rather than native histogram rows. Embedded exemplars remain typed envelope metadata. The OTLP decoder capability advances to 2 without a storage-format change; upgrade every serving ingress app/native pair before relying on the new point kinds. Local checks do not fence an older pool. A sampled span must not cause an otherwise supported scalar point to disappear.
- Caveat: very high active-series cardinality (100M+) may eventually justify a specialized TSDB block layout beside Parquet. Not v1.

### Traces

- Implemented `POST /v1/traces`: OTLP/HTTP JSON and protobuf, optionally gzip, using the existing tenant auth and append publication boundary. JSON uses hex IDs, integer enums and decimal-string int64 times. Responses mirror request encoding, with partial-success rejected-span counts, full-request 413 budget errors, and retryable 429/503 responses. An opt-in tonic gRPC listener shares the same ingestion path and requires native gRPC API 2; Collector bridges remain separate interoperability evidence. See [traces](traces.md).
- `Span` retains IDs/parent, trace state/flags, start/end times, name/kind, status, query-friendly attributes, resource/scope metadata and schema URLs, events/links, and dropped counts. A version-1 `otlp` payload envelope retains ordered original AnyValue pairs for every owner, with positional event/link lists: bytes/string identity, canonical decimal int64, float64/signed-zero, arrays/kvlists/empty values. Legacy nil metadata is omitted from maps, hashes and sort keys; no types are inferred from legacy strings. Current exact/regex predicates intentionally retain query-projection semantics rather than strict OTLP predicates. Envelope shape/version/value validation is native on encode, record/native read and compaction. A native JSON byte counter proves the full 16 MiB normalized budget before any append, including repeated projections and typed owners. Startup requires trace metadata capability 2. Upgrade all readers/writers/compactors and drain legacy keyed retries before enriched writes; compatible rollback is mandatory, even though physical Parquet columns are unchanged. The start time is `timestamp_ns`; manifest and query time bounds refer to starts, not duration overlap. IDs normalize to nonzero lowercase hex; times fit nonnegative signed int64. Malformed/unsupported AnyValues reject rather than flatten; supported non-finite doubles retain their canonical typed classes. Unknown protocol fields are ignored; resource entity references remain unsupported. The native decoder merges singular messages recursively under cumulative logical-owner budgets, with supplied charges retained through oneof replacement and no decoded BEAM expansion before validation. Shared wire capability 3 is required separately from storage/search capabilities; see [wire behavior](otlp-wire.md).
- Native wire preflight counts supplied spans, events and links (each independently bounded request-wide by `max_records`), resource/scope containers, duplicate attribute pairs, key/value/aggregate bytes and recursive structure before term expansion. JSON uses the same request-wide counters before AnyValue conversion. Native recursive attribute preflight also has a hard 64-level ceiling. No new queue or cluster state is introduced.
- Trace segments have typed `timestamp_ns`, `end_timestamp_ns`, `kind`, and `status_code` Int64 columns; nullable `service` and nonnullable `trace_id`, `span_id`, `name` Utf8 search columns; and a lossless JSON `payload` Utf8 column containing the complete normalized span. This initial schema duplicates promoted values in the payload to preserve nested metadata without a complex Arrow nested schema. Rows sort by `(service, timestamp_ns, trace_id, span_id)`; row groups cap at 8,192, timestamps use delta encoding, low-cardinality columns use dictionaries and chunks use zstd. Native time statistics and typed predicates filter before materializing payload terms; attribute predicates inspect only requested JSON fields. Trace readers retain immutable Erlang binaries through `OwnedEnv` and `Bytes::from_owner` instead of copying blobs for `ChunkReader`. Writers use a binary-backed output sink without a final blob copy. Public S3 MCP queries hold bounded top-k state in a disposable Rustler resource and finish JSON/projection natively; individual span maps never expand into the query process. The record/reference decoder remains available.
- `query_traces` shares raw query authorization/admission/deadline/heap/scan/result budgets. Exact IDs, service/name/kind/status, and typed span/resource attribute predicates are ANDed. Raw results sort newest-start first with deterministic tie breaking and preserve duplicates. No-limit responses over 5,000 fail explicitly; explicit limits can return incomplete traces. `get_trace` forbids time windows/projection/limits and either assembles every stored span in the chosen snapshot or errors. Roots, missing parents and duplicate/conflicting span IDs are reconciled natively without constructing a cyclic tree or silently deduplicating. `metadata_unknown_span_ids` identifies any legacy copies. Projected differences or disagreement between known typed copies prove conflicts; equal projected legacy/rich copies do not. Reconciliation retains at most 16 MiB of projected comparison keys in addition to bounded selected payload/full keys; missing metadata is never guessed. More than 5,000 stored spans or exhausted scan/result budgets never produce partial assembly; exporter completeness is not implied.
- Opt-in native Arrow compaction shares manifest-CAS publication/retirement/cleanup with metrics, without expanding spans into BEAM. Source sets are bounded; normalized payload and decoded column ceilings constrain merges independently from compressed object sizes. Over-budget source sets shrink before any upload, without truncating rows. Separate eligibility groups prevent trace-only workers from becoming expected metrics owners. `PULSO_TRACE_COMPACTION_ENABLED` is independent of metrics compaction. Active-object reduction alone does not bound permanent tombstones or replace retention.
- `PULSO_TRACES_RETENTION_DAYS` (zero by default) uses the same immutable paged catalog, irreversible event-time floor, staged bucket teardown, bounded deletion admission and orphan/catch-up sweeps as logs and metrics. The floor concerns span starts. Mixed-age segments may stay physically present, but their expired rows are hidden from queries and assembly. Whole expired requests return permanent OTLP 400, without rewriting keyed fingerprints; migration and storage errors remain retryable. Admission cannot freeze an earlier wall-clock cutoff through publication: every CAS checks the freshest root floor, including queued flushes and retries. Trace compaction honors paged source eligibility, bucket grouping and grace. The shared retention worker discovers idle trace scopes independently of merge workers. This replaces the unreleased trace-only retention/receipt design; format 3 means only the shared checksummed paged root.
- **Not implemented:** full TraceQL, zero/partial windows, exact name-iterator omission/dynamic dedicated-column parity, deployed Tempo/Grafana client parity, representative indexed planning, bounded historical catalogs without finite retention, and per-segment trace-ID bloom sidecars. Trace-ID point lookup still scans time-pruned manifest candidates. Parquet codecs have no Elixir fallback; invalid encode/decode is a hard error, not a JSON-storage alternative.

## Non-goals

- Serving as a UI. There is no dashboard, and there will not be one in-repo.
- Replacing every backend for every workload. Pulso targets the common case; hyperscale metrics with 1B+ active series may need a specialized system.
- Being a general-purpose log-and-metric database. The design is optimized for observability access patterns (recent-heavy, time-and-label-filtered, append-only).

## Open questions

- Compactor role: dedicated compactor nodes, or ingesters run compaction in the background?
- Multi-region: single region only in v1; multi-region S3 replication and query routing is a separate design.
- Tenant tiering: are hot tenants sharded to dedicated nodes, or homogeneous?
- Retention: an experimental event-time implementation exists across signals (see [Retention implementation](#retention-implementation)). Per-tenant policies, provider validation at production scale and finite-time reclamation of arbitrarily late uploads remain open.

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
encode, preserving optional typed metadata. Metadata-budget encoding failures
halve source sets before any upload, never truncate rows; a two-source overflow
remains an explicit error. Segments recovered by listing without summaries are skipped until
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
In flat manifests, tombstones remain permanently to prevent duplicate ingest. Format-3
manifests drop them with their expired time bucket (see below). Bounding flat-manifest
metadata requires an explicit ingest retry horizon in a later version. An ingest
retry that crashes between re-upload and manifest registration can leave an
unreferenced retired object; reclaiming abandoned uploads remains a follow-up.

The manifest becomes indispensable after compaction. Prefix reconstruction
cannot distinguish published replacements from orphan uploads or retired sources.
If a manifest is missing and compacted objects exist, reads and ingest return
`:compacted_manifest_missing` rather than reconstructing incomplete or duplicate
data. Recovery requires restoring the manifest from object-store version history
or a backup. Legacy prefixes without compaction retain their listing-based
migration path unless they carry the retention `.managed` marker.

Queries that lose a segment or format-3 metadata page to cleanup restart their
entire scan once against a fresh manifest. This applies to logs as well as
metrics; logs no longer skip missing objects silently. A second missing object
returns an error rather than silently omitting records. The retry uses its own snapshot without overwriting the shared
cache, so it cannot hide a later acknowledged append. This protects stale readers
even after extended refresh failures; the grace period alone cannot bound every
snapshot's lifetime.

The next metrics work is label posting indexes, followed by OpenTelemetry metrics ingestion and then alert evaluation.

## Retention implementation

Event-time retention is experimental and opt-in. Operator procedures live in
[retention](retention.md); this section records the design constraints.

### Format 3 manifests

A retention-managed (tenant, signal) keeps `manifest.json` as its only
conditional publication authority. Format 3 replaces the flat segment list with a
bounded root: the irreversible `floor_ns`, the recorded duration in days, a fixed
bucket width, a fresh nonce on every mutating write, an inline tail of recent
entries (at most 256 entries / 64 KiB) and bucket descriptors (hard ceiling
2,048). The encoded root is capped at 512 KiB, independently of the descriptor
count. A format-3 root is stored in a fixed JSON envelope,
`{"root_sha":"<sha256 hex>","payload":<raw JSON>}`. The digest covers the exact
stored payload bytes, so verification depends neither on Erlang term encoding
nor on JSON key order across releases. Readers check the fixed header, strip the
outer brace, verify the raw payload slice, and only then decode it; a format-3
payload without a valid envelope is rejected. Formats 1 and 2 stay plain JSON. A
checked-in fixture pins the envelope. Old releases reject format 3 instead of
erasing its fields.

Each segment belongs to exactly one fixed time bucket, chosen by the maximum
timestamp encoded in its canonical key. Query pruning uses each bucket's actual
aggregate time bounds, because a segment can contain records far older than its
bucket start. Older entries live in immutable pages under
`tenants/<t>/v4/signal=<s>/index/<bucket-start>/<bucket-id>/`: leaves of at most
256 entries / 64 KiB, and a bucket index of at most 1,024 page references /
512 KiB. Pages are uploaded before the root CAS that references them; a page PUT
alone never acknowledges data. References are scoped to the bucket's prefix,
carry size and SHA-256, and are verified on the first read. Each node keeps an
immutable metadata cache of at most 512 pages. Its 16 MiB limit counts the ETS
table's allocated memory (decoded maps, lists, inline binaries and overhead)
plus each page's encoded size and its reference strings, which stand in for
reference-counted binaries that ETS accounting omits; least recently used pages
are evicted first. Reference and data strings are recursively copied before insertion so cached
spill-produced leaf entries never pin the 512 KiB root they were decoded from. Because pages are
content-addressed, a verified cached page stays valid even if the stored object
later changes or disappears. Each bucket also has a cap on total metadata bytes
written, and one root write may touch at most four buckets. Exceeding a budget is
an explicit `:retention_capacity` error.

The width is chosen once, at activation, as `ceil(window / 508)` whole hours,
where the window is the duration plus the effective grace (the larger of the
retention and that signal's configured compaction grace) plus the future skew. Expired
buckets keep their descriptors until their grace ends, so the window, not the
duration alone, must fit 512 descriptors with four of headroom; this keeps the
root within its byte budget even with long tenant names. Defaults give 2 hours
for 30 days, 18 for 365 and 173 for 3,650. The width is persisted. Every pass
and `apply_policy/5` call rechecks the current window against it, so a grace or
skew change that no longer fits stops advancement with `:retention_capacity`
(ingest is not gated on it; registration stays floor-fenced); changing the width in place is unsupported. A managed
compaction call cannot use a grace above the effective configured grace. Normal appends still cost one segment
PUT plus one root write; spilling the inline tail adds leaf and index page writes.

Trace active-leaf references additionally carry complete-service and trace-ID
bloom summaries in their checksummed indexes, allowing exact lookup to skip
irrelevant leaves before charging metadata reads. Segment summaries retain
complete SHA-256 64-bit prefixes for at most 256 distinct IDs (2 KiB); a leaf
rebuilds its independent 32–2,048-byte bloom from those hashes rather than OR-ing
adaptive segment blooms with incompatible moduli. Wider segment ID sets carry a
separately versioned, fixed 2 KiB bloom using the same SHA-256-prefix hash and
probe scheme as leaf blooms. A mixed leaf ORs only canonical v1/2 KiB blooms,
rebuilding complete small hash sets at that exact geometry. The wide segment
omits redundant adaptive/hash fields; older readers treat it as unknown rather
than exclude data. Collisions and saturation only add reads. Missing, malformed
and legacy sets remain unknown and always scan. Leaf service sets have an independent 512-byte encoded ceiling; complete
references cap at 4 KiB and omit their optional summary if necessary. All these
bytes count against existing page/index budgets. Cache reference copying and
byte accounting recurse through these nested summaries, so they cannot pin an
index arena or evade the memory ceiling. This is not a trace-ID posting index;
dense or unknown metadata can still exhaust lookup budgets.

### Activation, policy and fences

Only `enforce` mode with a positive duration converts a manifest, either from a
maintenance pass; an append converts only a prefix with no data yet, and appends
to a non-empty legacy prefix return the retryable `:retention_migration_required`
until the worker converts it. Conversion first creates the permanent `.managed`
marker with a conditional create; the marker holds an immutable random seed from
which bucket generations and migration retirement identities are derived, and
pages are content-addressed and written with conditional creates, so retries and
restarts reuse identical pages. It then publishes the converted root
conditionally against the legacy manifest's ETag, under its own deadline
(`retention_migration_timeout_ms`, ten minutes by default). Ordinary queries,
appends and owner loads read flat manifests without a size limit in every mode,
so enforcement never makes a large legacy prefix unreadable; such a prefix
returns `:retention_migration_required` to appends until it is converted. A root
of unknown format (a cold cache or a fresh owner) is first read with the 512 KiB
format-3 cap; only a `:response_too_large` answer falls back to one unbounded
read, which the decoder still rejects if the body is an oversized format-3 root.
A root already known to be format 3 is never read unbounded.
Automatic conversion, policy inspection and maintenance read the legacy manifest
with a 16 MiB bound and refuse larger ones. `Retention.convert_offline/4` converts a quiesced prefix with an
explicit byte limit (up to 256 MiB), a heap cap and a timeout; a timed-out
conversion is not killed and may still complete. Canonical buckets that are
already wholly closed go into closed cleanup work buckets of at most 262,144
keys, keeping per-key deletion references and the grace period without creating
a descriptor for every historical hour.
Every bootstrap path checks the marker before rebuilding by listing or creating a
manifest; a marked prefix with a missing root returns `:managed_manifest_missing`.

The root's recorded duration is authoritative. A worker whose configuration
disagrees refuses to advance (`:retention_policy_mismatch`); changes go through an
explicit conditional `Pulso.Storage.S3.Retention.apply_policy/5` with the
expected current duration. A floor never moves backwards, and lengthening the
window cannot restore deleted data.

Registration is fenced by the latest root on every attempt: a segment is
eligible only if its minimum timestamp is at or above the floor and its maximum is
no further ahead of the node clock than the configured future skew. Missing
timestamps count as zero. `ManifestOwner` evaluates eligibility per caller, so
an expired registration does not fail unrelated callers in the same flush.
Normal tail spills perform page I/O for at most four buckets per publication.
The owner partitions waiters to at most four buckets, 256 segments and 64 KiB
of new encoded wire entries per publication; individually over-budget callers
are rejected and other callers are deferred. Together with the committed
64 KiB/256-entry tail, this bounds an affected spill to four references. Each
reference is at most 4 KiB. Admission leaves 16 KiB for that final live spill
plus 8 KiB for expiry, and four live-reference slots plus an expiry slot.
Optional non-affected spills retry without summaries and then skip capacity
failures rather than poisoning an otherwise eligible publication. The root
also explicitly checks its tail bounds. Expiry omits pruning summaries because
those buckets leave the query-visible state in the same CAS. The final spill of an
expiring bucket is exempt from its mutation quota and uses a reserved leaf,
reference and index allowance, so an exhausted bucket cannot trap the floor.
Expiring buckets reject all mutations, including compaction. Compaction sources
must share a bucket and be at or above the floor; a paged compaction rewrites
the source leaves and then writes the index once, and its bucket cursor rotates
past buckets with nothing to merge. After an ambiguous conditional
write, the writer rereads the root and treats its own nonce as proof of commit.

Queries clamp their lower bound to the snapshot's floor and read only live
buckets, fetching one index per overlapping bucket plus matching leaves. A
query may read 128 metadata pages plus two per bucket in the root and, by
default, 8 MiB within one deadline shared with its retry. Exact-ID trace queries
add 64 KiB per overlapping live bucket to the metadata-byte default, capped at
32 MiB total; explicit configured limits are never raised. This bounds longer
index fanout without removing page, byte, heap or deadline ceilings. Maintenance
operations keep 128 pages and 8 MiB. Exceeding either is `:metadata_scan_limit`, never a partial
result. A metadata budget exhausted after the caller's own deadline is
reported as `:query_timeout`. A failed refresh of a cached format-3 root is an
error, never a stale success, because cleanup may already have reclaimed what
the stale snapshot references. A cached flat root keeps serving its last
snapshot on a failed refresh, as before retention existed, after a marker probe:
a present marker turns the refresh failure into an error; a failed probe is
treated as "not managed" unless this node enforces retention for that signal.

### Expiry, cleanup and sweeps

A pass advances the floor to `max(previous, now - days)` and, in the same root
write, marks every live bucket ending at or before the floor as expiring, with a
deletion deadline of now plus the grace period. After the deadline, teardown runs
in stages recorded in the root: first it deletes every segment the bucket's
pages reference (checking tenant/signal scope and that each key's maximum
timestamp is below the floor), then it deletes the bucket's metadata prefix with
paginated listing, and finally it removes the descriptor. Teardown never reads
pages that the metadata stage may already have deleted. Failed objects are
retried on later passes without blocking other buckets. Compaction retirements
inside live buckets are deleted after their own deadlines. All per-key fences of
an expired bucket disappear with it: the floor already makes those records
unregistrable, so metadata stays proportional to the retained window instead of
the bucket's lifetime.

Uploads never published by any root (lost responses, crashes, completions after
expiry) are reclaimed by bounded sweeps. A grace-aged watermark advances only to
the earlier of a floor committed at least one grace period ago and the oldest
remaining bucket. Sweeps walk one UTC date partition at a time with the native
paginated `list_page` (lexical start-after), within a configurable horizon below
the watermark, deleting canonical segment keys whose maximum timestamp is below
it, plus the metadata prefixes of buckets below it. The native `get_bounded`
call rejects manifests and pages above their size cap before allocating them.

Uploads outside the normal horizon are handled by an explicit, durable catch-up
job (`Retention.start_catchup/4`) stored in the root. It covers a range from an
operator-chosen start up to the committed watermark and, while active, replaces
the scope's normal sweep. Each pass lists at most 128 keys from one date
partition and 128 from one expired metadata slot. A failed or unadmitted delete
marks the cycle dirty: healthy ranges keep advancing, a dirty cycle is followed
by a complete new cycle, and the job clears only after a failure-free cycle.

Deletion admission is node-wide: at most `retention_delete_limit` attempts per
`retention_interval_ms`, shared by retention and managed-prefix compaction
cleanup, and four concurrent DELETEs enforced by an atomic guard in the native
`delete_bounded` call. A slot is released only when the provider request
actually returns, so a killed BEAM caller cannot free a slot while its I/O is
still running. The worker discovers tenants with a bounded lexical walk (native
`discover_tenants`, 16 tenants per round, skipping each tenant's subtree) plus a
key-only continuation over locally cached scopes, without listing every
tenant or registry entry. It reserves half of its 64-scope queue for durable
discovery, never drops a scope it has already passed, and processes at most
eight scopes per pass. Native
`list_page` resumes with server-side start-after keys rather than opaque
continuation tokens. The grace period reduces, but does not eliminate, readers
losing objects: a missing object restarts the scan once, then errors.

These native calls (`get_bounded`, `list_page`, `discover_tenants`,
`delete_bounded`) are reached by every S3 deployment, not only retention-enabled
ones: the `.managed` marker probe uses `get_bounded`. The object-store NIF
therefore exports a retention API version, and application start fails with an
actionable error when the S3 backend is configured and a stale precompiled
artifact lacks it, rather than raising `:nif_not_loaded` on the first append.
Release tags build matching artifacts automatically; the version is not bumped
by hand.

Every maintenance step is a root transition that can be retried after a crash,
and overlapping workers are fenced by the root CAS. Pausing, an unmoving floor,
insufficient cleanup throughput, persistent provider failures, or uploads that
complete outside the sweep horizon void the physical bounds; this is not a
compliance-erasure feature. Receivers answer expired and future records with
permanent `400` responses (OTLP status code 3) and capacity, metadata-limit,
policy-mismatch, pending-migration and storage failures with retryable `503`
(OTLP code 14; Loki keeps `500` for unclassified storage errors). Append
preflight loads the root (and applies eligibility, capacity and the legacy
migration gate) only when the cached root is format 3 or the node enforces
retention for the signal; `observe` and `paused` nodes add no root read to
ingest for legacy prefixes. Otherwise a node with no cached root checks the
`.managed` marker at most once per second per scope, through the bounded
metadata cache. That probe is advisory: if it fails the append proceeds to its
segment upload, and the owner's own marker check before creating a manifest and
the root CAS (which rereads the root and re-evaluates eligibility against a
format-3 root) still refuse to publish into a managed prefix, at the cost of a
possible unpublished upload, as with any failed append. A foreground first append converts only a prefix whose
freshly loaded legacy root is empty; anything else returns
`:retention_migration_required` and is left to the worker. Mixing format-3 writers with older releases is unsupported.

Self-monitoring adds fixed-cardinality `kind=retention` operations (`advance`,
`cleanup`, `cleanup_retired`, `sweep`), the `metadata_page` object purpose, and
three node-local gauges without tenant labels: the worst capacity ratio among
cached managed scopes, the sum of their pending buckets, and their count.
