# Object storage costs

Pulso does not charge per active series or ingested record. Your object-storage
provider and the machines running Pulso still do. Lossless storage cannot have a
fixed cost for unlimited retained data: compressed bytes grow with data volume,
retention, and entropy. High cardinality can reduce compression and make queries
more expensive, even without a per-series price.

## What to measure

Track these independently, rather than using raw ingest volume as a proxy:

- Retained object bytes, including manifests, superseded objects awaiting cleanup,
  object versions, soft-deleted data, and orphaned uploads.
- Successful PUT and LIST requests, conditional-write conflicts, and retries.
- GET and HEAD requests, conditional-read outcomes, and downloaded bytes.
- Compaction reads, replacement writes, temporary storage overlap, and cleanup.
- Node CPU, memory, query latency, and rejected deliveries. Cheaper storage is not
  a saving if it requires substantially more compute or silently loses data.

[Self-monitoring](self-monitoring.md) reports logical object operations and
successful body bytes, not a billing ledger. Native retries and provider-side
versions need provider usage metering. Do not add the two overlapping Pulso
metrics views together.

## Tigris example

The published [Tigris pricing page](https://www.tigrisdata.com/pricing/) and
[Markdown pricing page](https://www.tigrisdata.com/pricing.md) both list these
Global/Standard marginal rates:

| Driver | Rate |
| --- | --- |
| Retained storage | $0.02 per binary GiB-month |
| Class A: PUT, COPY, POST, LIST | $0.005 per 1,000 requests |
| Class B: GET, HEAD and other reads | $0.0005 per 1,000 requests |
| DELETE, CANCEL | Free |
| Standard retrieval and egress | Free |

Tigris states that 304 Not Modified and 412 Precondition Failed responses are
not charged. Conditional reads still consume network round trips and node time.
Storage billing uses the average daily peak, not cumulative uploaded bytes.
Monthly account allowances are 5 GiB of Standard storage, 10,000 Class A and
100,000 Class B requests. Model marginal costs without those allowances when
comparing scaling behavior.

For Global/Standard, a useful approximation is:

```text
monthly storage bill ≈ 0.02 × average_daily_peak_GiB
                     + 0.000005 × billable_Class_A_requests
                     + 0.0000005 × billable_Class_B_requests
```

Apply the account allowances separately. Add notification charges if configured;
Tigris lists $0.01 per 1,000 events, not per webhook delivery. Confirm current
pricing and your contract before budgeting. The HTML and Markdown pages disagree
about multi-region and dual-region rates; the rates above are not a claim about
those configurations.

At these rates, two PUTs per second cost about $25.92 per 30 days before free
allowances. This can dwarf storage for small batches. A stored GiB held for a
month costs $0.02 regardless of whether it represents logs or metric samples.
Neither figure includes Pulso compute.

[Keyed retries](keyed-ingestion.md) now use conditional creates and streaming GET
verification after a failed create, including lost responses. A normal successful
first create needs no verification GET; retries can add up to two create/verify
rounds, with provider SDK retries additional. All instances contribute pressure.
A size-header mismatch can avoid consuming the streamed body but still incurs a
provider GET and transport traffic. Budget provider operations independently of
node-local consumed-byte counters.

Optional [metric footer-first pruning](metric-footer-pruning.md) can avoid
full-body bytes for proven-empty sources. Matching sources still GET the whole
object and can add one or two suffix GETs; small complete suffixes are reused.
Strong ETags/Range/If-Match support is required. Scan admission charges at least
full logical source sizes, even when downloaded bytes fall, and extra traffic
can hit limits sooner. Discovery remains whole-object. Measure the actual query
mix across all instances, not just native decode savings.

## Cost model

Requests, not bytes, dominate the bill of a busy Pulso deployment on a provider
that charges per request: one Class A request costs as much as storing about
270 KB for a month. Every acknowledged push waits for a Parquet segment PUT and a
conditional PUT of the signal's manifest root, so what matters is how many of
those each push causes.

### Requests

Per instance that receives a signal, with `W` the
[coalescing window](configuration.md#ingest-coalescing) in seconds:

| Request | Window off (`0`) | Window `W` |
| --- | --- | --- |
| Segment PUT | one per push | about `1/W`, more only when a window fills a quarter of its buffer (8 MiB of input terms, 100,000 rows or 512 requests); never more than one per push |
| Manifest root PUT | one per publication; publications run back to back, so at most about one per root write latency (0.3 to 0.6 s on Tigris) | at most `1/W`, shared by every push, keyed or not |
| Manifest page PUT | about one per 450 segments | the same, so about `1/(450 W)` |

Requests with an `Idempotency-Key` always upload their own segment but share
the root writes. Add per tenant:

- maintenance: each compaction merge is one PUT plus one root write for up to 32
  segments, each cleanup batch one root write, and tenant discovery a LIST each
  pass (about 0.1 Class A/s in total at one-minute passes);
- [metric hour finalization](configuration.md#long-dashboard-and-alert-ranges):
  its metric-grouped segments, rollups and one root write per settled hour, and
  [manifest backups](configuration.md#manifest-backups): one PUT per compacted
  prefix per backup interval;
- Class B: query segment and page GETs, compaction source GETs, and conditional
  root reads that return a changed root. `304 Not Modified` and `412` conflicts
  are free on Tigris.

Every instance that receives a signal runs its own window and publications,
so ingest request cost scales with the number of ingest instances. Conflicting
root writes between instances return free `412`s but are retried with one more
PUT. Run as few ingest instances as availability allows, or route each signal
to its own instances.

Monthly cost then follows directly; on Tigris Standard, with 2,592,000 seconds
in 30 days:

```text
monthly ≈ 12.96 × ClassA_per_second + 1.30 × ClassB_per_second
        + 0.02 × average stored GiB (live data + soft-deleted data)
```

Watch the actual rates with `pulso_object_requests_total{class,purpose}` and
storage with `pulso_manifest_referenced_bytes{signal,age}` (see
[self-monitoring](self-monitoring.md)). For example:

```promql
# Projected monthly request cost in USD at Tigris Standard rates
12.96 * sum(rate(pulso_object_requests_total{class="A"}[1h]))
  + 1.30 * sum(rate(pulso_object_requests_total{class="B"}[1h]))
```

The counters see one logical request per call: native client retries and
additional LIST pages are not counted, so compare against the provider's usage
report.

### Storage

Stored bytes are the compressed bytes ingested per second times the retention
period, plus what soft delete keeps:

```text
stored GiB ≈ Σ signal (segment_bytes_per_second × retention_seconds) / 2^30
           + (deleted_bytes_per_second + overwritten_bytes_per_second) × soft_delete_seconds / 2^30
```

Without [retention](retention.md) the first term grows every month for as long
as the bucket exists. Compaction rewrites data once and deletes the sources
after its grace period, and each root write replaces the previous root. With a
soft-delete window (7 days, for example), those deleted sources and superseded
roots stay billable for the window. Coalescing shrinks the second term: fewer
root writes overwrite fewer root bytes.

### Storage tiers

Tigris lifecycle rules can move objects to Infrequent Access ($0.01/GiB-month,
$0.01/GiB retrieval, 30-day minimum) or Archive Instant Retrieval ($0.004,
$0.03/GiB retrieval, 90-day minimum) by object age and key prefix. Pulso reads
any tier transparently, except Archive, which needs a restore first and is not
supported. Only segments are good candidates: limit rules to the prefix
`tenants/<tenant>/v4/signal=<signal>/date=`, which excludes manifest roots,
pages and backups that Pulso rewrites or reads constantly.

Transition only data you keep well past the minimum. An object deleted by
retention earlier than 30 days after its transition is still charged the full
30 days: moving segments at 30 days saves money only with more than 45 days of
retention, and every query over old data pays the retrieval fee on whole
segments. At $0.01/GiB-month saved, tiering is worth it only for large,
rarely-read history; requests usually matter more.

### Retention

At 1.3 TB of new segments per month (the production-shaped load test below:
metrics 1.04 TB, logs 0.22 TB, traces 0.08 TB), retain-forever storage adds
about $25/month to the bill every month: $300/month after a year. Choose
retention per signal from how far back people query, for example 30 days of
logs, 14 days of traces and 90 days of metrics (hourly rollups make long metric
ranges cheap to read), which caps storage near 3.4 TB ≈ $63/month at that
volume; metrics dominate, so their retention is the lever.

Enforced retention keeps the paged (format 4) manifest, so
[manifest backups](configuration.md#manifest-backups) and hour finalization
keep working on object stores without versioning, such as Tigris. Expired
segments are deleted after the grace period and a backup that records their
retirement; with 7-day soft delete they stay billable for 7 more days, about
a week of new data (≈ 0.3 TB, $6/month at that volume).

Trace compaction requires bucket versioning; leave it off on Tigris. Trace
segments are then one object per window, which coalescing already keeps small
in number.

### Production settings for Tigris

```yaml
ingestBatching:
  flushIntervalMs: 2000        # ~2 Class A requests per window per signal
  maxConcurrentRequests: 256   # the derived 28 slots return 429s at 70 pushes/s
metricsCompaction:
  enabled: true
  finalize: true
logCompaction:
  enabled: true
traceCompaction:
  enabled: false               # needs bucket versioning
manifestBackups:
  enabled: true
retention:
  mode: enforce                # after every node runs 1.32 or later
  manifestFormat: paged
  logsDays: 30
  tracesDays: 14
  metricsDays: 90
```

Also give dashboards enough query slots (`PULSO_QUERY_MAX_CONCURRENT`): the
default of 3 per node answered `429` to concurrent panels in the load test.

### Measured at production volume

A production-shaped load test (`scripts/production_load.exs`) sent 400,000
active series scraped every 30 s in 20 remote writes/s, 3,400 log lines/s in 40
Loki pushes/s, 1,000 spans/s in 10 OTLP exports/s, and dashboard queries every
30 s, to one Pulso node with 2 vCPU and a 2 GiB budget, against an object store
with Tigris latency (GET 140–190 ms, PUT 220–350 ms):

| | 1.29.9, window off (10 min) | Window 2 s (10 min) | Window 2 s, metrics compaction and finalization (75 min) |
| --- | --- | --- | --- |
| Pushes acknowledged (70/s offered) | 50/s; 102,000 `429` responses, 12,000 pushes dropped after retries | 70/s; no `429`, none dropped | 70/s; no `429`, none dropped |
| Acknowledgment p50 / p99 | 3.7–5.9 s / 5.4–14.4 s | 1.9–2.9 s / 3.2–4.5 s | 1.9–2.8 s / 3.1–4.8 s |
| Class A requests/s | 58 (1.17 per push) | 3.2 (0.05 per push) | 3.6 (0.05 per push) |
| Class A per month | $756 | $42 | $47 |
| Class B per month | $5 | $22 | $7 |
| Segment objects written per hour | 182,000 | 5,700 | 6,300 |
| Root bytes overwritten | 536 KB/s (7-day soft delete: $6/month) | 95 KB/s ($1/month) | 127 KB/s plus 89 KB/s of deleted compaction sources ($2/month) |
| Memory (RSS p50 / max) | 388 MiB max | 501 MiB max | 468 MiB / 1,011 MiB |
| Dashboard queries | all failed: thousands of segments per hour exceed the query segment budget | most answered; one-hour log volume, PromQL rate and Tempo search panels often hit query limits (`422`) | same |

With the window off, every push encoded and published its own segment, which
saturated both cores. The window turns per-push work into per-window work, so
the same node kept up with production volume. Live metric bytes grew faster
in the compaction run (725 KB/s against 401 KB/s), because compaction sources
stay referenced for the one-hour grace period and finalized hours add rollups.
A 75-minute run does not separate those two effects, so treat 2 TB/month as an
upper bound on storage growth and watch `pulso_manifest_referenced_bytes` in
production.

## Avoiding amplification

By default Pulso writes one immutable Parquet segment per nonempty append and
acknowledges only after publishing it in the manifest. Concurrent manifest
registrations can share a conditional write. [Ingest coalescing](configuration.md#ingest-coalescing)
lets unkeyed requests share the segment PUT within bounded node-local buffers
and spaces root writes one window apart. It is disabled by default; keyed
requests and buffer overflow upload their own segment. Batch at the collector
within Pulso's [ingest limits](ingest-limits.md), and measure delivery latency
and memory as well as requests. A larger batch or coalescing window is not
permission to acknowledge before durable publication.

Use bounded time ranges and exact metric-name, metric-label, or log service selectors.
New metric segments retain complete value sets for a few low-cardinality labels;
exact mismatches skip downloads. High-cardinality, unknown, and over-budget sets
still scan, including older segments. Compaction rebuilds these summaries without
aggregation or data loss. New log
segments with a complete small set of nonempty promoted services can be skipped
before download for exact `service`/`service_name` selectors. Old segments, large
service sets, and records needing resource-label fallback are still scanned.
These optimizations do not drop data or change query results. Other log labels,
regular expressions, and line filters still require segment reads.

[Metrics compaction](configuration.md#metrics-compaction) reduces small-object
query fan-out but adds reads, writes, and a grace-period storage overlap. It does
not reduce initial ingest PUTs. Test total lifecycle costs against the query mix
before enabling it. Manifests and permanent ingest-retry tombstones still grow;
compaction is not a bound on lifetime metadata.

Experimental [event-time retention](retention.md) is implemented for logs,
metrics and traces. Managed scopes add immutable metadata pages, cleanup requests
and a grace-period storage overlap; measure the full lifecycle, not just ingest
PUTs. Flat retain-forever catalogs and their tombstones still grow without bound.
A local segment cache remains unimplemented. Do not apply bucket lifecycle
deletion to referenced segments or pages: it leaves dangling manifest references.
Recovery requires coherent manifest, metadata-page and segment backups.
Infrequent Access adds retrieval charges and a 30-day minimum; Archive Instant
Retrieval adds retrieval charges and a 90-day minimum. Archive requires a restore
before reading. Tiering mutable manifests or short-lived compaction sources can
cost more than Standard, and automatic archive restoration is not supported.

## Correctness before cost

Do not remove labels and assume collisions will be aggregated elsewhere. Pulso
preserves supplied samples, including duplicates; conflicting metric values at
the same timestamp are resolved deterministically at query time with a warning,
not summed during ingest. It does not implement adaptive aggregation, log
sampling, or trace ingestion. Source-side sampling is an explicit data-loss
policy, not a storage optimization.

Rejected requests still consume network and compute, and some providers charge
for them. Monitor rejection rates separately from acknowledged deliveries and
check the provider's actual response-code billing rules.
