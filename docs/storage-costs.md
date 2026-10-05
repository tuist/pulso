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

## Avoiding amplification

By default Pulso writes one immutable Parquet segment per nonempty append and
acknowledges only after publishing it in the manifest. Concurrent manifest
registrations can share a conditional write. Optional [ingest coalescing](configuration.md#ingest-coalescing)
also lets concurrent unkeyed requests share the segment PUT, within bounded
node-local buffers. It is disabled by default; keyed requests and buffer overflow
keep the original direct path. Sequential producers cannot benefit from it.
Batch at the collector within Pulso's [ingest limits](ingest-limits.md), and
measure delivery latency and memory as well as requests. A larger batch or
coalescing window is not permission to acknowledge before durable publication.

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

Pulso does not yet implement retention or a local segment cache. Do not apply
bucket lifecycle deletion to active segments: it leaves manifest references
pointing at missing data. Compaction recovery requires preserving manifests.
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
