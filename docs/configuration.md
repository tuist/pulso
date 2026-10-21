# Configuration

A Pulso release reads its configuration from environment variables when it
boots. Missing or invalid required values stop the node at startup with a
message naming the variable, rather than failing later on traffic. The
[Helm chart](deployment.md) sets these variables from its values; this page
describes what each one means.

## Required

| Variable | Meaning |
| --- | --- |
| `SECRET_KEY_BASE` | Random secret of at least 64 characters, used by the web framework. Generate one with `openssl rand -base64 48`. The chart generates and keeps one for you. |
| `PULSO_TENANT_TOKENS` | JSON object mapping each tenant name to the SHA-256 digest of its bearer token. See [Tenants and tokens](#tenants-and-tokens). |
| `PULSO_S3_BUCKET` | Bucket that stores every acknowledged record. |
| `PULSO_S3_REGION` | Region passed to the S3 client. Providers that ignore regions still need a value, such as `auto` for Cloudflare R2. |
| `PULSO_S3_ACCESS_KEY_ID` | Access key for the bucket. |
| `PULSO_S3_SECRET_ACCESS_KEY` | Secret key for the bucket. |

Pulso only uses the static credentials above. It does not read instance
profiles, workload identity, or other ambient cloud credentials.

## Optional

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `4000` | HTTP listener port. |
| `PULSO_OTLP_GRPC_PORT` | unset | Enables the [OTLP/gRPC receiver](otlp-grpc.md) on this port (conventionally `4317`). Unset keeps it disabled. |
| `PULSO_LOKI_TAIL_MAX_PER_TENANT` | `10` | Concurrent [Loki live tails](querying.md#live-tail) per tenant on each instance. Further tails return HTTP 429. |
| `PULSO_LOKI_TAIL_POLL_INTERVAL_MS` | `1000` | How often an instance with open [live tails](querying.md#live-tail) reads a tenant's log manifest for pushes other instances received (100 to 60000). |
| `PULSO_OTLP_GRPC_IP` | `0.0.0.0` | Address the OTLP/gRPC receiver binds. |
| `PULSO_OTLP_GRPC_MAX_MESSAGE_BYTES` | `16777216` | Largest decompressed OTLP/gRPC message; larger messages fail permanently. |
| `PULSO_OTLP_GRPC_MAX_IN_FLIGHT` | `64` | Admitted OTLP/gRPC calls receiving/ingesting per instance; further calls wait within their deadline (HTTP/2 backpressure). |
| `PULSO_OTLP_GRPC_TIMEOUT_MS` | `30000` | End-to-end OTLP/gRPC deadline including admission wait, body completion and storage; expires as retryable `UNAVAILABLE`. A BEAM-handler watchdog fires this interval plus 1 second after handoff; it does not cancel already-running native/provider work. |
| `PULSO_S3_ENDPOINT` | Amazon S3 | URL of an S3-compatible endpoint, for example `https://<account>.r2.cloudflarestorage.com`. |
| `PULSO_S3_ALLOW_HTTP` | `false` | Allow a plain-HTTP storage endpoint. Use only for storage on a private network. |
| `PULSO_METRICS_FOOTER_PRUNING_ENABLED` | `false` | Optional bounded suffix-GET exclusion for ordinary metric queries. Requires strong ETags, suffix Range and GET If-Match; may add GETs/earlier scan rejection. See [provider prerequisites and bounds](metric-footer-pruning.md). |
| `PULSO_METRICS_COMPACTION_ENABLED` | `false` | Merge small metrics segments in the background. Read [Metrics compaction](#metrics-compaction) first. |
| `PULSO_TEMPO_SEARCH_ENABLED` | `false` | Opt in to experimental Tempo `/api/search`, resource/span/instrumentation/event/link tag discovery (names/values optionally filtered on parent-span strings; filtered names are complete conservative hints, not matching-key proofs) and corresponding MCP tools. Complete-context search may use publication-proven exact resource.service.name candidate-source pruning plus conservative trace-ID context selection; unknown/legacy sources still read, without time clipping. Retained catalog metadata/page/heap bounds can bind before the hard 8192-reference ceiling; actual fetched inputs cumulatively cap at 1024 / 32 MiB / 1,000,000 raw rows, plus 65,536 matching-or-legacy-unknown IDs and independent output bounds. Index-probe exhaustion reads conservatively and can fail late; unread irrelevant objects are not scrubbed. Requires metadata2/export3/search4/discovery12, tuple `{2,1,3,4,12}`, on every serving artifact. Representative postings/capacity/client acceptance remains pending. Discovery does not use this service/ID plan. Discovery also caps requested-owner-plus-attribute work at 2,000,000 per scan attempt before filtering, lowering flat-scope capacity as attribute counts grow. Search always returns the most recent matching traces, so `with (most_recent=true)` is optional (Grafana omits it); bounded same-span negated-regex/status/int64 OR/span-duration expressions use full-scope admission without the service planner and an additional 20M prefilter work ceiling per scan attempt; see [expression semantics](trace-query-expressions.md). Static intrinsic names/seven v2 enum-or-empty vocabularies require the same gate/admission/auth but read no storage and prove no stored presence. Default names combine all five namespaces plus static v2 suggestions under one 2M-work/5000-scope-name-pair budget (20 intrinsic reservations apply even for v1 flattening), with service.name hints even unfiltered and without original attrs; storage errors fail whole rather than emitting partial suggestions. None values combine resource/span/instrumentation originals, not all name scopes. Bounded resource/span string filters select one parent before three-owner union/work, a complete extension rather than Tempo's ignored-query behavior. Same-key equality, nonstring/nested/intrinsic filters reject; values retain original types, matching complex targets fail, and legacy/corruption fail even on nonmatches. Filtered-none needs query-aware unscoped execution or bounded reference, plus separate explicit windowed-filtered-none support; old unfiltered/window callbacks cannot silently ignore the new mode. Floats (including non-finite classes) and seven stored intrinsic strings and event:timeSinceStart exact signed int nanosecond offsets read bounded data, accepting same-parent string filters; event-time filters are a disclosed complete extension versus upstream's empty path. Effective event time0 may yield negative offsets; no events invent no values. Invalid requested event times fail even on nonmatches and parent+all event attrs charge before filtering. New explicit event-time and separate windowed-event-time execution is required, never old string/window callbacks. These are distinct from storage-free static vocabulary. Paired positive time bounds select retention-visible immutable segment envelopes, not span starts; all raw header/provider costs still apply, no indexed I/O gain or cross-window dictionary cache. Compaction can broaden suggestions, excluded payloads are not integrity-scrubbed. Matching capability-12 artifacts are required on every serving instance; flat/old adapters reject unsupported windows. Root name/service dictionaries select parent trace IDs then recover complete retained context, including roots outside predicate/window. Missing/ambiguous roots or missing original string root service fail409 whole, never default/skip. In-flight children-before-root and retention-straddling/multi-root traces can make broad/recent autocomplete unusable; this is a strict default-off extension, not transparent replacement. Root S3 input is the whole retained tenant, capped1024 objects/32MiB compressed/1M raw rows regardless of window/matches; no service/ID planner, local replay decodes all sources without another GET and cross-checks header/body rows. 65,536 candidates/16MiB bookkeeping,2M resource/span/link owner+attr work plus normal dictionary/result limits; dedicated root/window support required. Canonical trace:duration also measures complete retained max-end/min-start, without requiring roots. It may grow during ingest or shrink with retention, not exporter duration. Unfiltered emits int nanoseconds; any supported parent filter emits Go-style duration strings (including µs); both sort lexically. Do not cache across filter/type changes. Root capture/input/replay limits remain; duration work is2 resource/span owners+attrs,65,536 groups/80B per-summary charge within16MiB charged bookkeeping (not total native memory),5000 values/independent1MiB dictionaries/results. Broad nanosecond dictionaries commonly fail422. Dedicated duration/window support fences old root/string callbacks. Static duration/span:duration/traceDuration empty vocabularies remain unchanged. Zero/partial windows, stored span-duration intrinsics, exact upstream distinct-omission/dynamic dedicated-column parity, full TraceQL and deployed-client parity remain unsupported. See [Tempo search](traces.md#experimental-tempo-search). |
| `PULSO_LOG_COMPACTION_ENABLED` | `false` | Merge small log segments natively in the background. Read [Log compaction](#log-compaction) first. |
| `PULSO_TRACE_COMPACTION_ENABLED` | `false` | Experimental native trace merges and retirement cleanup. Requires all readers/writers/compactors upgraded and bucket versioning; see [trace compaction](traces.md#optional-native-compaction). |
| `PULSO_ALERTING_PRINCIPALS_JSON` | `[]` | Dedicated alerting principal array with `tenant`, `id`, `type`, `token_hash` and `capabilities`; at most 256 entries. See [alerting credentials](alerting.md#configure-credentials-and-evaluation). |
| `PULSO_ALERTING_EVALUATION_ENABLED` | `false` | Enable experimental native threshold/recording evaluation and durable recording outbox draining. Grafana execution remains unsupported; native Slack delivery has a separate opt-in. |
| `PULSO_ALERTING_NOTIFICATIONS_ENABLED` | `false` | Enable native Slack delivery from committed bounded outboxes. See [delivery semantics](alerting.md#native-slack-delivery). |
| `PULSO_ALERTING_NOTIFICATION_TARGETS_JSON` | `[]` | Operator-provisioned target descriptors (`tenant`, `id`, `type: slack_webhook`, `secret_env`), at most 256. Webhook URLs must live in the referenced environment secrets, not in descriptors. |
| `PULSO_ALERTING_SOURCE_BINDINGS_JSON` | `[]` | Explicit read-only imported graph mappings (`tenant`, original datasource `uid`, `type: native_promql` or `native_logql`), at most 256 unique pairs; no URLs. Same-tenant native Prometheus instant or metric-producing LogQL instant/range previews only. Does not enable imports or prove residency; see [graph previews](alerting.md#preview-an-imported-expression-graph). |
| `PULSO_ALERTING_POLL_INTERVAL_MS` | `5000` | Native evaluator discovery interval, integer `1000..60000`; measure object-store read cost before reducing it. |
| `PULSO_MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `POST /mcp`, such as `https://agent.example.com`. Requests that carry no `Origin` header, which is the case for most agents and servers, are always accepted; any other origin receives `403`. |
| `PULSO_INGEST_FLUSH_INTERVAL_MS` | `0` | Optional coalescing window for concurrent appends without an idempotency key; integer `0..1000`, where `0` disables it. See [Ingest coalescing](#ingest-coalescing). |
| `PULSO_INGEST_MAX_*` | see [ingest limits](ingest-limits.md) | Per-request source-record, attribute and request-node budgets for ingestion. OTLP distributions also cap expanded float samples at `PULSO_INGEST_MAX_RECORDS`; the separate fixed 16 MiB repeated-source metadata ceiling is unchanged. |
| `PULSO_INGEST_MAX_OTLP_NODES` | `262144` | Request-wide log/metric JSON-shaped node ceiling, independent of per-set nodes. Applies to JSON/protobuf logs and metrics; see [OTLP logs](logs.md) and [OTLP metrics](metrics.md). |
| `PULSO_RETENTION_ENABLED` | `false` | Run the experimental retention and cleanup worker even with all durations at `0`; it also runs whenever a duration is positive. See [retention](retention.md). |
| `PULSO_LOGS_RETENTION_DAYS` | `0` | Log event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, logs are kept indefinitely. |
| `PULSO_METRICS_RETENTION_DAYS` | `0` | Metric event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, metrics are kept indefinitely. |
| `PULSO_TRACES_RETENTION_DAYS` | `0` | Trace retention by span start in whole days, `0..3650`. Uses the same irreversible floor, paged catalogs, grace and cleanup as other signals. |
| `PULSO_RETENTION_MODE` | `observe` | `observe` previews, `enforce` converts manifests and expires data (irreversible), `paused` stops all retention work. |
| `PULSO_RETENTION_DELETE_GRACE_MS` | `3600000` | Grace between committed expiry and deletion, `60000..2592000000`. |
| `PULSO_RETENTION_INTERVAL_MS` | `30000` | Retention maintenance cadence, `1000..3600000`. |
| `PULSO_RETENTION_DELETE_LIMIT` | `512` | Maximum DELETE attempts per node per `PULSO_RETENTION_INTERVAL_MS`, `1..512`, shared by retention and compaction cleanup of retention-managed prefixes. At most four run at once. |
| `PULSO_RETENTION_TIMEOUT_MS` | `30000` | Deadline for one retention operation, `1000..600000`. |
| `PULSO_RETENTION_MIGRATION_TIMEOUT_MS` | `600000` | Deadline for converting one existing manifest during enforcement, `30000..3600000`. Normal maintenance keeps `PULSO_RETENTION_TIMEOUT_MS`. |
| `PULSO_RETENTION_FUTURE_SKEW_MS` | `600000` | Maximum record time ahead of the node clock on retention-managed prefixes, `0..86400000`. |
| `PULSO_RETENTION_SWEEP_HORIZON_DAYS` | twice the recorded days plus two | Days of expired partitions revisited by orphan sweeps, `1..7302`. |
| `PHX_HOST` | `example.com` | Host name used when Pulso generates absolute URLs. Pulso does not currently emit any, so this can stay unset. |

Boolean variables accept `true`, `1`, or `yes`; anything else is false.

## Tenants and tokens

Every ingest and query request belongs to a tenant. HTTP requests name it in the
`X-Scope-OrgID` header, the convention Loki, Mimir, and Cortex clients already
support; requests without the header use the tenant `default`. Model Context
Protocol tools take the tenant as an argument instead. Tenant names use letters,
digits, `_`, `.`, and `-`, up to 128 characters.

Requests authenticate with `Authorization: Bearer <token>`, or with HTTP Basic
credentials whose username is the tenant and whose password is the token, the
convention Grafana Cloud clients, Alloy destinations and Grafana datasources
use. When a Basic request has no `X-Scope-OrgID` header, its username names the
tenant; Basic credentials never authorize a different tenant. Pulso stores only the
SHA-256 digest of each token, so `PULSO_TENANT_TOKENS` never contains a usable
credential:

```sh
TOKEN=$(openssl rand -hex 32)
DIGEST=$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')
echo "{\"production\": \"sha256\$$DIGEST\"}"
```

Give the plaintext token to the collectors and agents for that tenant and put
the digest in `PULSO_TENANT_TOKENS`. A tenant that is not listed is rejected,
and so is a malformed digest, so a blank value cannot switch authentication off.

These ingest/query credentials give each tenant exactly one token. Alerting uses
separately configured per-principal credentials; a tenant-shared token does not
automatically grant alert administration. See [alerting](alerting.md).

Each tenant has exactly one ingest/query token. To rotate it, update the collectors and the
digest together, or briefly route the collectors through a second tenant name
while they roll over. Changing the variable requires a restart.

Data is isolated per tenant: a token for one tenant cannot read or write
another. Tenants are not a quota mechanism; [ingest limits](ingest-limits.md)
and query budgets apply to the whole node.

## Object storage

Pulso needs an S3-compatible bucket that supports conditional writes
(`If-None-Match: *` on create and `If-Match: <etag>` on replace). Amazon S3,
Cloudflare R2, MinIO, and RustFS support them; check your provider's
documentation before using another. Pulso refuses to report ready until it can list the bucket.

The credentials need these permissions on the bucket and its objects:

- `s3:ListBucket`
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`, used by metrics compaction to remove merged segments and by
  retention to remove expired data

An Amazon S3 policy for a bucket named `pulso-telemetry`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::pulso-telemetry"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::pulso-telemetry/*"
    }
  ]
}
```

The provider must honor conditional creation (`If-None-Match: *`) for keyed
segments as well as manifest conditional writes. New S3 nodes require matching
native object-store API `{1,1}` (retention, keyed publication). Upgrade/drain every
writer, including recording evaluators; local startup cannot fence older writers
in another pool. See [keyed ingestion](keyed-ingestion.md) before changing writer
versions or retrying a permanent HTTP409 byte conflict.

Use a dedicated bucket. Pulso writes under `tenants/` and expects nothing else to
modify that prefix. Its readiness check lists the `.pulso/` prefix. Do not configure
bucket lifecycle rules that expire objects under `tenants/`: Pulso tracks live
objects in per-tenant manifests, so an object that disappears underneath it makes
queries fail. Use Pulso's own [retention](retention.md) to expire data instead.

## Ingest coalescing

Set `PULSO_INGEST_FLUSH_INTERVAL_MS=50` (chart `ingestBatching.flushIntervalMs: 50`)
to let concurrent unkeyed requests for the same tenant and signal share a Parquet
segment and manifest publication. It is disabled by default. Keyed requests
always use the original direct path, preserving their retry fingerprints.
Sequential producers cannot share a flush, so they gain no request savings.

Each node-local buffer reserves at most 128 callers, 100,000 rows, and 10 MiB of
estimated input term bytes, including work already executing. Oversized requests
and overflow go through the original unbuffered path, not an unbounded queue.
These bounds are not node-wide ingest rate limits or exact heap-memory ceilings;
encoding and the request processes need additional memory. Idle buffers terminate
after 30 seconds. Buffers for different tenants, signals, or storage configurations
are isolated.

The window adds up to its configured delay before storage I/O. Every request
still waits for the shared segment PUT and manifest conditional write before
acknowledgment. No local WAL is introduced; a node crash before acknowledgment
requires collector retry. Unkeyed delivery remains at-least-once: a lost response
and retry can duplicate data, just as on the unbuffered path. A shared storage
failure fails all requests in that flush; it never falls back to individual writes
after an ambiguous publication. Invalid encodings are isolated before upload.

Enable this on a test deployment first. Compare provider PUT counts, received and
acknowledged records, failures, node memory, and delivery latency. Larger source
batches and low concurrency may already make coalescing unnecessary. Disable it
by setting the window back to `0` and restarting nodes; stored data needs no
migration.

## Metrics compaction

With `PULSO_METRICS_COMPACTION_ENABLED=true`, Pulso merges small metrics segments
from the same hour into larger ones and deletes the originals after a one-hour
grace period. This reduces the number of objects queries have to read.

Before enabling it:

- **Run the same Pulso version everywhere that writes to the bucket.** Older
  versions do not understand the metadata compaction records, and mixing them
  after compaction has started is unsupported.
- **Turn on bucket versioning or back up the manifests.** After compaction, the
  per-tenant manifest is the only record of which objects are live. If it is lost,
  Pulso returns an error rather than guessing, and recovery means restoring a
  previous manifest version.
- **Keep node clocks synchronized.** The deletion grace period assumes clocks
  agree to well within an hour.

## Log compaction

With `PULSO_LOG_COMPACTION_ENABLED=true` (chart: `logCompaction.enabled`),
Pulso merges small log segments from the same hour the same way, with the same
grace period, ownership among instances and prerequisites as metrics
compaction above. Without it, every log push stays its own object, so a query
over hours of frequently pushed logs reads thousands of objects.

Merges run natively without converting records: each merge reads at most 32
segments, 8 MiB of input and 100,000 rows, keeps every record and its typed
metadata exactly, and writes the same layout ingestion writes, ordered by service
and time. A merge that would exceed its row or 32 MiB string ceiling is skipped
rather than truncated. Older segments without typed OTLP metadata merge
unchanged. Stream discovery, queries, keyed retries and retention read compacted
segments like any other.

Each maintenance pass (every minute) keeps merging a tenant's segments while
merges make progress, up to 32 merges per signal, so compaction keeps up with
shippers that push every second instead of falling behind one merge a minute.

## Retention

By default Pulso keeps data indefinitely. Experimental event-time retention
expires logs and metrics after a configured number of days and deletes their
objects itself. Enforcing it converts manifests to a paged format that older
Pulso releases cannot read, and expired data cannot be restored. Read
[retention](retention.md) for the rollout procedure, modes, policy changes,
capacity limits and recovery before setting any `PULSO_RETENTION_*` variable.

## Query budgets

Each query is bounded by budgets shared by PromQL and LogQL. Set them with
these variables (through `extraEnv` in the Helm chart):

| Variable | Default | Meaning |
| --- | --- | --- |
| `PULSO_QUERY_MAX_SAMPLES` | `100000` | Samples a PromQL query may select. |
| `PULSO_QUERY_MAX_LOG_SAMPLES` | `5000000` | Per-line samples a LogQL metric query may retain (one per line its pipeline keeps). |
| `PULSO_QUERY_MAX_WORK` | `5000000` | Evaluation work units per query. |
| `PULSO_QUERY_MAX_SCAN_ROWS` | `1000000` | Stored rows a query may read. |
| `PULSO_QUERY_MAX_SCAN_BYTES` | `134217728` | Stored bytes a query may read. |
| `PULSO_QUERY_TIMEOUT_MS` | `10000` | Wall-clock limit for one query, including object-store reads (1000 to 300000). Queries past it fail with a timeout. Keep it below the client's own timeout (Grafana's datasource default is 30 s). |
| `PULSO_QUERY_PREFETCH` | `4` | Segments fetched ahead of the one being processed, hiding object-store latency. A query that scans many segments is latency-bound: with 25 ms per request, scanning 1,000 segments took about 25 s at 1, 5.8 s at 4 and 2.8 s at 8, with no more requests or memory. Larger values can read up to that many minus one segments more when a query stops early or exhausts a budget, and hold that many segments in memory per query. |

Log metric queries stream: they hold one decoded chunk of a segment plus
about 40 bytes per line their pipeline keeps (17 once packed), so their memory
grows with the kept lines, not the log bytes. `PULSO_QUERY_MAX_LOG_SAMPLES`
bounds those lines and is enforced while the query reads, and every selected
line also counts as work and every stored row against the scan budgets. Raise
the log sample, work and scan-row budgets together for high log volume and
size memory for four concurrent queries. PromQL
queries still materialize their samples and remain bounded by each query's
memory ceiling as well. A query over a budget fails with HTTP 422 rather than
returning a partial result.

## Fixed limits

Each node admits four concurrent queries, at most two per tenant, and bounds
each query's duration and memory. Requests are capped at 4 MiB on the wire and
16 MiB after decompression. Requests over these limits fail with an explicit
error rather than a partial result.
