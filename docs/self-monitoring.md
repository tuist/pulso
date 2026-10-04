# Self-monitoring Pulso

Scrape `GET /metrics` on **every node**, not a load-balanced service address that
alternates nodes. Send scrapes to the existing Prometheus/Grafana Cloud monitoring
destination during the pilot, **not back into Pulso**. The endpoint exports
Prometheus text format 0.0.4 without content negotiation or caching. It reads only
node-local counters, registry metadata, mailbox lengths, and VM statistics. It
never queries or appends to signal storage or waits for manifest owners blocked
in native I/O. Counter updates use ETS directly, without an exporter queue.

Counters reset on node or counter-process restart; use `rate`/`increase` to handle
resets. Canonical counters are initialized at zero; detailed series appear when
first observed. Gauges are always present. Observations and scrapes are not
transactions with storage and can be lost during process/node failure. These are
operational signals, not accounting ledgers or deployment-readiness gates.

## Exposure and independent collection

The endpoint does not authenticate with tenant ingest tokens and shares the
listener with ingest and MCP. Keep the **entire listener private** for the pilot.
Port-level network policy cannot isolate `/metrics` from ingest on that port.
If ingest or MCP is exposed outside the trusted monitoring network, a path-aware
proxy **must deny `/metrics` on that ingress**, or require separate monitoring
authentication for it. Deny router-equivalent paths too, including percent-encoded
segments, duplicate slashes, and trailing slashes. Scrapers may use the private
node listener or a separately authenticated proxy route. For a normalizing Nginx
public proxy, a `location ^~ /metrics { return 404; }` block denies the metrics
path and its suffixes; verify normalization and test aliases before exposing it.
Network policy alone is not an alternative to that rule.

Labels use fixed vocabularies. No tenant identifiers, credentials, query text,
object keys, incoming telemetry labels, or raw error messages are exported.
User-controlled inputs cannot grow the number of time series.

Example Prometheus job (replace the target with each node's private address):

```yaml
scrape_configs:
  - job_name: pulso
    scrape_interval: 15s
    scrape_timeout: 5s
    metrics_path: /metrics
    static_configs:
      - targets: [pulso-node-1.internal:4000]
```

A collector must retain its independent destination and retry policy. This
repository supplies the endpoint, not the Tuist collector or chart rollout.

## Canonical metric contract

The canonical families retain the contract already published by `Pulso.SelfMetrics`.
Their operation labels describe layers of work, and latency remains a summary
with count and sum only, without quantiles.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `pulso_operations_total` | Counter | `layer`, `operation`, `outcome` | Completed operations; outcomes are `success`, `error`, and `exception` |
| `pulso_operation_duration_seconds_count`, `_sum` | Summary | Same as above | Operation latency count and sum, including failed operations |
| `pulso_ingest_records_total` | Counter | `signal`, `outcome` | `accepted`: valid decoded records whose append succeeded; `rejected`: decoder drops; `failed`: valid decoded records whose append failed |
| `pulso_object_payload_bytes_total` | Counter | `direction` (`read`, `write`) | Successful GET body bytes and PUT payload bytes, including manifests |
| `pulso_compaction_segments_total` | Counter | None | Source segments merged by successful compactions |
| `pulso_compaction_deleted_objects_total` | Counter | None | Retirement deletions confirmed by successful cleanup calls |
| `pulso_manifest_queue_depth` | Gauge | None | Publication callers, including active flushes, plus manifest-owner mailbox messages |

Operation layers:

- `ingest`: `otlp`, `loki`, `remote_write`. Timing starts before body parsing and
  includes decoding, append, and response preparation. Partial-success responses
  remain successful requests, with decoder drops counted separately.
- `query`: `http` for compatibility queries/discovery; `mcp` for direct tool
  dispatch; `promql`, `logql_log`, and `logql_metric` for evaluators; and
  `storage_logs` / `storage_metrics` for scans through the storage dispatcher.
  These are **different layers of the same query**, not additive query counts.
  Choose one layer for a dashboard or alert. Authorization and validation errors
  count even when no scan starts.
- `object`: `put`, `put_if_match`, `put_if_none_match`, `get`,
  `get_if_none_match`, `delete`, `list`, `list_prefixes`. Each wrapper invocation
  counts once. A conditional GET returning not-modified is successful; conditional
  write conflicts are errors even if a higher layer later retries successfully.
- `compaction`: `compact`, `cleanup` measure maintenance calls;
  `worker_merge`, `worker_cleanup`, `worker_discovery` measure background workers'
  supervised waits, including deadline failures. These are separate layers, not
  additive counts. A task may publish successfully after its worker wait times
  out. No-op passes succeed without incrementing successful-progress counters.

## Detailed metric contract

The detailed view retains tool/route distinctions, object-purpose classifications,
latency histograms, and separate queue/runtime gauges. Families that would clash
with canonical names are explicitly namespaced. **Do not add canonical and
detailed counters together**: they observe the same work from different views.
Filter a single view, and a single query/maintenance layer, for each calculation.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `pulso_detailed_operations_total` | Counter | `kind`, `operation`, `purpose`, `outcome` | Completed ingest requests, public queries, logical object calls, and maintenance attempts |
| `pulso_detailed_operation_duration_seconds` | Histogram | Same as above, plus bucket `le` | Operation duration; buckets at 5 ms, 10 ms, 50 ms, 100 ms, 500 ms, 1 s, 5 s, 10 s, and +Inf |
| `pulso_ingest_delivery_records_total` | Counter | `signal`, `outcome` | Known receiver deliveries: `accepted` after append success; `rejected` combines decoder drops and decoded records whose append failed |
| `pulso_object_bytes_total` | Counter | `operation`, `purpose`, `direction` | Successful logical GET body bytes and PUT payload bytes, partitioned by purpose |
| `pulso_detailed_compaction_segments_total` | Counter | `operation` | Source segments merged by `compact`, or confirmed retirement deletions by `cleanup` |
| `pulso_compaction_timeouts_total` | Counter | `operation` | Worker deadlines exceeded for `merge`, `cleanup`, or `discovery`, even while native work remains in flight |
| `pulso_manifest_mailbox_messages` | Gauge | None | Sum of manifest-owner mailbox lengths |
| `pulso_manifest_pending_segments` | Gauge | None | Enqueued segments, including segments currently being published |
| `pulso_manifest_waiting_requests` | Gauge | None | Publication callers, including callers in the current flush |
| `pulso_query_occupied_slots` | Gauge | None | Registered PromQL tenant slots, including admitted native work that outlives the response deadline |
| `pulso_vm_memory_bytes` | Gauge | None | Total BEAM-reported memory |
| `pulso_vm_run_queue` | Gauge | None | BEAM scheduler run-queue length |

Detailed operation labels:

- `kind=ingest`: `otlp`, `loki` (both formats), `remote_write`. HTTP 4xx are
  `rejected`; 5xx are `error` or `exception`; successful/partial responses are
  `ok`. Parser failures count before controllers run. Each request counts once.
- `kind=query`: `http_promql`, `http_logql`, `http_labels`, and tool names
  `query_logs`, `query_metrics`, `query_logql`, `query_promql`. Tool errors count
  as `error` even when the MCP transport returns HTTP 200. Direct `Tools.call`
  calls collapse unknown names to `unknown_tool`; the MCP dispatcher rejects
  unknown names before dispatch, so they are not tool attempts over MCP. Internal
  evaluator/storage calls are not additional detailed public queries.
- `kind=object`: the same eight object operations as the canonical view.
  Outcomes distinguish `ok`, `error`, `exception`, `conflict`, `not_found`, and
  `not_modified`. `purpose=manifest` identifies `/manifest.json` keys, `segment`
  identifies `.parquet` keys, and `other` covers listings/other objects. Other
  operation kinds use `purpose=none`. Only meaningful byte directions are emitted.
- `kind=compaction`: `compact`, `cleanup`, including explicit and background
  calls. No-op passes count operations but add no segments. The separate timeout
  counter increments when the worker stops waiting; a late success also increments
  the eventual operation-success counter. These are different events.

## Counting boundaries and limitations

Record counters describe attempts, not unique stored records. Retried or
idempotently acknowledged batches count again. Canonical decoder rejection stays
separate from publication failure; the detailed receiver-delivery view combines
both as rejected deliveries. Direct storage appends contribute to canonical
accepted/failed counts, but not to the receiver-only delivery view. A storage
error can be ambiguous about publication: the counters describe acknowledgements,
not proof that nothing was written. Authentication, parsing, body/attribute/record
budgets, or decoding may fail before a complete count exists; those count requests,
not invented record estimates. Compare these signals with collectors' queues,
deliveries, and drops.

Object metrics are logical successful body bytes, not provider billing. They
exclude headers, listing response bodies, failed/ambiguous PUT payloads, native
client retries, provider pagination requests, and partial transfers. Conditional
GETs returning 304 add no body bytes; write conflicts add no successful bytes.
Application-level retries are new logical calls. Reconcile against provider
billing rather than deriving a bill solely from these counters.

A partially failed cleanup may have deleted objects. Object-operation counters
still show those attempts, but successful-cleanup progress excludes that pass.
Untrappable process termination cannot record completion. Durations end when the
underlying operation completes, while worker wait metrics and deadline counters
report timeouts immediately.

Queue gauges are instantaneous snapshots, not admission guarantees. Mailbox
messages and publication batches are different stages; inspect both. Registry
cleanup removes terminated owners. Scrapes do not wait for native I/O. PromQL
slots do not describe all log/raw query concurrency. No new query/ingest admission
or buffered ingest queues are introduced. Histogram updates may be observed
partway through an update; metric snapshots are best effort.

## Useful pilot queries

Keep the scrape job and node `instance` label in dashboards:

```promql
# Canonical published records per second by node.
sum by (instance, signal) (rate(pulso_ingest_records_total{outcome="accepted"}[5m]))

# Canonical MCP tool failures, without counting evaluator/storage layers again.
sum by (instance) (rate(pulso_operations_total{layer="query",operation="mcp",outcome="error"}[5m]))

# Canonical publication backlog.
pulso_manifest_queue_depth

# Detailed ingest p95, including failures.
histogram_quantile(0.95, sum by (instance, le, operation) (
  rate(pulso_detailed_operation_duration_seconds_bucket{kind="ingest"}[5m])
))

# Detailed public query failures.
sum by (instance, operation, outcome) (
  rate(pulso_detailed_operations_total{kind="query",outcome!="ok"}[5m])
)

# Logical manifest write bytes and conditional conflicts.
sum by (instance) (rate(pulso_object_bytes_total{purpose="manifest",direction="write"}[5m]))
sum by (instance) (rate(pulso_detailed_operations_total{kind="object",outcome="conflict"}[5m]))

# Worker deadlines exceeded even while native work is still running.
sum by (instance, operation) (rate(pulso_compaction_timeouts_total[5m]))
```

Alert on scrape failure (`up == 0`) through the independent monitor. Choose
latency, queue, rejection-rate, and compaction thresholds from the measured pilot
load. Instrumentation alone establishes neither safe capacity nor readiness.
