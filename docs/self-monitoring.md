# Self-monitoring

Pulso exposes node-local operational metrics at `GET /metrics`, in Prometheus
text exposition format (`text/plain; version=0.0.4`). Scrape **each node**, not a
load-balanced service address that alternates nodes. Counters reset on node or
metrics-process restart; use `rate`/`increase`, not differences between absolute
values across restarts. Series appear when first observed. Gauges are always
present, including zero values when a role is not running.

The endpoint does not authenticate with tenant ingest tokens and shares the
listener with ingest and MCP. Keep the **entire listener private** for the pilot.
Port-level network policy cannot isolate `/metrics` from ingest on that port.
If ingest or MCP is exposed outside the trusted monitoring network, a path-aware
proxy **must deny `/metrics` on that ingress**, or require separate monitoring
authentication for it. Deny router-equivalent paths too, including percent-encoded
segments, duplicate slashes, and trailing slashes. Scrapers may use the private
node listener or a separately authenticated proxy route. For a normalizing Nginx
public proxy, a `location ^~ /metrics { return 404; }` block denies the metrics
path and its suffixes; verify the rendered proxy's normalization and test aliases
before exposing it. Network policy alone is not an alternative to that rule.

The endpoint contains no tenant, token, expression, object-key, or raw-error
labels. Labels are drawn from a fixed vocabulary so
user-controlled inputs cannot grow the number of time series.

## Keep monitoring independent

Send scrapes to the existing Prometheus/Grafana Cloud monitoring destination
during the pilot, **not back into Pulso**. An object-store outage must not hide
Pulso's failure rates or saturation. `/metrics` reads only local ETS counters,
registry metadata, process mailbox lengths, and VM statistics. It does not ask
manifest owners to respond and does not query or append to any storage adapter.
Metric updates run synchronously in the observed process, without an unbounded
reporting mailbox. A metrics-process outage drops observations rather than
failing ingest, object operations, or queries.

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

## Metric contract

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `pulso_operations_total` | Counter | `kind`, `operation`, `purpose`, `outcome` | Completed ingest requests, public queries, logical object-store calls, and compaction/cleanup attempts |
| `pulso_operation_duration_seconds` | Histogram | Same as above, plus bucket `le` | End-to-end observed operation duration; buckets at 5 ms, 10 ms, 50 ms, 100 ms, 500 ms, 1 s, 5 s, 10 s, and +Inf |
| `pulso_ingest_records_total` | Counter | `signal`, `outcome` | Known record counts, with `signal=logs\|metrics` and `outcome=accepted\|rejected` |
| `pulso_object_bytes_total` | Counter | `operation`, `purpose`, `direction` | Successful logical GET response-body bytes and PUT payload bytes, with `direction=read\|write` |
| `pulso_compaction_segments_total` | Counter | `operation` | Source segments merged by successful `compact` calls, or retirement deletions confirmed by successful `cleanup` calls |
| `pulso_compaction_timeouts_total` | Counter | `operation` | Background worker deadlines exceeded for `merge`, `cleanup`, or `discovery`, recorded even while native work is still running |
| `pulso_manifest_mailbox_messages` | Gauge | None | Sum of manifest-owner mailbox lengths on this node, including requests arriving while native I/O is blocked |
| `pulso_manifest_pending_segments` | Gauge | None | Sum of enqueued segments, including segments currently being published |
| `pulso_manifest_waiting_requests` | Gauge | None | Sum of callers in owners' publication batches, including batches currently in flight |
| `pulso_query_occupied_slots` | Gauge | None | Registered PromQL tenant slots on this node; includes admitted native work that outlives the response deadline |
| `pulso_vm_memory_bytes` | Gauge | None | Total BEAM-reported memory |
| `pulso_vm_run_queue` | Gauge | None | BEAM scheduler run-queue length |

Operation labels:

- `kind=ingest`: `otlp`, `loki` (both wire formats), `remote_write`.
  Latency includes parsing/decompression, authentication, decoding, storage, and
  response preparation. `ok` includes partial-success responses; watch rejected
  records too. HTTP 4xx are `rejected`; 5xx are `error` or `exception`.
  Parser failures, including gzip expansion limits, count even when no controller
  runs. Each request counts once.
- `kind=query`: `http_promql`, `http_logql`, `http_labels`, and MCP tool names
  `query_logs`, `query_metrics`, `query_logql`, `query_promql`. Direct calls to
  `Tools.call` collapse unknown names to `unknown_tool`; the MCP dispatcher
  rejects unknown tool names before dispatch, so they do not appear as attempts
  over MCP. MCP tool errors count as `error` even when the
  transport correctly returns HTTP 200. HTTP queries include validation and
  authorization failures; internal storage scans are not counted as additional
  public queries. MCP protocol errors before tool dispatch are not tool attempts.
- `kind=object`: `put`, `put_if_match`, `put_if_none_match`, `get`,
  `get_if_none_match`, `delete`, `list`, `list_prefixes`. Outcomes distinguish
  `ok`, `error`, `exception`, `conflict`, `not_found`, and `not_modified`.
  `purpose=manifest` identifies `/manifest.json` keys, `segment` identifies
  `.parquet` keys, and `other` covers listings and other objects. Other operation
  kinds use `purpose=none`.
- `kind=compaction`: `compact`, `cleanup`, including explicit maintenance calls
  and background worker calls. Successful no-op passes count as operations but
  add no segments. Errors/exceptions are observable without storing error text.

## Counting boundaries and limitations

Accepted records increment only after the storage adapter reports successful
publication. They count acknowledged receiver deliveries, **not unique stored
records**: a successful idempotent retry counts again. Decoder drops increment
rejected records. A decoded batch whose append fails counts all supplied decoded
records plus decoder drops as rejected; callers may retry them later. A storage
error can be ambiguous about publication, so this describes acknowledgements,
not proof that nothing was written.

Authentication, parser, body-size, attribute, and record-budget failures can
happen before a complete decoded record count exists. Those increment rejected
**requests**, not invented record counts. Do not interpret the record counter as
an exact count of every record sent or lost. Compare it with collector delivery,
queue, and drop metrics during rollout.

Object bytes are logical successful body bytes, not provider-billed network
traffic. They exclude HTTP headers, LIST response bodies, failed/ambiguous PUT
payloads, and retries inside the native client. Conditional GETs returning 304
add zero body bytes; conditional-write conflicts add zero successful bytes.
Application-level retries count as new logical calls. Reconcile these counters
with provider billing rather than deriving a bill solely from them.

Cleanup segment counts only cover successful calls with confirmed durable
progress. A partially failed cleanup may have deleted objects; its object
operation counters still show those attempts, but it adds no confirmed cleanup
segment total. Compaction durations end when the underlying task finishes, even
if the background worker stopped waiting at its deadline. The separate
`pulso_compaction_timeouts_total` increments when the worker stops waiting, not
when the task finishes, so a permanently stalled native call remains observable.
A timeout followed by a late successful publication increments both the deadline
counter and the eventual operation-success counter; these are different events.

Queue gauges are instantaneous, node-local snapshots, not admission guarantees.
Mailbox messages and publication batches are different stages; inspect both.
Registry entries disappear on owner termination. Reading queue metadata does not
wait for a native storage call to finish. PromQL occupied slots are not a gauge
of all log/raw queries, and this change adds no new query or ingest admission.
Counter and histogram updates are not a transaction with acknowledged storage or
with a scrape: observations can be lost on process/node failure, and simultaneous
updates may be observed partway through a histogram update. The endpoint is an
operational signal, not an accounting ledger or deployment-readiness gate.

## Useful pilot queries

```promql
# Failed or rejected ingest requests per second
sum by (operation, outcome) (
  rate(pulso_operations_total{kind="ingest", outcome!="ok"}[5m])
)

# Acknowledged record deliveries per second
sum by (signal) (rate(pulso_ingest_records_total{outcome="accepted"}[5m]))

# Ingest p95, including failures
histogram_quantile(0.95, sum by (le, operation) (
  rate(pulso_operation_duration_seconds_bucket{kind="ingest"}[5m])
))

# Public query failures (MCP errors are not hidden behind HTTP 200)
sum by (operation, outcome) (
  rate(pulso_operations_total{kind="query", outcome!="ok"}[5m])
)

# Logical manifest write bytes per second
sum(rate(pulso_object_bytes_total{purpose="manifest", direction="write"}[5m]))

# Conditional publication conflicts per second
sum(rate(pulso_operations_total{kind="object", outcome="conflict"}[5m]))
```

Alert on scrape failure (`up == 0`) through the independent monitor. Establish
latency, queue, rejection-rate, and compaction thresholds from the measured pilot
load; the endpoint alone does not establish safe capacity or readiness.
