# Self-monitoring Pulso

Scrape `GET /metrics` on **every node** with Prometheus or Alloy, and send these
metrics to the existing monitoring destination, not to Pulso itself. The endpoint
exports Prometheus text format 0.0.4 without content negotiation or caching. It
reads only disposable node-local counters and registry snapshots: storage outages
must not prevent a scrape. No database, signal-storage writes, or exporter queue
is involved.

Like `/healthz` and `/readyz`, this endpoint has no bearer authentication. Keep it
on the private network and restrict access with the deployment's ingress and
network policy. It does not expose tenant IDs, object keys, credentials, labels
from incoming telemetry, query text, or raw error messages.

## Metrics

| Metric | Labels | Meaning |
| --- | --- | --- |
| `pulso_operations_total` | `layer`, `operation`, `outcome` | Completed requests/calls, including errors. Outcomes are `success`, `error`, and `exception`. |
| `pulso_operation_duration_seconds_count`, `_sum` | same | Latency summary with count and sum only. No quantiles. Includes failed operations. |
| `pulso_ingest_records_total` | `signal`, `outcome` | `accepted`: decoded records whose storage append returned success after publication. `rejected`: malformed records counted by the decoder. `failed`: valid decoded records whose append failed. |
| `pulso_object_payload_bytes_total` | `direction` (`read`, `write`) | Successfully returned GET payload bytes and successfully written PUT payload bytes, including manifests. |
| `pulso_compaction_segments_total` | none | Source segments merged by successful compactions (not the number of replacements). |
| `pulso_compaction_deleted_objects_total` | none | Retired objects removed by successful cleanup calls. |
| `pulso_manifest_queue_depth` | none | Sum of pending publication callers (including an active flush) and messages waiting in manifest-owner mailboxes. |

All counter dimensions are fixed enums. Counter updates use ETS directly rather
than sending messages to an aggregator. A node or counter-process restart resets
counters; use `rate`/`increase` so resets are handled. Each scrape is a best-effort
snapshot, not an atomic snapshot across counters.

### Operation layers

- `ingest`: `otlp`, `loki`, `remote_write`. HTTP status determines the outcome;
  timing starts before body parsing and includes decode, append, and response
  preparation. A partial-success OTLP response remains a successful request, with
  rejected records counted separately.
- `query`: `http` for the compatibility query/discovery endpoints; `mcp` for tool
  calls; `promql`, `logql_log`, and `logql_metric` for evaluator calls; and
  `storage_logs` / `storage_metrics` for scans through the storage dispatcher.
  These are **different layers of the same query**, not additive query counts.
  Choose one layer for a dashboard or alert. MCP argument/auth failures are counted
  even when no scan starts. HTTP validation failures are counted at the HTTP layer.
- `object`: `put`, `put_if_match`, `put_if_none_match`, `get`,
  `get_if_none_match`, `delete`, `list`, `list_prefixes`. Each wrapper invocation
  counts once; a conditional GET returning not-modified is a success. Conditional
  write conflicts are errors, even when a higher layer later retries successfully.
- `compaction`: `compact`, `cleanup` measure the maintenance calls themselves;
  `worker_merge`, `worker_cleanup`, `worker_discovery` measure the background
  worker's supervised waits, including deadline failures. These are separate layers,
  not additive counts. A native call may finish successfully after its worker wait
  has timed out. No-op maintenance passes are successful operations with no added
  progress. Failures increment operation counters, not successful-progress counters.

## Interpreting rejection, queues, and costs

Record counters describe processing attempts, not unique stored records. Retried
or idempotently acknowledged batches count again. If authorization, headers,
compression, protobuf, or JSON parsing fails before the record count is known,
only the failed request is counted. Do not invent rejected-record estimates from
body size. Failed publication is distinct from decoder rejection because senders
may retry it.

Queue depth describes today's manifest publication bottleneck. Pulso does not yet
have buffered ingest queues. It includes non-publication mailbox messages and is
an approximate operational gauge, not an admission limit. Registry values vanish
when their owners exit. Scrapes never call an owner or wait for its storage work.

Object metrics are not provider billing counters: native SDK retries, paginated
listing requests, HTTP headers, failed/partial transfers, and provider overhead
are not observable at this wrapper. Compare them with provider billing instead
of presenting them as exact charged operations or network bytes. Cleanup can
delete some objects before returning an error; object-operation counts still
record those calls, while successful-cleanup progress does not count that pass.
Calls terminated by an untrappable process kill cannot record completion.

## Example queries for the Tuist pilot

Filter by the scrape job and keep the node's `instance` label:

```promql
# Published log records per second, by node.
sum by (instance) (rate(pulso_ingest_records_total{signal="logs",outcome="accepted"}[5m]))

# Failed ingress requests, including failures before a record count is available.
sum by (instance, operation) (rate(pulso_operations_total{layer="ingest",outcome="error"}[5m]))

# Average ingest latency in seconds.
sum by (instance, operation) (rate(pulso_operation_duration_seconds_sum{layer="ingest"}[5m]))
/
sum by (instance, operation) (rate(pulso_operation_duration_seconds_count{layer="ingest"}[5m]))

# MCP tool failures, without double-counting scans or evaluator calls.
sum by (instance) (rate(pulso_operations_total{layer="query",operation="mcp",outcome="error"}[5m]))

# Publication backlog.
pulso_manifest_queue_depth

# Conditional-write conflicts or storage errors.
sum by (instance, operation) (rate(pulso_operations_total{layer="object",outcome="error"}[5m]))

# Successful payload traffic, including manifests and compaction reads/writes.
sum by (instance, direction) (rate(pulso_object_payload_bytes_total[5m]))
```

Select alert thresholds from the measured pilot workload. This endpoint provides
instrumentation, not evidence of Tuist capacity or deployment readiness.
