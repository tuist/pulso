# Scaling

Every Pulso instance can accept any authorized ingest request and serve any
query, because the bucket is the only authority: manifests are published with
conditional writes, and local buffers, caches and counters are disposable. You
scale by running more instances, and you can add or remove them at any time.

## Size each instance by its memory limit

Each instance bounds its own memory with admission slots. It derives them from
its container memory limit (or `PULSO_MEMORY_BUDGET_MB`): ingest slots at about
32 MiB each, interactive query slots with a heap ceiling each, and a fixed
reserve. See [node memory envelope](configuration.md#node-memory-envelope) for
the formula and overrides. Requests beyond the slots are refused with retryable
overload errors (HTTP `429` with `Retry-After`, gRPC `RESOURCE_EXHAUSTED`, or the
query error `query_overloaded`). Grafana Alloy, the OpenTelemetry Collector and
Prometheus retry those, so an overloaded instance slows senders down instead of
being OOM-killed.

Two things are not covered by the slots:

- **Manifests of tenants without retention.** Each signal's manifest is a flat
  list that grows with every append. Enable [retention](retention.md) (and
  compaction) for long-running tenants so manifests stay bounded.
- **Native query memory outside the heap**, such as decoded segments and
  accumulators. Each is bounded per query, not per node.

## Per-tenant limits

Without configuration, each signal's ingest slots are already shared fairly
between the tenants sending to an instance: a tenant above its equal share is
refused while another active tenant needs a slot (see
[ingest admission](configuration.md#ingest-admission)). That protects tenants
from each other under load but does not cap a tenant on an idle instance.
`PULSO_TENANT_LIMITS_JSON` (chart `tenantLimits`) adds configured bounds on
top, from a `default` entry and per-tenant overrides:

```json
{
  "default": {"ingest_max_concurrent": 8, "query_max_concurrent": 2},
  "tenants": {"acme": {"ingest_bytes_per_second": 10485760, "query_max_concurrent": 4}}
}
```

| Field | Meaning |
| --- | --- |
| `ingest_max_concurrent` | Ingest requests one tenant may process at once on an instance, across signals. A request stops counting once it hands its slot off while waiting for publication. |
| `ingest_bytes_per_second` | Token-bucket rate over request wire bytes (`Content-Length`; requests without one count as 1 MiB). |
| `ingest_burst_bytes` | Bucket size; defaults to twice the rate and at least 4 MiB, so one maximum-size request always fits. |
| `query_max_concurrent` | Interactive queries one tenant may run at once on an instance (default 2). |

A missing field means no tenant-specific ingest bound. Requests over a tenant's
bound get the same retryable `429` (gRPC `RESOURCE_EXHAUSTED`) as node overload,
with reason `tenant_concurrency` or `tenant_rate` in
`pulso_admission_rejections_total`. Limits are charged only after the caller's
credentials verify for the tenant they name, so a forged `X-Scope-OrgID` cannot
spend another tenant's allowance. Sentry ingestion uses only the fair share.

Limits are per instance: behind a balancer spreading a tenant over N instances,
the tenant can use up to N times its rate. Divide the intended cluster-wide rate
by the number of ingest instances when you set it.

## Autoscale on saturation, not memory

With fixed slots, memory stays roughly flat under load, so it is a poor
autoscaling signal. Use slot saturation and CPU instead. These are exported on
the private [`/metrics`](self-monitoring.md) endpoint of every instance:

| Signal | Expression |
| --- | --- |
| Ingest saturation | `pulso_ingest_inflight_requests / pulso_ingest_max_concurrent_requests` |
| Ingest refusals | `sum by (signal) (rate(pulso_ingest_admission_total{outcome="rejected"}[5m]))` |
| Query saturation | `pulso_query_inflight / pulso_query_max_concurrent` |
| Query refusals | `rate(pulso_admission_rejections_total{layer="query",reason="node"}[5m])` |

Ingest refusals include `tenant_limited` ones (fair-share refusals while free
slots were kept for other tenants). Query refusals with reason `tenant` and
`pulso_admission_rejections_total{layer="ingest"}` (`tenant_concurrency`,
`tenant_rate`) come from per-tenant limits, not node capacity; adding instances
does not help with those.

## Separate pools

By default every instance runs every role. `PULSO_ROLES` (chart `pools.enabled`)
splits them:

- **ingest** runs the receivers (OTLP over HTTP and gRPC, Loki push, remote
  write, Sentry);
- **query** runs query, discovery, MCP and management APIs;
- **maintenance** runs compaction, retention, alert evaluation, error projection
  and reliability workers, and serves no traffic.

Separate pools scale independently, and a heavy query can no longer take memory
or CPU from ingest on the same instance. An instance answers requests for a role
it does not run with a retryable `503`, so a misrouted batch is retried rather
than lost. With `pools.enabled`, the chart creates one Service per traffic pool
and routes receiver paths (`/v1/*`, `/otlp/v1/*`, `/loki/api/v1/push`,
`/api/v1/write`, `/api/v1/push`, `/api/prom/push`) to the ingest pool and all
other paths to the query pool. Point in-cluster senders at the
`<release>-ingest` Service and readers at `<release>-query`.

## Clustering

Instances work without knowing about each other. With `clustering.enabled` the
chart joins them into one Erlang cluster through a headless Service, and
rendezvous hashing assigns each tenant's background compaction, retention and
alert evaluation to one maintenance instance instead of every instance
attempting it. This only reduces duplicate work: conditional object writes keep
results correct when ownership overlaps during membership changes. Keep the pods
on a trusted network; Erlang distribution authenticates with a shared cookie
only. Requests are never forwarded between instances, so data never crosses the
cluster connection.

## Limits that do not scale with instances

- Per-instance limits are not cluster limits. Each instance enforces its own
  slots.
- Every instance that writes a tenant's signal competes on the same manifest
  with conditional writes. More instances mean more conflicts and retries for
  busy tenants, and more object-store requests overall.
