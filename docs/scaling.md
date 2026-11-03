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

## Autoscale on saturation, not memory

With fixed slots, memory stays roughly flat under load, so it is a poor
autoscaling signal. Use slot saturation and CPU instead. These are exported on
the private [`/metrics`](self-monitoring.md) endpoint of every instance:

| Signal | Expression |
| --- | --- |
| Ingest saturation | `pulso_ingest_inflight_requests / pulso_ingest_max_concurrent_requests` |
| Ingest refusals | `rate(pulso_admission_rejections_total{layer="ingest",reason="node"}[5m])` |
| Query saturation | `pulso_query_inflight / pulso_query_max_concurrent` |
| Query refusals | `rate(pulso_admission_rejections_total{layer="query",reason="node"}[5m])` |

Refusals with reason `tenant` or `tenant_*` come from per-tenant limits, not
node capacity; adding instances does not help with those.

## Limits that do not scale with instances

- Per-instance limits are not cluster limits. Each instance enforces its own
  slots.
- Every instance that writes a tenant's signal competes on the same manifest
  with conditional writes. More instances mean more conflicts and retries for
  busy tenants, and more object-store requests overall.
