# Querying

Pulso has no user interface. Agents query it through the Model Context Protocol,
and dashboards or scripts use its Prometheus- and Loki-compatible HTTP APIs. Every
query is scoped to one tenant and authenticated with that tenant's bearer token.
See [tenants and tokens](configuration.md#tenants-and-tokens).

## Model Context Protocol

`POST /mcp` implements the stateless
[`2026-07-28`](https://modelcontextprotocol.io/specification/2026-07-28) revision
of the protocol over Streamable HTTP. There is no handshake or session: each
request carries its protocol version and client capabilities in
`params._meta` and mirrors them in the `MCP-Protocol-Version`, `Mcp-Method`, and,
for `tools/call`, `Mcp-Name` headers. Clients that only speak older revisions
receive `400` naming the supported version.

Send the tenant's token as `Authorization: Bearer <token>`. Browser clients must
also have their origin listed in `PULSO_MCP_ALLOWED_ORIGINS`.

All tools are read-only and take the tenant as an argument:

| Tool | Purpose |
| --- | --- |
| `query_logs` | Return log records for a time range, optionally filtered by service. |
| `query_logql` | Evaluate a LogQL expression and return the same envelope as `/loki/api/v1/query_range`. |
| `query_metrics` | Return raw metric samples for a time range, filtered by label matchers. |
| `query_promql` | Evaluate a Prometheus Query Language expression, instant or range. |

`tools/list` returns each tool's full input schema. Invalid arguments, such as a
reversed time range, return an explicit error instead of an empty result.

## Prometheus API

`GET|POST /api/v1/query` and `/api/v1/query_range` accept the standard Prometheus
parameters (`query`, `time`, or `start`/`end`/`step`) with the tenant in the
`X-Scope-OrgID` header. Timestamps accept Unix seconds or date-time strings with
a timezone; steps accept seconds or durations such as `15s`. Client `timeout`
values are capped at ten seconds. Results use the Prometheus vector and matrix
formats, so Grafana can use Pulso as a Prometheus data source within the
supported subset below.

Pulso implements a subset of the
[Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/):

- Selectors with label matchers and positive `offset`
- `rate`, `increase`, `irate`, and `delta`
- `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, and
  `count_over_time`
- Nested aggregations with `by` and `without` grouping

```text
sum by (job) (rate(http_requests_total{job="api"}[5m]))
avg without (instance) (process_resident_memory_bytes)
```

Not supported yet: binary operators, scalar expressions, subqueries, histograms
and `histogram_quantile`, negative offsets, `@`, and stale-marker semantics.
Unsupported expressions and parameters return an error.

## Loki API

`/loki/api/v1/query` and `/loki/api/v1/query_range` evaluate LogQL, and
`/loki/api/v1/labels` and `/loki/api/v1/label/<name>/values` support label
discovery. They take the tenant in the `X-Scope-OrgID` header, like Loki.

## Limits

Each node runs a bounded number of queries at once, at most two per tenant, and
bounds each query's duration, memory, scanned objects, and result size. A query
that exceeds a budget fails with an explicit error rather than returning a
partial result; narrow the time range or the selector and retry. Samples that
conflict at the same timestamp resolve deterministically and add a warning to
the response.
