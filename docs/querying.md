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
values are capped at ten seconds. Results use Prometheus scalar, vector, and matrix
formats, so Grafana can use Pulso as a Prometheus data source within the
supported subset below.

Pulso implements a subset of the
[Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/):

- Selectors with label matchers and positive `offset`
- `rate`, `increase`, `irate`, and `delta`
- `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, and
  `count_over_time`
- Nested `sum`, `avg`, `min`, `max`, `count`, and `group` aggregations with
  `by` and `without` grouping; `topk` and `bottomk` reject NaN and ranking
  parameters at or above the signed int64 maximum; values below one yield no results
- Scalar literals, unary signs, arithmetic (`+`, `-`, `*`, `/`, `%`, `^`),
  comparisons with optional `bool`, and `and`, `or`, and `unless`
- Vector matching with `on`, `ignoring`, `group_left`, and `group_right`;
  ambiguous matches and duplicate output series return explicit errors
- Classic `histogram_quantile`, `clamp_min`, `clamp_max`, `label_replace`,
  `vector`, `scalar`, `time`, `round`, `abs`, `sort`, and `sort_desc`
- Stale markers: instant selectors stop at the latest stale marker until a new
  sample arrives; range functions ignore stale markers
- IEEE-754 NaN and infinities, returned as `"NaN"`, `"+Inf"`, or `"-Inf"` in
  Prometheus result values

```text
sum by (job) (rate(http_requests_total{job="api"}[5m]))
avg without (instance) (process_resident_memory_bytes)
```

```text
histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))
sum(rate(http_requests_total{status=~"5.."}[5m])) / clamp_min(sum(rate(http_requests_total[5m])), 0.001)
```

Classic histograms use ordinary float bucket series with a `le` label and a
`+Inf` bucket. Bounds accept Go floating-point spelling (including `.5`,
hexadecimal floats, and case-insensitive infinity); malformed bounds are ignored,
like Prometheus. Use numeric ordered boundaries and a positive-infinity final
bucket: NaN boundaries do not define a meaningful classic histogram. Histograms
with different metric names remain distinct; use explicit aggregation to combine
them before computing quantiles. Sums and averages use compensated summation to limit cancellation. This is not native histogram support. Native histogram samples
and exemplars are explicitly counted as rejected during ingestion.

Not supported yet: subqueries, negative offsets, `@`, native histograms,
exemplars, and functions outside the list above. Regular expressions use a
bounded RE2-compatible subset: unsupported constructs return errors, including
lookarounds, backreferences, quoted literals (`\Q...\E`), and some character-class
and escape forms. Full Prometheus language compatibility is not claimed.
Unsupported expressions and parameters return an error.

Raw `query_metrics` results preserve storage markers as JSON strings:
`"stale"`, `"nan"`, `"infinity"`, and `"negative_infinity"`. These are distinct from
the Prometheus evaluator's string-valued result numbers.

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
the response. A stale marker wins a conflict with a finite sample at the same
timestamp; otherwise the maximum value wins, with ordinary NaN ordered last.
Out-of-order samples are sorted before evaluation. This is deterministic query
resolution, not ingest-time duplicate rejection.

Selected-sample and evaluation-work budgets cover all selectors and operations
in one expression. Binary/function intermediate output points also share a
bounded budget. Expressions have at most 256 nodes and 64 levels of AST depth.
Object scan budgets still apply independently to each selector; indexed scans
and aggregate object-byte accounting remain follow-up work.
