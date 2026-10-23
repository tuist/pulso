# Protocol coverage

Pulso's compatibility target is the **public protocols and their full feature
breadth**, not the subset used by one application or deployment. A successful
client example is evidence for that profile, not the product boundary or proof
of full conformance. Today several surfaces remain intentionally incomplete.
Use the linked contracts before moving a mixed telemetry pipeline. Coverage is
evaluated against explicitly targeted protocol versions, not an implicit promise
to support every historical client/session protocol.

## Current boundaries

| Surface | Available | Still unsupported or incomplete |
| --- | --- | --- |
| OTLP logs | HTTP JSON/protobuf/gzip and opt-in [gRPC](otlp-grpc.md) (gzip/zstd); typed bodies and resource/scope/log metadata; source times, context IDs and event names | Resource entity references and remaining OTLP JSON forms |
| OTLP metrics | HTTP and opt-in [gRPC](otlp-grpc.md). Gauges; cumulative and delta sums; cumulative and delta explicit and exponential histograms (exponential projected to classic buckets); summaries; typed owners, original counts, optional fields and embedded exemplars | Unspecified temporality; delta-to-cumulative conversion (deltas are stored as reported); native-histogram storage; full OTLP-to-Prometheus naming/resource promotion; dedicated exemplar queries |
| OTLP traces | HTTP JSON/protobuf/gzip and opt-in [gRPC](otlp-grpc.md); original typed resource/scope/span/event/link data, bounded complete exact-ID assembly and export | Resource entity references, remaining OTLP JSON forms, and proof of exporter completeness |
| Prometheus remote write | 1.0 and 2.0 scalar samples, stale markers and non-finite values, native histograms (integer and float, exponential and custom-bucket), exemplars, and retained metric metadata (type/help/unit; per-series in 2.0, with OpenMetrics suffixes trimmed from family names), 2.0 written-count headers, also at `/api/prom/push` and `/api/v1/push` | 2.0 created (start) timestamps |
| Prometheus queries/discovery | Documented bounded PromQL subset, native histograms (Prometheus 3.5.0 semantics), `/api/v1/query_exemplars`, instant/range HTTP APIs, sample-backed label/series discovery, build info, query validation, `/api/v1/metadata` from retained remote-write metadata, and MCP queries | Full language/function/modifier coverage, OTLP-sourced metadata, and full TSDB/client parity |
| Loki ingestion/queries | JSON/Snappy protobuf push and OTLP at `/otlp/v1/logs`; documented bounded LogQL queries; complete stream label, value and series discovery; index stats and volume; build info; WebSocket live tail; detected labels and fields; query-time patterns and `|>`/`!>` filters; MCP log reads | Full LogQL; `aggregateBy=labels` volume; full client parity |
| Tempo compatibility | Experimental bounded v1/v2 exact-ID export; separately opt-in retained-context search and discovery; documented typed predicates; TraceQL metrics with `topk`/`bottomk` (`rate`, `count`/`min`/`max`/`sum`/`avg_over_time`, `quantile_over_time`, `histogram_over_time`); echo and build info | Full TraceQL including structural/spanset operations, second-stage metrics (`topk`, `compare`), exemplars, full indexed planning, discovery parity and deployed-client acceptance |
| Alerting | Native bounded thresholds, authored mixed-source expressions, experimental recordings and delivery | Full Grafana imported-rule execution, lifecycle, routing, notification and template semantics |
| MCP | Stateless 2026-07-28 tools, capability-scoped Pulso writes and request-scoped subscriptions | Legacy initialize/session protocols; infrastructure remediation is deliberately a separate server |

Shared [OTLP wire support](otlp-wire.md) includes bounded recursive singular-message
merging, repeated-field append and protobuf oneof selection, plus exact quoted
and unquoted schema-scoped decimal/exponent integer forms without float rounding
or fractional underflow. Receiver-scoped JSON rejects duplicate known fields,
including escaped/null-valued duplicates, while ignoring unknown names/objects and
foreign signal roots. JSON objects with multiple populated oneof members still
reject. Trace uint32 counts and fixed32 flags accept exact quoted forms too;
enums still require numeric JSON tokens, not quoted digits. Remaining
null/presence behavior and entity references remain gaps. Named enum strings and snake_case keys are invalid OTLP
spellings, not missing generic ProtoJSON features. Shared wire capability **3**
is required across serving ingress app/native pairs.

See [logs](logs.md), [metrics](metrics.md), [traces](traces.md),
[querying](querying.md), [metric discovery](metric-discovery.md),
[trace expressions](trace-query-expressions.md) and [alerting](alerting.md) for
exact supported syntax, data types, defaults and safety ceilings. A Collector
bridge can supply OTLP/gRPC transport, but does not establish every processor,
queue, retry, temporality or TLS/authentication profile. A Collector conversion
must not be assumed to preserve fields without independent evidence.

## Completeness and operational meaning

- Unsupported OTLP point forms produce explicit **source-point** rejection counts
  on unkeyed partial-success requests. Inspect these counts: many exporters do
  not replay a rejected subset. A keyed request with any rejected point fails
  whole instead of acknowledging different content under the retry key.
- Unsupported query syntax, incomplete storage reads and exhausted budgets fail
  the whole query. Pulso does not drop predicates or return a successful truncated
  trace to make a compatibility check pass.
- Source metadata and projected Prometheus labels/samples are different views.
  Integer counts above 2^53 remain exact in source metadata, not in float samples.
  Naming projections do not infer producer identity, aliases or original types.
- Source-point, projected-sample, request-node, metadata, result, scan and work
  ceilings are independent. Supporting a feature does not remove its resource
  bounds or imply that every valid-sized wire payload can be stored or queried.
- Every authorized instance can serve the shared object-storage authority.
  There is no node-local durable delta accumulator, sticky-routing requirement
  or external mutable schema registry. New cumulative conversion never repairs
  ambiguous producer ownership or conflicting cross-request metric families.
- Upgrade all applicable app/native pairs before relying on new support. A local
  capability check cannot fence an older pool. No protocol feature pass alone
  establishes provider capacity, admission fairness, crash recovery or cutover
  acceptance.

Public schema fixtures and independent semantic references are kept separately
from optional live client exercises. Ordinary tests replay frozen evidence with
the native project toolchain; they do not download or execute SDKs, Collectors,
Go producers or external reference servers.
