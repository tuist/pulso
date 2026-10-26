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
| Prometheus remote write | 1.0 and 2.0 scalar samples, stale markers and non-finite values, native histograms (integer and float, exponential and custom-bucket), exemplars, retained metric metadata, 2.0 written-count headers, and [source created/start timestamps](metrics.md#remote-write-20-start-timestamps) (legacy rc.3 per-series and rc.4 per-point); also at `/api/prom/push` and `/api/v1/push` | Optional zero-sample injection/start-aware rates, newer stateless-mode validation, full TSDB/client/compliance-suite acceptance |
| Prometheus queries/discovery | Documented bounded PromQL subset, including quoted UTF-8 selector names and basic math (`ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`); native histograms (Prometheus 3.5.0 semantics), `/api/v1/query_exemplars`, instant/range HTTP APIs, sample-backed label/series discovery, build info, bounded single-line query formatting, `/api/v1/metadata` from retained remote-write metadata, and MCP queries | Full language/function/modifier coverage, quoted names outside selectors, exact multiline printer parity, OTLP-sourced metadata, and full TSDB/client parity |
| Loki ingestion/queries | JSON/Snappy protobuf push and OTLP at `/otlp/v1/logs`; documented bounded LogQL queries, including instant-only `approx_topk` with exact ranking and instant/range `approx_count_distinct` with exact string counting under existing budgets; request-local `X-Loki-Query-Limits` maxQueryLength for finite reads; complete stream label, value and series discovery; index stats and volume (`aggregateBy=series` and bounded selector-only `aggregateBy=labels`, using actual line bytes rather than Loki chunk estimates); build info; WebSocket live tail; detected labels and fields; query-time patterns and `|>`/`!>` filters; MCP log reads | Full LogQL; probabilistic sketches/distributed merging/high-cardinality acceleration; other per-request policy fields; index-backed volume cost/estimate parity; full client parity |
| Tempo compatibility | Experimental bounded v1/v2 exact-ID export with V2 same-span filters and ancestor/descendant depths; separately opt-in retained-context search and discovery; documented typed predicates; TraceQL metrics with `topk`/`bottomk` and core `span:id` grouping (`rate`, `count`/`min`/`max`/`sum`/`avg_over_time`, `quantile_over_time`, `histogram_over_time`); echo and build info | Full TraceQL including structural/spanset operations, `compare()` and metric-expression arithmetic, exemplars, unlimited-series overrides, full indexed planning, discovery parity and deployed-client acceptance |
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

Selected Mimir, Loki and Tempo public service-integration scenarios also replay
against native Parquet and isolated local S3 test stores. They cover bounded
ingest/query/discovery behavior, not the complete upstream suites, real provider
durability/capacity or deployed-client acceptance. The
[source selections and remaining coverage gaps](../plans/protocol-breadth.md#upstream-service-integration-ports)
separate exact cases from explicit harness/behavior adaptations. In particular,
Loki's adapted long-range metric check supplies an explicit resolution; it is
not evidence for the upstream no-step expectation near the window's end.

Mimir's `TestFormatQueryAPIEndpoint` now replays its exact expression and
formatted result. Formatting covers Pulso's supported grammar over GET/form
POST, without executing queries or accessing storage. Output preserves operator
precedence, selector order, values and vector matching, with the same 16 KiB
input/output ceiling. It removes comments/redundant parentheses and stays
single-line; exact upstream multiline formatting remains a separate gap.
Upgrade query-serving applications; no native/storage/keyed-byte change is needed.

Additional ports cover all three Mimir UTF-8 name-validation rows over both
remote-write versions, Loki's one-minute request-length rejection, and fifteen
Tempo metric-only interval/aggregate/ranking subtests. Mimir ingestion already
retained UTF-8 names; the query parser now accepts their quoted selector syntax.
Loki checks effective finite bounds before storage and never raises server
budgets. It deliberately rejects malformed, duplicate and unsupported policies,
unlike upstream's permissive header fallback; live tail rejects this finite-window
header. Upgrade all query-serving applications before using quoted names or
relying on policy enforcement. Neither change affects native ABI, storage or
keyed bytes. The Tempo metric cases passed unchanged; they omit exemplar
requests/assertions and gRPC transport, with timed/random input frozen for
offline replay. Separate asymmetric controls check ranking identities and
span-weighted duration averages. These are selected public behaviors, not full
language, policy, exemplar or client acceptance.

Further ports cover Tempo's two empty-selection counts and three HTTP step
validation cases, plus Mimir's exact two-series fingerprint fixture. Ungrouped
trace counts/rates now return zeros for empty selections; grouped and numeric
observations remain empty. Range admission caps the requested interval quotient
at 10,000 before alignment, without increasing other ceilings. The small-step
case preserves HTTP 400, not Tempo's diagnostic wording. Mimir's fixture already
passed and its two series do not collide under Pulso's hash, so it is not proof
of Pulso hash-collision handling. Upgrade all query-serving app/native pairs for
consistent trace metrics; storage, keyed bytes and the native ABI are unchanged.

All fifteen rows of Tempo's public `TestDefaultQueryRangeStep` engine table also
replay through authenticated HTTP and native Parquet. Omitted range steps now
follow its window-dependent granularity, including 50 ms minimum steps below
one minute. Explicit steps, instant queries and interval admission are unchanged.
These engine-table ports are distinct from the upstream service-integration
selection; they do not establish metrics-expression, exemplar or client parity.

The continuation includes all seven selected Mimir ingester-query table rows,
native/custom histogram and exemplar distributor cases, all four Loki
categorized-label cases, detected-field windows and grouped stream counts,
basic Tempo unscoped/default-window search, and all 31 selected V2 exact-ID
filter-depth combinations. V2 validates the complete stored trace before
returning an explicit filtered PARTIAL subset; nonmatching unknown/conflicting
copies and resource errors still fail whole. A large filter URI returns 414 at
the default HTTP ceiling instead of upstream's 400; this is documented rejection,
not exact status parity. Filtering uses extra bounded query-task heap/CPU without
changing storage formats or the native tuple.
Independent public Tempo engine fixtures cover span-over-resource precedence;
omitted HTTP bounds search bounded retained context, not an ingester-only tier.
Tempo requires search capability **5**, tuple `{2,1,3,5,12}`. Remote-write
float/histogram timestamp collisions now normalize to the float, and empty-valued
labels are removed before identity/merging. Invalid label names/duplicates still
reject before removal; a nonempty reserved RW2 symbol fails the entire request
with zero written counts and no metadata publication. Drain affected collision
and empty-label keyed retries and replace all receiving application/native pairs
before resuming them; the native tuple does not fence older receivers. These scoped passes
do not close the remaining distributed, provider, language or client gaps.
