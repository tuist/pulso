# Tempo public HTTP integration cases

Pinned source: [grafana/tempo at 1a7373ee774b2ee5a40957c2310179b0ef7ff843](https://github.com/grafana/tempo/tree/1a7373ee774b2ee5a40957c2310179b0ef7ff843).
`cases.json` transcribes data and expectations from
[`integration/api/search/search_tags_test.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/api/search/search_tags_test.go).
This is a selected public API port, **not the complete Tempo integration suite**.

| Upstream test | Imported cases |
| --- | --- |
| `TestTagEndpoints` | All seven two-span input batches; v1 tag-name list and bare `service.name` values; six v2 value-table rows: no filtering, first/second resource attribute, too restrictive, both batches/name values, only resource attributes |
| `TestTagValuesWithSpecialCharacters` | All 19 enabled attribute/value pairs, in resource and span scopes; explicit encoded and unescaped slash requests |
| `TestSearchTagValuesV2_badRequest` | HTTP rejection of `app.user.id`; Pulso's diagnostic rather than Tempo's parser wording |
| [`TestSearchTraceQL`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/api/search/search_test.go) and [`SearchTraceQLAndAssertTrace{WithRange}`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/util/harness_api_reads.go) | Find the ingested trace with `{ .<original-key> = "<value>" }`, both without bounds and with explicit bounds. Random trace/attribute selection becomes the first batch's fixed `firstRes` and `firstSpan`; this preserves the found-trace assertion/query shape, not the random producer's full input. |
| [`TestTraceByIDV2Filtering`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/api/tracebyid/trace_by_id_filtering_test.go) | All 31 depth-table combinations, the exact six-span parent graph, two integer predicates, span-ID sets and COMPLETE/PARTIAL expectations; malformed query and active/ignored depth assertions. The oversized-URI response is an explicit adaptation below. |
| [`TestQueryRangeExemplars`, metric and HTTP-validation subtests](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/api/metrics/query_range_test.go#L306-L536) | All three aligned/unaligned/default-step interval rows (3/4/122 samples), all five instant/range aggregate comparisons, the instant-average comparison, all six topk/bottomk rows, both empty-result counts, and HTTP rejection of `0`, `0s` and `35ms` steps. Small-step diagnostic wording is explicitly adapted below. Exemplar production/requests and transport permutations are excluded. |
| [`TestQueryRangeTypeHandling` and `TestQueryRangeMaxSeriesDisabled`, metric-cardinality selections](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/integration/api/metrics/query_range_test.go#L636-L725) | Two spans with `foo` string `123` versus integer `123`, and the timed one-span producer, queried with `{} \| rate() by (span:id)`, five-minute bounds and `5s` step. COMPLETE and exact producer-cardinality assertions are retained. Exemplar requests, streaming and the unlimited-series configuration are not selected. |
| [`TestDefaultQueryRangeStep`, public engine table](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/pkg/traceql/engine_metrics_test.go#L35-L65) | All fifteen original window/expected-step pairs, retaining start zero and end equal to the window. Assert the expected spacing through HTTP/native-Parquet range samples rather than a private helper; the one-span producer is a separately identified Pulso adaptation. This is an engine-table port, not an additional upstream service-integration test. |

The separate malformed-name, exact-percent, nested `/values`, missing-key, route
suffix, authentication, search-window rejection and full-response public-engine
checks are Pulso regressions, not additional upstream integration cases.

## Replay and adaptations

```sh
mix test test/pulso_web/controllers/tempo_upstream_integration_test.exs
```

Sixty-five async ExUnit tests send real authenticated OTLP/HTTP JSON/protobuf requests to an
owned Bandit listener, persist native Parquet through Pulso's S3 adapter against
an owned loopback object store, restart manifest supervision, and query JSON and
protobuf responses. Default tests need no Go, Docker, remote services or network
downloads. All data and expectations are checked in. Additional sections below
describe the public engine step rows, arithmetic tables and separately labelled
Pulso controls.

The upstream harness converts Jaeger batches to OTLP/gRPC and queries both live
and flushed backend data. This port expresses the same resource/span attributes
as OTLP/HTTP JSON, fixes random IDs to distinct deterministic values, and uses
relative source times with the same one-microsecond durations. Pulso publishes
Parquet before acknowledgement, so both unbounded and paired ±2-hour windows run
after publication with fresh manifest state, not against a WAL. Source names,
values, span counts and selected filter expressions are unchanged; expected
dictionaries are sorted because upstream sorts before comparison. JSON empty
lists and protobuf default omission are compared through their protocol shapes.
Protobuf expectations use Pulso's existing encoder; this tests content
negotiation/transport consistency, not independent wire conformance (covered by
the separate public protobuf fixtures).

### Special-character test deadlines

The original Go test uses separate `t.Run` cases for each key, scope and
WAL/backend phase. The Elixir port retains all 19 enabled pairs together in each
fixture but separates resource/span scopes and retained/explicit windows into
four independently owned tests. Each ingests all 19 batches and restarts
manifest supervision before querying. JSON and protobuf assertions, the full
expected responses and the separate encoded/raw-slash test are unchanged.
There are still 152 retrievals, now 38 per test instead of 152 sharing one
60-second deadline. This adds three fixture setups; it is not a query-speed
improvement or a production timeout change.

Run this subset with `mix test test/pulso_web/controllers/tempo_upstream_integration_test.exs --only special_characters`.
An instrumented replay of the formerly failing seed617 passed all four tests
in 1.8–5.3 seconds each with `timeout=60000` and `trace=false`. That confirms
the ordinary deadline was retained, not relaxed by tracing/slow-test options.
It does not prove the cause of every historical timeout or provider capacity.

## Failures reproduced and fixed

- Bare v1 `service.name` returned 400. V1 now accepts bare original keys as
  unscoped targets; existing canonical scoped/intrinsic extensions retain
  precedence. This is not every legacy-v1 alias/collision behavior.
- Quoted attribute identifiers, such as `span."attr space"`, returned empty
  dictionaries instead of the original value. The HTTP boundary now decodes
  JSON-compatible double-quoted identifiers before applying the existing key
  and query budgets. Invalid quotes/escapes reject, not an empty success.
- An unescaped slash in a key returned 404 even though an encoded slash worked.
  The router now captures the full target path, removing only the terminal
  `/values`. URL decoding remains exactly once, preserving literal `%20` keys.

The first executable baseline was 2 passing / 4 failing tests before runtime
changes. The search continuation initially had 6 passing / 2 failing HTTP tests:
both the unscoped lookup and omitted time bounds returned 400. A separate public
unscoped-engine replay failed at the parser with `unsupported_trace_selector`.
Unscoped predicates now preserve span-key precedence, not resource/span OR;
HTTP omitted bounds and paired zeros scan all bounded retained context. Pulso
has no ingester-only tier, unlike upstream default request routing. Partial or
mixed-zero bounds deliberately still reject. The search grammar extension
requires search capability **5**, current tuple `{2,1,3,5,12}`, on every serving
app/native artifact. Segment format, keyed bytes and feature gates are unchanged.

## Independent unscoped search oracle

`produce_search.go.txt` executed inside the actual public **Tempo v2.9.0**
`tempodb/encoding/vparquet4` package, using its real Parquet block builder,
fetcher and `traceql.NewEngine().ExecuteSearch`. Source archive SHA-256:
`5b652be7d03b3329a5b986fd8678ea4a9c4849adf2966baf19d20eec01046a44`.
Its `batch`/`value` helpers come from the existing
[`produce_tempo_search.go.txt`](../produce_tempo_search.go.txt).

The separate synthetic six-trace/six-span input exercises resource-only,
span-only, differing same-key strings, a wrong-typed overriding span value,
identical same-key strings and missing attributes. Eight expressions run with
both explicit and paired-zero bounds, producing **16 full responses**. They
establish equality, regex, negative predicates, original int64, AND/OR, span
precedence and independent sampling of both requested owners. These are public
engine controls, not transplanted upstream integration-table rows. Provider
metrics are omitted; sample attributes are sorted by key, without dropping
same-key duplicates. These scenarios do not establish every physical-layout,
dedicated-column or complex-value behavior.

`search_expression_test.exs` pins all checksums and compares the full responses
with native execution, the independent Elixir evaluator, bounded old-adapter
fallback and positive-window MCP queries. The HTTP suite separately ingests the
frozen protobuf through the real endpoint/S3 path, restarts manifest supervision
and checks all 16 responses. Ordinary replay requires no Go or downloads.

| File | SHA-256 |
| --- | --- |
| `search-input.protobuf` | `dec8004d8460e2f487c50af8093acb71e2eebfcbad84a93b921ebe269d184ea0` |
| `search-oracle.json` | `3b655f0433d4a93634098e89286a9c5c6db55e8ae8a6bda10e1ceb7d5d98c602` |
| `produce_search.go.txt` | `fac8fd302ae2e1085c2c98ac2270a259f64a1dcfbe86818c213e1ee2faba64cd` |

Optional regeneration: copy the helper and producer into the pinned public
package, then run with Go 1.25.10 and vendored dependencies:

```sh
PULSO_ORACLE_DIR=/tmp/pulso-unscoped-oracle GOMAXPROCS=2 \
  go test -mod=vendor -p 2 ./tempodb/encoding/vparquet4 \
  -run '^TestPulsoUnscoped$' -count=1 -timeout=180s -v
```

This actual public-engine producer run was executed; the latest upstream
multi-service/Docker integration suite was inspected, not executed.

## V2 exact-ID filtering continuation

`filter-cases.json` transcribes all 31 rows from the pinned trace-by-ID table.
Input span IDs/parents, status-code attributes and expected retained ID sets are
unchanged; source times are relative, as in the other HTTP fixtures. Two added
upstream-derived wrappers cover the table and invalid/ignored depth parameters.
The baseline had **9/11 passing tests**: filtering returned unsupported-parameter
400, and the oversized-filter assertion returned 414 instead of upstream's 400.
No upstream Go cluster suite was executed to generate these expectations.

Filtering now runs after the existing complete validated/deduplicated assembly,
under the same query admission, scan, heap, deadline and result bounds. It uses
the bounded same-span expression evaluator, not general TraceQL. Independently
bounded ancestor and descendant walks start only at matches. No-match results
are empty PARTIAL traces; a whole-trace selection is COMPLETE. JSON/protobuf
checks cover all 31 rows; field numbers 3/4 and PARTIAL enum value 1 are also
checked independently of Pulso's encoder against `pkg/tempopb/tempo.proto`.
Filtered conversion expands bounded rows on BEAM; native unfiltered export is
unchanged. There is no additional GET, storage/keyed-byte or native-ABI change.

The oversized 150 KiB query deliberately expects **HTTP 414** at the default
HTTP URI ceiling, not upstream's 400. A separate parser check rejects that same
input. This is rejection parity, **not exact status-code parity**; the HTTP
ceiling was not relaxed. The two upstream recent/backend repetitions become one
durable S3 path, not simulated live-store coverage.

Additional Pulso filtering controls cover typed strings/regex/int64,
duration/status and boolean groups; no matches/blank filters; unchanged full
reads; V1/auth rejection; expression/depth/scan ceilings; cycle termination,
missing parents, maximum int64 depth and independently seeded graph walks; and
nonmatching legacy/conflicting spans failing the whole lookup. Two HTTP/native-S3
regressions additionally prove that malformed stored link IDs or event timestamps
fail with 500 before either a nonempty or empty selection, in JSON and protobuf.
Both failed with 200 before the fix. Export tests cover malformed owner/core/
event/link fields, exact ID widths/nonzero IDs, valid numeric boundaries and the
5,000-span limit. Validation-only checks cover every stored span before filtering;
only selected spans allocate export maps and encoded timestamps/IDs, without a
discarded conversion pass or second full-trace binary. Protobuf spans become
encoded binaries individually, avoiding retention of every span's whole nested
iodata tree without separately copying each small attribute/event. A bounded
5,000-rich-span check compares the public filtered export with exact native
protobuf under the default 16-million-word query heap.
These controls are not counted as upstream cases.

| File | SHA-256 |
| --- | --- |
| Upstream `integration/api/tracebyid/trace_by_id_filtering_test.go` | `5e3a1700e9bc3d580316651ed5ccc95b52e3f59ba6bfcf07ea6df874e86c0b56` |
| `filter-cases.json` | `f7dc3635a0b5b9e676fe02b21e9543eb728c519c24c9c3acf0e25fe72507a550` |

## Metric table continuation

The fifteen metric-only subtests preserve the upstream queries, range lengths,
steps, reductions, interval counts and ranking cardinalities. They passed on the
unchanged runtime (seed 67), alongside the existing seventeen wrappers and a
separately identified ranking control: **33 passed**. This is additional regression
coverage, not evidence of a newly fixed metric implementation bug.

The source's timed producer becomes exactly twenty half-second ticks, with its
three one-span batches per tick. Names, attribute keys, fixed values and
one-microsecond durations are unchanged; UUIDs and trace/span IDs become distinct
deterministic strings. The query clock is fixed to a recent minute boundary,
with source starts from clock minus 13 seconds to minus 3.5 seconds. This keeps
the complete input inside the source's aligned windows without sleeps, timers or
wall-clock-dependent empty results. Unaligned ends use exactly half a second
past that boundary. Input reaches the real OTLP/HTTP and durable Parquet/S3 path,
then manifest supervision restarts before querying. There is no simulated
ingester/backend double read or gRPC-streaming coverage.

The upstream helper normally also sends an unasserted exemplar-count request
parameter; the metric-only selection omits it. **This is not a port of exemplar
behavior**, `compare()`, arithmetic between metric expressions, or every subtest
in `TestQueryRangeExemplars`. Metric JSON content is checked over HTTP, not
protobuf or the source helper's second gRPC transport. In addition to the
upstream aggregate consistency assertions, independent expectations use the exact
sixty input spans and their durations, so mutually incorrect range/instant
responses cannot agree their way through the tests.

Two separately named Pulso controls exercise unequal series counts 1/3/5 through
both ranking directions, both attribute scopes and both metric endpoints; and
unequal counts/durations across three intervals. The latter checks exact
timestamps and span-weighted instant averages rather than an average of interval
averages. These are not additional upstream cases. No runtime, native ABI,
storage format, keyed bytes, budgets or upgrade requirements change for this
Tempo coverage increment.

| Upstream source | SHA-256 |
| --- | --- |
| `integration/api/metrics/query_range_test.go` | `7749294d4d682939a602e656cac41728f2b9c63344e913999d5c7d58170078f2` |
| `integration/util/harness_api_writes.go` | `53ea99c9dad9c6e2645e4004aeb587fe6d8bf6235ae7aab3bbb191e5124d66d3` |

## Span-ID metric grouping continuation

The two new source selections failed before runtime edits: the full HTTP replay
had **34/37 passing wrappers** (seed 71), with both upstream-derived requests
and the new Pulso control returning HTTP 400 for `by (span:id)`. No source
assertion was weakened. The numeric/string `foo` values, default helper's `x=y`,
`xx=""`, service/name, one-microsecond duration, five-minute query geometry,
five-second step and source cardinality assertions are preserved. Random IDs
become distinct fixed IDs. The five-second/half-second producer freezes to ten
ticks with starts from query time minus 7.5 to minus 3 seconds. This preserves
the one-span-per-tick helper and counts actual input, without sleeps or races
between upstream's final timer/ticker events. Both cases use persisted S3 after
manifest-supervision restart, not separate recent/backend phases.

The selection is explicitly **metric-cardinality only**: the unasserted
`exemplars=100` parameter and gRPC-streaming repetition are omitted. Pulso does
not reproduce the `max_series=0` override or unlimited-series semantics; its
10,000-series hard ceiling remains. A separate public query test checks exactly
10,000 distinct IDs succeeding and 10,001 failing whole with `trace_group_limit`,
while an ungrouped count still sees all 10,001. A local-fixture iteration first
caught out-of-window and uppercase core-ID mistakes in that boundary test; only
the fixture changed, not validation or limits.

Grouping now reads the already validated core span ID without attribute-owner
expansion and emits the canonical `span:id` label with a 16-character lowercase
hex string. The pinned public implementation confirms the protocol spelling in
[`enum_attributes.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/pkg/traceql/enum_attributes.go#L210-L211),
string typing in
[`block_traceql.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/tempodb/encoding/vparquet4/block_traceql.go#L3199),
and fixed-width formatting in
[`SpanIDToHexString`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/pkg/util/traceid.go#L54-L65).
No upstream runtime code is copied. The parser admits this intrinsic only for
grouping, and native compilation independently rejects it as a numeric value.

Additional controls distinguish intrinsic IDs from ordinary `span.id` and
`resource.id`, preserve typed string/int64 `foo` groupings, combine reused span
IDs across distinct traces, retain leading zeros and high unsigned bits, and
normalize uppercase ingress to lowercase output. Both instant and range metric
endpoints run these checks. Bare `id`, `trace:id` and numeric ID aggregations
still reject. Legacy core-only grouping remains supported without inventing
unknown original attributes. These controls are not additional upstream ports.

Trace metrics capability **2** is independently required at startup alongside
the unchanged trace tuple `{2,1,3,5,12}`. Upgrade all serving app/native pairs
before depending on the syntax. No encoder, storage format, keyed bytes,
migration or retry-drain requirement changes. Five grouping keys, series/point/
byte ceilings and shared query/scan budgets remain unchanged. These offline
ports do not establish full typed metrics, deployed clients or capacity parity.

Final cross-protocol replay passed **149 tests** (seed 76), including all 37
Tempo HTTP wrappers and the trace metrics unit/HTTP suites. Whole-repository
`mix precommit` passed **1807 tests**, with one existing optional promtool skip
and 37 optional exclusions (seed 75). Codec release tests passed all 133 cases;
strict Credo, release Clippy, formatters and whitespace checks passed. Direct
native validation also rejected numeric ID operations and six grouping keys.

| Additional inspected source | SHA-256 |
| --- | --- |
| `pkg/traceql/enum_attributes.go` | `7150265d29922a9209821369a9df1d9450eadfcfc66476eae82cf13125c84062` |
| `pkg/util/traceid.go` | `1158046cd8942d42e052c10c7562c40a593ff660623ad705e56f17cc1bf19787` |
| `tempodb/encoding/vparquet4/block_traceql.go` | `87d66734e1e26a66fa467f461d217cff84bf92ab31af19f21ec1013e0d5729a4` |

## Empty selections and interval validation continuation

Five more rows from the pinned `TestQueryRangeExemplars` are ported: both
empty-result count queries (`{status=error}` and the absent `span.randomattr`),
and HTTP steps `0`, `0s` and `35ms`. The same frozen sixty-span fixture remains
non-error and lacks that attribute. Bounds retain the helper's five minutes
before / one minute after the anchor, with its `5s` step for empty selections.
Both empty cases preserve the at-least-one-series and zero-exemplar assertions.
The unsupported exemplar-count request and gRPC transport remain excluded;
zero exemplars here do not prove exemplar production or selection support.

Both empty-result assertions failed before the native fix (seed 78), returning
`series: []`; the initial baseline used the receiver-default step, later made
explicit as the upstream helper's `5s`. A public-query control also failed because
it expected exact zero samples. Ungrouped count/rate finalization now returns its
zero identity only after a successful scan. It does not invent labels for grouped
queries or numeric/histogram/quantile observations. Separate public-query
controls check every aligned timestamp/value for nonmatching selectors, empty
tenants and out-of-window spans, plus instant zeros and unchanged empty
grouped/numeric results. Legacy/corrupt inputs and exhausted budgets still fail,
not zeros.

The three HTTP validation rows initially passed **2/3** (seed 80): `35ms` over
six minutes wrongly returned HTTP 200. Admission now checks the original range's
integer interval quotient against the pinned default **10,000**, before boundary
alignment. The native aligned-window ceiling remains 11,000, not an operator
override. A public-query boundary control accepts exactly 10,000 requested
intervals and a sub-step remainder (10,001/10,002 aligned output samples), rejects
10,001 requested intervals, and leaves instant requests at one interval.
**Diagnostic adaptation:** the small-step port preserves HTTP 400 rather than
upstream's exact `step of 35ms is too small, minimum step for given range is 36ms`
text; Pulso retains its generic invalid-parameter diagnostic. Zero-step source
cases only assert rejection. No source safety ceiling is raised.

Runtime changes are independently written, not copied upstream implementations.
Upgrade all serving app/native pairs for consistent query semantics; no storage,
keyed bytes, native ABI or migration changes are introduced by this increment.
The earlier span-ID capability remains **2**. Pinned runtime inspection confirms
the ungrouped placeholder series and pre-alignment interval validation:

| Additional inspected source | SHA-256 |
| --- | --- |
| [`pkg/traceql/engine_metrics.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/pkg/traceql/engine_metrics.go#L768-L774) (`UngroupedAggregator.Series` at lines 994–1005) | `cd05466f788a7a0705578fd79aa47d38a1408ca9609cdbd9b9e6fa8493058e01` |
| [`modules/frontend/metrics_query_validation.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/modules/frontend/metrics_query_validation.go#L16-L34) | `dde4a210f65c885d59844b089a04e61cbb43e89eca0955acda6aaaca303966ad` |
| [`modules/frontend/config.go`](https://github.com/grafana/tempo/blob/1a7373ee774b2ee5a40957c2310179b0ef7ff843/modules/frontend/config.go#L152) | `5fa9a3e016101b5f3ed6e79a26f9eb91a3373f19131a194b9f1ffb4fd9b0a10c` |

The final three-service plus trace metrics unit/HTTP replay passed **157 tests**
(seed 83), including all **42 Tempo wrappers**. `mix precommit` passed **1815**
with one existing optional promtool skip and 37 optional exclusions (seed 82).
Codec release tests passed all **133** cases; strict Credo, release Clippy,
formatters and whitespace checks passed. These remain offline HTTP/native-S3
regressions, not a run of upstream Tempo or deployed-client acceptance.

## Default metric-step continuation

All fifteen `TestDefaultQueryRangeStep` rows preserve the public source's
start-zero/window-end geometry and expected duration exactly: 59 seconds through
30 days. Expected steps are transcribed into milliseconds for the public JSON
timestamps, not computed by Pulso. Each case ingests one rich span at one second
using the existing fixture helper, restarts manifest supervision and queries
`{} | count_over_time()` without `step`. It checks the first sample timestamp,
every adjacent sample spacing and total count one. The original unit test has
no producer; this persisted span and HTTP transport are explicit adaptations,
not copied upstream input or an execution of Tempo itself.

The unchanged-runtime baseline failed **7/15 upstream rows** (seed 105): 59s,
6h, 24h minus 1s, 24h, 2d, 7d and 14d. The minimum-step Pulso control also
failed; the explicit-step control already passed. The independently written
default policy now rounds the window/240 baseline at 50 ms below one minute,
or at the largest applicable hour/5m/1m/15s/5s/1s granularity otherwise, with a
one-second minimum in that second branch. No Tempo runtime code is copied.

Two local controls cover a one-nanosecond window, the 50 ms floor, subminute
rounding, both sides of the minute boundary and a shifted nanosecond epoch;
explicit `700ms` spacing, instant ignored-step/count behavior, and unchanged
HTTP rejection above 10,000 requested intervals. These are not additional
upstream rows. The complete Tempo service plus metrics unit/HTTP suites passed
**74 tests**, seed 106, including all **59 HTTP wrappers**.

The broader Loki HTTP/WebSocket/module/LogQL, Mimir service and Tempo replay
passed **510 tests**, seed 107; the separate batch suite passed **9**, seed 108.
All seventeen additions passed eleven consecutive runs (**187 executions**,
fixed seed 109). Compile with warnings as errors, strict Credo across 437 source
files, scoped formatting and whitespace checks passed. No native source changed;
native unit tests were not separately rerun for this application-only policy.

Upgrade every query-serving application for consistent omitted-step results.
Trace metrics capability **2**, storage formats, keyed bytes and all existing
query/result bounds remain unchanged; no native ABI change, migration or retry
drain is introduced. These offline checks do not establish metrics exemplars,
expression arithmetic, streaming, deployed clients or provider/capacity parity.

| Additional pinned source | SHA-256 |
| --- | --- |
| `pkg/traceql/engine_metrics_test.go` | `6568615e285b21b5094c218444d1a93aa8996d2fda82b5eb65d44f9e1637fe49` |
| Selected lines 35–65 (`TestDefaultQueryRangeStep`) | `b4c1bf93cbfc446f35d2a87c9f4e227524adb9bd7227fd226533f3490b1c19bf` |
| Inspected `pkg/traceql/engine_metrics.go` (policy at lines 37–81) | `cd05466f788a7a0705578fd79aa47d38a1408ca9609cdbd9b9e6fa8493058e01` |

## Metric arithmetic and scalar-filter continuation

`metrics-math.json` freezes the complete selected test tables at the same
revision. `Pulso.Traces.MetricExpressionConformanceTest` replays all rows;
`PulsoWeb.TempoUpstreamIntegrationTest` additionally sends all 87 scalar queries
through authenticated HTTP and native Parquet against isolated loopback S3.
Ordinary ExUnit replay needs neither Go nor a downloaded source tree.

| Source | Selection | Replay |
| --- | --- | --- |
| `pkg/traceql/engine_metrics_test.go:1044–1707`, `TestScalarMath` | All 87 cases, original 11 spans and exact nanosecond durations; all 522 dense expected values | Memory/native and HTTP/Parquet; NaN slots omitted at response serialization; float/int constant division, precedence, operand order, filters and infinity retained |
| `tempodb/tempodb_metrics_test.go:1289–2644`, every `math_` row | All 23 cases, 100 original single-span traces, complete L1/L2/L3 expectations | Native batch/final evaluator, asserting L3 samples and labels; not a literal HTTP port |
| `pkg/traceql/parse_test.go:1966–2179`, `TestMetricsMathExpression{Errors}` | All 18 positive and 6 negative rows | Public metrics parser acceptance/rejection; stored expected Go ASTs are provenance, not asserted Elixir AST shapes |

The storage tests retain start **1 ns**, end **50 s**, step **15 s** and spans
51–100. The final bucket timestamp of **60 s** does not change the scan end.
Two identical input sets simulate the original engine's two additive L1
observations, preserving doubled counts/rates and unchanged maxima before
arithmetic. HTTP aligns the end to 60 s and would admit additional spans, so
these 23 storage oracles intentionally bypass HTTP window alignment. L1/L2
arrays are preserved for provenance; replay asserts the original final L3.
Random IDs become deterministic while the original fixed trace-30 ID remains.
Incidental producer fields unused by these queries are omitted.

The AGPL-licensed `metrics-math-producer.go.txt` reproduces the complete JSON
using only Go's standard library. It executes the original scalar **test-table
transform closures**, and evaluates original storage expected-value expressions
with data-only stand-ins. This is not execution of Tempo's engine. Independent
source-AST checks verified all scalar values (including signed zero/nonfinite
classes), 376 storage samples across all phases, 163 selected-range hashes and
the three complete source-file hashes. Both generated files retain attribution;
no Tempo runtime implementation is copied.

| Artifact or complete source | SHA-256 |
| --- | --- |
| `metrics-math.json` | `06f0fe58b96d42e15d8f6c8de7a4a30e5343a016c0cacda1d983e47d917d5738` |
| `metrics-math-producer.go.txt` | `d0b67faf3bc9cdda5e1ad978c164601abace0789c56c3ed816bd4edb80246ffc` |
| `pkg/traceql/engine_metrics_test.go` | `6568615e285b21b5094c218444d1a93aa8996d2fda82b5eb65d44f9e1637fe49` |
| `tempodb/tempodb_metrics_test.go` | `e4315a0ebb5d7ef3fc03097718c8bca7a8a826a4436964b85e4ca77397bf8f1f` |
| `pkg/traceql/parse_test.go` | `3eef0b3f465ed516623b34568ddd11023739d27612df74c2e26b75916f8dd1af` |

Pulso controls cover instant span-weighted arithmetic, two branches sharing
exactly one GET per Parquet object, fail-whole HTTP scan quotas, shared native
series ceilings (5000 groups across two leaves succeed, 5001 fail), typed label
identity/order/missing groups and invalid-query rejection before storage.
Integer literal/folding overflow deliberately rejects rather than using Go's
machine-int wraparound. JSON infinities use Pulso's established string encoding,
not a claim about Tempo's HTTP nonfinite encoding. Native trace metrics
capability **3** is required; trace storage/search tuples and keyed bytes do not
change. Exemplars, full TraceQL, streaming, provider capacity and deployed-client
acceptance remain separate gaps.

## Shared regex literal controls

The local `:regex_literals_batch` wrapper replays eight quote/octal patterns
over eight OTLP-ingested one-span traces through native Parquet/S3, after
restarting local manifest authority. Independent whole-label answers come from
[`regex/go-controls.json`](../../regex/go-controls.json), not Pulso or inferred
Tempo results. Both resource/span retained search and JSON/protobuf filtered
discovery run through HTTP; authenticated HTTP MCP exercises window-local
`search_traces` and both resource/span raw `query_traces` regex filters.
Unfinished literal quotes remain fully anchored. Shared fixture provenance
preserves the complete Go parser-table patterns and independently executed
matching answers. These local controls use the Tempo integration harness, but
are not additional upstream integration rows or full client/provider/capacity
acceptance. Existing trace tuples, storage/keyed bytes and resource ceilings
are unchanged; upgrade every query-serving app/native pair for these forms.

The local `:regex_admission_batch` replays all 53 shared Go admission patterns
over eight OTLP-ingested traces through resource/span retained search and
native Parquet/S3. Independent whole-label expectations distinguish counted
repetition bounds from literal braces; invalid patterns reject before storage.
[`regex/README.md`](../../regex/README.md) preserves the eight original Go
parser-table patterns, local controls, source hashes and matching provenance.
These are not additional Tempo integration rows or a full regex parity claim.

The same wrapper also replays 32 local literal-class cases from
[`regex/class-controls.json`](../../regex/class-controls.json), preserving
`&&`/`~~` members with negation, folding, POSIX classes and adjacent ranges.
All three protocol wrappers reject valid classes before implementation
(seed 669); the affected 370-test suites pass seed 670. Trace tuples and all
resource ceilings stay unchanged; complete class grammar remains open.

The same wrapper also replays 72 local literal-bracket/hyphen-range cases from
[`regex/range-controls.json`](../../regex/range-controls.json). Independent Go
answers preserve scalar ranges, literal `[`/POSIX-token boundaries and
whole-value anchoring, with no storage requests for invalid ranges, escapes,
groups, flags or counts. All three HTTP wrappers rejected valid `[[]` before
this fix (seed 673). These are local controls, not extra upstream integration
rows or full regex/client/capacity acceptance; trace tuples and limits are unchanged.

The same wrapper replays 44 raw-header and 58 inline-flag cases from
[`regex/name-controls.json`](../../regex/name-controls.json) and
[`regex/flag-controls.json`](../../regex/flag-controls.json), for both resource
and span predicates. Invalid headers reject before storage; empty/repeated
flags preserve whole-value matching and scope. The shared matcher also checks
actual Go anchored capture maps and `U`/lazy greediness. All three wrappers
failed the respective baselines (seeds 680/682). These are independent Go
controls, not extra Tempo integration exports; see the [regex provenance](../../regex/README.md).

## Explicit remaining gaps

Not imported: intrinsic-name filters, partial/invalid-query fallback, unscoped
filter fallback, OR discovery, and exact dedicated-column name-hint omission
from the rest of `TestTagEndpoints`. Pulso continues to reject unsupported
filters rather than ignore them. The upstream disabled double-slash case stays
excluded. Backtick identifiers and every Go string-escape spelling are not
established by these double-quoted fixtures.

Also not ported here: gRPC streaming queries, broader trace-by-ID filters/diffs, partial
search time bounds, structural/spanset queries, metrics
exemplars and `compare()`, tenant federation, overrides, receiver
variants, fault/restart/limit matrices, metrics generation, and Tempo-specific
Kafka/WAL/livestore/block orchestration and cloud-provider tests. Existing Pulso
trace/export/search/metric tests remain separate evidence, not transplanted
upstream acceptance. Loopback S3 is not provider/capacity/deployed-client proof.

## License

The copied tables and derived ExUnit test retain Tempo's **AGPL-3.0-only**
license; `LICENSE` is copied verbatim from that source revision. They are not
relicensed under Pulso's MIT license. No Tempo runtime implementation is copied.
