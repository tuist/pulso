# Loki public integration cases

`test/pulso_web/controllers/loki_upstream_integration_test.exs` adapts actual
Grafana Loki integration cases from revision
`5aa4b9212d64d723f63b2770452054aa23df114d` (the requested HEAD pin), inspected in
an external checkout. The inputs and expected labels, lines and numeric results
are transcribed from these tests, not generated from Pulso's answers.

The adapted test file retains upstream's **AGPL-3.0-only** license and attribution
to Grafana Labs contributors. The unmodified upstream license is in
[`LICENSE`](LICENSE). No upstream runtime implementation is vendored or imported.
The test harness and production fixes use Pulso's own HTTP/storage interfaces.

## Source-to-test mapping

There are **28 integration-derived ExUnit checks**: 27 preserve the selected public
request/response semantics, and one metric-range check has an explicit resolution
adaptation described below. They are not 28 complete upstream integration suites.
Seven further checks port **23 executable upstream LogQL counter-rate scenarios**
(21 successful/empty results and two invalid queries), not seven additional
microservice suites. Four more checks port **89 unwrap scenarios** from the same
engine script: 17 first/last evaluations and 24 grouped-average evaluations each
over logfmt, JSON and structured metadata, including post-unwrap filters. Two
further checks port **11 extraction-error scenarios** (nine successful/empty
results and two rejected queries). Three checks port **38 scalar/vector engine
scenarios**: 28 successful scalar expressions, five invalid scalar forms and
five vector-literal scenarios. Three more wrappers port **28 label-filter unit
cases** (all binary-filter, byte-unit and error-filter table rows), not additional
service suites. Four runtime-quantity wrappers port **109 cases**: six Loki
extractor rows, 43 Go duration successes, 18 UTF-8 duration errors, 37 go-humanize
byte successes and five byte errors. Three query-quantity wrappers port **41
source inputs**: 11 Loki duration tokens, 13 Prometheus range-duration successes,
nine range-duration errors and eight byte-token examples, with the zero-window
and `0B` adaptations below. One additional extractor-error wrapper preserves
Loki's invalid-duration input, grouping and exact diagnostic, with the explicit
error-selection adaptation below. Thirty-one HTTP/S3 controls in the
same file are Pulso-authored; additional pipeline/evaluator controls live in
the existing LogQL test files. The string-literal continuation adds one wrapper
for three Loki lexer query rows, two HTTP wrappers for a 104-row frozen corpus,
and parser replay. These are scanner/decoder checks, not more service suites.
The IP continuation adds two wrappers for 15 original upstream unit-table rows
and one separate local HTTP pipeline/budget control.
The line-filter OR continuation adds two wrappers for 27 original matcher-table
queries (121 line decisions) and 29 engine evaluations, plus one local control.
The label-filter conjunction continuation adds one wrapper for 36 original
engine evaluations, plus one local truth-table/error/admission control.
The logfmt continuation adds two wrappers for 60 original unit/engine
evaluations, plus one local HTTP Unicode/error/budget control and a separate
native/reference regression. The source-expression continuation adds one wrapper
for 15 frozen unit-table rows and one local lookup/admission/budget control.
These are not additional service suites.

| Upstream source | Selected cases | Checks |
| --- | --- | ---: |
| [`integration/loki_micro_services_test.go`, `TestMicroServicesIngestQuery`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L110-L209) | `query`, `label-names`, `label-values`, `series`, `series error`, `stats error`, `per-request-limits`, `tail` | 8 |
| [`integration/loki_micro_services_test.go`, `TestProbabilisticQuery`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L779-L886) | Exact four `lineA`–`lineD` writes under `job=one/two`; instant `approx_topk(1, count_over_time({job=~".+"}[1h]))` returns only `job=one`, value `3`. Pulso ranks exactly under existing bounds, not with upstream sketches. | 1 |
| [`integration/loki_micro_services_test.go`, `TestApproxCountDistinctQuery`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L888-L992) | Four exact `mac` logfmt writes with overlapping `shared` values under `job=devices,version=1,device=a/b`; `query` groups by version and `query-ungrouped` uses `by ()`, both returning `3`. Multi-shard statistics assertions are excluded. | 2 |
| [`integration/per_request_limits_test.go`, `TestPerRequestLimits`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/per_request_limits_test.go#L21-L64) | One `lineA` write; limited range and default-time label requests reject, then a header-free range request returns `lineA`, in one sequential check | 1 |
| [`integration/labelaccess_test.go`, `TestLabelAccessTestCases/disabled`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L78-L112) and its [ingest](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L297-L305) / [assertion helpers](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L385-L470) | `logs-query`, `label-values-query`, `series-query`, `metrics-query`, `metrics-range-query` | 5 |
| [`integration/loki_micro_services_test.go`, `TestCategorizedLabels`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L1070-L1291) | `no header - no parser`, `no header - with parser`, `with header - no parser`, `with header - with parser` | 4 |
| [`integration/loki_sample_query_dataobj_test.go`, `TestSampleQueryStreamDataObjEquivalence`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_dataobj_test.go#L149-L213) and [public result checks](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_dataobj_test.go#L276-L327) | Counts `a=3,b=2`, unwrapped sums `a=60,b=3`, line-filter count `a=3` | 3 |
| [`integration/explore_logs_test.go`, `Test_ExploreLogsApis/detected_fields`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/explore_logs_test.go#L117-L223) | All six log lines; `non-split queries` (`now-1m` to `now+1m`, foo string/1, color string/3); `split queries` (`now-24h` to `now+1m`, foo string/1, color string/4) | 3 |
| [`integration/loki_sample_query_ordering_test.go`, `TestSampleQueryStreamOrderingEquivalence`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_ordering_test.go#L23-L105) | Public `sum by (cluster, job, app) (count_over_time({cluster="prod"}[1h]))` result with all three labels and counts `a=3,b=2,c=1`, counted once | 1 |
| [`pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L588-L739) | All 23 `rate_counter` evaluations over six fixtures: reset compensation, overlapping/non-overlapping windows, offsets, single/absent observations, zero-point/spacing caps and their order, structured metadata and invalid unwrap/grouping | 7 |
| Same engine script, [first/last](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L99-L147), [logfmt averages](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L1044-L1115), [JSON averages](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L1118-L1189), [metadata averages](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L1192-L1263) | All 89 successful evaluations in these selections, preserving noise, empty streams, group/drop/binary combinations, offsets, sparse windows, NaN endpoints and post-unwrap label filters; the two first/last invalid-query rows are outside this selection | 4 |
| Same engine script, [failed conversions](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L1747-L1775) and [metadata rate fixture](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L911-L912) / [queries](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest#L938-L943) | Eight absence/min/max/error-label evaluations and three rate evaluations; exact error labels and zero-valued failed conversions, with two query failures | 2 |
| [`binary_operations.logqltest:1–87`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/binary_operations.logqltest#L1-L87) | Complete scalar arithmetic, comparisons and precedence sections: 28 scalar results, three invalid integer-base spellings, invalid scalar set operation and invalid vector negation | 2 |
| [`functions.logqltest:8–23`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/functions.logqltest#L8-L23) | Both `vector(2)` and `vector(3.14)` instant/range results and invalid nested `vector` argument | 1 |
| [`pkg/logql/log/label_filter_test.go:16–289`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/label_filter_test.go#L16-L289) | All 13 `TestBinary_Filter`, 11 `TestBytes_Filter` and four `TestErrorFiltering` rows, with typed comparisons, compound predicates, three conversion-error cases/keep decisions and negative error-label filters. Diagnostic details are explicitly adapted. | 3 |

The integration-derived checks cover nine upstream fixtures: the four `lineA`–`lineD` pushes, their separate
`job=one/two` probabilistic-query fixture, the four overlapping `mac` pushes, the standalone
limits case's single `lineA` push, the four `line1`–`line4` pushes with
missing/classified environment labels, the four
structured-metadata/parser pushes, the five `msg=... n=...` pushes, the six
`foo=bar color=...` pushes, and the six `a1`–`c1` pushes across three streams. The
data-object check asserts public results, **not** equivalence between Loki's two
physical engines. Repeated engine-specific count/sum assertions are not counted
twice.

## Harness adaptations

- Every check owns its tenant, dependency context, Bandit API listener, local
  S3-compatible `CompactionStore`, manifest supervision and authentication.
  Tests are `async: true`; no application/OS environment or shared state is
  mutated. The production endpoint is reached using real Req HTTP requests.
- Each upstream `PushLogLine` becomes the same JSON stream/line/metadata push.
  Its client-added default `job="varlog"` is kept in the label-access case.
  Empty structured metadata is an empty JSON object, as in the upstream client.
- One acknowledged push writes one native Parquet segment via the real Rust S3
  client, encoder and manifest CAS. Setup checks the expected one/four/five/six segment
  objects and their `PAR1` framing. Successful reads assert actual segment GETs;
  invalid series/stats requests assert that no segment is downloaded.
- Loki's cluster startup, TSDB/index-gateway/Kafka wiring, WAL, ingester restart,
  flush and polling are replaced by Pulso's synchronous durable append boundary.
  There is no ingester-only copy or second storage engine to flush or compare.
- Original relative timestamps are retained. The anchor is ten seconds before
  the wall clock so the label-access fixture's `now+3s` record is already visible
  to the upstream-style label APIs that omit time bounds. Range queries preserve
  the upstream client's seven-day start and `now+1s` end; instant queries preserve
  `now+1s`. The label-access query clock advances four seconds as upstream does.
- Lines/streams/dictionaries are compared order-independently wherever upstream
  uses `ElementsMatch`; metric sample values retain Loki text formatting.
- Only the **disabled** label-access case is selected. It still sends the
  upstream `X-Prom-Label-Policy` header containing `{env="dev"}` and expects all
  four streams. This is not evidence of enabled LBAC enforcement in Pulso.
- All four categorized-label cases retain the exact upstream lines, labels,
  metadata and parser. Without the header, merged labels remain on each stream.
  With `X-Loki-Response-Encoding-Flags: categorize-labels`, the single stream
  has only `job="fake"`; each value carries its own `structuredMetadata` and
  `parsed` label maps, including an empty object for the entry without either.
  The third value is a JSON object, not an encoded JSON string: the upstream
  integration client converts it to a string internally, whereas its public
  [marshal test](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/util/marshal/marshal_test.go#L280-L314)
  spells out the wire object. No client conversion is copied into the HTTP API.

### Categorized-label failure replay and local controls

Before the response-encoding change, selecting `--name-pattern 'with header'`
ran both new upstream cases and both failed at the encoding-flags assertion:
`data.encodingFlags` was `nil`, expected `["categorize-labels"]` (seed 365094).
After the fix both complete expected-stream/per-entry assertions pass
(seed 674624). At that point the integration file contained 28 checks: 22 selected
upstream cases and six separately identified local controls.

The local HTTP/S3 controls cover equal-valued parser/metadata writes, stream
suffix collisions, repeated parsing, rename/template/drop/keep provenance,
unchanged stored objects, identical lines/timestamps and exact duplicate entries,
forward/backward global limits with filtering and line rewriting, GET/form POST
for both query endpoints, exact/case-sensitive/duplicate/unknown encoding flags,
unchanged metric samples and fail-whole scan budgets. Unknown flags are echoed
without invented behavior, following the pinned
[flag parser](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/util/httpreq/encoding_flags.go#L98-L109).
The two additional unit controls exercise all five parsers with promoted labels,
equal values, self/missing renames and the unlimited materializing evaluator path.

Category provenance is initialized before stream/attribute merging and updated
at each pipeline mutation, never reconstructed by comparing final values or by
matching lines. It is allocated only for the negotiated log-query encoding.
Global selection and tie-breaking precede category grouping and remain unchanged;
duplicate entries are not collapsed. The extra per-label origin map adds query
work and heap use, and per-entry category objects can enlarge responses. Existing
scan/deadline/task-heap bounds remain authoritative, not a new category-specific
byte ceiling or a capacity benchmark. No persistent registry, storage-format,
native ABI or keyed encoder-byte change is introduced. No-header HTTP and MCP
representations remain unchanged; metric flags only affect the response envelope.

### Explore and stream-ordering coverage-only replay

Before changing any runtime code, the four newly selected public scenarios all
passed (seed 824886, four passed and 23 excluded). These are coverage additions,
not evidence of a new protocol bug or justification for a runtime change.

The Explore fixture preserves all six exact lines and timestamps (`now-45m`,
`now-5s`, and `now`, two at each timestamp) under `job="fake"`. Its two explicit
windows and expected field types/cardinalities are unchanged. Like the upstream
Go response struct, the assertions inspect exactly `label`, `type` and
`cardinality`; they do not imply parity for parser hints or other response
members. The full log assertion keeps the three separate red entries.
The source's "split" and "non-split" names identify its request windows;
Pulso does not reproduce Loki query-frontend splitting or cache behavior.

The ordering fixture preserves `job="varlog",cluster="prod",app="a"/"b"/"c"`,
the `a1,a2,a3` / `b1,b2` / `c1` lines, their per-stream `now-(30-i)m` timestamps,
the exact grouping query and its complete three-series count result. It does
**not** duplicate data into both a store and a retained ingester or compare
timestamp-first/stream-first execution and their statistics. Those upstream
permutations are omitted, not simulated by double ingest or counted twice.
Every acknowledged Pulso write already crosses the durable S3/manifest boundary.

One separately marked Pulso HTTP/S3 control checks the plausible window/sample
regressions exposed by the Explore fixture: both inclusive equal-time endpoint
groups, exclusion of all lines between those groups, newest-two sampling across
all six segments, and GET/form-POST agreement. It also passed on the unchanged
runtime. No approximation, resource-ceiling change, storage/keyed-byte/native
ABI change, asynchronous acknowledgement or upgrade requirement is introduced
by this coverage-only extension.

### Per-request query-length failure replay and local controls

The newly selected upstream `per-request-limits` case preserves all four
`lineA`–`lineD` writes, the exact `X-Loki-Query-Limits` header with
`{"maxQueryLength":"1m"}`, the default-time label-name request and the expected
error substring `the query time range exceeds the limit (query length`.
The upstream client [omits label-name query parameters](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/client/client.go#L680-L692);
the port does too. It additionally proves HTTP 400 and no Parquet download.
Before the runtime change this exact request failed the new assertion by
returning HTTP 200 with `{"status":"success","data":["job"]}` (seed 799307).
After the change the integration and controller files passed all 71 checks
(seed 955696). At that increment the integration file contained 36 checks: 23 selected
upstream cases and 13 separately identified local controls.
The broader Loki controller/Loki module/LogQL replay passed all 354 checks
(seed 695902); compile-with-warnings-as-errors, targeted formatter verification
and whitespace checks also passed.

Pinned source inspection confirms the [header name, policy fields and first-header
selection](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/util/querylimits/propagation.go#L17-L77),
the [tightening-only/zero behavior](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/util/querylimits/limiter.go#L28-L38),
and [malformed-header fallback](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/util/querylimits/middleware.go#L25-L36).
The header uses Prometheus [`model.Duration`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/vendor/github.com/prometheus/common/model/time.go#L208-L280):
nonnegative integer `y`, `w`, `d`, `h`, `m`, `s`, `ms` components, in strictly
descending unrepeated order, up to signed-int64 nanoseconds; bare `"0"` is valid.
It is not Go's fractional/ns/us duration grammar or a bare seconds number.
The pinned [configuration-limit guide](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/docs/sources/configure/loki-limits.md#L49-L63)
describes maximum query time range, not additional header parsing guarantees.

This initial increment implemented only `maxQueryLength`, as a request-local ceiling
on the parsed/defaulted `end - start`, without narrowing or truncating the
request. Exact equality passes; a single excess nanosecond rejects before any
storage access. This covers range/finite instant queries, labels, label values,
series, index stats, both volume aggregations and volume endpoints, detected
labels/fields/field-values and patterns, with GET/form POST agreement. Default
windows are checked too. Instant metric evaluation has a zero-length evaluation
window; range selectors and offsets are not capped by this field (the separate
`maxQueryInterval` policy was unsupported). An instant log read with a positive
limit requires an explicit finite `start`. Live tail rejects this header because
its subscription has no finite end. No-header behavior is unchanged.

Deliberate stricter boundaries, **not exact upstream malformed-policy parity**:
one header of at most 1024 bytes, a JSON object with exactly the case-sensitive
`maxQueryLength` field (or `{}`), and a duration string of at most 128 bytes.
Malformed/empty headers, null/nonobject/numeric policies, duplicate headers or
keys (including escaped-key duplicates), negative/fractional/overflowing values,
unknown fields and all other Loki policy fields return HTTP 400
`invalid_parameter`, rather than silently dropping the policy. Loki instead
uses its first header, Go struct decoding (including last duplicate scalar-key
selection), ignores unknown fields, and logs/ignores malformed policies. At this
increment Pulso did **not** implement `maxQueryInterval`, `maxQueryLookback`,
`maxEntriesLimitPerQuery`, `maxQueryTime`, `requiredLabels`,
`minimumLabelsNumber`, `maxQueryBytesRead`, or `tsdbShardingStrategy`.
Missing header, `{}` and valid zero durations add no restriction; they never
disable existing admission, deadline, scan, result, work or heap budgets.
The [query-policy continuation](#query-policy-continuation) below supersedes the
unsupported interval, entry and selector-policy items.

Seven new Pulso-authored controls independently cover the exact-one-minute versus
one-minute-plus-one-nanosecond boundary on 15 read variants, provider-request
counters unchanged on rejection, default windows, malformed/duplicate/unsupported
policies, zero/compound/int64-boundary durations, unchanged stored objects,
instant versus unbounded reads, request-local/authenticated two-tenant isolation
and server scan-budget precedence even with zero or large client limits.
They do not count as additional upstream ports. No global knob, persistent state,
storage/keyed-byte/native-ABI change or migration is introduced. Every serving
instance must carry the change before clients can rely on this optional header;
it is not an operator-enforced security quota because a client can omit it.

### Tail and standalone per-request limits coverage-only replay

The existing integration file passed all 36 checks before this extension (seed
780689). Both new upstream-derived checks passed on their first execution,
before adding any local control and without changing runtime code (seed 782486,
two passed and 36 excluded). They add four public request/response scenarios:
one tail result and the standalone limit test's two rejections plus header-free
success. The standalone test deliberately retains that sequence in one ExUnit
check, rather than counting its three requests as separate upstream tests.

The tail port reuses the existing four acknowledged `lineA`–`lineD` writes,
including the two identical `now-45m` timestamps and the two `now` timestamps,
the exact `{job="fake"}` query and the complete order-independent four-line
expectation. The pinned [integration tail helper](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/client/client.go#L811-L844)
uses the wall clock at connection time, **not** `Client.Now`, for its `now-1h`
start. The port does too, with `limit=100` and no `delay_for` parameter for zero
delay, following the [logcli request builder](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logcli/client/client.go#L177-L203).
The fixture anchor remains ten seconds behind wall time as documented above;
no sleep advances time. The receive loop blocks on actual WebSocket frames with
a ten-second read timeout, preserving multiplicity across frames, instead of
upstream's goroutine, mutex and `Eventually` polling. Socket closure is guaranteed
even when an assertion fails. JSON frame decoding replaces the Go tail-response
client's decoding; public `streams`/`values` are not rewritten.

Unlike the unauthenticated upstream cluster client, the socket sends the owned
tenant and Bearer token through the production endpoint's authentication gate.
Historical entries are downloaded from native Parquet/S3, not retained in a
simulated ingester. This proves the selected WebSocket result on Pulso's durable
storage path, not Loki microservice topology, query-frontend/ingester fan-out,
cross-instance continuity, drop/backlog/delay behavior or completeness. The
upstream `tail` case contains no post-connect writes; its live behavior must not
be inferred from the four historical lines alone.

The standalone `TestPerRequestLimits` port preserves its **single** `lineA` at
`now-45m` under `job="fake"`, its exact `{"maxQueryLength": "1m"}` policy,
seven-day-start/`now+1s` range query, default-time label-name request and expected
error substring `the query time range exceeds the limit (query length`.
The policy is also sent on the initial push, as the pinned header-injecting client
does. Removing the policy for the same tenant must return exactly `lineA`, not an
empty successful response. The port additionally asserts HTTP 400, no Parquet
GET on either rejection, one persisted segment and a real GET on success.
The source comment mentions two tenants, but its executable fixture only creates
`org1` and writes one line; the port does not invent a second upstream tenant.
An owned unique tenant replaces literal `org1` for concurrent isolation. The
source's fresh second client clock is represented by the same captured fixture
clock; no latency-dependent window change is needed to include the old line.
Pulso's always-available request-local policy replaces Loki's all-in-one process
and `per-request-limits-enabled` startup flag. This is not configuration or
topology equivalence, or coverage of any other per-request policy field.

One separately marked Pulso control checks authenticated history-to-live
handoff: wrong credentials and another tenant name reject before payload reads,
an explicit start excludes an older stored entry, nonmatching live labels and
pipeline entries do not become frames, and a matching acknowledged write arrives
with its parsed/formatted value. All four writes have Parquet framing; a later
finite query independently reads both matching stored lines. The finite query
clock advances four seconds to include the live fixture's `now+3s` record.
Its initial local-control run caught a harness mistake: the unadvanced helper
ended at `now+1s`, so it correctly returned only `history` (seed 775823). Only
that control's finite query clock was corrected; upstream inputs/expectations
and runtime remained unchanged. Socket process termination is awaited with a
monitor, not sleeps or polling. This control is not counted as an upstream port.

The final integration replay passed all **39 checks** (seed 72535): 25 selected
upstream-derived checks and 14 local controls. The targeted Loki HTTP/WebSocket,
Loki module and LogQL suite passed **373 checks** (seeds 803106 and 163268).
The three added checks also passed 11 consecutive randomized runs (33 test
executions), using `--repeat-until-failure 10`. Compile-with-warnings-as-errors,
targeted formatter verification and `git diff --check` passed. This extension
changes only tests and this provenance file: no runtime gap, approximation,
resource-ceiling change, storage/keyed-byte/native-ABI change, asynchronous
acknowledgement or upgrade requirement was introduced.

### Metric-range resolution discrepancy, not exact parity

The upstream `metrics-range-query` helper asks for a seven-day range without
`step`, writes all four lines in its last four seconds, and expects the maximum
per-step sum to be four. Initial replay exposed Pulso's fixed 15-second default:
it rejected the request for exceeding the evaluation-step ceiling. Pulso now uses
the public Loki window-derived default, with separate regression controls for
the one-second minimum, integer-second rounding, an empty step, an explicit
override and the original seven-day-plus-one-second window length.

At the upstream default of 2,419 seconds for that seven-day-plus-one-second
window, the final regular evaluation is 51 seconds before the requested end,
before all four writes. The replay therefore returns a valid empty matrix,
not the upstream helper's expected four. The adapted count check explicitly
requests `step=60` while retaining all original lines, timestamps, labels, query,
seven-day bounds and maximum-four assertion. **It is not an exact port of the
no-step request.** No extra terminal evaluation sample is invented and no
resource ceiling is weakened to force that upstream expectation. The upstream
cluster suite was inspected, not executed here, so this observation is not a
claim that the upstream test itself was reproduced failing in Loki.

For the documented public step semantics see the pinned
[`TestHttp_ParseRangeQuery_Step`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/loghttp/params_test.go#L209-L246)
case (one hour defaults to 14 seconds).

### Bounded exact `approx_topk` public-result replay

`TestProbabilisticQuery` preserves the four exact lines, the two `now-45m`
timestamps and two `now` timestamps, `job=one/two`, the exact query, vector type,
and complete expected `job=one` / `3` result. The existing instant helper retains
the upstream client's `now+1s` evaluation time. The baseline failed with HTTP
400 `parse_error` before any runtime changes (seed 84). The imported assertion
is not weakened to accept an error or an arbitrary single series.

Pulso implements this operator using **exact top-k ranking under the existing
scan, selected-log-sample, work, intermediate-point, deadline and heap bounds**.
This is deliberately not Loki's count-min sketch/heap, sketch merging, sharding
or high-cardinality acceleration. A low output rank does not exempt its input
from those bounds. There is no new runtime dependency, setting, native ABI,
storage format or keyed-byte change. Every query-serving application must
upgrade before a non-sticky client can rely on the syntax; this operator alone
requires no native upgrade, data migration or keyed-retry drain.

Three separately named Pulso controls cover GET/form POST, complete two-series
values, ranks above cardinality and at signed-int64 maximum, inner grouping,
lookback changes, empty selection and arithmetic; zero/negative/fractional/
decimal/exponent/overflow/missing ranks and every outer grouping placement;
and fail-whole scan/intermediate-result budgets with `k=1`. Whole-tree mode
validation rejects even a right-hand nested `approx_topk` in a range query
before its left-hand selector can issue any provider request. The integral
float control reproduced a successful query for `1.0` before integer syntax was
preserved for this operator (seed 86). Existing aggregations retain their
previous parameter normalization and range behavior.

The instant-only, no-outer-grouping and positive integer rules are grounded in
the pinned [metric-query documentation](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/docs/sources/query/metric_queries.md),
[syntax constructor](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/ast.go#L1955-L1973)
and [instant-only evaluator test](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/count_min_sketch_test.go#L122-L127).
The separate [LogQL execution fixture](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/functions.logqltest#L133-L171)
also exercises these restrictions; it was inspected, not counted as another
ported integration test. Diagnostics use Pulso's own envelope, including
`:instant_query_required`, not upstream's sketch-specific error wording.

The first complete integration replay passed **43 tests** (seed 85), and the
broader Loki HTTP/WebSocket, module, LogQL and mixed-query-batch regression
passed **386 tests** after the integer-literal correction (seed 87). Neither
run launches Loki or proves deployed-client/provider behavior or sketch parity.

Combined Mimir/Loki/Tempo service and trace-metrics unit/HTTP validation passed
**169 tests** (seed 89). The full precommit run was **1824/1827 passed**, with
three timing-related failures, one optional promtool skip and 37 exclusions
(seed 88). The filtered-intrinsic discovery timeout, gRPC admission reply timing
and 150k-span trace-query deadline all passed unchanged when rerun with
`mix test --failed --max-cases 1 --seed 88 --warnings-as-errors` (3 passed, 8.9s).
The runner was heavily loaded, but the cause remains unproven; this is not a
green full-suite claim or a fix for those timing failures. Compile, formatting,
unused-dependency and strict Credo checks passed. No budget or timeout was raised.

Two later combined runs each passed **168/169** (seed 90): one existing Loki
category-control push returned 500 with four concurrent cases; an existing
Tempo legacy-filter ingest returned 503 even with serial cases. Both passed
alone without runtime changes. Their original response bodies were not
captured; Loki push failures now retain that diagnostic. The cause remains
unverified, including under serial execution. The final isolated selection of
all twelve additions passed **12 tests, 100 excluded** (seed 91), including
the strengthened exact result-length assertions. No flaky retry is built into
the tests, and these later failures are not hidden by the earlier green run.

### Bounded exact distinct-count public-result replay

The two `TestApproxCountDistinctQuery` ports preserve all four original lines,
their `now-20m`, `now-10m`, `now-15m`, `now-5m` timestamps, complete stream
labels, exact queries, vector types, one-series results, expected labels and
value `3`. The client helper's `now+1s` evaluation time is unchanged.
Both failed with HTTP 400 `parse_error` before the runtime changes (seed 101),
then passed with the implementation (seed 102). Summing per-stream counts would
return four, so the repeated `shared` value discriminates that incorrect approach.

Pulso computes exact string cardinalities in each existing range window;
it does not port Loki's runtime, HyperLogLog hashing/estimation, protobuf
sketches, sharding or sketch merging. Both upstream `Shards > 1` assertions
are deliberately omitted rather than simulated. High-cardinality results may
differ from an estimate, and this bounded implementation provides no sketch
acceleration or distributed execution evidence.

The pinned [grammar](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/syntax.y#L120-L123),
[validation](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/ast.go#L1705-L1748),
[extractor grouping](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/extractor.go#L108-L135),
[raw-string/missing/empty extraction](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/metrics_extraction.go#L407-L501)
and [instant/range execution test](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/approx_count_distinct_test.go#L273-L405)
were inspected for the additional semantics, not counted as additional ports.

Four separately marked Pulso HTTP/S3 controls cover default/explicit/empty
grouping, labels versus parsed fields, exact numeric-looking strings, duplicate/
missing/empty values, arithmetic, GET/form POST, complete per-step matrices,
open lower/inclusive upper nanosecond boundaries, offset and empty windows;
invalid parameter/grouping/unwrap requests before any provider access; and
fail-whole scan, selected-log-sample, materialized-sample, work and result limits.
Retained strings are detached from decoded arenas; duplicates spend both
sample quotas before window deduplication. The existing materialized-sample
limit defaults to 100,000, separate from the five-million selected-log ceiling.
This is not a new string-byte memory ceiling or a capacity benchmark.

The combined Loki HTTP/WebSocket/module/LogQL/batch and Mimir/Tempo service
regression passed **503 tests**, seed 104. All six additions also passed eleven
consecutive randomized runs (66 executions, `--repeat-until-failure 10`).
Compile-with-warnings-as-errors, scoped formatting, strict Credo (437 files)
and whitespace checks passed. No upstream process or deployed provider was
launched. Only query-serving applications need upgrading for the
syntax; no storage/keyed-byte/native ABI change, migration or retry drain.

### Counter-rate execution fixtures

The counter section preserves all six upstream datasets, including every noise
line, repeated timestamp interval and structured-metadata value, all 23 queries,
and their complete expected labels/points/gaps or rejection. The script's epoch
is translated to a recent integer-second anchor; relative sample/query times
and offsets are unchanged. Expected numbers are transcribed from upstream,
not regenerated by Pulso. Successful cases additionally replay as form POST.
The two invalid-query checks assert Pulso's HTTP 400 rather than Loki's exact
error text. Native Parquet framing and actual payload GETs remain asserted.

The pinned [script harness](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/README.md)
executes these through the real Loki engine over TSDB-backed chunks. Pulso
replays the source inputs/expectations against its own HTTP/S3 path, not Loki's
storage engines, shard permutations or query frontend. The upstream Go suite
was inspected, not launched. No Go/network dependency is added to ExUnit.

The baseline failed all six successful-result groups with
`unsupported_rate_counter` (seed 117); the rejection group already passed.
After implementing independently written counter reduction, all seven passed
(seed 118). Missing/empty counter fields supply no observation, consistent with
the [upstream extractor](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/metrics_extraction.go#L370-L397);
present malformed values still fail whole evaluated windows. This does not
by itself change other unwrap operators; the continuation below does.

Two new HTTP/S3 controls cover missing/empty fields versus malformed middle
samples, sparse windows, pipeline errors and fail-whole scan/sample/work/result
budgets. Three further controls run against both memory and native-capable
adapters: exact nanosecond window/1.1-spacing boundaries, deterministic
equal-time record ordering independent of arrival/numeric values, and IEEE
special values/zero elapsed time. These are local controls, not upstream ports.
One local boundary expectation was corrected after recognizing that moving the
window back one nanosecond admits its previous lower-edge reset observation;
the upstream expectations and runtime were not changed to accommodate it.

Counter observations use detached canonical record keys and the existing
bounded Elixir fold; packed native samples cannot carry these tie keys. No
native fast path is claimed, and Loki equal-time ordering parity is unproven.
No resource ceiling, ABI, storage/keyed bytes or ingestion behavior changes.
Query-serving applications must upgrade; no native upgrade or migration is
needed for this operator.

Final full precommit passed **1869 tests**, one existing skip and 37 opt-in
exclusions (seed 121), including all three upstream service-port files. Strict
Credo checked 438 files without findings; formatting and whitespace checks
passed. The existing alert-rule validation check now admits valid counter
queries with the disabled default and still rejects counters without unwrap.
No upstream process, deployed provider or capacity benchmark was run.

### Missing/empty unwrap-field continuation

The 71 new evaluations retain all four upstream fixtures, original queries,
complete result labels/values and absent points. Each runs over both GET and
form POST through authenticated HTTP, native Parquet segments and real S3
requests against the test-owned store. The clock is anchored to an integer
second 180 seconds before the harness clock; source times and offsets are
unchanged. The executable upstream engine script was inspected, not launched;
ExUnit needs neither Go nor a running Loki server.

All four new groups failed with `sample_extraction_error` before runtime edits
(seed 122), then passed after the fix (seed 123). All unwrap aggregations now
skip missing/empty fields before series creation, including conversions and
absence checks. Present malformed values still fail evaluated windows. The
existing conservative nonempty `__error__` guard remains, even on missing-field
rows. The post-unwrap filtering continuation below supersedes that remaining
filtering gap; full parser-error generation remains open.

Four new local checks across memory/native-capable adapters cover all ordinary
unwrap operators, explicit empty fields, all-missing windows, malformed values,
retained-sample versus work limits and actual native-path execution. Existing
conversion controls now contain missing/empty rows. The materializing reference
independently removes missing values before grouping/window presence. Random
parity inputs now include explicit empties and validate supported operator,
unwrap and grouping combinations rather than comparing unsupported ASTs.

Native log metric capability is **4**, with required tuple `{1,1,4}`; the startup
fence prevents older native semantics from answering. Upgrade all query-serving
app/native pairs. No stored/keyed bytes, ingest behavior or resource ceilings
change. The targeted LogQL plus Loki HTTP suite passed **267 tests** (seed 124).

Full `mix precommit --max-cases 4 --seed 125 --warnings-as-errors` passed
**1877 tests**, with one existing skip and 37 opt-in exclusions, including
Mimir/Loki/Tempo replays. All 133 release-mode codec tests, strict Clippy, Rust
formatting and strict Credo passed. No check was ignored. Compaction/CAS warnings
remain unresolved; no deployed-provider or capacity validation was run.

### Post-unwrap filtering and explicit extraction-error selection

The continuation adds 29 unchanged query/result scenarios: six grouped-average
evaluations each for logfmt, JSON and metadata, eight failed-conversion
absence/min/max evaluations, and three structured-metadata rate evaluations.
The grouped-average checks extend the existing exact fixtures rather than
replacing their noise lines or synthesizing results. All successful cases run
over GET and form POST through the real HTTP/S3/Parquet path; the two failing
queries assert HTTP 400. The source's `SampleExtractionErr` message substring
is not asserted against Pulso's differently shaped HTTP error envelope.

Conversion now precedes post-unwrap filters. Explicit error-preserving filters
keep ordinary failed conversions as zero observations, with error identities
retained through range grouping and removable by outer aggregation. The error
preservation policy is independently implemented from the pinned stage-hint
contract, not copied runtime code. Reading details or supplying a preservation
label cannot bypass the ordinary error guard. Non-filter stages after unwrap
and converting reserved-error comparisons reject before provider reads.

All six initial memory/native-capable evaluator checks failed before the fix
(seed 231). The expanded controls and HTTP wrappers pass together (14 checks,
seed 233), including rejected-observation sample/work bounds, incoming errors,
spoofed preservation labels, grouped errors and duration/byte failure removal.
Post-filter/error-preserving queries deliberately use the bounded Elixir path;
ordinary queries retain the native path. No ABI/storage/keyed-byte change.
This does not implement parser/numeric-label-filter error generation, exact Go
diagnostic escaping, all conversion-overflow semantics or parser-hint parity.
Duration/byte errors retain Pulso-specific diagnostic text. The conservative
pipeline-error guard on missing unwrap targets remains.

## Gaps found and fixed

1. Series and index stats silently discarded LogQL pipeline stages and returned
   HTTP 200. The two copied upstream error cases now receive HTTP 400 containing
   `only label matchers are supported`. Shared selector validation also protects
   label-name/value discovery. Separate Pulso regression coverage exercises all
   four endpoints with line filters and parsers over GET and form POST.
2. The fixed 15-second range-query default rejected ordinary long windows sent
   by the upstream client. The default now follows the requested window, while
   explicit resolutions and existing query budgets remain authoritative.
3. The response-encoding header was ignored. Queries now negotiate categorized
   stream/per-entry output using explicit origin tracking through the existing
   pipeline, including overwrites that happen to retain the same value.
4. The per-request query-limits header was ignored. A bounded, tightening-only
   `maxQueryLength` now rejects excessive finite read windows before storage;
   unsupported or ambiguous policies fail explicitly rather than imply support.
5. The upstream `approx_topk` public-result query returned a parse error. It now
   ranks exactly within ordinary query budgets, with instant-only, positive
   integer and no-outer-grouping restrictions. Sketch-based acceleration remains
   unimplemented rather than being inferred from this small public fixture.
6. Both upstream distinct-count queries returned parse errors. They now count
   exact strings across streams within each output group/window, with missing/
   empty fields omitted and all retained observations charged before deduplication.
7. Counter-rate expressions returned `unsupported_rate_counter`. They now
   reduce chronologically with every reset compensated, bounded window-edge
   extrapolation, offsets and upstream single/empty-window behavior.
8. Missing/empty unwrap fields incorrectly failed numeric range queries. All
   unwrap aggregations now skip these fields without creating observations or
   charging retained samples; present malformed values still fail whole windows.
9. Label filters after unwrap were rejected or could run before conversion.
   They now run after extraction and before grouping, including explicit
   selection/removal of extraction-error samples under the existing budgets.

## Unported boundaries

This selection does not establish full Loki compatibility. It omits enabled
LBAC/aggregated metric policies, deletion APIs, multi-tenant federation,
per-request lookback/time/byte/sharding policies, unselected WebSocket tail
behaviors, OTLP promotion/drop policies, probabilistic sketches/high-cardinality
acceleration and distributed merging, ruler evaluation,
Kafka deduplication, scheduler/ring changes, schema/provider migration, bloom
builders, data-object reader statistics/cache/cost parity and capacity testing.
The selected Explore requests do not establish query-frontend split/cache
equivalence. The selected ordering result does not establish retained-ingester
versus chunk-store deduplication, execution-order permutations or their statistics.
Existing Pulso-specific tests for some of these surfaces are separate evidence,
not additional upstream ports. S3 here is the owned local test provider, not a
deployed cloud-provider durability, client acceptance or performance proof.

### Scalar result-type and arithmetic batch

The 33 scalar scenarios retain every expression and expectation from the
complete three source sections, including the five failures. The vector
selection retains all five cases; its unrelated loaded logs are omitted
because none is queried. All requests retain source evaluation times and
range grids, run through real authenticated Phoenix HTTP with GET/form POST
(the nested-vector rejection is GET only), and assert zero provider requests.
Every successful scalar also gets an additional three-step range replay;
these 28 replays are local controls, not extra upstream cases. The batch makes
131 HTTP requests across the three engine wrappers. Source diagnostic text is
not ported: failures assert Pulso's HTTP400 error codes and nonempty messages.
Exact Loki error-body parity remains open.

Before runtime changes, the scalar envelope and invalid vector-negation checks
failed; the vector control passed (seed 245). The seven tagged engine/local
checks pass after implementation (seed 247). Local controls distinguish
signed zeros, vector/scalar division in both orders, nested zero denominators,
malformed/overflowing numbers, authorization and encoding flags. Separate MCP,
batch-quota and mixed-expression preview checks preserve scalar types without
weakening work/output ceilings. Pure scalars are not native threshold vectors;
use a vector expression for a threshold. No source runtime implementation,
native ABI, storage format or keyed bytes change. Selected tests are not
deployed-client/provider/capacity acceptance.

### Typed label-filter errors and float comparisons

The three label-filter wrappers port all 28 rows of the pinned unit tables
through authenticated Phoenix HTTP and native S3/Parquet, without executing Go.
Source label bags, comparison values, boolean outcomes and `LabelFilterErr`
classes are preserved. Each row adds an isolated `job` label and a constant
`sample log line` payload; Go filter constructors become equivalent LogQL
predicates with canonical nanosecond/byte literals. The eleven byte-unit rows
retain upstream's independent integer constants (`42 * 1000`, `42 * 1024`,
etc.), rather than comparing two spellings processed by the same unit converter.
Both transports replay the binary-filter table. Number/byte conversion
diagnostics retain Pulso's `invalid <type> value "..."` text; duration
diagnostics now preserve the source text, as documented below. The full
`TestBinary_Filter` table failed on its duration-error row before runtime edits
(seed 250); the byte-unit control passed unchanged.

Typed filters now distinguish absent from present-empty values, retain
conversion failures for later error filters, and preserve the first nonempty
error. Numeric comparisons use float64 rather than arbitrary-precision integer
comparison, including NaN/infinity, hex floats, separators and large-integer
rounding. Numeric/duration/byte equality accepts `==` as well as `=`; string,
selector and drop/keep syntax is unchanged. Compound AND evaluates both children
and threads the resulting labels; OR short-circuits only when its left matches.
Eight separately identified controls cover these boundaries across memory and
native-capable adapters, category provenance, fail-whole HTTP metrics and
retained-sample/work budgets. Source grammar and filter semantics were inspected;
no upstream runtime implementation was copied.

Native ordinary-decimal comparisons stay native and use float64 on both sides.
Unsupported spellings or conversion failures fall back to the bounded Elixir
pipeline instead of silently returning a nonmatch. The required log tuple is
`{1,1,6}`, superseding earlier snapshots in this file: upgrade every
query-serving app/native pair. Existing data, keyed bytes and limits do not
change. Complete parser-error generation, Go diagnostics and quantity-parser
parity remain open. Validation of this continuation is recorded in the protocol
breadth checkpoint; no provider/deployed-client/capacity acceptance is implied.

### Runtime duration and byte conversion continuation

This continuation supersedes the runtime-quantity caveats above, not the
remaining query-literal or Go diagnostic-text caveats. The four new wrappers
use authenticated HTTP, real native Parquet and the loopback S3 provider:

| Pinned source | Complete selection | Rows |
| --- | --- | ---: |
| [Loki `metrics_extraction_test.go:121–201,228–239`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/metrics_extraction_test.go#L121-L239) | Both successful duration owners, all three successful byte owners, dynamic logfmt duration | 6 |
| [Go 1.27.3 `src/time/time_test.go:945–1005`](https://github.com/golang/go/blob/go1.27.3/src/time/time_test.go#L945-L1005) | Entire `parseDurationTests` table, preserving the repeated minimum-duration row | 43 |
| [Go 1.27.3 `src/time/time_test.go:1016–1043`](https://github.com/golang/go/blob/go1.27.3/src/time/time_test.go#L1016-L1043) | All UTF-8 `parseDurationErrorTests` rows; three invalid-UTF-8 strings cannot be JSON HTTP inputs and are explicitly excluded | 18 |
| [go-humanize v1.1.0 `bytes_test.go:7–118`](https://github.com/dustin/go-humanize/blob/40736da3e8ed16369b5e6f109548f243d43a7e34/bytes_test.go#L7-L118) | Entire `TestByteParsing`, `TestByteErrors` and `TestParseBytesExactIntegers` tables | 42 |

Go 1.27.3 is Loki's pinned toolchain; go-humanize v1.1.0 is its pinned module.
The byte converter at that module revision is byte-identical to Loki's vendor
copy, including the exact-uint64 integer path. Go tables retain their BSD notice
in [`Go-LICENSE`](Go-LICENSE); the byte tables retain their MIT notice in
[`go-humanize-LICENSE`](go-humanize-LICENSE). No upstream runtime code is shipped
or required by Pulso's tests.

The extractor constructors become one-observation grouped `avg_over_time`
queries, preserving the original input owner, parser, labels and output value.
An isolated job selector supplies fixture identity. Dependency tables replay
through both runtime label filtering and unwrap; durations exercise both
`duration` aliases. Canonical nonnegative ns/B filter thresholds assert exact
integers independently of the runtime converter. Negative durations use their
sign for filtering and their full seconds value for unwrap. Values exceeding
the existing signed-int64 query-literal ceiling are tested through positive
filtering, unwrap and separate exact-uint64 utility controls, not invalid RHS
literals. HTTP float outputs compare numerically against the source integers;
conversion to seconds follows Go's split whole-second/remainder formula.

Error rows assert `LabelFilterErr` retention/removal, fail-whole metric queries
and failed unwrap removal. Empty producer fields still skip unwrap rather than
creating an error observation. Exact diagnostic strings are explicitly adapted
to Pulso's current converters. Separate local controls exercise fractional
rounding, unsigned boundaries, malformed/long inputs, Unicode spacing,
query-literal isolation, form/range replay and unchanged query budgets.

Both successful-table wrappers failed before wiring the runtime converters:
Greek `μs` and lowercase byte suffixes were rejected (seed 269). All 80 success
rows were independently reconstructed from Go ASTs and their original expected
constant expressions, matching input order and values exactly. A temporary
Go 1.27.3 executable using the actual pinned converters agreed with Pulso on
12,079 cases, including fractional accumulation, overflow, Unicode whitespace,
large integers and long malformed inputs. That differential check is validation,
not an external runtime dependency of the permanent tests. Final combined checks
are recorded in the protocol-breadth checkpoint. This is not a complete Loki
engine, deployed-client, provider-capacity or replacement acceptance claim.

| Additional complete source file | SHA-256 |
| --- | --- |
| Loki `pkg/logql/log/metrics_extraction_test.go` | `21317d2e294e6bbed25160a1b7cc2486dc106fda5c481b7ab7e915b06ec6b16d` |
| Go 1.27.3 `src/time/time_test.go` | `24ad31fae98b70cce5ad9c448e955b24277085752914f30a4de6014fdf02c04b` |
| go-humanize v1.1.0 `bytes_test.go` | `42eb9e12a664cc769670740c7004e498fb9a2e16773b92b71b427c0b4f629db2` |
| go-humanize v1.1.0 `bytes.go`, also Loki's vendor copy | `0a152d19cf6af3aaa2f365cdbcf7cd073f14eff13a001920c098c08a6eb310f2` |
| `Go-LICENSE` | `911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad` |
| `go-humanize-LICENSE` | `a973b4498c13eb74baa2a8e5c351426a6826f2fcdd909916dbe53ee2e755fd71` |

### Query quantity literal continuation

This continuation supersedes earlier query-literal-quantity caveats, not complete
parser/diagnostic or deployed-client acceptance. Three more wrappers use real
authenticated HTTP, native Parquet and the loopback S3 store:

| Source selection | Inputs | Adaptation |
| --- | ---: | --- |
| [Loki `lex_test.go:187–214`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/lex_test.go#L187-L214), complete `Test_parseDuration` | 11 | Original input tokens and expected ns constants become equality thresholds against canonical producer ns strings, then log and metric HTTP queries. This does not run Loki's lexer suite. |
| [prometheus/common v0.72.1 `model/time_test.go:101–180`](https://github.com/prometheus/common/blob/v0.72.1/model/time_test.go#L101-L180), complete `baseCases`; [all `TestParseBadDuration` inputs, lines 430–441](https://github.com/prometheus/common/blob/v0.72.1/model/time_test.go#L430-L441) | 22 | Bracketed count windows retain all 13 expected durations and nine rejections. Three zero values parse but deliberately reject metric evaluation. Nonzero windows place rows exactly on and 1ns after the lower boundary; GET/form POST must count only the latter. The negative-helper API is not a Loki range parser. |
| [Loki `lex_test.go:47–58,104–106,116–117`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/lex_test.go#L47-L117) and [`parser_test.go:1752–1770`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/parser_test.go#L1752-L1770) | 8 | Standalone `123KB/PB/PiB` and embedded `0B/1B/250kB/200MiB` lexer inputs become equality thresholds; independent unit constants supply numeric values. The final `2.5KiB=2560` expectation is explicit in the parser test. Other stages/operators in the containing source queries are not claimed as ports. |

The model source is byte-identical to Loki's vendored v0.72.1 copy. These tables
retain Copyright 2013 The Prometheus Authors and Apache-2.0 attribution; the
unmodified license is [`Prometheus-common-LICENSE`](Prometheus-common-LICENSE).
Its original [`NOTICE`](Prometheus-common-NOTICE) is retained too.
The Loki tables keep this directory's existing AGPL attribution. Ordinary tests
require neither Go, an upstream process nor downloads.

Two local HTTP wrappers distinguish fractional rounding and signed/unsigned
limits with unequal adjacent canonical inputs, optional byte suffixes,
`-0` duration thresholds, signed fractional offsets, parser rejection before
provider reads, range/form replay and fail-whole work budgets. Synthetic
nanosecond evaluator windows in older tests now enter through their AST
interface, retaining every prior expected value and nanosecond boundary; the
public bracket grammar no longer accepts those non-Loki spellings.
The earlier 43-duration and 37-byte runtime success tables now publish one
multi-stream fixture each, rather than accumulating one segment per table row.
This retains every input and expected query result while avoiding quadratic
whole-object catalog scans in a conversion test; layout/capacity parity is not
part of these dependency tables. No timeout or query budget was raised.

Before implementation the existing query-quantity regression failed on
`0.3333333333333333333h`: exact-decimal truncation returned 1199999999999 instead
of Loki's 1200000000000ns (seed 273). A temporary Go 1.27.3 executable ran the
original pinned scanner, lexer quantity functions, model parser and byte
converter against 5,832 generated token/range decisions. **5,831 matched exactly;
one deliberate difference remains:** `0B` emits a BYTES token in the lexer test
but the pinned scanner also reports `literal has no digits`, which the full
parser treats as an error. Pulso keeps accepting `0B` rather than reproducing
that contradiction. This is not a hidden skip or a claim that Loki's engine or
HTTP API was executed. Full-suite validation is recorded in the protocol plan.

| Complete source file | SHA-256 |
| --- | --- |
| Loki `pkg/logql/syntax/lex.go` | `87f4f258644d77347b808179603b3e9d13355ba8d686355c45d985af1c1cc8f9` |
| Loki `pkg/logql/syntax/lex_test.go` | `aceed26bb5d6cc03b8f3840a301560c915e98be8ff46393a38b7b7d040f6bf42` |
| Loki `pkg/logql/syntax/parser_test.go` | `0a8466fd5d9cab3f852f9a18a52dc54b7b1a1ab0c9c6af53ca1df21d9f38ce99` |
| Loki `pkg/logql/syntax/query_scanner.go` | `f204413020e233ca18b9b5a7108ec9d940b64f73bbf406ea591230fae1245183` |
| prometheus/common v0.72.1 `model/time_test.go` | `87fa69034510ed023d4f0ee37d8ef113f4beaa03b5bae46a2a021623dab48da9` |
| `Prometheus-common-LICENSE` | `c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4` |
| `Prometheus-common-NOTICE` | `600244c8052c8c1d307043fafacbc83b429c171dd8e08f5a537c0a54014585ee` |

### Duration diagnostic continuation

Duration filters and both duration unwrap spellings now expose exact Go-style
diagnostics instead of Pulso's generic text. The binary-filter error row
[`label_filter_test.go:144–152`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/label_filter_test.go#L144-L152)
again retains its complete source labels, including
`time: unknown unit "weeeeee" in duration "2weeeeee"`.
One added wrapper ports the complete
[`metrics_extraction_test.go:239–251`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/metrics_extraction_test.go#L239-L251)
invalid-duration row: `bar=foo`, line `foo=not_a_number`, `bar,buzz` grouping,
kept zero value and `SampleExtractionErr` details. HTTP cannot directly expose
the extractor's keep boolean, so the query explicitly selects `__error__!=""`
to retain its failed zero sample; `duration_seconds` additionally checks Pulso's
supported alias. This is an extractor-table port, not a new service suite.

[`duration-errors.jsonl`](duration-errors.jsonl) contains 34 independent Go
1.27.3 outputs: all 18 UTF-8 inputs from
[`src/time/time_test.go:1016–1043`](https://github.com/golang/go/blob/go1.27.3/src/time/time_test.go#L1016-L1043),
then the two Loki inputs above, then 14 separately identified Pulso controls.
Each row is `[input, full_error]`. The existing dependency-table wrapper now
checks the complete 18 duration filter diagnostics, not only their error class.
All three invalid-UTF-8 source inputs remain excluded from JSON HTTP and are
checked directly in the existing quantity tests, without claiming HTTP support.
The HTTP wrappers check missing units, overflow-before-unknown-unit
precedence, escaped quotes/backslashes, newline/tab/NUL, replacement characters,
both micro symbols, emoji and DEL. Go's time package quotes non-ASCII/control
bytes as lowercase `\xNN`, but leaves DEL raw; JSON quoting is not equivalent.
GET/form filters select exact details, range metrics retain zero-valued failed
samples only with explicit error selection, and first-error/work-budget guards
remain. These controls add no timeout or budget exemptions.

[`duration-errors-producer.go.txt`](duration-errors-producer.go.txt) uses Go's
AST parser to read the original table and calls the real `time.ParseDuration`.
It does not contain or call Pulso or copy Go's runtime implementation. The
Go-derived fixture retains Copyright 2009 The Go Authors under the existing
[`Go-LICENSE`](Go-LICENSE); the adapted Loki tests retain AGPL attribution.
Ordinary Elixir replay needs neither Go nor downloads. To reproduce manually:

```sh
tmp=$(mktemp -d)
cp test/fixtures/logs/loki-upstream/duration-errors-producer.go.txt "$tmp/producer.go"
GOTOOLCHAIN=go1.27.3 go build -o "$tmp/producer" "$tmp/producer.go"
root=$(GOTOOLCHAIN=go1.27.3 go env GOROOT)
"$tmp/producer" "$root/src/time/time_test.go" > "$tmp/duration-errors.jsonl"
cmp "$tmp/duration-errors.jsonl" test/fixtures/logs/loki-upstream/duration-errors.jsonl
rm -rf "$tmp"
```

All three new/strengthened diagnostic wrappers failed before runtime edits
(seed 280): generic text differed from the original expected details and exact
detail selectors returned no rows. The complete LogQL/Loki controller subset
then passed 342 tests (seed 281). A temporary differential probe compared
14733 real Go duration decisions (364 valid, 14369 errors) with zero mismatches,
including exact error text and invalid-byte inputs. This is converter evidence,
not a Loki engine/HTTP or deployed-client claim. Full validation is recorded in
the protocol plan. Numeric/byte diagnostics and complete parser errors remain
open; valid quantities, storage/native formats and limits are unchanged.

| Artifact/source | SHA-256 |
| --- | --- |
| `duration-errors.jsonl` | `37cd06516a4c49f0902fd49535fe4ad4c0e41e22d38c80cb5c27565082ebe35d` |
| `duration-errors-producer.go.txt` | `f87cdc0b037279914328d8467575fa4ec723f867e9db8daae7f67d5afafe42c9` |
| Go `time_test.go:1016–1043`, selected bytes including trailing newline | `e72a6f494ca22fb459e3ff9dbeef1bef07944a2b77374a2ccffd48b2282b44f6` |
| Loki `metrics_extraction_test.go`, complete file | `21317d2e294e6bbed25160a1b7cc2486dc106fda5c481b7ab7e915b06ec6b16d` |
| Loki `metrics_extraction_test.go:239–251`, selected bytes including trailing newline | `a9c76bb3baac974de3453eb392cc7f021ae0f9c3575603bbcfb0aa00fdb96d06` |
| Loki `label_filter_test.go:144–152`, selected bytes including trailing newline | `69cec29e40ed3e102065ef1e0c3857a9c4177b10b181018916b01290aa98b3c7` |

### Byte diagnostic continuation

Byte filters and `unwrap bytes(...)` now expose the original go-humanize
diagnostics rather than Pulso's generic conversion text. The existing complete
[`TestByteErrors`, `bytes_test.go:63–84`](https://github.com/dustin/go-humanize/blob/40736da3e8ed16369b5e6f109548f243d43a7e34/bytes_test.go#L63-L84)
wrapper checks all five exact errors. The existing Loki binary-filter wrapper
also retains the original `bytes=2qb` labels, keep decision and
`unhandled size name: qb` from
[`label_filter_test.go:153–161`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/label_filter_test.go#L153-L161).
These strengthen existing dependency/unit-table ports, not new service suites.

[`byte-errors.jsonl`](byte-errors.jsonl) contains those six source inputs and
28 explicitly local controls, each `[input, full_error]`, generated by the real
go-humanize v1.1.0 converter with Go 1.27.3. Numeric syntax/range errors precede
unit lookup; unknown suffixes precede uint64 scaling overflow. Diagnostics use
the numeric prefix after comma removal, not the entire input. The upstream
converter counts digit runes but slices bytes, so even valid UTF-8 fullwidth,
Arabic-Indic and Unicode 17 Tolong Siki digits can yield truncated-byte numeric
errors. Complete digit runes print literally; truncated bytes use lowercase
`\xNN` quoting. This is not Go duration's all-non-ASCII byte quoting.

Unknown suffixes use Go whitespace trimming and simple Unicode lowercasing,
including U+0130 → `i`, not Elixir's `i` plus combining dot. Two separate valid
HTTP controls check `1KİB` → 1024 and `2Mİ` → 2097152 through filters and unwrap.
The one additional local wrapper selects exact diagnostics with GET/form POST,
retains failed zero samples only with explicit `__error__` selection, preserves
the first byte error across a failing duration filter and verifies the shared
work-budget rejection. Existing missing/empty-field handling remains unchanged.
Invalid UTF-8 input is not a JSON HTTP case or a diagnostic-parity claim.

The first 343-test LogQL/Loki replay exposed a separate parser failure: the
NUL-containing error was correct, but its JSON-quoted `\u0000` selector remained
undecoded (342 passed, one failed, seed 284). The initial fix decoded
fixed-width `\uXXXX`/`\UXXXXXXXX` scalars and rejected malformed/incomplete/
surrogate/out-of-range escapes. The string-literal continuation below corrects
the surrogate policy using Loki's actual decoder. The existing selector
test ports three successful and three rejected Unicode rows from Go 1.27.3
[`strconv/quote_test.go:251–253,299,310–311`](https://github.com/golang/go/blob/go1.27.3/src/strconv/quote_test.go#L251-L311),
with additional local scalar-boundary/NUL/raw controls. Those Go rows retain
the existing BSD notice/license. The strengthened parser check failed before
the lexer fix (seed 285). All four focused parser/HTTP wrappers then passed
(seed 286), without changing their queries or expected diagnostics. A separate
6036-case comparison against actual Go `strconv.Unquote` produced 5788 decoded
strings and 248 rejections, with zero mismatches. This does not close other
escape-form, quote-form or parser-error compatibility gaps: Go's decoder was
not sufficient evidence for Loki's literal semantics.

[`byte-errors-producer.go.txt`](byte-errors-producer.go.txt) reads the original
error table with Go's AST parser and calls the dependency; it does not copy its
runtime implementation. The existing MIT license/attribution applies. The
annotated v1.1.0 tag object is `33caae6791a4e1e97a7aaade5c4ff23f97ff9815`;
the source commit is `40736da3e8ed16369b5e6f109548f243d43a7e34`. The older source
link above now uses that peeled commit, without changing the selected bytes.
Normal replay needs neither Go nor downloads. Manual regeneration:

```sh
root="$PWD"
tmp=$(mktemp -d)
cp test/fixtures/logs/loki-upstream/byte-errors-producer.go.txt "$tmp/producer.go"
(
  cd "$tmp"
  GOTOOLCHAIN=go1.27.3 go mod init pulso-byte-errors
  GOTOOLCHAIN=go1.27.3 go get github.com/dustin/go-humanize@v1.1.0
  GOTOOLCHAIN=go1.27.3 go build -o producer producer.go
  mod=$(GOTOOLCHAIN=go1.27.3 go env GOMODCACHE)
  ./producer "$mod/github.com/dustin/go-humanize@v1.1.0/bytes_test.go" > regenerated.jsonl
)
cmp "$tmp/regenerated.jsonl" "$root/test/fixtures/logs/loki-upstream/byte-errors.jsonl"
rm -rf "$tmp"
```

All three new/strengthened wrappers failed before production edits (seed 283).
A temporary differential replay checked 21510 real dependency decisions
(4292 successes, 17218 exact errors), with zero mismatches. A separate complete
Unicode-scalar comparison verified the selected digit classes, simple lowercasing
and whitespace semantics against Go 1.27.3. Producer regeneration matched every
fixture byte. These are converter comparisons and Pulso HTTP/native-Parquet/S3
ports, not execution of Loki's engine/HTTP suite or provider/deployed-client/
capacity acceptance. Full validation is recorded in the protocol plan. Numeric
conversion diagnostics and complete parser errors remain open. No storage,
keyed bytes, native ABI or resource ceiling changes.

| Artifact/source | SHA-256 |
| --- | --- |
| `byte-errors.jsonl` | `7c62324e91859e61b24ae4a9caac8e3752396d8a6f4dc11b18f05c204d1daebb` |
| `byte-errors-producer.go.txt` | `5b78a7295257bfbe80f200431ec46b9c40873205b32c8a402b13dbcab5a012f9` |
| go-humanize `bytes_test.go`, complete file | `42eb9e12a664cc769670740c7004e498fb9a2e16773b92b71b427c0b4f629db2` |
| go-humanize `bytes_test.go:63–84`, selected bytes including trailing newline | `76dd0af3fbe35afc3b3f16d48f597058ff391eabd3c497fffa588789bb4e0f90` |
| Loki `label_filter_test.go:153–161`, selected bytes including trailing newline | `cc5ebde9e19bfe89fb2bce1b53a024a74829d36da8817c5de3c6bc5c189e592e` |
| Go 1.27.3 `src/strconv/quote_test.go`, complete file | `7fc6b61c20729f12e7fd6cd7d5b2c01cf312d5b35db8d173276c2acef588d2b6` |

### Numeric diagnostic continuation

Numeric filter and plain unwrap failures now retain Go `strconv.ParseFloat`
syntax/range reasons and complete original values, with Go Unicode 17 quoting.
The existing typed-filter wrapper no longer adapts the numeric diagnostic in
[`label_filter_test.go:162–171`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/label_filter_test.go#L162-L171).
One additional wrapper transcribes both `not convertable` extractor cases from
[`metrics_extraction_test.go:201–227`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/metrics_extraction_test.go#L201-L227),
preserving stream versus structured-metadata ownership, grouping, exact errors
and kept zero samples. Constructor grouping becomes `by (bar,buzz)`; explicit
`__error__` selection exposes those failed samples through HTTP instead of
triggering the ordinary fail-whole metric guard. These are extractor cases,
not additional Loki service-suite scenarios. The existing AGPL notice applies.

Two separate local HTTP/S3 wrappers replay all 48 frozen
[`number-errors.jsonl`](number-errors.jsonl) rows: all nine float64 errors in
Go 1.27.3 [`atoftests`, `src/strconv/number_test.go:314–335`](https://github.com/golang/go/blob/go1.27.3/src/strconv/number_test.go#L314-L335),
Loki's `not_a_number`, and 38 local controls. Controls distinguish malformed
overflow from genuine range errors, decimal/hex/separator grammar, whitespace,
named controls, NUL/DEL, quotes/backslashes, printable Unicode, non-ASCII spaces,
unassigned/private-use/noncharacter scalars and Unicode 17 additions. GET/form
POST select exact details; range unwraps preserve failed zero samples only
with explicit error selection. Empty unwrap values remain skipped. First-error
preservation and a zero-work rejection retain their existing policies.

[`number-errors-producer.go.txt`](number-errors-producer.go.txt) uses Go's AST
parser to extract every error input from the original float64 table and calls
the actual Go converter, never Pulso. The Go BSD notice/license is retained in
`Go-LICENSE`. To regenerate, copy the producer to `main.go`, build it with
Go 1.27.3 and run it with that toolchain's `src/strconv/number_test.go` path;
compare stdout byte-for-byte with `number-errors.jsonl`. Normal Elixir replay
needs neither Go nor downloads. No Loki engine or service process was launched.

Before runtime edits, the strengthened numeric-filter wrapper and both new
diagnostic wrappers failed (seed 288); the two original syntax-only extractor
cases already passed. All four then passed (seed 290). An independent
Go comparison matched 1,177,600 complete error strings: every Unicode scalar
prefixed with `x`, then all 65,536 two-byte strings prefixed with `x`, including
invalid UTF-8 (which is not claimed as HTTP JSON coverage). The combined SHA-256
was `aae8e85270e175bc1eddb1f6e6636fd0b63ad19eabd07e5ab4d52dd4350aec11`.
A separate 2,200-case decimal/hex/exponent/suffix comparison matched 159 valid
float64 bit patterns and 2,041 complete errors. PCRE2's Unicode 16 printable
categories need all 4,803 new Unicode 17 printable scalars to match Go 1.27.3;
the classification was compared over every Unicode scalar before integration.
This closes runtime numeric conversion diagnostics, not complete parser errors,
then-open quoted/raw string escapes, general numerical parity or provider/client/
capacity acceptance. Storage, keyed bytes, native ABI and ceilings are unchanged.

| Artifact/source | SHA-256 |
| --- | --- |
| `number-errors.jsonl` | `726e80e3f1c22e13b89160d9c07e58d5da6d361f49f37e56d91c7eed0bfad7b5` |
| `number-errors-producer.go.txt` | `6f7ebf5bf3158d73bc9bf596f2e7b9d68ce31f2c799fc57bc5d19e5583829de5` |
| Go 1.27.3 `src/strconv/number_test.go`, complete file | `292286be4aa9216ea586bc151e0b492f959fa5ec493ad45533f2a7ae324f9226` |
| Go `number_test.go:314–335`, selected bytes including trailing newline | `78f61c50b5a05c4586ea4f9e0ef37052324b6ff867f9b4acaad51e77f3f5a535` |
| Go 1.27.3 `src/strconv/quote.go`, complete file | `6602960ec8777de9926c1d02fbf5cb0828d4aefa38083865f4af17b6892c09a2` |
| Go 1.27.3 `src/strconv/isprint.go`, complete file | `e6f4b35fbf38f9c9c8b251e5d3cf94daa880789cb89825f11f9734ba4a42e9ff` |
| Loki `metrics_extraction_test.go`, complete file | `21317d2e294e6bbed25160a1b7cc2486dc106fda5c481b7ab7e915b06ec6b16d` |
| Loki `metrics_extraction_test.go:201–227`, selected bytes including trailing newline | `96ac720f7c3065dc93e2dc95a06a7b76daf69212e55f79c3d3bb0146e1d565fe` |
| Loki `label_filter_test.go:162–171`, selected bytes including trailing newline | `c8387f2d0ae352eb9cf1cb438a5823d21c25765648d2de828cfa8d3422831dcb` |

### String-literal continuation

The parser and three HTTP/Parquet/S3 wrappers now preserve the pinned Loki
string-literal profile. One wrapper uses the three original query inputs from
[`pkg/logql/syntax/lex_test.go:20–21,67`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/lex_test.go#L20-L67):
raw/escaped `\w+` and an escaped quote. Upstream asserts token types; the local
adaptation asserts selected labels/lines through authenticated GET/form POST.
The raw query's incidental spacing is normalized. The existing AGPL attribution
applies. No Loki service or complete engine suite was launched.

[`string-literals.jsonl`](string-literals.jsonl) has **104 rows**: all 35 inputs
from Go 1.27.3's `unquotetests`, all 28 `misquoted` inputs, and 41 separately
marked local controls. The original Go table inputs are retained, but their
expected results are deliberately replaced by actual Loki scanner/decoder
results. This is not a claim that Go's unquote expectations all pass unchanged.
The Go inputs and producer retain their BSD-3-Clause notice in `Go-LICENSE`.
Each row contains `source`, base64-encoded literal bytes, and either base64
decoded bytes or null for rejection. Normal Elixir replay needs no Go/downloads.

There are **46 accepted** literals: 40 UTF-8 values and six raw-byte values.
All replay through parser byte assertions, HTTP label/substring filters and
native-capable count queries. Invalid decoded byte strings must not match the
UTF-8 string `ÿ`; they are not inserted as invalid JSON log values. Of the
**58 rejected** literals, 56 valid-UTF-8 sources return HTTP400 on both transports
without provider access; two invalid-UTF-8 sources are parser-only checks.

The oracle follows
[`lex.go:188–201`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/lex.go#L188-L201):
only `String`/`RawString` tokens, valid UTF-8 token text, successful vendored
Prometheus `strutil.Unquote` and no scanner errors. It uses Loki's copied
[`query_scanner.go`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/query_scanner.go),
not the installed Go `text/scanner`. The decoder is
[`vendor/github.com/prometheus/prometheus/util/strutil/quote.go`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/vendor/github.com/prometheus/prometheus/util/strutil/quote.go).
These runtime sources are used only for manual generation, not shipped here.
The producer uses Go `strconv.Unquote` only to read AST source literals.

Important differences from the earlier Go-only assumption:

- Single quotes are character tokens and reject, even though the shared decoder
  alone could accept them. Unknown escapes, short octal escapes, octal >255,
  literal NUL, invalid UTF-8 source and quoted LF reject.
- Named escapes and exactly two hex/three octal digits decode. Hex/octal values
  remain bytes rather than being UTF-8-encoded as code points.
- Fixed-width Unicode escapes encode runes; each surrogate produces U+FFFD,
  including two replacements for a surrogate pair. Positive out-of-range values
  reject, but the decoder's int32 overflow turns `\U80000000`–`\UFFFFFFFF` into
  their low byte. Five local controls protect this boundary and byte behavior.
- Quoted CR and raw CR/LF/backslashes survive. The raw CR row intentionally
  differs from Go's expected carriage-return stripping.

Four of five focused wrappers failed before runtime changes (seed293). After
the fix all five passed (seed295). The two pre-existing permissive-regex corpus
examples now use raw strings, preserving their intended regex contents.
A separate temporary differential probe compared **68,895** actual scanner/
decoder decisions: 66,846 accepted and 2,049 rejected, with zero mismatches.
It covers all 65,536 four-hex-digit escapes, byte/octal combinations, eight-digit
Unicode boundaries including overflow, literal/escaped source bytes, quote
forms and malformed suffixes. The probe JSONL SHA-256 was
`e64476d208c7b3b93ea7212ec147f030b1fc50950722c17d048aa85499c0dd04`.
This closes literal decoding, not exact parser-error text/positions, general
LogQL, formatter parity, provider/deployed-client acceptance or capacity.
Storage, keyed bytes, native ABI and resource ceilings are unchanged.

[`string-literals-producer.go.txt`](string-literals-producer.go.txt) AST-extracts
the source inputs and calls the actual scanner/decoder. With Go1.27.3 selected,
manual reproduction from the repository root is:

```sh
root="$PWD"
tmp=$(mktemp -d)
base=https://raw.githubusercontent.com/grafana/loki/5aa4b9212d64d723f63b2770452054aa23df114d
mkdir -p "$tmp/syntax" "$tmp/strutil"
curl -fsSL "$base/pkg/logql/syntax/query_scanner.go" -o "$tmp/syntax/query_scanner.go"
curl -fsSL "$base/vendor/github.com/prometheus/prometheus/util/strutil/quote.go" -o "$tmp/strutil/quote.go"
printf 'package syntax\nconst maxInputSize = 131072\n' > "$tmp/syntax/input_limit.go"
printf 'module pulso-string-oracle\n\ngo 1.27.3\n' > "$tmp/go.mod"
cp test/fixtures/logs/loki-upstream/string-literals-producer.go.txt "$tmp/main.go"
go -C "$tmp" run main.go "$(go env GOROOT)/src/strconv/quote_test.go" > "$tmp/actual.jsonl"
cmp "$tmp/actual.jsonl" "$root/test/fixtures/logs/loki-upstream/string-literals.jsonl"
rm -rf "$tmp"
```

The scanner shim retains the exact `maxInputSize` constant from `parser.go:42`;
it does not substitute a different scanner implementation. Fixture regeneration
was byte-identical. Selected range hashes include trailing newlines.

| Artifact/source | SHA-256 |
| --- | --- |
| `string-literals.jsonl` | `bf2ce4bc85e21ede3c3c760d9d7c20a48f482177dedff123b228d1f937094b65` |
| `string-literals-producer.go.txt` | `586913de349c2ca93b272057a70d1dc7e1381f2130c4b2c4e064ce5237f6fc6a` |
| Go1.27.3 `src/strconv/quote_test.go`, complete file | `7fc6b61c20729f12e7fd6cd7d5b2c01cf312d5b35db8d173276c2acef588d2b6` |
| Go `quote_test.go:243–281`, complete `unquotetests` selection | `64d783d1d64923e49e8c13616eb8e5d266297e563a866cf3ef7986db0d1f52b1` |
| Go `quote_test.go:283–312`, complete `misquoted` selection | `cb1fccd413fe36f3dcad6204fc6b7420b815a58aa84a76096e14474e94b52163` |
| Loki `pkg/logql/syntax/lex_test.go`, complete file | `aceed26bb5d6cc03b8f3840a301560c915e98be8ff46393a38b7b7d040f6bf42` |
| Loki `lex_test.go:20–21`, two regex rows | `4aa8c1bb26ac19c70f5165ff275d7fead8296bb5f40771853d64ea74e87dd53a` |
| Loki `lex_test.go:67`, escaped-quote row | `574b41e4e07d428fea596c0bc94170e2d8963bb3b96a5cafbb5b244e5107f82a` |
| Loki `pkg/logql/syntax/lex.go`, complete file | `87f4f258644d77347b808179603b3e9d13355ba8d686355c45d985af1c1cc8f9` |
| Loki `pkg/logql/syntax/query_scanner.go`, complete file | `f204413020e233ca18b9b5a7108ec9d940b64f73bbf406ea591230fae1245183` |
| Loki vendored Prometheus `util/strutil/quote.go`, complete file | `c6f45f4d323c2144d952952a583a45bfd25833248981a0874545b8af18e814f1` |
| Loki `pkg/logql/syntax/parser.go`, complete file | `a38e8d4760af3969d5316e456db8abed93655a66fd81f8cfdd2fa50226142667` |

### IP-filter continuation

[`ip-filters.jsonl`](ip-filters.jsonl) preserves all **15 table rows** from
[`pkg/logql/log/ip_test.go`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/ip_test.go):
eight `Test_IPFilter` cases (seven valid patterns with all 43 original input
lines and one invalid range), three `Test_IPLabelFilterTy` cases (two successes,
one rejection), and four `Test_IPLineFilterTy` cases (two successes, two invalid
regex operators). Per-row source ranges are frozen alongside original patterns,
inputs, expected selected indexes, operators and rejection/keep decisions.
The original AGPL-3.0-only attribution and `LICENSE` apply.

Two ExUnit wrappers replay these unit-table expectations through authenticated
GET/form-POST HTTP, real native Parquet encoding/decoding and S3 manifest CAS.
The first also checks the 43 lines' negative complements, equivalent label
predicates and instant count results. Those permutations are additional local
assertions, not new upstream cases. Invalid requests must return HTTP400
without provider access. The upstream constructors do not supply query errors,
so exact parser-error wording/positions are not asserted. A separate local
wrapper covers range counts, post-unwrap filtering, formatted-line ordering,
compound predicates, case-insensitive IP spelling/whitespace,
family/range/scanner boundaries, missing/empty labels, prior-error behavior and
fail-whole work/log-sample/result limits. The memory evaluator's existing
IP-rejection check now asserts selection and invalid-prefix rejection instead.

IP keyword/whitespace handling follows the pinned lexer's lowercase function
lookup and parenthesis lookahead (`lex.go:223–243,393–409`). A local HTTP control
failed before this token fix (seed302), then all four focused IP tests passed
(seed303). This does not imply case-insensitive support for every LogQL keyword.

Before implementation all three new HTTP wrappers failed (seed297). The matcher
was independently written in Elixir; no upstream runtime implementation is
vendored in production. Its candidate scanner follows
[`ip.go`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/ip.go),
and ranges follow the vendored `go4.org/netipx` version
`v0.0.0-20260823151212-3075585bcbeb`. A temporary differential comparison ran the
actual pinned Loki matcher against **745,807** pattern/input decisions, including
326 invalid patterns and 43,352 positive matches, with no mismatches after fixing
CIDR-to-range fallback for a zone containing `/`. The probe covered all prefix
lengths around both family bounds, mapped addresses, zones, malformed candidates,
delimiters, long runs and deterministic generated addresses. Its JSONL SHA-256
was `83d9325772f6af1abd86f3cc9a4b6d2d801fabbdcdffb58555eed3ec389bf7d6`.
This is selected semantic evidence, not full LogQL, upstream service execution,
provider/deployed-client or capacity acceptance. Resource ceilings and
native/storage/keyed formats are unchanged; IP predicates never use pushdown.

[`ip-filters-producer.go.txt`](ip-filters-producer.go.txt) uses Go's AST to extract
the original test tables, not Pulso answers or a replacement IP implementation.
Fixture regeneration was byte-identical. With Go1.27.3 selected, reproduce from
the repository root:

```sh
root="$PWD"
tmp=$(mktemp -d)
base=https://raw.githubusercontent.com/grafana/loki/5aa4b9212d64d723f63b2770452054aa23df114d
curl -fsSL "$base/pkg/logql/log/ip_test.go" -o "$tmp/ip_test.go"
cp test/fixtures/logs/loki-upstream/ip-filters-producer.go.txt "$tmp/main.go"
go build -o "$tmp/producer" "$tmp/main.go"
"$tmp/producer" "$tmp/ip_test.go" > "$tmp/actual.jsonl"
cmp "$tmp/actual.jsonl" "$root/test/fixtures/logs/loki-upstream/ip-filters.jsonl"
rm -rf "$tmp"
```

Normal Elixir replay requires no Go or downloads. Selected hashes include the
trailing newline; runtime sources were inspected/probed but are not shipped.

| Artifact/source | SHA-256 |
| --- | --- |
| `ip-filters.jsonl` | `db30a4d870fd6105eec0c5379b9448f203ccc379d0ccfe5c7183a8dcfc2b9756` |
| `ip-filters-producer.go.txt` | `e45dca60e4b158aeef5ae1f7890122b3d024a39d9d0b2760a8f64533bc39881c` |
| Loki `pkg/logql/log/ip_test.go`, complete file | `4674d36a3f6d06727b6c6fd9107d84b100b7e2ae2dc9a9eb43f22e237ecc1bd8` |
| Loki `ip_test.go:22–121`, core table | `472f4c4aa31e2546b4d9449cda256f5032efe4846b4ace5603c0eb3c2dc5bdb5` |
| Loki `ip_test.go:156–180`, label table | `789a30c1e03a2ebf5082623927250bd35d14684db1a0e724f65a58fbeded0897` |
| Loki `ip_test.go:211–240`, line operator table | `c2a91a5628b4be3b0611be111bfa35c23f6e21bafc3f96df7ff736141abc3893` |
| Loki `pkg/logql/log/ip.go`, complete file | `ba938c2d4637cc0d5a4f1973bf9eb2775bb6ac6480f7b0fc78e5a7b54c5648b4` |
| Loki vendored `go4.org/netipx/netipx.go`, complete file | `c4fca40ebe816f931299e85a48a8b25f73c18df1a8ec8486fad00bf4b8486dd1` |

## Line-filter OR continuation

[`line-or.jsonl`](line-or.jsonl) preserves two selections at the same pinned
Loki revision, with source ranges on all 56 rows:

- All 27 OR queries and 121 original line keep/drop decisions from
  [`Test_FilterMatcher`, `ast_test.go:400–589`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/ast_test.go#L400-L589).
- All 29 evaluations from
  [`line_filters.logqltest:178–280`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/line_filters.logqltest#L178-L280):
  18 instant and 11 range queries, with 26 successful evaluations and three
  rejected IP forms. Both original load fixtures, repeat counts, queries,
  numeric strings and sparse `_` samples are retained.

Two HTTP wrappers replay these selections through GET and form POST,
authentication, native Parquet and the real Rust S3 client against a test-owned
S3-compatible store. The matcher rows share the original `app="foo"` label but
use disjoint exact timestamp slices, two seconds apart, so queries need no
edits. Equal-time results compare timestamp/line/label multiplicities, not an
ordering that the upstream unit table never asserted. Each row also checks its
kept count through `count_over_time`. The engine fixtures retain original
relative timestamps on a whole-second anchor ten minutes before the test clock;
their disjoint app selectors permit one combined durable batch. `_` means no
sample, not zero. Rejected queries require HTTP400 and no provider access;
exact upstream parser-error text is not asserted.

A separate Pulso-authored HTTP control covers empty alternatives, uppercase
`OR`, adjacent AND groups, filtering after line formatting, invalid later
branches despite an always-matching first branch, malformed tokens, AST limits
and fail-whole work/log-sample/result limits. Negative groups exclude the union
of matches. All branches validate before evaluation; IP is invalid under regex
and pattern operators. Existing order-sensitive helpers remain unchanged.
All three new wrappers failed before implementation (seed305); they pass with
the implementation (seed307). These are upstream unit/engine ports, not more
upstream service suites, deployed clients or representative capacity evidence.

[`line-or-producer.go.txt`](line-or-producer.go.txt) is an attributed
AGPL-3.0-only, standard-library Go data extractor: Go AST for the matcher table
and a narrow reader for the selected engine script. It neither runs Loki nor
evaluates LogQL using Pulso. Regeneration was byte-identical. A separate Elixir
source check verified all queries, original decisions, loads, evaluation
geometry, expected labels/values and rejection rows against the pinned source.
Normal Elixir replay needs no Go or downloads. Reproduce from the repository
root with Go1.27.3 selected:

```sh
root="$PWD"
tmp=$(mktemp -d)
base=https://raw.githubusercontent.com/grafana/loki/5aa4b9212d64d723f63b2770452054aa23df114d
curl -fsSL "$base/pkg/logql/syntax/ast_test.go" -o "$tmp/ast_test.go"
curl -fsSL "$base/pkg/logql/internal/logqltest/testdata/line_filters.logqltest" -o "$tmp/line_filters.logqltest"
cp test/fixtures/logs/loki-upstream/line-or-producer.go.txt "$tmp/main.go"
go build -o "$tmp/producer" "$tmp/main.go"
"$tmp/producer" "$tmp/ast_test.go" "$tmp/line_filters.logqltest" > "$tmp/actual.jsonl"
cmp "$tmp/actual.jsonl" "$root/test/fixtures/logs/loki-upstream/line-or.jsonl"
rm -rf "$tmp"
```

Selected-range hashes include the trailing newline. The unmodified AGPL license
and existing attribution apply; no upstream runtime implementation is shipped.

| Artifact/source | SHA-256 |
| --- | --- |
| `line-or.jsonl` | `b9e5f98f59330ebc9b8da2cb872df84980a59fa74cc67b975fd035db444a0a42` |
| `line-or-producer.go.txt` | `f203ba12dc986330e7ab57fcb665e3a8327c7bb8f5296d10062ccbd60b5d71de` |
| Loki `pkg/logql/syntax/ast_test.go`, complete file | `b058fddd8f947e159e62e694394819eb358557e040c8c27f4f69d47a16be95a6` |
| Loki `ast_test.go:400–589`, complete selected lines | `0a27ffcd8a1368e2bb7ded50d24e6baf0429f282b0e9cac3f38129bccec6492b` |
| Loki `pkg/logql/internal/logqltest/testdata/line_filters.logqltest`, complete file | `399cdeefd80ca9fd70053a00f402cd250b94358b2091cb667569a4e84222e25b` |
| Loki `line_filters.logqltest:178–280`, complete selected lines | `c447f103f2f8f19dec1ca14f70015fcefb3f1d823e24dcc480c71eb17b7945c8` |

## Label-filter conjunction continuation

[`label-boolean.json`](label-boolean.json) freezes all 36 evaluations from the
complete logical-operator, precedence, short-circuit, first-error and mixed
parsed/metadata sections of
[`label_filters.logqltest:902–1039`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/label_filters.logqltest#L902-L1039)
and [1181–1196](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/label_filters.logqltest#L1181-L1196).
Five original load fixtures retain all 15 loads, lines, structured metadata,
relative times, repeat counts and original query strings. There are 28 instant
and eight range evaluations, 27 successful and nine rejected. Expected labels,
numeric strings and sparse `_` samples come from the script, not Pulso.

One async HTTP wrapper replays all evaluations through GET and form POST,
authentication, native Parquet and the real Rust S3 client against an owned
S3-compatible store. Each fixture uses its own durable batch and a whole-second
anchor; anchors are 300 seconds apart to isolate original overlapping selectors
without editing queries. `_` remains absent rather than zero. Rejected metric
queries require HTTP400, a generic `evaluation_failed`/`pipeline_error` envelope
and no partial data. Pulso does not expose Loki's diagnostic text in that
envelope. Supplementary raw-log requests explicitly select `__error__!=""` over
the original `(0,60s]` window to inspect `LabelFilterErr` and original diagnostic
substrings. Script backslash-escaped quotes are normalized only for those
supplementary checks; the frozen rejection strings are unchanged.

A local HTTP wrapper covers exhaustive three-predicate truth tables, uppercase
keywords, parentheses, adjacency without whitespace, pipeline boundaries,
eager conjunction errors, invalid regex/IP operands, malformed commas, AST
admission and work/log-sample/result limits. Invalid queries assert no provider
access. Both wrappers failed before the parser change (seed310); both pass
with it (seed313). These are engine-script ports and local controls, not more
Loki service integration suites, deployed-client or capacity acceptance.

Explicit `and` binds tighter than `or`, but comma/implicit conjunctions attach
the remaining right expression. `a,b or c` means `a AND (b OR c)`, as shown by
the original expected tree in
[`parser_test.go:1616–1649`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/syntax/parser_test.go#L1616-L1649).
A temporary interpreter of the pinned generated yacc tables independently
matched 256 four-predicate mixed-operator/parenthesis expressions and all 4096
Boolean decisions against Pulso. This validates parser-table behavior, not a
running Loki engine. Existing AST evaluation and native fallback policies,
resource ceilings, storage formats and keyed bytes are unchanged.

[`label-boolean-producer.exs.txt`](label-boolean-producer.exs.txt) is an attributed
AGPL-3.0-only standard-library Elixir extractor, not a LogQL evaluator. It
requires only the pinned script, never derives expectations from Pulso, and
regenerates the JSON byte-identically. A separate source check verified every
query, source range, load, metadata field, request time, expected label/value
and rejection substring. Normal replay needs no downloads or producer run.
Reproduce from the repository root using the pinned Elixir toolchain:

```sh
tmp=$(mktemp -d)
base=https://raw.githubusercontent.com/grafana/loki/5aa4b9212d64d723f63b2770452054aa23df114d
curl -fsSL "$base/pkg/logql/internal/logqltest/testdata/label_filters.logqltest" -o "$tmp/label_filters.logqltest"
mise exec -- elixir test/fixtures/logs/loki-upstream/label-boolean-producer.exs.txt \
  "$tmp/label_filters.logqltest" > "$tmp/actual.json"
cmp "$tmp/actual.json" test/fixtures/logs/loki-upstream/label-boolean.json
rm -rf "$tmp"
```

Hashes of selected source lines include the trailing newline. The existing
unmodified AGPL license and attribution apply; no upstream runtime is shipped.

| Artifact/source | SHA-256 |
| --- | --- |
| `label-boolean.json` | `d596a76dd71cc3d7182696e739fe10d4326ff8943a9392d5d298bf802e977d7b` |
| `label-boolean-producer.exs.txt` | `c1b1c03bd50cab1e6273c26f7fb1a322e6b84b92959caa4c66ea5d9c355ed1b7` |
| Loki `pkg/logql/internal/logqltest/testdata/label_filters.logqltest`, complete file | `821d706c4c4621c8dd5b30d239a7d45088bf292fe0730f324bf9dffb85d92e50` |
| Loki `label_filters.logqltest:902–1039`, complete selected lines | `1c004658ee5856ccb2ff368294f64cc8df2d1026b12ad6cb8d7b8e59f492fa94` |
| Loki `label_filters.logqltest:1181–1196`, complete selected lines | `82c028d6857423aa491561009aa8ce74339c4926054bfb41c3632eaebedca146` |
| Loki `pkg/logql/syntax/parser_test.go`, complete file | `0a8466fd5d9cab3f852f9a18a52dc54b7b1a1ab0c9c6af53ca1df21d9f38ce99` |
| Loki `pkg/logql/syntax/syntax.y.go`, complete file | `c4d1e1e9c1df0aedead6cc68d34d2e496a3871ce5830a452da2915f6dc11d3cc` |

### Logfmt decoding continuation

[`logfmt.json`](logfmt.json) freezes **60 evaluations** at the same Loki pin:

| Source selection | Preserved cases | Evaluations |
| --- | --- | ---: |
| [`pkg/logql/log/parser_test.go:1191–1417`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1191-L1417) | First 19 `TestLogfmtParser_parse` rows, ending with `= in value`, each in non-strict and strict modes | 38 |
| [`parser_test.go:1478–1511`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1478-L1511) | All four `TestLogfmtParser_keepEmpty` rows, both modes | 8 |
| [`parsers.logqltest:1–33`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/parsers.logqltest#L1-L33) and [`84–114`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/parsers.logqltest#L84-L114) | Four complete engine fixtures, eight loads/28 entries, including noise and metadata; eight instant and six range evaluations, with two rejected queries and one explicit empty result | 14 |

Two ordinary ExUnit wrappers replay every evaluation through authenticated GET
and form POST against native Parquet and an owned loopback S3 store. Unit rows
gain only a per-case selector label, removed from the compared label bag;
original labels, lines, values and exact error details stay unchanged. The nil
line becomes an empty binary. Strict expectations preserve upstream's rule:
use `want_strict` only when nonempty, otherwise use `want`. Hint expressions
remain inert provenance; replay extracts all labels, not hint optimizations.
Engine fixtures use whole-second anchors 300 seconds apart, retaining every
relative time, input, noise stream, metadata field, sparse slot and oracle.
Rejected metrics retain Pulso's generic `evaluation_failed` envelope; separate
log reads assert the original `LogfmtParserErr` label. These are unit/engine
ports, not execution of upstream service integration suites.

Default logfmt now skips malformed pairs and resumes. Strict decoding stops at
the first error, retains prior fields and publishes exact byte-position details.
Bare keys/empty values are omitted unless requested, and normalized duplicates
keep the first retained value in extract-all mode. Explicit field-list lookup
was unchanged in this batch; the source-expression continuation below narrows
that remaining gap without changing duplicate ownership. Quoted values decode
escapes and Unicode surrogate pairs, with replacement runes mapped to spaces. Native unflagged ASCII
decoding agrees; Unicode falls back to the bounded Elixir pipeline. The serving
log tuple is **`{1,1,6}`**: upgrade every query-serving app/native pair. Storage,
keyed bytes and existing ceilings do not change.

The structured-metadata collision row (`parser_test.go:1418–1429`) and the
collision/drop/re-added-label engine block (`parsers.logqltest:34–83`) are
explicitly excluded, not silently skipped failures. Pulso's immutable original
stream provenance and parser-overwritable structured metadata remain different
from pinned Loki ownership. Hint-driven early exits, full LogQL/parser errors,
explicit-expression parity, client acceptance and provider capacity are not
established by this batch.

[`logfmt-producer.go.txt`](logfmt-producer.go.txt) is an attributed AGPL-3.0-only
standard-library Go AST/source extractor, not a runtime parser. It preserves
raw source selections, mappings and omissions. Regeneration is byte-identical;
an independent Elixir source reader checked every selected field, load, query,
oracle and hash. Ordinary replay needs no Go, downloads or reference process.
Reproduce from the repository root with the optional Go toolchain:

```sh
tmp=$(mktemp -d)
base=https://raw.githubusercontent.com/grafana/loki/5aa4b9212d64d723f63b2770452054aa23df114d
curl -fsSL "$base/pkg/logql/log/parser_test.go" -o "$tmp/parser_test.go"
curl -fsSL "$base/pkg/logql/internal/logqltest/testdata/parsers.logqltest" -o "$tmp/parsers.logqltest"
cp test/fixtures/logs/loki-upstream/logfmt-producer.go.txt "$tmp/main.go"
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go build -o "$tmp/producer" "$tmp/main.go"
"$tmp/producer" "$tmp/parser_test.go" "$tmp/parsers.logqltest" > "$tmp/logfmt.json"
cmp "$tmp/logfmt.json" test/fixtures/logs/loki-upstream/logfmt.json
rm -rf "$tmp"
```

Source-range hashes include the final newline. The existing AGPL license and
attribution apply; no upstream runtime implementation is shipped.

| Artifact/source | SHA-256 |
| --- | --- |
| `logfmt.json` | `f88dc5c80516c39766c4cc47b29396662c18bd6644c29929e56c6a4889550be4` |
| `logfmt-producer.go.txt` | `40a227ce2cbabb9d4c906df5cfb998204efb3773a6324592dbe835c4b108489f` |
| Loki `pkg/logql/log/parser_test.go`, complete file | `346b91ced2d38e51d8cf7fd8acb3335ea5796e9286fd901b45dd625c0b3634ff` |
| Loki `parser_test.go:1191–1417`, complete selected lines | `b3edcdf714a59a9c4e0fa72d278a0aa29b04cca87c0c7fd86e45cb9ae705d160` |
| Loki `parser_test.go:1478–1511`, complete selected lines | `b1ae5d994b1ddcee6a48a0bbc0c25c11396bcdfc6ec5f5704efd887b680b6e18` |
| Loki `pkg/logql/internal/logqltest/testdata/parsers.logqltest`, complete file | `07d63941842085ca40bbc3944884377e802df38ede67027b9702594e840114ea` |
| Loki `parsers.logqltest:1–33`, complete selected lines | `34ffa7d094824610a97595ab6b51fb08771244ce67577b92505a6c66705f8b3d` |
| Loki `parsers.logqltest:84–114`, complete selected lines | `833dfc5e469102714b653d42fbf881ccf876fa14a9c3edd3f604931d20d9d90a` |

### Logfmt source-expression continuation

[`logfmt-expressions.json`](logfmt-expressions.json) adds **15 frozen rows** at
the same Loki revision, extracted by the attributed AGPL-3.0-only
[`logfmt-expressions-producer.exs.txt`](logfmt-expressions-producer.exs.txt):

- Nine original `TestLogfmtExpressionParser` rows from
  [`parser_test.go:1611–1757`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1611-L1757),
  preserving the shared UTF-8 line, extraction destinations/sources, initial
  labels and complete expected labels. The tenth, structured-metadata collision
  row is explicitly excluded, retained only in the raw provenance selection.
- The constructor rejection from
  [`parser_test.go:1765–1769`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1765-L1769).
  HTTP asserts rejection before storage, not Loki's exact parser-error envelope.
- All five field-grammar rows from
  [`logfmt/logfmtexpr_test.go:19–48`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/logfmt/logfmtexpr_test.go#L19-L48).
  The empty row asserts a nil path but does **not** assert a nil error. Its HTTP
  rejection follows execution of the pinned generated parser, not an upstream
  nil-error assertion. Grammar rows have no source line; a separately identified
  local line provides both accepted names for HTTP admission/lookup checks.

One HTTP/native-Parquet/S3 wrapper replays all rows through GET and form POST.
The nine extraction rows gain an isolated selector label, removed for comparison.
The `level="debug"` input also gains Pulso's ingest-promoted
`detected_level="debug"`: the test asserts it explicitly, then compares the
remaining complete label bag to the untouched upstream oracle. It is not an
exact Loki-ingest claim. The three expression engine cases are already in
`logfmt.json` and its 60-case replay; they are not counted again.

A separate HTTP control fails before the fix: digit/Unicode-normalized keys
return empty values and a missing original-stream field invents an empty
suffix. It now checks normalized aliases, absent-source defaults, quoted inner
source names, whitespace/EOF/NUL/bracket handling, literal backslashes and
non-path extra components. Invalid sources reject even after a nonmatching
filter, without provider reads. Work, retained-sample and result budgets still
fail whole. Explicit stages remain in Elixir; no native/storage/keyed format,
ceiling or serving tuple changes. Upgrade query-serving applications.

A temporary executable of the **actual pinned** field lexer/generated parser
compared 1,536 composed expressions with Pulso's acceptance and compiled source
key, with zero mismatches. This is grammar evidence, not full expression-stage
or engine parity. The value/ordering continuation below covers duplicate
destinations, destination-key capture and replacement-rune values separately.
Collision/previous-parser ownership, shared-source aliases and hints remain open.

The producer verifies complete source hashes and preserves raw rows/ranges,
locations, expected labels and explicit omissions. It needs only Elixir; normal
test replay reads JSON without Go, network or a reference process. Reproduce
with unmodified pinned files:

```sh
mise exec -- elixir test/fixtures/logs/loki-upstream/logfmt-expressions-producer.exs.txt \
  /path/to/loki/pkg/logql/log/parser_test.go \
  /path/to/loki/pkg/logql/log/logfmt/logfmtexpr_test.go > /tmp/logfmt-expressions.json
cmp /tmp/logfmt-expressions.json test/fixtures/logs/loki-upstream/logfmt-expressions.json
```

| Artifact/source | SHA-256 |
| --- | --- |
| `logfmt-expressions.json` | `a22cc43ee8ff380771ccc4abbd33006d6be9c20bc6f59e0b2661cba65b99e49b` |
| `logfmt-expressions-producer.exs.txt` | `1adcbebc0c7bb320fa4d8d933f357151f66f24ff4f0610bf3a746153c6aed0e8` |
| Loki `pkg/logql/log/logfmt/logfmtexpr_test.go`, complete file | `4e99db1e4d50399fb7935313efeff81514d655d0003d2544c8f0198c6f180177` |

### Logfmt explicit destination/value continuation

Two additional HTTP/native-Parquet/S3 tests cover source-derived controls for
[`NewLogfmtExpressionParser` and `Process`, parser.go:537–650](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser.go#L537-L650).
These are **Pulso-authored controls**, not additional copied upstream table rows.
The original 15-row expression and 60-evaluation parser/engine fixtures remain
unchanged and replay alongside them.

Nineteen retained cases run through GET and form POST in both strict modes:
**38 evaluations / 76 requests**. They distinguish destination-key capture,
source-before-destination and reversed line order, remapping priority, normalized
keys, last-definition duplicate destinations, repeated/empty/bare values,
literal/escaped replacement characters, lone surrogates and valid surrogate pairs.
The expected labels also matched execution of the actual pinned Loki parser
with its real unhinted `LabelsBuilder`, not a reconstructed oracle.

A second control checks metric sums **30** without strict parsing and **10**
with strict errors excluded. A later destination wins over its source; an emptied
replacement-character value is skipped by unwrap, not retained as zero or an
invalid numeric value. Invalid source expressions still reject before provider
reads even when overwritten by another definition and behind a nonmatching filter.

An additional temporary comparison exercised **13,104** actual pinned Loki
parser executions against Pulso, including swapped sources, duplicate
destinations, normalized/empty keys, malformed input, recovery and both strict
modes, with zero mismatches. The original `TestLogfmtExpressionParser` and
`TestXExpressionParserFailures` also passed in the pinned Go checkout. Normal
Elixir replay requires no Go, network or upstream process. No upstream runtime
implementation is shipped.

| Pinned runtime source | SHA-256 |
| --- | --- |
| Loki `pkg/logql/log/parser.go`, complete file | `e844f4036afcb11d8e84235a77b837a82f21d6c3819a05d3c150eb7812786bfd` |

At this continuation, shared-source aliases and cross-parser/base/metadata
ownership were still excluded. The ownership continuation below supersedes
those exclusions; hint-driven early exits remain open.
This is not full Loki, provider-capacity or deployed-client acceptance. Explicit
stages stay in Elixir; serving tuples, native/storage/keyed formats and resource
ceilings do not change. Upgrade query-serving applications.

### Logfmt ownership continuation

`logfmt-ownership.json` adds the complete previously omitted
[`parsers.logqltest:34–83`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/parsers.logqltest#L34-L83)
and structured-metadata conflict rows from
[`parser_test.go:1418–1429`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1418-L1429)
and [1728–1739](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L1728-L1739).
The attributed AGPL producer extracts source tables with Go's standard library;
it neither executes Loki nor derives expectations from Pulso. Normal replay uses
only Elixir/Rust and the JSON. The earlier fixture files remain byte-identical.

The three engine fixtures preserve all six load rows and 16 loaded entries,
including noise, original metadata and relative times. Their ten evaluations
include five instant, two range and three categorized selects. The two unit rows
add three evaluations (strict=false/true for extract-all, false for expressions).
All **13 evaluations / 26 GET or form-POST requests** replay through authorized
HTTP and actual native Parquet/S3. Flat table oracles remain flat; categorized
assertions come only from the upstream categorized engine expectations.

Two additional Pulso-authored HTTP checks exercise first-extraction history
across line rewrites/drops for logfmt, regexp, pattern, JSON and unpack; explicit
JSON overwrite; and explicit-logfmt suffix halting before a later strict error.
Shared-source aliases now choose one lexical destination, a deterministic member
of Loki's map-dependent possible outputs, not exact nondeterministic parity.
Native/reference metric controls check cross-parser metadata-owner transfers.
An additional **1512** temporary actual pinned pipeline comparisons agreed on
labels, categories and output lines. Selected upstream parser/label Go tests
also passed. No upstream runtime implementation is shipped.

This closes the selected ownership gaps, not general JSON source-order/flattening,
parser grammar/errors, hint-driven early exits, client or capacity acceptance.
Every query-serving app/native pair requires log tuple `{1,1,7}`. Storage/keyed
bytes and resource ceilings are unchanged.

| Artifact or selected source | SHA-256 |
| --- | --- |
| `logfmt-ownership.json` | `1e4bd917cacee53c3f4305ffae1a44ab0e3a3e3f30ed260a869f860582e12104` |
| `logfmt-ownership-producer.go.txt` | `aeeaa2b834b34ffbfa2f9f5febe18dec495b232c9d1a25211a0c4aa2bb6f1b1b` |
| `parsers.logqltest:34–83`, including final newline | `3cc5a88ffeaf5a7345f45ea30c8547f0f9442829214934ea2bd457b25084dc0c` |
| `parser_test.go:1418–1429`, including final newline | `bca8bf0dbfd04eed93b9c01afb1bbc6fd95c62fc7f38e8ed2789364d915fac74` |
| `parser_test.go:1728–1739`, including final newline | `027d8a086475c5eaa8f3647c31d2aa449ddc02f46d462edc32e61b1c840e1eba` |

## JSON parser continuation

[`json-parser.json`](json-parser.json) preserves all 16 rows from
[`parser_test.go:13–247`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/parser_test.go#L13-L247)
and all 42 evaluations from the complete seven-fixture JSON block in
[`parsers.logqltest:115–281`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/internal/logqltest/testdata/parsers.logqltest#L115-L281).
The AGPL-attributed standard-library producer reads source tables/engine text,
not Loki or Pulso runtime answers. Its header contains regeneration commands;
offline regeneration matches the fixture byte-for-byte. Normal replay needs
only Elixir/Rust, without Go or network access.

Two HTTP wrappers replay **58 upstream evaluations / 116 GET or form-POST
requests** through native Parquet/S3. They retain all 20 load rows/38 entries,
including noise and metadata, 32 instant/9 range/one categorized select,
eight metric-query rejections, complete labels and sparse samples. The engine's
explicit-field ownership control at169–170 remains included. Source-relative
times are translated together to a recent anchor. For the one stream-level
fixture, HTTP ingestion additionally promotes the original level to
`detected_level`; that value is asserted separately alongside the original
expected labels. Metric errors use Pulso's existing whole-query error envelope,
while retained error labels/details match the original result oracles.

The hinted unit row checks its unchanged public label result without exercising
the upstream hint object. All 31 path-bookkeeping entries and the original
harness are inert provenance, not a path/hint API conformance claim.

An additional retained HTTP control distinguishes nested/source-order collisions,
first duplicates, arrays/nulls, exponent/large-integer/signed-zero spelling,
strings containing number-like text, numeric unwrap, and missing versus null
explicit sources. Another **1216** temporary actual pinned pipeline comparisons
agreed on labels, categories and lines. Plain JSON evaluation remains in the
bounded Elixir path. Full malformed-input partial extraction/diagnostic parity
and hint-dependent early exits remain open. Explicit paths are covered by the
continuation below. No further native tuple, storage/keyed bytes or limit changes
are needed.

| Artifact or selected source | SHA-256 |
| --- | --- |
| `json-parser.json` | `a55a02235a2ef5c4b57337aff0a2fff7ec19ef2076344ba1a7a5f219b07bae1e` |
| `json-parser-producer.go.txt` | `273ac6910af6e6a624a317ad8bf374627150c228d67ea68625d0e67ad1a9fd6b` |
| `parser_test.go:13–247`, including final newline | `815a1bb938b2b070bfe97aac13ad8af518699724f3ba5a091c68186d5de1a504` |
| `parser_test.go:248–273`, including final newline | `a99a1d2da9cc993891e535fd2168fef10dda2b33779d13a4277ca55a1076d7b9` |
| `parsers.logqltest:115–281`, including final newline | `73cfb68f09361f06c8757bee70fbfe0acdcb57369387852508135955cd8f8f9f` |

## Explicit JSON path continuation

`json-expressions.json` and its attributed AGPL-3.0-only standard-library Go
producer preserve the pinned Loki revision above. The complete selections are
`parser_test.go:403–815`, `jsonexpr/jsonexpr_test.go:10–143`, and
`parsers.logqltest:283–303`, with harnesses recorded separately as provenance.
All complete-file and inclusive-range hashes are retained in the fixture.

The fixture contains **29 expression rows / 30 ordered field definitions**,
**five constructor failures**, **18 grammar rows**, and **five engine
evaluations** over two loads/eight entries. This is **57 source
rows/evaluations, 56 executable oracles**: the empty-path grammar row has no
upstream error assertion and remains inert provenance. The hinted row checks
its public output without exercising the hint object. Constructor admission
uses Pulso's existing error envelope, not exact Loki constructor error text.

The Elixir suite replays dot/bracket/UTF-8 keys, root/nested array indices,
source-order duplicate destinations, missing versus null destinations, exact
object/array source text and numeric lexemes. Engine queries retain all sparse
samples and expected rejections through authenticated GET/form POST and native
Parquet/S3. The earlier explicit-field ownership case at169–170 remains in the
plain-JSON fixture rather than being duplicated here.

Another **3072 actual pinned Loki pipeline comparisons** agree on labels,
ownership, lines and escapes. Local retained controls distinguish field-definition
order from source order, duplicate nested paths, raw object text from unescaped
array text, and pre-storage rejection of invalid paths. General malformed-input
partial extraction/diagnostic parity and hint-driven early exits remain open.
No additional native tuple, storage/keyed-byte or resource-limit change is needed.
Ordinary replay uses only Elixir/Rust; Go and the upstream checkout are needed
only to reproduce the fixture.

| Artifact or selected source | SHA-256 |
| --- | --- |
| `json-expressions.json` | `a2dcdea5eafc68f5738992e1760a90decf435387a76de8a56928b6d0f927790b` |
| `json-expressions-producer.go.txt` | `a2355d0dbc2c4b7b60afbde7afd6cb19e8fb45acd0792d30fbdc6908ca0ee05e` |
| `parser_test.go:403–758`, including final newline | `bbddc32b9b3b04173f25836930a64ebe34e1c7166af6294c11e4b4ed2e8e5f92` |
| `jsonexpr/jsonexpr_test.go:10–128`, including final newline | `66d2155d9a8a829268d4714e37cf70453e13ed374e5ba50cc80b4940a69a401c` |
| `parsers.logqltest:283–303`, including final newline | `7d23975cef34a6cbad539bd2f1374ba8a0ea807cf1246066f3d04055503dba88` |

## Query-policy continuation

The integration suite ports **25 admission cases** from the pinned Loki
revision, grouped in five checks and replayed through **64 authenticated
GET/form-POST requests**. The original queries and acceptance/rejection decisions
are unchanged:

- `pkg/logql/engine_test.go:40–81`: all seven
  `TestEngine_checkIntervalLimit` rows with a `10m` interval ceiling. Loki's
  downstream wrapper is a topology permutation, not another public query.
- `engine_test.go:583–609`: all three `TestEngine_MaxRangeInterval` rows,
  a `1d` ceiling, start 0/end 100000 seconds, 60s steps, forward direction and
  limit 1000. The 100,000 generated logs are not imported: the assertion is
  admission only, not sample-value parity.
- `pkg/querier/queryrange/roundtrip_test.go:1374–1426`:
  `TestTripperware_EntriesLimit`, preserving `{app="foo"}`, limit 10000,
  ceiling 5000, forward direction, six-hour window and exact HTTP error text.
- `roundtrip_test.go:1428–1475`: all six
  `TestTripperware_RequiredLabels` rows and exact missing-matcher errors.
- `roundtrip_test.go:1477–1580`: all eight
  `TestTripperware_RequiredNumberLabels` rows, including disabled, below, exact
  and above limits for log and metric queries, with exact rejection text.

Upstream middleware supplies canned response values, not input logs. Replay
therefore preserves admission oracles without inventing equivalent sample
results. Operator limits are injected through the request header using the
same values. Local controls separately exercise nonempty native Parquet/S3
queries, field/value APIs, patterns, offsets, scalar expressions, repeated
matchers, pipeline-label exclusion, default limits, malformed policies,
source-parenthesized ranges and request isolation. Rejections assert unchanged
provider counters; successful nonempty reads assert actual results.

The implementation adds `maxQueryInterval`, `maxEntriesLimitPerQuery`,
`requiredLabels` and `minimumLabelsNumber`, retaining existing header bounds
and fail-closed parsing. Interval diagnostic quantities use exact nanoseconds
rather than the source's `model.Duration` rendering. Missing required names
precede minimum-count errors. Required names are deduplicated; repeated matcher
names count toward the minimum, matching Loki's behavior. These are optional
client restrictions, not operator security quotas or selectivity proofs.
Lookback, byte and sharding policy fields remain unsupported.
No server resource ceiling, native tuple, stored or keyed bytes change.

| Complete pinned source or selected range, including final newline | SHA-256 |
| --- | --- |
| `pkg/logql/engine_test.go` | `af7679959cd5476a72ddb239d283098297c2583d5df22fd5f5484376b664db36` |
| `engine_test.go:40–81` | `a399d483686ab24a0433aca2a507acef2b73767b5f9b746ec479800deed1b50c` |
| `engine_test.go:583–609` | `eb7ab705ff0f27b4cd8bb99aebe845cf370e007402a2957b003e9fdf3a76f82a` |
| `pkg/querier/queryrange/roundtrip_test.go` | `f47963e9b1ecae4f9aaa8797c619e6a61c536d1ca336fcdd1823a42e9a390239` |
| `roundtrip_test.go:1374–1426` | `b8506849f12be9c9c7d24a3c515c37cf87a5a6363ae22d03d3a22a54a0401a38` |
| `roundtrip_test.go:1428–1475` | `9e74cfab7e372d9514b97912451138b63677622482f993a3564964d1f2e5f143` |
| `roundtrip_test.go:1477–1580` | `3cd792eba6686f2e9be4c7138a62300a6dfbd9bc2f83461d3f605a862e9d5c3d` |

### Request execution deadlines

`maxQueryTime` follows the same pinned revision's `QueryTimeout` minimum-positive
rule in `pkg/util/querylimits/limiter.go:76–85`, with reference tests
`limiter_test.go:34–110` (`TestLimiter_Defaults`), `112–154`
(`TestLimiter_RejectHighLimits`) and `156–192` (`TestLimiter_AcceptLowerLimits`).
The complete `limiter_test.go` SHA-256 is
`333744f72da824494eb612f4cbd4d4c22493305c359e653ad1e4ddfc03273a07`.
These three upstream tests were executed offline and pass. The defaults test's
second struct contains 29s but its actual timeout assertion uses the first
struct's 30s: replay follows the assertion, not the unused expectation.

Three local public regression checks exercise actual expiration, not copied
upstream response values. A blocked test-owned adapter covers all 15 finite
request shapes through GET and form POST under a 200ms client/3000ms server
deadline. Worker termination and repeated admission are checked. Separate
absent/empty/zero/larger-policy controls retain a 200ms server deadline.
These shorter times preserve the upstream rule rather than claiming unchanged
30s/29s/100s fixture geometry. Server-timeout-zero behavior is not imported:
Pulso requires a positive server timeout. Another check blocks an actual native
S3 Parquet GET, observes HTTP 504, releases the provider and reads the original
line again with absent/larger client policies. Nonempty Parquet/S3 controls
also check zero-policy and later-request isolation. HTTP helpers disable retries
so one assertion observes exactly one request, including deliberate 504s.

This is worker execution enforcement, not HTTP parsing/encoding or immediate
provider/native cancellation. Normal regression replay uses Elixir/Rust only.

### Range bracket and parenthesis placement

The 17 first/last evaluations from `range_aggregations.logqltest:99–147` now
replay in six equivalent source/range placements through HTTP GET and form POST
(102 evaluations / 204 requests). Original lines, timestamps, noise, gaps,
offsets and NaN outputs are unchanged. The five additional query spellings per
oracle are adaptations from `pkg/logql/syntax/syntax.y:125–151`, not new original
engine cases. This admits pipelines after brackets/offsets and recursive whole
range parentheses without splitting pipelines across a bracket. Local controls
cover post-unwrap filters, leading scalar/field arguments, grouping, both sides
of the depth bound and rejection before storage. Normal tests need no Go.

An executed comparison against the actual pinned `syntax.ParseExpr` agrees on
all 336 generated admission cases: 90 accepted and 246 rejected. These are
source-derived controls, not upstream table rows or a claim of all LogQL syntax.
The pinned `TestParse` suite also passes offline.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `range_aggregations.logqltest:99–147` | `2a07c4bbbbe8dd034f5a02336d29aee7b16eca8a110fd447fd291fb0f962dbb5` |
| `pkg/logql/syntax/syntax.y:125–151` | `7b9c0c44df2360690d334568ae8b8edff615968a79982e4f5cd839018d94b9a5` |
| Complete `pkg/logql/syntax/parser_test.go` | `0a8466fd5d9cab3f852f9a18a52dc54b7b1a1ab0c9c6af53ca1df21d9f38ce99` |

## Metric label replacement

The complete `functions.logqltest:24–132` block at the same pinned Loki revision
is transcribed into the existing HTTP suite (`:label_replace_batch`): four
fixtures, 64 loaded entries including noise, and 19 evaluations (17 successful
instant, one range, one invalid regex), replayed through 38 GET/form requests.
Original query semantics, labels, values and relative times are unchanged;
wall-clock bases keep the complete fixtures in the past. Loads with identical
stream labels are combined into one push, without removing entries. Each fixture
uses an isolated tenant/store. Regex rejection is checked before provider reads.

The cases cover complete-value capture matching, destination overwrites/deletion,
same-source rewrites, missing-source defaults and final stream/metadata/parser
ownership. Additional local controls cover newline defaults/flags, named/braced
captures, nesting, nonaggregating instant collisions and wrapper admission.
They are not counted as original upstream cases. No copied runtime implementation
or Go/download requirement is added to ordinary replay. Full parser-error wording,
distributed merging, provider layouts and deployed-client acceptance remain open.

The actual pinned `TestLogQLScripts/functions.logqltest` engine suite also passed
offline, not merely fixture extraction.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `functions.logqltest:24–132` | `293d2851e35622358f8530b6c96e2b19f04d7bbb674c13bbdcc7e4f97ec98a5b` |
| Complete `pkg/logql/evaluator.go` | `11389e04d4275073162d77235ed0883adb90be3d53366d8168ea2df072c65075` |

## Regexp, pattern and unpack continuation

The complete `parsers.logqltest:304–446` block at the same revision is transcribed
into the existing HTTP suite (`:remaining_parsers_batch`): ten fixtures, 66
original loaded entries and 30 evaluations (11 regexp, nine pattern, ten unpack),
replayed through 60 GET/form requests and actual Parquet/S3. Original lines,
labels, relative times, noise and output oracles remain intact.

The complete six-row regexp constructor and eleven-row pattern error tables
check admission through 34 HTTP requests. Nine unhinted unpack unit rows replay
their original input/labels/errors with an extra selector-only `job` label for
isolation; the one internal-hint duplicate remains excluded. Additional unpack
controls cover nonstring `_entry`, duplicate fields/entries and huge numbers.
These are 12 retained tests, not 12 upstream evaluations.

Four engine regressions failed before the fixes: numeric unpack fields were
promoted, malformed unpack input was ignored, regexp accepted no named capture,
and pattern lost short-line captures. All imported cases now pass. The actual
pinned engine block and regexp/unpack/pattern unit suites also pass offline.
Another 528 runtime comparisons agree on lines, flat labels and label categories,
including Unicode patterns, missing delimiters, duplicate ownership and chaining.
These generated controls are not additional original upstream table cases.

Malformed-object diagnostic/partial-extraction and hint-driven early-exit parity
remain open. No native/storage/keyed bytes, resource ceilings or ordinary
Elixir/Rust replay requirements change.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `parsers.logqltest:304–446` | `2e705e7e322c9d09e63430298d78ad5ec0b485c4ebb00b8a7d81cfd1b9022ab3` |
| `pkg/logql/log/parser_test.go:1083–1101` | `ebf455d4bf42d9c7479845ed93232efa36531ed3970a9cfebb8fff3aa779ba83` |
| `pkg/logql/log/parser_test.go:1781–1930` | `4534547e21cdd5ebcc78c9bdef21dbe24b9528e053515064308ec26c5d78ebb8` |
| Complete `pkg/logql/log/pattern/pattern_test.go` | `bc830a879c5681def5fcce992a7939e8fd0cc48c21734f2b550d6532ea4ef8ac` |

## Label formatter assignment continuation

The complete three-row `Test_validate` table and first four basic rows from
`Test_labelsFormatter_Format` at the same pin are transcribed into the existing
HTTP suite (`:label_format_assignment_batch`). Two retained tests cover duplicate
and `__error__` destination rejection, overwrites, renames and label interpolation.
Four original assignment rows plus seven runtime-derived controls each run with
stream, structured-metadata and parsed owners through instant/range GET/form
requests and actual Parquet/S3. Two additional admission controls cover mixed
template/rename duplicates and a literal `__error__` destination.

The generated controls are separate from the seven original table rows. They
cover self-renames, missing sources, rename/template ordering, stage-local label
snapshots and later parser history. Both retained tests fail before the fix
(seed380) and pass afterward (seed381). The broad Loki suite passes 514 tests
(seed382); actual pinned `Test_validate` and `Test_labelsFormatter_Format` pass
offline. The later Unicode-prefix control reproduced a byte/grapheme offset
error in both line and label templates, corrected by slicing at the byte offset.
All 723 additional actual pipeline comparisons agree on line, flattened labels
and categories, including Unicode, empty snapshot and chained-stage controls.

The previous local assertion that self-renames preserve their source was wrong:
the actual pinned pipeline deletes it. The regression expectation is corrected,
not skipped. Full template functions/control flow, error generation and
pipeline-error ownership remain open. No native/storage/keyed-byte or ceiling
change is required, and normal replay still needs only Elixir/Rust.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `pkg/logql/log/fmt_test.go:554–584` | `24b9d49ed4da93905dd11ef841b45d65b0c1b380e5a0aa983b39cbbac3160549` |
| `pkg/logql/log/fmt_test.go:817–834` | `2179cea410ff9655083b75a905cde08321cd238b343599f1c4cccb40a4a2c3aa` |
| Complete `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Complete `pkg/logql/log/fmt.go` | `0a1830ef146924bdeff22d371fcb2489b8ffb44e8eb08503ee17c6c0f0418132` |

## String template function continuation

At the same Loki revision, 18 engine evaluations and six original line-formatter
rows are transcribed into four retained HTTP tests tagged
`:template_functions_batch`. These cover `formatters.logqltest:1–29`, the
complete metadata block `41–54`, and the complete assignment block `56–83`.
The six unit rows are `combining`, `lower and upper`, `missing`, `function`,
`line` and `default`. Expected lines, counts, sparse points, relative times,
noise and input labels come from Loki, not Pulso. GET and form POST both run
through actual Parquet/S3. The six line rows also run as label-format controls;
that reuse is not six additional upstream source rows.

Replay adaptations are explicit: nil unit input becomes an empty line; a unique
`job` isolates each unit row; HTTP promotion adds `detected_level` alongside the
source stream's `level`; the assignment block uses `app=b` instead of `app=a`
to coexist with the preceding metadata block in one owned tenant. No metric
value, loaded time, repeat count or rejection expectation is changed.

Ten additional string controls cover direct/piped argument order, legacy
aliases, nested calls, defaulting, predicates, Unicode simple casing, quoted
delimiters, Go escapes, raw carriage returns, current-line access and the
64-level nesting boundary. Runtime-error controls check unchanged inputs,
metric fail-whole behavior and explicit error selection. Admission controls
check malformed and deliberately unsupported templates before storage. A fifth
retained test uses the public evaluator to check exact 16 MiB output and a
one-byte overflow in materialized/top-k/metric paths, with error preservation.

Actual pinned line/label formatter suites and the complete formatter engine
script pass offline. An additional 1,888 generated runtime comparisons agree,
including all 1,112,064 Unicode scalar values under both upper and lower, plus
strings across stream, metadata and parsed owners. Exact template error details
are excluded from that comparison:
Pulso retains its diagnostic text, not Go's source-position envelope. Runtime
and application-check evidence is in `.amp/in/artifacts/loki-template-functions-*`.

The three `now()` runtime-error engine evaluations at `33–39` are explicitly
not ported: date functions remain unsupported. Other functions, Go control
flow/variables, numeric/rune literals, field chains, trim markers/comments,
non-UTF-8 byte literals and exact error text/ownership remain open. Non-UTF-8
literal values reject rather than reaching byte-unaware JSON/regex consumers.
Output above 16 MiB fails the whole query instead of becoming TemplateFormatErr.
There is no native/storage/keyed-byte change. Normal replay needs Elixir/Rust,
not Go or downloads; this is not provider/client/capacity acceptance.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `formatters.logqltest:1–29` | `6aefa37f5baeeba7bc8ea31e0e182a74335691545374a406cf8e505c2c43b27e` |
| `formatters.logqltest:41–54` | `38b8ddc32e52d3da75d36709181c459f4e071dd0f3730dde1e6ff9b23b56430f` |
| `formatters.logqltest:56–83` | `acc2945087bed24eb9e0d08533eed060c0baa94d108d5ddbbf39128de34f430c` |
| `fmt_test.go:48–56` | `c942d399615b3e42aa13794fa612d9e284959f08836ff8253c94f4f9e5c7da23` |
| `fmt_test.go:106–114` | `abb5347cd8b87708129424b33c7bb664b111e71e41f5d4d5a537aeaec912d7c6` |
| `fmt_test.go:217–234` | `15bf7e6ff3e030443de95c71b6c16eadb543eead042cf2944130b4dd4435f317` |
| `fmt_test.go:334–351` | `7de769d404937b8acb90f348dd2ac56eee1fa80f9bd7fa4edee3d251b87b643e` |
| Complete `formatters.logqltest` | `c95c60b63d489f2c19bc2f60db92c31e2ea6716a82c469c04c87310fce99a759` |

### Replacement, cutset, title and encoding continuation

At revision `5aa4b9212d64d723f63b2770452054aa23df114d`, eight more
`pkg/logql/log/fmt_test.go` rows are adapted into two HTTP tests tagged
`:template_string_transforms_batch`: six line rows (`replace`, `title`, `trim`,
`urlencode`, `urldecode`, `base64encode`) and two label rows (`base64encode`,
`template error`). Original templates, non-time inputs, successful lines/labels
and error classifications are retained. The runtime-error details assert
nonempty diagnostics, not Go's exact text/source-position envelope.

Nil line inputs become empty strings, unit timestamps become test-local past
timestamps, and a unique `job` isolates each source row. None of the selected
queries depends on its timestamp. Successful line rows also run as label-format
controls; that is not six additional source rows. All streams publish in one
request/segment to avoid quadratic fixture catalog growth. GET and form POST
queries use authenticated HTTP and real native Parquet/loopback S3.

Nine independent successful controls distinguish rune boundaries from grapheme
boundaries, trim cutsets from literal prefixes, legacy argument ordering,
nonoverlapping/literal replacement, empty searches, historical title boundaries,
query-form escaping, Unicode round trips and Base64 padding. Seven extra failure
templates cover wrong types, malformed URL escapes and the deliberate non-UTF-8
decode fence. The error wrapper checks original input preservation, metric
failure, explicit error retention and error exclusion. A third retained test
checks exact 16 MiB outputs for replacement and both encoders, then adds one
source byte: intermediate amplification fails whole in materialized/top-k/metric
paths even if an outer trim would erase the result and errors are retained.
Empty-search amplification has a separate control.

Both complete actual pinned formatter unit suites pass offline. Another 2,096
runtime comparisons agree on lines, flattened labels and categories: 1,824
string/owner/formatter combinations plus 272 Unicode batches covering all
1,112,064 scalar values in word-initial, word-boundary and noninitial title contexts.
Exact error details and non-UTF-8 decoded byte strings are outside that comparison.
The latter deliberately become `TemplateFormatErr` instead of reaching
byte-unaware consumers. Base64 decoding, legacy counted `Replace`, regex/date/math
functions, numeric/rune constants, control flow and exact diagnostics remain open.
These are adapted unit rows replayed through HTTP, not a new upstream HTTP engine
fixture or deployed/provider/capacity proof. No native/storage/keyed bytes change.
Normal replay still requires only Elixir/Rust, not Go or downloads.

| Exact `fmt_test.go` selection, including final newline | SHA-256 |
| --- | --- |
| `66–83` (`replace`, `title`) | `f4c38bc4c96bf4cfbec29ca0f6a9fa5d3265d132c324d559964ccea2a2a38c54` |
| `95–105` (`trim`) | `52a814b0b557c35fba49ecc81a09b72746fba254587c006b9248bdda6c35fe5b` |
| `115–132` (`urlencode`, `urldecode`) | `4aca04e9684535cfbe0f4bafac49d63b9595ddaf5e8957a4743e55e70f935a57` |
| `446–454` (`base64encode`, line) | `a74e382211ce1da46fda47084dee479dc726df13d9dc0f11bab86bceeb4e63e5` |
| `608–617` (`template error`, label) | `b3c76e52154bf85c31399d1598caf4694506922e4dfe5cd9b1f4abf1874bae13` |
| `704–711` (`base64encode`, label) | `c23869eb29ea138584360b0dbcb54850cfa88061c029fbb681d5a634e63aadf9` |

### Integer-argument formatter continuation

At the same pinned revision, six complete `pkg/logql/log/fmt_test.go` line rows
(`Replace`, `repeat`, `indent`, `nindent`, `alignLeft`, `alignRight`) replay in
the HTTP test tagged `:template_integer_functions_batch`. Original templates,
input labels, lines and expected results are preserved. Nil lines become empty
strings, timestamps become test-local past timestamps, and unique jobs isolate
the rows. Every successful line row also runs as a label-format control; those
controls are not extra upstream source evaluations. Source coverage across the
three string-template increments totals 38 evaluations.

Nine independent successful controls cover Unicode-scalar width rather than
byte/grapheme width, negative/zero/exact widths, empty/trailing indentation,
empty repeats with maximum counts, counted/empty-search/nonoverlapping literal
replacement, Go base/separator integer syntax and signed-int64 boundaries.
An additional HTTP test checks eleven runtime-error templates, unchanged inputs,
metric error rejection/retention/exclusion, and pre-storage rejection of invalid
or deliberately unsupported literals. Pulso deliberately admits only signed-int64
integer constants: Go can parse unsigned constants outside that range and report
an overflow at execution. Integral float spellings, complex/rune literals and
exact Go error diagnostics remain outside this slice.

A public evaluator test tagged the same way checks exact 16 MiB outputs for all
six names, rejects larger intermediates through log/top-k/metric paths even
under an outer trim or explicit error retention, and rejects huge nonempty
expansions before allocation. Output limits count bytes even when alignment
widths count Unicode scalars. Expansions fail whole, not as TemplateFormatErr.

Both complete pinned formatter unit suites pass offline. All 6,156 actual pinned
pipeline comparisons agree on line, keep, flattened labels and all three label
categories across stream/metadata/parsed inputs; only exact error details are
excluded. These include 1,440 repetition/indentation/alignment comparisons,
4,536 counted-replacement comparisons, 132 integer-literal comparisons and
48 additional type/arity-error comparisons. Normal regression replay needs
Elixir/Rust only, not Go or downloads. This is adapted unit coverage over real
HTTP/Parquet/S3, not a new upstream integration fixture or provider/client/capacity
claim. Native/storage/keyed bytes do not change. Base64 decoding, other functions,
control flow/variables, field chains, float/complex/rune constants, trim
markers/comments, byte-string semantics and exact errors remain open.

| Exact `fmt_test.go` selection, including final newline | SHA-256 |
| --- | --- |
| `57–65` (`Replace`) | `9a2854f8874ea594dc8464b3db1657ada6236ec9a94e5b5f423146ff01887696` |
| `133–159` (`repeat`, `indent`, `nindent`) | `e115c45fa068df2678cc6ddb716d404fdbfeb250322bafe2c5305558489c6a7b` |
| `473–490` (`alignLeft`, `alignRight`) | `ea6870da491aecad9bb4958ccca5ba31502ab1c65b627a5c9f65b2b40607ce4b` |
| Complete `fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |

### Base64 decoding and public byte-slicing continuation

At the same pinned Loki revision, five complete `fmt_test.go` rows replay in
the two HTTP tests tagged `:template_decode_slice_batch`: the combined
`substr`/`trunc` line row, two Base64-decoding line rows and two label rows.
Original templates, labels, input lines and outputs are preserved; nil lines
become empty strings, label rows use an arbitrary unchanged `test line`, and
test-local timestamps/jobs isolate the rows. Line rows also run as label-format
controls, not additional source evaluations. The four string-template
continuations now total **43 source evaluations** through GET/form POST and
native Parquet/loopback S3; ordinary replay needs only Elixir/Rust.

Seven independent successful controls distinguish bytes from Unicode
scalars/graphemes, cover negative/zero/extreme bounds, Unicode/NUL round trips,
noncanonical Base64 padding bits, nested pipelines and CR/LF handling.
Thirteen corrupt-Base64 controls preserve frozen ordinary diagnostic strings
from the actual pinned runtime, including original byte offsets after Loki's
automatic padding. Corruption discards partial decoded bytes and does not
attach `TemplateFormatErr`. Thirteen runtime-failure templates check bounds,
types, arity, input preservation and metric error rejection/retention/exclusion.
The valid decoded metric control independently asserts 24 line bytes.

**Public-runtime trap:** Loki's formatter initialization installs Sprig's
byte-based `trunc`/`substr` from its function map. Similarly named rune-based
helpers and their Unicode unit rows in Loki are not public-runtime oracles.
Public `substr` does not clamp every invalid bound. Decoded/sliced non-UTF-8
values deliberately produce `TemplateFormatErr` until downstream byte handling
exists, even if a later encoding could restore UTF-8. These stricter cases are
not claimed as upstream byte-string parity. Exact template errors remain open.

Another **32,792** actual pinned pipeline comparisons agree on line, keep,
flattened labels and stream/metadata/parsed categories; 3,308 exercise runtime
template failures, with exact error details excluded. The 34,156 total inputs
include 5,824 byte-slicing, 28,316 Base64 and 16 type/arity cases. The other
**1,364** results contain non-UTF-8 bytes in Loki; those independently check
Pulso's deliberate error/input-preservation fence, not parity. Base64 inputs
include all strings of zero through four symbols over an eight-symbol
alphabet, valid padded/unpadded Unicode inputs with inserted bytes at every
position, all 256 single decoded bytes and noncanonical padding controls.
Scratch-only validation also confirms exact 16 MiB decoding, over-cap rejection
and shrinking slices on oversized source strings.

Both retained tests fail on the unsupported baseline (seed404) and pass after
implementation (seed405). Both complete actual pinned formatter unit suites
pass offline. The broad Loki/LogQL selection passes 496 tests (seed406); full
`mix precommit` passes 2,248 tests, one skipped and 37 opt-in exclusions
(seed407, 189.4s), including Mimir/Loki/Tempo replay. Strict Credo checks
445 files without findings. Final differential and byte-ceiling checks pass
again after the small bounds-check simplification. Formatting/whitespace checks
pass; temporary comparison tooling is removed. Evidence is under
`.amp/in/artifacts/loki-template-decode-*`.
This is adapted upstream unit coverage over Pulso's HTTP/storage path, not
execution of an upstream service integration suite or provider/client/capacity
acceptance. Native/storage/keyed bytes and resource ceilings do not change;
upgrade query-serving applications.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `fmt_test.go:84–94` (`substr and trunc`) | `e56d547a5aba5974ff73c636e61d5205ae03ecdeaaaf4c5575090fc45010685f` |
| `fmt_test.go:455–472` (`b64dec`, line) | `c7321f51aa576388f16607ca29a20724469b443336a82c72cfc8d76b40f685fe` |
| `fmt_test.go:712–727` (`b64dec`, label) | `c68dc074ee8c6b081b44a44f1b5a25757fecfaf26d65bbbf9adae94623456d10` |
| Complete `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Complete `pkg/logql/log/fmt.go` | `0a1830ef146924bdeff22d371fcb2489b8ffb44e8eb08503ee17c6c0f0418132` |
| Complete `vendor/github.com/Masterminds/sprig/v3/strings.go` | `4fd5d874e57fbe3491a16c828e1e3e43e3f01e8756cfcad7c9d3d098e4668fa6` |

### Conditional and logical template continuation

The three complete `contains`/`hasPrefix`/`hasSuffix` rows at pinned Loki
`fmt_test.go:160–186` replay in the HTTP tests tagged
`:template_conditionals_batch`. Templates, source labels and expected output
remain unchanged; nil input lines use an arbitrary unchanged `original`, with
test-local jobs/timestamps. Original line-format rows also run as label-format
controls. These bring the five string-template continuations to **46 source
evaluations** through GET/form POST and native Parquet/loopback S3. Ordinary
replay needs no Go, upstream service or downloads.

Seventeen separate successful controls cover nested `if`/`else if`/`else`,
empty/integer/string truthiness, lazy operand-returning `and`/`or`, pipeline
argument order, `not`, variadic `eq`, typed comparisons and byte-counting `len`.
They distinguish short-circuited argument evaluation from eager pipeline commands
and eager equality arguments, and exercise skipped over-cap expansions. Depth
controls cover pure, mixed conditional/parenthesis and else-if chains at 64;
13 invalid/unsupported/over-depth templates reject before provider requests,
including unreachable branches. Thirteen runtime-error templates check argument
types/arity, selected failures, unchanged line/destination and metric error
rejection/explicit retention. One public evaluator regression proves that
selected branches share the exact 16 MiB output cap and overflow fails whole,
including error-selected metric and top-k paths. Expanding a condition still
consumes its intermediate budget; skipped expressions do not execute.

Another **32,298** comparisons use the actual pinned Loki parser/pipeline over
both formatters and stream, metadata and parsed label owners. All agree on
admission, line, keep, flat labels and the three label categories, excluding
exact runtime error details. They comprise 23,958 variadic logical/equality,
4,356 ordered/binary comparison, 726 nested conditional, 2,904 lazy/error,
132 `not`/`len` and 222 syntax/admission inputs. There are 32,148 admitted
cases (including 10,260 runtime template errors) and 150 admission rejections.
Expected values come from actual Loki execution, not Pulso. This is not a
claim of exact errors, full template support or upstream HTTP acceptance.

The rendering/command extraction first preserved all six earlier formatter
tests (seed408). Both new HTTP tests failed on the unsupported baseline
(seed409); all three retained conditional/budget tests pass after implementation
(seed410). Both complete pinned formatter suites pass offline. The broader
LogQL/Loki selection passes 426 tests (seed411), and strict Credo checks 445
files without findings. Full `mix precommit` passes **2,251 tests, one skipped
and 37 opt-in exclusions** (seed412, 153.6s), including Mimir/Loki/Tempo replay,
strict compilation, unused-dependency and formatter checks. All three targeted
tests pass again after adding mixed-depth and long else-if boundary controls
(seed413). Formatting/whitespace checks pass, and temporary comparison tooling
is removed. Existing deliberate compaction/CAS/URI fault diagnostics remain
visible. Evidence is under
`.amp/in/artifacts/loki-template-conditionals-*`.

Only the selected branch runs, but all branches compile before storage; unknown
functions cannot hide in an unreachable branch. Conditional and parenthesis
nesting share the existing 64-depth limit, and selected branches share the
existing per-template output cap. `with`, `range`, variables, field chains,
float/complex/rune constants, other functions, trim markers/comments, non-UTF-8
bytes and exact diagnostics/error ownership remain open. Native/storage/keyed
bytes and resource ceilings are unchanged; upgrade query-serving applications.

| Exact source selection, including final newline | SHA-256 |
| --- | --- |
| `fmt_test.go:160–186` (three conditional predicate rows) | `79835f78420c095e892a72a2aa639c9d188753ca3867974ded25e8b5497f3868` |
| Complete `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |

### Template whitespace and comment continuation

Loki uses Go's `text/template` lexer for whitespace trimming and comments. Two
HTTP tests tagged `:template_whitespace_batch` adapt **15 original Go 1.27.3
lexer/parser rows**: 13 successful inputs and two malformed-comment rejections.
These are underlying formatter cases, not new Loki source rows or service
integration suites; the earlier Loki formatter count remains 46. The exact
inputs remain unchanged. Go's AST/token expectations become rendered strings
(for example, `"x"{{3}}"y"` becomes `x3y`) or admission failures, with job labels,
an unchanged `original` line and timestamps supplied by the isolated HTTP
harness. Both line/label formatting and GET/form POST use native Parquet and
loopback S3. Ordinary replay needs only Elixir/Rust. Copyright 2011 The Go
Authors and the BSD-3-Clause license are retained alongside the existing
[`Go-LICENSE`](Go-LICENSE); no upstream implementation is copied.

Sixteen additional successful controls distinguish literal ASCII whitespace
from Unicode spaces, vertical tabs/form feeds and dynamically rendered spaces.
They exercise negative integers versus trim markers, quoted/raw delimiter text,
inert unknown functions in comments, first-closing/non-nesting comments,
conditional branches and trim boundaries that must not cross another action.
Fourteen further invalid cases complement the two Go rejection rows; all 16
reject before provider requests, including malformed comments and unknown
functions outside comments in dead branches. Runtime-error controls preserve
the original line/destination and metric error rejection/explicit retention.
A separate public evaluator regression retains a full 16 MiB rendered-space
value despite adjacent trim markers, ignores an enormous commented expression,
and rejects a one-byte overflow through materialized/top-k/metric paths even
with explicit error retention. Trimming never rescues oversized output.

Another **29,826** actual pinned Loki parser/pipeline comparisons agree across
both formatters and stream/metadata/parsed owners on admission, lines, keep,
flat labels and all three categories, excluding exact runtime error details.
Inputs comprise 13,608 ordinary actions, 13,608 comments, 432 conditional
placements and 2,178 marker/ASCII/Unicode-boundary combinations. There are
12,120 admitted cases (including 1,260 runtime template errors) and 17,706
admission rejections. The oracle executes Loki, not Pulso. This is not a full
template/parser-error, deployed-service, provider or capacity acceptance claim.

All three new tests fail on the unsupported baseline (seed414), then pass
(seed417) after implementation and correction of one test's outer LogQL newline
escaping. Complete Go `TestLex`, `TestParse` and `TestErrors` selections pass
offline, as do both complete pinned Loki formatter unit suites. Full
`mix precommit` passes **2,254 tests, one skipped and 37 opt-in exclusions**
(seed418, 177.8s), including Mimir/Loki/Tempo replay, warnings-as-errors
compilation, unused-dependency and formatter checks. Strict Credo checks 445
files without findings. Existing deliberate compaction/CAS/URI fault diagnostics
remain visible; formatting/whitespace checks pass and temporary comparison
tooling is removed. Source mapping, hashes and runtime comparison evidence are retained under
`.amp/in/artifacts/loki-template-whitespace-*`.

Only space/tab/CR/LF adjacent source text trims; backward/forward byte scans
avoid unanchored regex backtracking. Comments are recognized immediately after
an opener or its two-byte trim marker; the first `*/` must be followed by `}}`
or exactly one whitespace byte plus `-}}`. Unknown functions inside comments
remain inert, but malformed comments and unsupported real actions still reject
before storage. Existing depth, query length and output ceilings remain;
native/storage/keyed bytes are unchanged. Upgrade query-serving applications.
Variables, other control flow/functions, non-UTF-8 bytes and exact diagnostics
remain open.

| Exact Go 1.27.3 `src/text/template/parse/` selection, including final newline | SHA-256 |
| --- | --- |
| `parse_test.go:186–187` (comment) | `a955d1ac227f184dcc7ce00579f8577b06f2abfcaa078e3e3a222b9276cb5a98` |
| `parse_test.go:257–263` (all seven trim rows) | `555fbb99647a57ab163bbce91c17ab60cf7e7ca9f44cfbe4e5b3a5b5907660d5` |
| `parse_test.go:270–271` (both newline-comment rows) | `c28b85fe81f354decd08f791617dbfdb07290b0ab7bf4cc741cdc7388f354a98` |
| `lex_test.go:94–99` (comment) | `32ac8237c9b7da76515fc7be9367ac30cf24b328d690a79747ca453593d6098d` |
| `lex_test.go:308–321` (both trim rows) | `12623eb31c86737165b43f79afc9fd5f2424d271b1ca24300a3e464f48598c6a` |
| `lex_test.go:378–385` (both malformed comments) | `30f92a0258b536802e778632067499ddb46fd56b2465e1d050f368c707fcf2c9` |
| Complete `parse_test.go` | `4e355963d9cf59595e91e032c3afada9a9448363e95d90042cdc6cf5747532ec` |
| Complete `lex_test.go` | `34435c1485c3c48dac3b3b41c2d80c5435de65dab7130d90b2fc183b280df9d0` |

### Integer arithmetic template continuation

Four complete rows from Loki revision `5aa4b9212d64d723f63b2770452054aa23df114d`
replay in two HTTP tests tagged `:template_integer_math_batch`: line-formatter
`mathint`, integer `min`/`max`, `mod`, and label-formatter `math`. Original
templates, field values and successful outputs are preserved. A job label
isolates each row; the unused nil input is replaced by an `original` sentinel.
Replaying the line templates as label assignments is an additional control,
not another upstream evaluation. The six function continuations now total
**50 Loki source evaluations**; Go whitespace/comment rows remain separate.

Sixteen additional successful controls cover exact decimal-string truncation,
base-zero parsing and separators, invalid/overflow-to-zero conversion, booleans,
signed64 wrapping, min-int division, signed remainder, variadics, pipeline
argument order, nested arithmetic, integer consumers and lazy logical branches.
Twelve runtime-error templates verify unchanged inputs, `TemplateFormatErr`
and existing metric-error selection through both formatting stages and
GET/form POST. Five unsupported templates reject without provider requests,
including an unreachable floating-point helper and Sprig's nonexposed `add1`.
A public evaluator regression keeps an arithmetic-generated exact 16 MiB repeat
and rejects one extra byte across materialized/top-k/metric/error-selected paths.

The public helpers are `int`, `add`, `sub`, `mul`, `div`, `mod`, `min` and `max`.
Conversions follow pinned Sprig/cast semantics: decimal fractions truncate
before base-zero integer parsing, never through float64; invalid/overflowing
strings become zero. Arithmetic wraps each step to signed64; division truncates
toward zero, remainder keeps the numerator's sign and zero divisors produce
formatting errors. Existing output/depth/query budgets remain unchanged.
Float helpers/constants, non-UTF-8 byte results and exact diagnostics remain open.
Upgrade query-serving applications; native/storage/keyed bytes do not change.

All **20,826** additional actual pinned Loki parser/pipeline comparisons agree
across both formatters and stream/metadata/parsed owners, including **1,458**
runtime template errors. Exact error-detail strings alone are excluded.
The comparisons comprise 3,372 casts (including long/malformed strings),
13,608 binary operations, 2,160 variadic operations, 756 pipeline-order cases,
756 nested-expression cases, 126 arity cases and 48 boundary/lazy/error controls.
These are direct runtime checks, not upstream HTTP, deployed-provider or capacity
acceptance. Temporary comparison tooling is removed after validation; evidence
is under `.amp/in/artifacts/loki-template-integer-math-*`.

The grammar-only extraction first passes nine existing checks (seed419).
All three retained arithmetic/budget tests fail before implementation (seed420)
and pass afterward (seed421). Both complete pinned formatter unit suites pass
offline. Full `mix precommit` passes **2,257 tests, one skipped and 37 opt-in
exclusions** (seed422, 291.1s), including strict compilation, unused-dependency
and formatter checks. Strict Credo checks 445 files without findings.
Existing compaction/CAS/URI fault diagnostics remain visible; no failing check
was suppressed. Normal Elixir replay needs neither Go nor network access.

| Exact pinned source selection, including final newline | SHA-256 |
| --- | --- |
| `fmt_test.go:235–243` (`mathint`) | `57f0b75a6e13514e5ceed04a2f9dbe15c3a5205422a89c91d29dff43f814b7f7` |
| `fmt_test.go:262–270` (`min`/`max`) | `b4b678c353989eb71fb1ef609636b6e13566b5dc978067e7fa7f4eb18cd38140` |
| `fmt_test.go:289–297` (`mod`) | `a642af8c6467e6e50fe993f8ac0b4027a531700fd23937a320e40f6f4a94c66f` |
| `fmt_test.go:594–599` (label `math`) | `b57bd9f70cba772193afe7e980d9dd17566e03364ab5e86bc0bb5312580fb777` |
| Complete `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Complete `vendor/github.com/Masterminds/sprig/v3/functions.go` | `e8965eeef01361ac86be9467b9034f8894445a525c2d8df19a78b0bd15242f8e` |
| Complete `vendor/github.com/Masterminds/sprig/v3/numeric.go` | `5465d61e984077478688b63cef6c73caf2550fd3d1d77a871775f03d147f63ff` |
| Complete `vendor/github.com/spf13/cast/number.go` | `32347774a230e033076555310e1d491256b18fdd55f67726e06d6f5d6300432f` |

### Quantity template continuation

All four complete quantity rows from Loki revision
`5aa4b9212d64d723f63b2770452054aa23df114d`, `fmt_test.go:383–418`,
replay in two HTTP tests tagged `:template_quantities_batch`. Original
templates, fields, input `1` and successful outputs are preserved. Only the
unused timestamp is translated to the test-owned clock, and a job label
isolates each row. Replaying these line templates as label assignments is an
additional control, not another upstream evaluation. The seven function
continuations now total **54 Loki source evaluations**; Go whitespace/comment
rows remain separate.

Twelve additional successful controls cover fractional byte rounding, large
unsigned quantities, both microsecond spellings, signed64 duration boundaries,
shortest-general float rendering, float truthiness/defaults/comparisons,
explicit integer conversion, integer consumers and lazy branches. Eighteen
runtime-failure templates verify unchanged inputs and destinations,
`TemplateFormatErr`, ordinary metric rejection and explicit error selection.
Both stages run through authenticated GET/form POST and native Parquet/loopback
S3. Two of the failure templates deliberately enforce the stricter overflow
policy below, rather than asserting upstream parity.

`bytes` returns float64 byte counts; `duration` and `duration_seconds` return
float64 seconds using integer seconds/remainders before float conversion, as
Go's `time.Duration.Seconds` does. Existing runtime quantity converters supply
their parsing, rounding, overflow and diagnostic profiles. Results retain
their float type through pipelines, comparisons and integer helpers; mixed
float/integer comparisons fail rather than coerce. Integer helpers truncate
in-range floats toward zero. Out-of-range float-to-integer conversion returns
a formatting error instead of selecting an implementation-dependent Go result;
plain rendering of large byte quantities remains supported. See the
[Go conversion specification](https://go.dev/ref/spec#Conversions_between_numeric_types).
Existing output/depth/query budgets and native/storage/keyed bytes do not
change; upgrade query-serving applications.

Of **66,570** additional actual pinned Loki parser/pipeline comparisons,
**66,438 agree exactly**, excluding exact error-detail strings, including
**4,254 upstream runtime errors**. The other **132 deliberately reject
nonportable overflowing float-to-integer casts**. These controls check input
and destination preservation and the specific Pulso error separately; no
unexpected differences remain. Both formatters and stream/metadata/parsed
owners are exercised. These are direct runtime comparisons, not upstream
HTTP, deployed-provider or capacity acceptance. Temporary comparison tooling
is removed after validation; evidence is under
`.amp/in/artifacts/loki-template-quantities-*`.

Both retained tests fail before implementation (seed423) and pass afterward
(seed424). Both complete pinned Loki formatter unit suites pass offline.
Final full `mix precommit` passes **2,259 tests, one skipped and 37 opt-in
exclusions** (seed426, 168.2s), including strict compilation, unused-dependency
and formatter checks. Strict Credo checks 445 files without findings. Existing
deliberate compaction/CAS/URI fault diagnostics remain visible; no failing
check was suppressed. The final runtime comparison rerun has the same totals.
Normal Elixir replay needs neither Go nor network access. General float
literals/math, other template functions/control flow, non-UTF-8 byte values
and exact diagnostics remain open.

| Exact pinned source selection, including final newline | SHA-256 |
| --- | --- |
| `fmt_test.go:383–391` (`bytes 1`) | `4e85b051cf5dfb0bf9de17ec3c30b5c3fb962781a0d853d96e47e7f5b5ee4dcc` |
| `fmt_test.go:392–400` (`bytes 2`) | `f63e31111dcf5151300dad2a61b82b578440cfff71c3c4bed12cad2f5613f3e9` |
| `fmt_test.go:401–409` (`duration 1`) | `3f43540d66132bd885074855ccd313fcd4c5b2523e155ab4539172a9169128b3` |
| `fmt_test.go:410–418` (`duration 2`) | `5ea9c7a343c7893b1088dbe401d790ff13f7531e68c8b7734157292189e351b9` |
| Complete `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| `metrics_extraction.go:569–583` (quantity conversions) | `f76b26735563d01b28b30ca1787b8fda63cc8eab1065789c87a7c9a25a7dcb0f` |
| Complete `pkg/logql/log/metrics_extraction.go` | `6118400c0a9cbd491b7843b45f54932cf8ec47ae6726e33f27b222318d802214` |

### Regex count template continuation

Four complete formatter rows from Loki revision
`5aa4b9212d64d723f63b2770452054aa23df114d` replay in two HTTP tests tagged
`:template_count_batch`: both line `count` rows and both label `count` rows.
Original templates, source labels, destinations and count outputs are
preserved. Nil line inputs use an `original` sentinel; label rows retain the
harness input `test line`. Unused timestamps translate to the owned clock,
and an extra job label isolates each case. The original invalid-count unit
case additionally replays as a template formatting failure; this adapts a
direct function-call error assertion, not an upstream HTTP oracle.
The eight function continuations total **58 source formatter evaluations**;
the invalid direct-call case and Go lexer rows remain separate.

Fifteen independent successful controls cover empty/nullable expressions,
greedy/lazy alternatives, non-overlap, Unicode scalars and combining marks,
multiline final-line anchors, strict end anchors, ASCII Perl classes/word
boundaries, Unicode case folding/script properties, absent labels, typed
integer consumers, conditional truthiness and lazy invalid-pattern branches.
Their expected outputs were checked against the actual pinned Loki pipeline.
Ten failure templates cover the original invalid pattern plus malformed/
unsupported patterns, arity, argument types and integer/string consumers.
Both stages run through authenticated GET/form POST and native Parquet/loopback
S3, including ordinary metric-error rejection and explicit error selection.

Counting runs natively with the existing bounded non-backtracking Go-style
regex policy, never PCRE execution or a retained match list. Each accepted
match plus one final probe consumes work. Native scans stop at the remaining
work allowance; metric expressions share their work budget, and log streams
share a request-local counter across entries/actions under the same ceiling.
A public evaluator regression proves success below the ceiling and fail-whole
work errors across materialized/top-k/metric/error-selected paths and across
entries. Another retained test checks **1,380 randomized/control pairs** against
an independent Elixir iterator and both exact native work boundaries. That
reference corpus uses portable regex forms; the separate pinned Loki controls
cover PCRE's multiline/script differences and an adversarial backtracking shape.

All **29,952** additional actual pinned Loki parser/pipeline comparisons agree,
including **3,120 runtime formatting errors**, excluding exact error details.
The matrix preserves 48 patterns × 26 texts × four templates × both formatters
× three label owners. It checks output, errors and separate stream/metadata/
parsed categories. An initial PCRE prototype had 68 differences on multiline
final-line anchors and Unicode script extensions; the final native path fixes
all of them. These comparisons are not upstream HTTP, deployed-provider or
capacity acceptance. Evidence is under `.amp/in/artifacts/loki-template-count-*`;
temporary comparison tooling is removed after validation.

All three initial regressions fail before implementation (seed427); all four
retained checks pass with the native implementation (seed430). Both complete
pinned formatter unit suites and `Test_InvalidRegex` pass offline. Final full
`mix precommit --seed 432` passes **2,263 tests, one skipped and 37 opt-in
exclusions** in 170.7s, including strict compilation, unused-dependency and
formatter checks. Strict Credo checks 445 files without findings. The final
rebuilt native artifact repeats the same 29,952 comparisons without differences;
Rust formatting and whitespace checks pass. Existing deliberate compaction/
CAS/URI fault diagnostics remain visible; no failing check was suppressed. Upgrade
serving app/native pairs to log codec tuple `{1,1,8}`; storage/keyed bytes and
configured resource ceilings are unchanged. Normal replay needs neither Go
nor network access. Regex replacement, other template functions/control flow,
exact diagnostics and full Go regexp syntax/capacity parity remain open.

| Exact pinned `pkg/logql/log/fmt_test.go` selection, including final newline | SHA-256 |
| --- | --- |
| `26–36` (line `count`) | `292e35a49274ec8ef93adce3f1a2975795e08e6c2bf98887fe6ba209a4f37b35` |
| `37–47` (line `count regex`) | `ca58cbeb0eb6957bb22739a7594f5de0c6456ce08fe13e8bd0f20fd105368f3f` |
| `645–653` (label `count`) | `e4e2858c721620c8238f1a09c33bf7e5153b7de18ba1aecaeba45f1221763ddf` |
| `654–659` (label `count regex no matches`) | `3145da2249a41d71bca29d13190462aff25a8ab0dc546b06f4fcd0a135da93ec` |
| `808–814` (invalid `count` call) | `80e206dde4626f211780471a531dc339897a8a26ff2777582a5dc8cf3324f0db` |
| `779–781` (label harness inputs) | `6e1140e6e70063b3b41c26b619e93f32f9b3c6b7707fbdcf6f2bd54750f04e92` |
| Complete file | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |

## Regex replacement template continuation

The retained HTTP suite replays both complete original Loki formatter rows
at pinned `pkg/logql/log/fmt_test.go:187–204`: `regexReplaceAll` and
`regexReplaceAllLiteral`. Templates, input labels and output oracles stay
unchanged. The nil line becomes an `original` sentinel; unused timestamps
translate to the owned clock and an extra job label isolates each case.
Both formatters replay through authenticated GET/form POST and native
Parquet/loopback S3. The nine function continuations now retain **60 source
formatter evaluations**. The two direct invalid-function assertions at
`794–807` adapt separately to template formatting failures; they are not
upstream HTTP oracles.

Seventeen independent successful controls cover whole-match/numbered/named
captures, maximal Unicode reference names, leading zeros, missing/unmatched
captures, literal dollars, malformed references, literal replacement,
non-overlap, Unicode-scalar empty-match progress, multiline/strict-end
anchors, ASCII classes/word boundaries, script properties, pipeline argument
order, absent labels and lazy invalid-pattern branches. Nine failure templates
cover invalid/unsupported patterns, arity and argument types. Both stages also
exercise metric-error rejection and explicit error selection. Successful
expected outputs agree independently with the actual pinned Loki pipeline.

[`regex-replace.json`](regex-replace.json) preserves all **87** original
Go **1.27.3** `regexp/all_test.go` replacement table rows, including the
literal harness's extra replays of dollar-free expanded-mode rows. Across
**135 mode evaluations** (72 expanded, 63 literal), Pulso returns **135 exact
outputs**, including the ten formerly rejected repeated-name cases.
The continuation below removes only Pulso's former rejection metadata;
all original success oracles remain unchanged. Each row records its table and line;
the file records the pinned URL and complete-source hash. The attributed
BSD-3-Clause [`regex-replace-producer.go.txt`](regex-replace-producer.go.txt)
extracts original AST string values using only Go's standard library and
rejects changed input before emitting JSON. See adjacent `Go-LICENSE`.
No expectation comes from Pulso. Offline build/vet, byte-identical regeneration,
an incorrect-source negative control and an independent Elixir source audit
pass. Ordinary replay needs neither Go nor network access.

All **107,520** additional actual pinned Loki parser/pipeline comparisons
agree, including **15,360 runtime formatting errors**, excluding exact error
details. The matrix preserves 28 patterns × 16 texts × 20 replacements ×
both helpers × both formatters × three label owners, comparing output and
separate stream/metadata/parsed categories. An independent Elixir iterator
adds **2,760** replacement comparisons alongside the existing 1,380 count
comparisons. These are not upstream HTTP, deployed-provider or capacity
acceptance. Evidence is under `.amp/in/artifacts/loki-template-regex-replace-*`.

Replacement uses two bounded native passes with no retained match list:
byte admission proves the complete result fits 16 MiB before allocation,
then rendering writes directly into an exactly sized Erlang binary. Each
pass charges matches, a final probe, dollar scanning and capture-name bytes,
even for unknown references that emit nothing. Public evaluator controls
exercise shared work across entries/actions/helpers, exact long-name work
boundaries, zero-output reference exhaustion and exact/oversized intermediate
output behavior under later truncation and explicit error selection. Work
and output exhaustion fail whole, never as preservable formatting errors.
Upgrade serving app/native pairs to log tuple `{1,1,9}`; storage/keyed bytes
and configured ceilings are unchanged. Numeric/repeated names and literal
quoting are covered by the continuation below. Full Go regexp syntax/capacity
parity, other functions/control flow and exact diagnostics remain open.

All five retained checks pass against the final rebuilt native artifact
(seed439); the additional long-name boundary fails without its byte charge
(seed438). All 107,520 pinned pipeline comparisons repeat without differences.
Full `mix precommit --seed 440` passes **2,267 tests, one skipped and 37 opt-in
exclusions** in 433.7s, including strict compilation, unused-dependency and
formatter checks. Strict Credo checks 445 files without findings; all 133 codec
Rust tests, Rust formatting and strict all-target Clippy pass under `mise exec`
(Rust1.97.1). A PATH-only Rust1.94.0 Clippy probe reported two lints in unchanged
trace pruning; none was suppressed. Full logs retain timeout/CAS/acceptor
diagnostics; these checks do not close the separate reliability follow-ups.

| Exact pinned source selection, including final newline | SHA-256 |
| --- | --- |
| Loki `fmt_test.go:187–195` (`regexReplaceAll`) | `b0a5e173c05c837a80fad54e169b1516ab2caee067cd0e22b9559a76c6512409` |
| Loki `fmt_test.go:196–204` (`regexReplaceAllLiteral`) | `1f24db67a28dd9c6855da9a93f86258fa9636dbe474ef89fae0cfe9103d01437` |
| Loki `fmt_test.go:794–800` (invalid expanded call) | `0ad2b1ee699b1d6ba9392b96dc438c4ed35295a17ce907b18d7a105c069f5d16` |
| Loki `fmt_test.go:801–807` (invalid literal call) | `aedbf8d9b6c8d555284b1e5d0f7a0f65ea0733da09f0826e02c51a682155b94a` |
| Complete Loki `fmt.go` | `0a1830ef146924bdeff22d371fcb2489b8ffb44e8eb08503ee17c6c0f0418132` |
| Complete Loki `fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Complete Go `regexp/all_test.go` | `caffaf144a8db14a7fd0476f8cc4ba51dba750f3b3f32e26cfee6629ee33057c` |
| Complete Go `regexp/regexp.go` | `a3e6d904aba2f2e5e4fb8fbbdbe817b317f9ce4bd1e1f1edfe4bcb0b0c552fcc` |

## Named-capture and literal-quoting continuation

The existing `:template_regex_replace_batch` now requires every original
replacement oracle to succeed, including repeated names. Capture numbering is
unchanged; named expansion chooses the first participating capture in source
order, with an empty match taking precedence over later captures. Both Go
capture-name spellings accept ASCII letters/digits/underscores, including
numeric and long names. Reference parsing preserves leading-zero and
one-billion cutoff behavior, including numeric names beyond unsigned-int64.

Twenty-two additional successful HTTP controls cover those rules, lexical
capture-header lookalikes in classes/escaped literals, counting, and Go's
`\Q...\E` quoting. They retain all five quoted-literal patterns from Go
1.27.3 `regexp/syntax/parse_test.go:152–156`; private AST expectations are not
presented as HTTP output oracles. Additional controls cover quoted metacharacters,
Unicode case folding, quoted capture headers, EOF termination and both sides of
the duplicate-lookup work boundary. Four invalid-pattern controls retain Go's
empty-quoted-repeat rejection at line500, class-quote rejection and invalid
capture names. Original replacement/quoted controls are verified independently
with the three actual pinned Loki helper functions extracted from `fmt.go`
and executed by Go's `text/template`, not Pulso-derived expected values.
An initial run uses Go's standard-library `regexp`; a second links Loki's actual
`grafana/regexp` dependency at `v0.0.0-20250905093917-f7b3be9d1853`, selected by
both its pinned `go.mod` require and replace directives. Both oracle output files
are byte-identical, and the final native artifact repeats all 97,332 comparisons
without differences. This remains isolated helper/pipeline evidence, not a
whole-server run.

The HTTP fixture publishes all 55 rows together, including the octal continuation
below, retaining every input and isolated stream label while avoiding a growing
manifest of one-row segments.
This is a test-layout change, not production query acceleration. Both formatting
stages and GET/form POST still replay through native Parquet/S3.
Duplicate candidate probes share the existing work ceiling in both admission
and rendering passes; exhaustion fails whole even under explicit error
selection. No ceiling or storage/keyed bytes change. Upgrade serving app/native
pairs to log tuple `{1,1,10}`. Selector and `regexp` parser admission remains
unchanged; this is not full Go regexp, provider or capacity acceptance.

| Additional pinned Go source, including final newline | SHA-256 |
| --- | --- |
| Complete `regexp/syntax/parse_test.go` | `4cd14979742f9b43d287c0c27f62bacead4b1e4e6fe56f8d47570cfff9481d2b` |
| `parse_test.go:152–156` | `c56d70060d4eeec1300b4bb42441e6a69a38cbad7a9ad3c812f51ee4d698c87d` |
| Complete `regexp/syntax/parse.go` | `f93b85f4b63a6f6ecb5c1452a6c0ded1fef5953f9c73e5d814dfc08207e7d1b6` |
| `parse.go:1030–1044` (quote execution) | `e57b5caa404de1e2ccce534d8c18c8416a7d36e01834a9f1a2230bdf639e7019` |
| `parse.go:1255–1270` (capture-name grammar) | `aab63e277415f38f5adb192134bf5820651cb16593ba61570d1871a21f335256` |

## Octal regex-escape continuation

The same HTTP wrapper retains both complete hex/octal patterns from Go 1.27.3
`regexp/syntax/parse_test.go:123–124`, exercised with independently chosen inputs
on both sides of the original ranges. These are parser-table patterns adapted
to matching behavior, not original HTTP oracles or private AST assertions.
Twelve additional successful controls cover NUL, one/two/three-digit consumption,
the first non-octal digit, the fourth digit, Unicode values above 255, case
folding, classes, quantifiers, captures, literal replacement and quoted/escaped
backslashes. Four invalid escape controls preserve input/error behavior.
All fourteen successful additions replay in both formatting stages through
GET/form POST and authenticated native Parquet/S3 storage.

Octal escapes denote Unicode characters through 0777, not raw bytes. A leading
zero needs no further digit; a nonzero first digit requires a second octal digit,
so lone backreferences still reject. Normalization preserves literal syntax,
class ranges and capture numbering. The extension stays template-local, with
unchanged selector/regexp-parser admission, output/work limits and keyed bytes.
It shares the unreleased log tuple `{1,1,10}` with the continuation above.

The original octal pattern fails before implementation (seed639), and all four
expanded wrappers pass seed641 with ordinary timeouts. A temporary independent
Go executable runs the three extracted pinned Loki helpers through real
`text/template`. All **144,480** comparisons with Pulso's actual pipelines agree,
including **2,268 runtime errors**, excluding diagnostic wording. The matrix
covers every three-digit octal escape from 000 through 777 in plain, capture,
class-range, negated-class and case-folded repeated patterns, with neighboring
characters, plus invalid/short/fourth-digit and lexical-boundary controls.
Both formatters, three label owners, four replacements, both replacement modes
and counting participate. These are helper/pipeline comparisons, not a whole
upstream server, provider, deployed-client or capacity test. The second executable
links the exact `grafana/regexp` version above and produces a byte-identical
oracle file for this matrix too. Offline/read-only build and vet pass; actual
linked identity is captured, not tool/cache attestation. Normal regression replay
uses only Elixir/Rust; temporary comparison tooling is removed afterward.

Final full precommit seed642 passes **2403 tests**, one skipped/37 excluded, in
249.3s with ordinary timeouts and four concurrent cases. All 133 codec Rust tests,
strict all-target Clippy, strict Credo over 447 files, formatting and whitespace
checks pass. Existing deliberate fault diagnostics remain visible; these checks
do not close historical provider/load-sensitive timeout/lifetime findings.
All work remains local, uncommitted, unpushed and undeployed.

| Additional Go source selection, including final newline | SHA-256 |
| --- | --- |
| `regexp/syntax/parse_test.go:123–124` | `19a0ffa83b31e73f0ee9b933c5451152b1b7795a8114df94e851ea46f59350d7` |
| `regexp/syntax/parse.go:1443–1486` | `e167cf6c5d6a67d7ebc30a509dc5a4f857c1d073dc1d9e6398d6d03ad9cec4b1` |
| Complete pinned Loki `go.mod` | `ab9a2d3797d06db7dc71bb587fc229135c1f61fae7b88aa450cdf3459986318a` |

The fork module content checksum is
`h1:cLN4IBkmkYZNnk7EAJ0BHIethd+J6LqxFNw5mSiI2bM=`; its `go.mod` checksum is
`h1:+JKpmjMGhpgPL+rXZ5nsZieVzvarn86asRlBg4uNGnk=`. Fork audit and comparison
logs are retained under `.amp/in/artifacts/loki-template-*`.

## Shared hexadecimal regex controls

The local `:regex_hex_batch` wrapper uses independent Go matching answers from
[`regex/go-controls.json`](../../regex/go-controls.json), not extra Loki engine
or HTTP source rows. Eight lines/label values are ingested together and read
from native Parquet/S3. Four anchored patterns exercise padded hex, counted
repetition, Unicode folding and Unicode scalars, with positive/negative stream
selectors, native and post-format line filters, label filters, GET/form POST and
native log metrics. A separate Mimir wrapper covers 18 anchored patterns over
the same eight values through remote write and HTTP/Parquet queries, including
optional/counted lazy controls. Six Go-checked malformed lazy
suffixes also exercise native byte and Mimir HTTP admission: `a???` formerly
selected a row instead of returning HTTP400 (seed646). Valid `a??` stays valid.

Both public wrappers fail on valid braced-hex repetition before the guard fix
(seed643), as does the shared Go-oracle matcher regression. Hex escape braces
must not become repetition counters; actual following nested/possessive
repetitions and malformed escapes still reject. Existing bounds and native ABI,
storage/keyed bytes are unchanged. Upgrade all query-serving native artifacts.
This closes that shared lexical bug, not full Go regex or Tempo regex parity.

The following local `:regex_literals_batch` wrapper replays 30 quote/octal
patterns over 15 ingested values, with independent substring and whole-label
answers from the same shared fixture. It covers positive/negative stream
selectors and label filters, native/post-format line filters, GET/form POST,
native log metrics and named octal capture extraction through actual Parquet/S3.
Omitted terminal `\E` never consumes whole-label anchors. The corresponding
Mimir and Tempo wrappers exercise their public surfaces too; these are local
controls in upstream harnesses, not additional upstream integration rows.
The shared fixture README retains Go source hashes and matching-oracle
provenance, including all five complete quote-parser patterns and the octal
pattern. Native ABI, storage/keyed bytes and existing resource ceilings stay
unchanged; all query-serving app/native pairs need the fix. Other class/capture
grammar, full regex/service/client parity and provider capacity remain open.

The local `:regex_admission_batch` replays all 53 shared Go admission patterns
over eight pushed log values through positive/negative whole-label selectors
and substring line filters, GET/form POST and native Parquet/S3. Independent
answers distinguish 999/1,000/1,001 counts from literal braces. Invalid patterns
must reject before storage. The shared
[`regex/README.md`](../../regex/README.md) retains eight original Go parser
patterns plus local controls, not additional Loki engine/integration exports.
Existing work/output/program limits and native/storage/keyed formats stay
unchanged. Both shared fixtures match Loki's pinned regexp fork byte-for-byte.

The same batch additionally replays 32 local literal-class cases from
[`regex/class-controls.json`](../../regex/class-controls.json), keeping `&&`/
`~~` literal through whole-label and substring filters, negation, folding,
POSIX classes and adjacent ranges. All three HTTP wrappers reject valid
classes before implementation (seed 669); the affected 370-test suites pass
seed 670. All three frozen fixtures match the pinned Loki fork. Other literal
bracket/`--` class forms are covered by the following increment; complete
regex/client/capacity parity remains open.

The same wrapper now also replays 72 local bracket/range admission cases from
[`regex/range-controls.json`](../../regex/range-controls.json). Independent Go
answers distinguish literal `[` and left-to-right scalar hyphen ranges from
POSIX tokens, invalid descending ranges and nonscalar endpoints. Quotes and
repetitions after literal brackets retain their admission/anchoring policies.
All three HTTP wrappers rejected valid `[[]` before this fix (seed 673).
These are local controls, not extra Loki engine exports; existing limits,
native/storage/keyed formats and the three earlier frozen fixtures are unchanged.

The wrapper additionally replays 44 raw-header and 58 inline-flag cases from
[`regex/name-controls.json`](../../regex/name-controls.json) and
[`regex/flag-controls.json`](../../regex/flag-controls.json), through line and
whole-label predicates and GET/form POST. Invalid names/header repairs reject
before storage. Empty/repeated flags preserve matching and scope; the shared
matcher also checks actual Go anchored capture maps and `U`/lazy greediness.
All three wrappers failed the respective baselines (seeds 680/682). These are
independent Go controls, not additional Loki engine exports. All six producer
modes also match Loki's pinned regexp fork; see the [regex provenance](../../regex/README.md).

The local `:regex_parser_flags_batch` replays the shared flag fixture's eight
named patterns through extraction and metric filtering over eleven retained
values. Go's raw-pattern unanchored capture maps supply exact parser-label
expectations, separately from whole-label answers. The unchanged admission path
rejects valid quantifiers after directives with PCRE's `quantifier does not
follow a repeatable item` (seed686). Admission now inspects native capture
metadata without compiling PCRE or returning a capture/name map. The six
original Loki regexp constructor rows remain intact; four separate local
controls exercise quoted/class fake headers, unnamed directives and a real
zero-repeated capture. These are not additional exported Loki source rows.
That admission increment required tuple `{1,1,11}`. The subsequent
`:regex_erased_capture_batch` also checks empty values and first-extraction
ownership for optimized-away groups, requiring then-current tuple `{1,1,12}`.
Its executed Go oracles, original-schema ceiling and independent actual Loki
pipeline comparisons are documented in the [capture continuation](../../regex/README.md#captures-erased-by-optimization).
Stored/keyed bytes and thread pools are unchanged.

The subsequent flag-separated nesting continuation requires current tuple
`{1,1,13}`. `:regex_parser_flags_batch` now checks 24 named patterns over eleven
retained values, including 15 source-derived nested-repetition controls. Exact
Go capture maps cover inner/outer U/lazy choices, subsequent flags, no-series
metric filters and unchanged original capture indices. The three existing
Mimir/Loki/Tempo admission wrappers also replay the new fixture through
HTTP/Parquet and local S3-compatible HTTP stores (seed706), rejecting invalid
patterns before storage. These are local controls, not extra exported Loki rows;
see the [nesting provenance](../../regex/README.md#flag-separated-nested-repetitions).

## Rune constant template continuation

Tests tagged `:template_runes_batch` preserve all eight character-number
oracles at Go **1.27.3** `src/text/template/parse/parse_test.go:68–75` and all
three malformed character-number inputs at `82–84`. All seven character
tokens in `lex_test.go:157–174` are represented too. That lexer row tests
tokenization, not execution of several constants as one command; HTTP replay
evaluates each literal separately. These source-number/lexer cases are
separate from the 60 Loki formatter evaluations. Original BSD-3-Clause notices
are retained in adjacent `Go-LICENSE`.

The constants retain their integer Unicode code points, including octal/hex
byte escapes: `'\xFF'` means 255, not a non-UTF-8 string. Twenty-five successful
templates retain the eight original numeric outputs plus Unicode boundaries,
all source lexer tokens, NUL, apostrophe/backslash, literal carriage return,
arithmetic, integer argument types, comparison, default/conditional truthiness,
pipelines and lazy branches. Expected outputs independently agree with the
actual pinned Loki parser/pipeline. An AST-based independent Go reader verifies
all eight original outputs and three rejection inputs against the installed
pinned source; the complete `TestNumberParse` and `TestLex` suites pass offline.

Fifteen malformed templates reject in actual pinned Loki and Pulso, including
inside unreachable branches and after an absent line filter. Authenticated
GET/form POST controls prove no object-store requests occur. Four wrong-type/
non-callable-value templates preserve original lines/destinations with
`TemplateFormatErr`, including ordinary metric rejection and explicit error
selection. Both formatters replay through native Parquet/loopback S3; normal
replay needs neither Go nor network access.

All **10,400** additional actual pinned Loki parser/pipeline comparisons agree,
including **2,600 runtime errors**, excluding exact error details. They cover
650 literal spellings × eight typed/rendering/conditional templates × both
formatters, with 285 distinct Unicode scalar values, output and separate label
categories. These are not upstream HTTP, deployed-provider or capacity acceptance.
Evidence is under `.amp/in/artifacts/loki-template-runes-*`.

Both initial regressions fail before implementation (seed441); all three
retained checks, including the previous integer-admission control, pass afterward
(seed442). Full `mix precommit --seed 443` passes **2,269 tests, one skipped and
37 opt-in exclusions** in 238.5s, including strict compilation, unused-dependency
and formatter checks. Strict Credo checks 445 files without findings. Full logs
retain timeout/CAS/URI diagnostics; reliability follow-ups remain open. Existing
budgets and native/storage/keyed bytes are unchanged by rune support; upgrade
query-serving applications. Float/complex literals, other template functions/
control flow and exact diagnostics remain open.

| Exact Go 1.27.3 `src/text/template/parse/` selection, including final newline | SHA-256 |
| --- | --- |
| `parse_test.go:68–75` (all character-number oracles) | `de4ab13db6ba22d3f57cb40a3e7ba9019df822342b02b2b58a50c01ded6075e6` |
| `parse_test.go:82–84` (all malformed character-number inputs) | `9aa89eb4942766707bba50070d3500f1644a314d2f924e9619d3a9133fb20fa3` |
| `lex_test.go:157–174` (all character tokens) | `31c12ac45bf2c2afd966c1c81240f4e889deedae736dc65dbcc17b5e5412dd53` |
| Complete `parse_test.go` | `4e355963d9cf59595e91e032c3afada9a9448363e95d90042cdc6cf5747532ec` |
| Complete `lex_test.go` | `34435c1485c3c48dac3b3b41c2d80c5435de65dab7130d90b2fc183b280df9d0` |
| Complete `node.go` | `22404f3d7c64251c6f359b97d61c2a52418e9f3a34e18811efe2f5924f756b98` |
| Complete `lex.go` | `51bf08bd87c3ee27e5128dde81449f4efe6f52aab9bf7c88349f66c077c1e40b` |

## Floating-point constant template continuation

Tests tagged `:template_floats_batch` retain all ten non-complex floating-number
rows from Go **1.27.3** `src/text/template/parse/parse_test.go:45–54`, separately
from the Loki formatter row total. An independent Go source-AST audit evaluates
the original numeric oracle expressions, then verifies all ten retained values.
Original BSD-3-Clause notices remain in adjacent `Go-LICENSE`.

Twenty-six successful templates cover those rows plus decimal/scientific/hex
spellings, separators, signed zero, subnormals/underflow, maximum finite float,
typed comparisons, conditional/default behavior, integer helpers and string
functions' typed integer parameters. Direct integral constants can satisfy
integer parameters; parenthesized/piped floats cannot. All retained outputs
independently agree with the actual pinned Loki parser/pipeline. Nine malformed
or nonfinite spellings and one deliberately unsupported complex literal reject
before storage, including skipped branches and nonmatching line filters. Runtime
type errors preserve inputs and the existing metric-error selection policy.
Both formatting stages replay through authenticated GET/form POST and native
Parquet/loopback S3; normal replay needs neither Go nor network access.

Of **6,912** actual pinned Loki parser/pipeline comparisons, **6,874** agree
exactly excluding error details, including **2,834 runtime errors**. The other
**38 deliberately reject nonportable out-of-range float-to-integer casts**:
32 coercing integer-helper cases and six direct positive-int64-boundary cases.
Go's number-node integral test itself casts float64 to int64; on this arm64
runtime its rounded upper boundary can produce a saturated integer. Pulso
requires an in-range value rather than selecting that machine-specific result.
There are zero unexpected differences. These are not upstream HTTP, deployed
provider, full formatter or capacity acceptance. Evidence and the explicit
rejection ledger are under `.amp/in/artifacts/loki-template-floats-*`.

Both initial regressions fail before implementation (seed444); all three
retained checks, including the previous integer-admission test, pass afterward
(seed445). Full `mix precommit --seed 446` passes **2,271 tests, one skipped and
37 opt-in exclusions** in 258.7s. Existing timeout/CAS/URI diagnostics remain
visible; reliability follow-ups remain open. Existing budgets and native/storage/
keyed bytes are unchanged; upgrade query-serving applications. Complex literals,
other template functions/control flow and exact diagnostics remain open.

| Exact Go 1.27.3 source selection, including final newline | SHA-256 |
| --- | --- |
| `src/text/template/parse/parse_test.go:45–54` | `ef313a58de94709223cb949fa89a4365079403257fe0e4e4ea5e81af008f450d` |
| Complete `parse_test.go` | `4e355963d9cf59595e91e032c3afada9a9448363e95d90042cdc6cf5747532ec` |
| Complete `src/text/template/exec.go` | `ca14feb551ec536deeddfd46efdfa60d1c328e6d1b00cd5aa6ca944e379c282a` |

## Floating conversion, extrema and rounding continuation

Tests tagged `:template_float_helpers_batch` retain three complete original Loki
formatter rows: `fmt_test.go:271–288,298–306` at the pin above. These cover
`minf`/`maxf`, `ceil`/`floor` and `float64` pipelines, raising the Loki source
formatter total from 60 to **63**. Sixteen separate successful controls and ten
runtime-error templates cover coercion, signed zero, nonfinite classes, typed
comparisons, integer precision, all variadic threshold types, skipped branches
and existing metric-error selection. Nonportable nonfinite-to-integer casts
deliberately reject rather than choosing Go's implementation-dependent result.
Both formatters replay through authenticated GET/form POST, native Parquet and
loopback S3. Normal replay requires neither Go nor network access.

All **27,928** actual pinned Loki parser/pipeline comparisons agree, including
**30 runtime errors**, excluding exact diagnostics. The matrix covers malformed
and boundary numeric strings, float extrema and all decimal precisions from
−326 through 311, plus custom thresholds. All 19 retained successful templates
independently agree with Loki; a separate source-AST audit verifies the three
original rows. Evidence is under `.amp/in/artifacts/loki-template-float-helpers-*`.

Sprig's static `TestRound` expects `round 123.5555 3` to yield `123.556`, but the
actual pinned Loki/Go **1.27.3** runtime yields **123.555**: inlined `Modf` fuses
the multiplication/subtraction and obtains a fractional residual just below
0.5. The independent control preserves that runtime result, not an altered
upstream test oracle. Pulso emulates its bounded exact binary residual and Go's
decimal-power multiplication order. This is a pinned runtime profile, not
cross-compiler/platform bit parity or unchanged Sprig-table replay. Original MIT
and BSD notices remain in adjacent `Sprig-LICENSE` and `Go-LICENSE`.

The initial two regressions fail before implementation (seed447). Final full
`mix precommit --seed 450` passes **2,273 tests, one skipped and 37 opt-in
exclusions** in 290.0s, including the fused-residual fix. Strict Credo checks
445 files without findings. Timeout/CAS/URI diagnostics remain visible and the
reliability follow-ups remain open. Upgrade query-serving apps; existing budgets
and native/storage/keyed bytes are unchanged. Decimal arithmetic helpers,
remaining control flow and exact diagnostics remain open.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Loki `fmt_test.go:271–288` | `02a5fa0de98fe184b3ea85b0efcdf435d263a90b9e080d432381892fcc5a302e` |
| Loki `fmt_test.go:298–306` | `90f9f2bb1d11e5077c2e535e76d84939b7cbe6d098d7b6f887672afa73d5fea3` |
| Complete Loki `fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Sprig v3.3.0 `numeric.go` | `5465d61e984077478688b63cef6c73caf2550fd3d1d77a871775f03d147f63ff` |
| Go 1.27.3 `src/math/pow.go` | `848a205994a0a507046a91a02ad4a95172af2fa5f6f11847acb8cb634471b670` |
| Go 1.27.3 `src/math/modf.go` | `03f1cdef8c6a368d60a88a90edf5e0b1720f3789a068a6b74b840c28c9bb037d` |

## Decimal arithmetic template continuation

Tests tagged `:template_decimal_batch` retain both complete `mathfloat` and
`mathfloatround` Loki formatter rows, `fmt_test.go:244–261` at the pin above.
Original templates, labels and result strings remain unchanged; their empty
input replays through both formatting stages, authenticated GET/form POST,
native Parquet and loopback S3. The Loki formatter source-row total is **65**.
Thirteen separately identified successful controls distinguish decimal folds
from binary float arithmetic, cancellation, operand order, float64 input
precision loss, per-step 16-place division, underflow/overflow, zero signs,
typed output consumption and defaulting. Ten runtime-error templates retain
inputs and existing metric-error selection; skipped failures do not execute.
Normal replay requires neither Go nor downloads.

All **14,012** actual pinned Loki parser/pipeline comparisons agree, including
**1,524 runtime errors**, excluding exact diagnostics. They cover pairwise
numeric extremes and malformed/nonfinite strings, 1,000 seeded asymmetric
three-operand combinations, variadic folds up to 30 operands, typed consumption
and skipped branches, through both formatters with separate label categories.
All 15 retained successful templates independently agree with Loki. A separate
Go source-AST audit verifies both original rows' exact templates, fields and
expected strings. Both new regressions fail before implementation (seed451)
and pass afterward (seed452).

The first full precommit run (seed453) passed 2,274/2,275 tests, with one
`query_timeout` in the existing 150,000-span MCP top-k test while other local
validation ran concurrently. The unchanged trace test passed in isolation;
three instrumented repeats also passed. Their 5,000-row projected queries took
1.356–1.386s and full-metadata queries 1.298–1.748s, under the unchanged 10s
request deadline. This suggests contention sensitivity, not a proven root cause
or a closed reliability finding. No deadline, fixture or resource ceiling was
widened. Diagnostic logs are retained alongside the compatibility evidence.

The final full rerun, `mix precommit --seed 453 --trace --max-cases 8`, passes
**2,275 tests, one skipped and 37 opt-in exclusions** in 338.9s without
overlapping heavy validation. The unchanged 150k-span test completes in 8.9s
including fixture construction and all four queries. Final targeted formatter
checks pass (seed454), strict Credo checks 445 files cleanly, and both runtime
matrices remain exact after float dispatch consolidation. The earlier timeout
and standing reliability items remain recorded, not erased by the green rerun.

Each operand converts through float64 to its shortest decimal; intermediate
decimal folds do not round back to float64. Division alone rounds each step to
16 decimal places, halfway away from zero. A function return may underflow or
overflow, but a subsequent decimal function rejects nonfinite input. This is
not arbitrary-precision input parsing, full formatter/client or capacity proof.
Existing query length/depth, worker heap/deadline and output ceilings remain
authoritative. No package dependency or native/storage/keyed bytes change;
upgrade query-serving apps. Original MIT notices remain in `Sprig-LICENSE`
and `shopspring-decimal-LICENSE`. Evidence and audits are under
`.amp/in/artifacts/loki-template-decimal-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Loki `fmt_test.go:244–261` | `da4640f86bb19f0a91eb02890844ef318629337a2c2c1dcffff47497457fc259` |
| Complete Loki `fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| Sprig v3.3.0 `functions.go` | `e8965eeef01361ac86be9467b9034f8894445a525c2d8df19a78b0bd15242f8e` |
| shopspring/decimal v1.4.0 `decimal.go` | `6e6a5e2d57405434811b03b47fd7dee7fefe1327823bcdce0437f75678694955` |
| shopspring/decimal v1.4.0 `LICENSE` | `b92ba0f6ee02f2309628bfdadb123668a17c016e475ba477b857d33470d9d625` |

## Complex template constant continuation

The complete seven complex-number rows in Go 1.27.3
`src/text/template/parse/parse_test.go:55–65` retain their original literal
inputs and component values through authenticated GET/form POST, native
Parquet and loopback S3. The upstream table asserts numeric values/type flags;
HTTP replay renders those values with Go's complex formatting. These are
supplementary Go oracles, not additional Loki source formatter rows: that
total remains 65. Original BSD-3-Clause notices remain in `Go-LICENSE`.

Six additional literal controls and nine composed templates cover signed
zeros, decimal/scientific/hexadecimal components and separators, non-octal
imaginary integers, underflow, truthiness, equality, defaulting, lazy logical
selection, numeric coercion and typed argument consumption. All 22 retained
successful templates independently match actual pinned Loki in both
formatters. Eleven malformed/overflowing literals independently reject in
both upstream constructors; HTTP replay proves they reject before provider
reads even behind nonmatching line filters and skipped branches. Ten executed
type-error templates preserve inputs and explicit metric-error selection.
Both new regressions fail before implementation (seed455) and pass afterward;
all five selected complex/float regressions pass (seed456).

All **8,240** actual pinned Loki parser/pipeline comparisons agree, including
**4,744 runtime errors**, excluding exact diagnostics. The matrix covers
pairwise finite component extremes, signed-zero/subnormal/underflow cases,
complex and mixed-type comparisons, variadic equality, generic numeric casts,
typed arguments, skipped branches and both formatting stages, with distinct
stream/metadata/parsed label ownership. Strict Credo checks 445 files cleanly.
Full `mix precommit --seed 457 --trace --max-cases 8` passes 2,277 tests,
one skipped and 37 opt-in exclusions in 282.7s. The unchanged 150k-span
trace test completes in 7.6s including setup and four queries; the earlier
contention-sensitive timeout remains an open reliability item. Normal replay
needs no Go, Loki process or downloads.

Zero-imaginary direct constants can satisfy typed numeric arguments, but
parenthesized/piped values remain complex; generic `int`/`float64` casts return
zero, not the real component. Nonportable integer-cast exclusions remain.
This closes finite complex constants, not full templates, exact errors,
client or capacity acceptance. Existing query, worker and output ceilings
remain authoritative; no dependency/native/storage/keyed-byte change. Upgrade
query-serving apps. Evidence is retained under
`.amp/in/artifacts/loki-template-complex-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/parse/parse_test.go:55–65` | `d90bc83884ab2b7e06344a850c7aa0f2c38b5c0a7dcf349b532e36a0c57fad2e` |
| Go 1.27.3 `text/template/parse/node.go:653–676` | `2892920b1f80c82c4b75ca53d6e091ab769370b8426853c39fda9120c2ab7a45` |
| Go 1.27.3 `text/template/parse/lex.go:583–640` | `a99710ad89c1171e873de78c49a509ae39d348dab0f91e4b4ebf7289ea1c306a` |

## Root label-map and indexing continuation

Go 1.27.3 `src/text/template/exec_test.go:1302–1303` preserves the two original
string-byte/rune comparison inputs and Boolean oracles, wrapped only in action
delimiters for HTTP replay. These supplementary Go cases do not increase the
65 Loki source formatter evaluations. Original notices remain in `Go-LICENSE`.

Twelve successful controls add root-map lookup/length/rendering, missing keys,
successive indices, UTF-8 byte values, signed comparisons, numeric conversions,
typed int consumption and skipped failures. All 14 successful templates match
actual pinned Loki in both formatters. HTTP replay uses authenticated GET/form
POST, native Parquet and loopback S3; separate controls preserve unusual metadata
keys, same-stage template snapshots and earlier-stage label changes. Twelve
executed type/bounds failures preserve inputs and explicit metric-error selection.
A dot after a pipe rejects before storage, even in an unreachable branch.
Both new HTTP regressions fail before implementation (seed458); all five
selected index/complex/budget tests pass afterward (seed461).
Full precommit seed462 passes 2280 tests, one skipped and 37 opt-in exclusions
in 279.6s, without a recurrence of the earlier trace-query timeout.

All **7,638** actual pinned Loki parser/pipeline comparisons agree, including
**4,442 runtime errors**, excluding exact diagnostics. The matrix includes
empty maps, absent/metadata/stream keys, successive indexing, UTF-8 edge bytes,
negative/past-end/nonintegral indices, typed uint8 comparisons/conversions,
skipped branches and both formatters with separate label ownership. Strict
Credo checks 445 files cleanly after sharing existing built-in dispatch.

Root-map rendering accepts exactly 16 MiB, rejects one byte more and admits
the byte below the boundary. A public evaluator regression proves oversized
map formatting fails whole on log/top-k/metric paths despite later label drops
and explicit error selection; map length over the same input succeeds. Size
is proven before joining the complete map text. Generic arrays/JSON values,
field chains, non-UTF-8 literal strings, exact errors and full built-in/client/
capacity acceptance remain open. Existing ceilings and native/storage/keyed
bytes are unchanged; upgrade query-serving apps. Normal replay requires no
Go, Loki process or downloads. Evidence is retained under
`.amp/in/artifacts/loki-template-index-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:1302–1303` | `ccefbead1e9e7b72294298577f1be7df34031a8683c7dbdc5c3c85e7a25e6374` |
| Go 1.27.3 `text/template/funcs.go:174–238` | `dedf3074be117d077169ecfc5058b4f5c5f522fe856995b85efd74e4a3ae477b` |

## Go string-slicing continuation

All six string rows from Go 1.27.3 `src/text/template/exec_test.go:564–569`
retain their original templates and success/error oracles. The original
`tVal.S="xyz"` struct field becomes the string label `S="xyz"`; arrays,
pointers and Go interface fields in the adjacent table are not ported. These
supplementary BSD-3-Clause Go cases do not increase the 65 Loki formatter-source
evaluations; original notices remain in `Go-LICENSE`.

The two tests tagged `:template_slice_batch` replay all six original rows,
plus fourteen successful and eleven type/bounds controls through
authenticated GET/form POST, native Parquet and loopback S3. Two separate
failure controls retain Pulso's stricter rejection of UTF-8-splitting bytes,
including before `b64enc`. Successful controls distinguish bytes from Unicode
scalars, cover all default/exclusive/empty boundaries, uint8 indices, explicit
conversions, nested calls, pipeline argument order and skipped errors. Metric
replay checks rewritten byte counts and error rejection/explicit preservation.
Both new tests fail before implementation (seed463); thirteen selected
slice/index/formatter/budget tests pass afterward (seed465).
Full precommit seed466 passes 2282 tests, one skipped and 37 opt-in exclusions
in 284.9s, without a trace-query timeout recurrence.
All eighteen retained successful templates independently match actual pinned
Loki in both formatters; an independent source audit verifies all six Go rows.

Of **4,240 actual pinned Loki pipeline comparisons**, **4,076 agree**, including
**3,728 runtime errors**, excluding exact diagnostics. The other **164**
deliberately reject non-UTF-8 intermediate strings instead of passing them to
later functions; repaired Base64 outputs independently confirm upstream's byte
slices. There are no unexpected differences. Stream/metadata/parsed ownership
is checked separately in the matching cases. Strict Credo checks 445 files
without findings. Boundary probes accept cap-1/cap bytes, reject cap+1 and prove
a later slice cannot hide an oversized intermediate expansion.

This closes string `slice`, not arrays, arbitrary byte-string propagation,
exact error text, full templates or client/provider/capacity acceptance.
Resource ceilings and native/storage/keyed bytes are unchanged; upgrade query
applications. Normal regression replay needs no Go, Loki server or downloads.
Evidence is in `.amp/in/artifacts/loki-template-slice-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:564–569` | `a5a37868c4f4c161e22c0da20cf1290b7f025478a71ed8e2a2d4084e52aa66ef` |
| Go 1.27.3 `text/template/exec_test.go:133–138` | `85e68cc021df82d0cb200174a5a61e1be43b5ad658a0a73a5645f4484c7f3a7d` |
| Go 1.27.3 `text/template/funcs.go:236–287` | `ea4113f30550c1e5766d97a984551c85153f0cb933fbc70dfe9b56d6731d9360` |

## Printing and explicit escaping continuation

Seven original Go 1.27.3 `text/template/exec_test.go` cases retain their complete
templates and expected text: `print`, numeric argument separation, `println`,
HTML escaping, URL-query escaping and two mixed integer/float print rows.
Exact selected ranges and hashes are below. Original unused Go root values
become the label map. Nil and the `js` table have separate continuations below;
pointers, typed interfaces, methods and `printf` are not ported.
These supplementary BSD-3-Clause cases do not increase
the 65 Loki formatter-source evaluations; notices remain in `Go-LICENSE`.

The HTTP test tagged `:template_print_escape_batch` replays all seven originals
and seventeen controls through authenticated GET/form POST, native Parquet and
loopback S3. All 24 successful templates independently match actual pinned Loki
in both formatters, and a source audit checks the seven original inputs/oracles.
Controls cover empty/typed variadic arguments, nonstring adjacency, empty-string
separators, line feeds, complex/uint8/nonfinite values, root maps, missing keys,
pipeline ordering, all HTML-sensitive bytes, existing entities, Unicode crossing
a replacement-chunk boundary and skipped errors. Metric replay counts the
resulting bytes, including the trailing newline.

All **8,504 actual pinned Loki pipeline comparisons** agree, including
**1,488 runtime errors**, excluding exact diagnostics. They cover all 128 ASCII
bytes, Unicode and chunk boundaries, both formatters, variadic mixed types and
nested runtime failures, with separate stream/metadata/parsed ownership.
The new HTTP regression and extended budget regression fail before implementation
(seed467); nine selected printing/escaping/slicing/index/budget tests pass after
implementation (seed470). Strict Credo checks 445 files without findings.
Full precommit (seed471, trace-serialized to one concurrent case) passes **2,283 tests**, with
one skipped and 37 excluded, in 511.7s, including strict compile, dependency and
format checks. No timeout assertions fail; earlier contention sensitivity remains
an open reliability item, not closed by this pass.

The existing public evaluator budget test now proves exact 16 MiB output and
fail-whole overflow for all four built-ins, on log/top-k/metric paths despite
later shrinking/error selection. The first HTML implementation exhausted the
unchanged worker heap by retaining millions of replacement matches; bounded
replacement chunks fixed it without changing limits or fixture sizes. Complete
escaped size is proven before replacement, and joined text is bounded too.
This is explicit text escaping, not context-aware HTML/JavaScript sanitization.
`printf`, typed-interface/pointer values, non-UTF-8 byte propagation, exact
diagnostics and full formatter/client/provider/capacity acceptance remain open.
Existing ceilings and native/storage/keyed bytes are unchanged; upgrade query
applications. Normal replay requires no Go, upstream service or downloads.
Evidence is in `.amp/in/artifacts/loki-template-print-escape-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:474–475` | `3d014f3da739a70215e0a0ec05d752a53e889ab564456d4c79d9d88e5895acc8` |
| Go 1.27.3 `text/template/exec_test.go:477` | `be921b06be21e49c8b5533476021b30c19e885238ba6f9e6291a273b8a76d2e2` |
| Go 1.27.3 `text/template/exec_test.go:490–491` | `21f02c71510c60b2abf40c7e1cef1a383cba4396bed7ae8f0ee10c187e335637` |
| Go 1.27.3 `text/template/exec_test.go:502` | `40ad5b5d3d1262070512b6129f2c5a43a5d4633cfab0f41b37ef97545ad8e594` |
| Go 1.27.3 `text/template/exec_test.go:676–677` | `46ac33d6f718545a0a73b6a2887c6249353439cfcb8fd1e94fd45f7c7b865fb1` |
| Go 1.27.3 `text/template/funcs.go:599–651` | `4a322ba96f3676b941ddb41e8595792869222a9e1ed2f96bea305ba8cdf7caa3` |
| Go 1.27.3 `text/template/funcs.go:744–774` | `6812af7e0df2dd7e340e12d1c739e8dfd07c59c7feb3165f4e71187c9807a984` |
| Go 1.27.3 `fmt/print.go:1184–1208` | `52c8e20d88bb2f56d8ee4582e9b5b0e1c1eb957758a146c756e6d4af67ce4171` |

## Untyped nil continuation

Three original Go 1.27.3 `text/template/exec_test.go` cases retain their complete
templates and expected text/failure outcomes: `nil action` (293), `if nil` (448)
and `print nil` (476). The unused Go root value becomes the label map; typed
nil pointers/interfaces and methods are not projected into that string map.
These supplementary BSD-3-Clause cases do not increase the 65 Loki source
formatter evaluations. Notices remain in `Go-LICENSE`.

Two HTTP tests tagged `:template_nil_batch` replay 24 successful templates,
21 runtime failures and two admission controls through authenticated GET/form
POST, native Parquet and loopback S3. They preserve nil versus missing-string
identity, lazy Boolean/default behavior, equality, numeric coercion, typed
argument errors, skipped nil commands and metric error-selection policies.
Printing keeps nil a nonstring `<nil>`; escaping converts it to a string
`<no value>` before joining, changing both spelling and argument spacing.
Bare nil commands, including `if nil`, fail at execution, not construction.
All 24 retained successful templates independently match actual pinned Loki
in both formatter stages, not only Pulso's own expected outputs.

All **7,528 actual pinned Loki pipeline comparisons** agree, including
**812 runtime errors**, excluding exact diagnostics. Comparisons cover mixed
strings/booleans/int/float/complex/uint8/root values, nil equality in both
positions, arithmetic, lazy branches, function-returned nil and separate
stream/metadata/parsed ownership. Both new HTTP tests fail before implementation
(seed472) and pass afterward (seed473). Full precommit (seed478) passes **2,286
tests**, one skipped and 37 excluded, in 1536.4s, including strict compile,
dependency and formatter checks. `--trace` serializes that run to one concurrent
case despite `--max-cases 8`; it is not concurrent-suite or capacity evidence.
Existing output, work and worker limits
are unchanged; no native/storage/keyed-byte change is needed. Upgrade query apps.
`printf`, typed pointer/interface values, non-UTF-8 byte propagation,
exact diagnostics and complete formatter/client/provider/capacity acceptance
remain open. Normal replay requires neither Go nor upstream services/downloads.
Evidence is in `.amp/in/artifacts/loki-template-nil-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:293` | `cd6712b5187a4873637ca079ebb8db219ed59a8d2d2bfe0dbdd1a202496e11b5` |
| Go 1.27.3 `text/template/exec_test.go:448` | `a62a338b1415f9797f9c53fe4d0dd9903938ddd0e75bf384555360eaa426de98` |
| Go 1.27.3 `text/template/exec_test.go:476` | `9400ce2c3bfa9e4004041f63dffd42aa4114d5475d90b8a993b58c8a2b3e6f40` |

## JavaScript escaping continuation

All eight original Go 1.27.3 `text/template/exec_test.go:1037–1044`
`TestJSEscaping` input/oracle pairs are unchanged. Public replay wraps the
function inputs in the string label `.text` and calls `{{js .text}}`, rather
than testing only the underlying Go function. Fourteen successful controls and
three nested runtime failures exercise variadic typed/nil joining, all special
ASCII bytes, Unicode 17 printability, supplementary nonprintable values,
pipeline ordering and a multibyte scalar crossing a replacement-chunk boundary.
These supplementary BSD-3-Clause cases leave the 65 Loki formatter-source
evaluations unchanged; original notices remain in `Go-LICENSE`.

The HTTP test tagged `:template_js_batch` replays through authenticated GET/form
POST, native Parquet and loopback S3, including metric byte counts and error
preservation/rejection. All 22 retained successful expectations independently
match actual pinned Loki in both formatters; a separate Go AST source audit
checks all eight original input/oracle pairs without executing Pulso.

All **19,060 actual pinned Loki pipeline comparisons** agree, including
**304 runtime errors**, excluding exact diagnostics. Grouped input strings cover
all **1,112,064 valid Unicode scalars**, not just selected Unicode categories;
additional cases exercise typed/nil arguments, runtime failures and both sides
of UTF-8 chunk boundaries with separate stream/metadata/parsed ownership.
The existing numeric-diagnostic classifier was extracted unchanged for shared
use; its four existing HTTP regressions pass before the new behavior (seed475).
The JS HTTP regression and extended output-budget regression fail without the
implementation (seed476); ten selected JS/nil/numeric/string-budget tests pass
afterward (seed477). Full precommit (seed478) passes **2,286 tests**, one skipped
and 37 excluded, in 1536.4s, including strict compile/dependency/format checks.
Trace mode serialized it to one concurrent case, not the supplied eight-case
setting. No timeout assertions fail; the earlier reliability item remains open.

The existing public evaluator budget test accepts exactly 16 MiB escaped output
and fails whole above it on log/top-k/metric paths despite later shrinking/error
selection. JS proves full escaped size before output allocation and processes
bounded UTF-8-aligned chunks. It deliberately preserves Go's minimum-four-digit
`\u` spelling, including more than four digits above U+FFFF, not JSON surrogate
pairs or context-aware sanitization. Existing limits and native/storage/keyed
bytes are unchanged; upgrade query apps. `printf`, typed pointer/interface
values, non-UTF-8 byte propagation, exact errors and full formatter/client/
provider/capacity acceptance remain open. Normal replay needs no Go, upstream
process or downloads. Evidence is in `.amp/in/artifacts/loki-template-js-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:1037–1044` | `39a579017ddc2c692ff76924d7315a967b4937d7944366a4d32914d76ec61308` |
| Go 1.27.3 `text/template/funcs.go:653–742` | `85088bef5977cf1eaaa4816e9051cae51d1c5a703aa62f5cf6801e195992f752` |

## With scope continuation

Ten original Go 1.27.3 `text/template/exec_test.go` cases retain complete
templates and output oracles: the root-independent `with` rows at 580–584,
586, 588–589 and the two `else with` rows at 599–600. Their unused typed Go root
becomes the label map; struct/slice fields, typed nils and variable declarations
are not projected into it. These supplementary BSD-3-Clause rows leave the
65 Loki formatter-source evaluations unchanged; notices remain in `Go-LICENSE`.
An independent standard-library Go AST audit verifies all ten exact pairs.

Two HTTP tests tagged `:template_with_batch` replay those ten rows, 22 successful
template controls, three root-snapshot/empty-map pipeline controls, eight runtime
errors, six grammar/unknown-function rejections and the existing depth ceiling.
Authenticated GET/form POST uses native Parquet and loopback S3, including
metric error-selection policies. Successful branches change dot, false branches
retain it, `if` preserves it, and later actions restore it. `$` retains the first
template's label snapshot within one label-format stage, refreshed by a later
stage. Looking up scalar fields is a runtime error, not an empty map lookup.
The existing budget regression also checks exact 16 MiB output and accumulated
outer-plus-branch overflow on log/top-k/metric paths despite later trimming or
explicit error selection. Nesting 64 succeeds; 65 deliberately rejects under
Pulso's existing ceiling, not upstream's much deeper parser limit.

All **2,387 actual pinned Loki pipeline comparisons** agree, including
**516 runtime errors**, excluding exact diagnostics. All retained successful
templates and runtime failures independently match actual Loki in both stages;
the six retained syntax/function rejections also match its actual parser.
Comparisons cover typed constants/coercions, nil, missing/empty labels, nested
branches, root snapshots and separate stream/metadata/parsed ownership. The two
HTTP regressions and extended budget regression fail on the unsupported compiled
baseline (seed479); all three pass with implementation (seed480), and final
scope/snapshot/budget controls pass (seed481). Strict Credo checks 446 files cleanly.

Existing ceilings and native/storage/keyed bytes are unchanged; upgrade query
apps. Variables, iteration, date/other functions, field chains, byte-string
propagation, exact errors and full formatter/client/provider/capacity acceptance
remain open. Normal Elixir replay needs neither Go nor upstream services or
downloads. Evidence is in `.amp/in/artifacts/loki-template-with-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:580–584` | `685d7a61af2815eb512a37be20b6f7d3e71d901485b6e8fe863509ba8942f2c0` |
| Go 1.27.3 `text/template/exec_test.go:586` | `6cd6ad1e4f2d8915964ca92efe199649cb55bdf108a9a06a2cf09eebc8e74364` |
| Go 1.27.3 `text/template/exec_test.go:588–589` | `8fdb30573812a01b5e72cb85918829de186dc01947d1c5f2c01256ba4984ea8a` |
| Go 1.27.3 `text/template/exec_test.go:599–600` | `81c021434a8128fd7b0271fd0cf72138955f3e4c3db1f7f4db403e16c9bce37f` |
| Complete Go 1.27.3 `text/template/exec_test.go` | `c05cc4721d16f543b78431711c985918fbeffeb5fb4f99e48d2e92655e4d7d0b` |

## Action and control-header variable continuation

Go 1.27.3 `text/template/exec_test.go:336–343` supplies three complete,
root-independent assignment rows: simple assignment, nested assignment and
assignment to the nearest shadowed declaration. Templates and output oracles
are unchanged and independently verified by a standard-library Go AST reader.
These BSD-3-Clause supplementary rows retain the adjacent Go license and do
not change the 65 Loki formatter-source evaluation total.

Two HTTP tests tagged `:template_variables_batch` replay those three rows,
26 successful controls, ten runtime failures, eight upstream admission failures
and one explicit stricter rejection. GET/form POST uses authentication, native
Parquet and loopback S3, including metric error selection and fresh bindings
across destinations/entries. The existing budget regression additionally proves
exact 16 MiB output and fail-whole accumulated variable/branch output or oversized
unused initializers, including top-k and metric paths despite later shrinking
or error preservation. No limit, native/storage/keyed byte or dependency changes.

All **6198 actual pinned Loki pipeline comparisons** agree, including **1454
runtime errors**, excluding diagnostics. All retained successful/error templates
independently match actual Loki in both formatters. Eight constructor failures
also match its parser; declaration inside a parenthesized pipeline is valid
upstream but still deliberately rejected here. Coverage includes typed values,
lazy branches, nearest assignment, shadow restoration, self-initialization,
undefined assignment, lexical visibility of skipped branch declarations, root
reassignment and separate stream/metadata ownership. These are bounded template
semantics, not full formatter/client/provider/capacity acceptance.

Both new HTTP regressions fail on the compiled unsupported baseline (seed485).
Both plus the existing scope controls and extended budget test pass afterward
(seed486). Strict Credo checks 446 files cleanly. Full precommit (seed487, eight
concurrent cases without trace) passes **2290 tests**, one skipped and the
original 37 excluded, in **156.7s**, including every new variable regression.
No timeout assertion fails; load-sensitive reliability remains open.
Bindings inside parenthesized
pipelines, iteration, field chains, date/other functions, byte-string propagation
and exact diagnostics remain open. Normal replay needs no Go or upstream service.
Evidence is in `.amp/in/artifacts/loki-template-variables-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:336–343` | `cb4b633e769b0a038aa1a72cf5216e5c7830d6124c3a5d0016608f09551281df` |
| Complete Go 1.27.3 `text/template/exec_test.go` | `c05cc4721d16f543b78431711c985918fbeffeb5fb4f99e48d2e92655e4d7d0b` |

## Nested-binding and field-chain continuation

The variable tests also carry `:template_nested_bindings_batch`, extending the
same three original Go rows to 48 successful controls, 16 runtime failures and
13 constructor rejections. The former stricter nested-binding rejection is
removed. Arguments thread bindings left to right, lazy operators skip binding
effects and lexical validation still rejects forward references. Declarations
and nearest assignments survive nested initializer effects with block scope.
Both HTTP tests fail on the unsupported compiler (seed489), then pass (seed490).
All **16,622 actual pinned Loki pipeline comparisons** agree, including **3764
runtime errors**, excluding diagnostics. The complete HTTP/budget suites pass
**213 tests** (seed491). These are runtime comparisons, not copied expected
values from Pulso.

Two HTTP tests tagged `:template_field_chains_batch` retain the complete original
Go 1.27.3 `text/template/exec_test.go` `bug9` success row (707) and `bug14a/b/c`
failure templates (720–722). The successful root map is unchanged; the three
failures do not depend on the upstream struct fixture and use the ordinary
label-map root. An independent Go AST reader verifies all four retained
templates and success/failure oracles. BSD-3-Clause attribution and Go-LICENSE
remain unchanged. These supplementary Go rows do not change the 65 Loki
formatter-source evaluation total.

Another 14 successful, ten runtime-error and seven constructor controls cover
saved label maps, parenthesized results, function/variable chains, source-space
boundaries, scalar failures, nil propagation and skipped nil-field arguments.
They replay both formatters through authenticated GET/form POST, native
Parquet and loopback S3, including preserved-input metric error selection and
admission before storage. The original successful test row retains the
`{"cause":"neglect"}` root entry; HTTP adds only the selecting `job` and control
`text` labels. Both HTTP regressions fail before the change (seed492), then pass
(seed494). The complete HTTP/budget suites pass **215 tests** (seed495).

All **18,838 combined actual pinned Loki pipeline comparisons** agree, including
**5278 runtime errors**, excluding diagnostics. The comparison exposed that nil
function results propagate through field chains and skip field arguments,
unlike missing string-valued map keys; the implementation and regressions retain
that distinction. All 20 retained nested/chain constructor rejections also match
actual Loki. Cached compiled pipelines run across entries and distinct
stream/metadata ownership contexts. This is not generic JSON/array/object-method,
full formatter/client/provider/capacity parity.

Budget controls retain exact 16 MiB output, cumulative branch/binding output
and executed intermediate checks, including unused nested initializers and
failed field lookups. Overflow fails whole for log/top-k/metric queries despite
error selection or later shrinking; lazy-skipped initializers and nil-receiver
arguments do not allocate. Strict Credo checks 446 files cleanly. Limits,
dependencies and native/storage/keyed bytes are unchanged; upgrade query apps.
Normal replay needs no Go, network or upstream service. Iteration, JSON values,
date/other functions, byte-string propagation and exact diagnostics remain open.
Evidence is in `.amp/in/artifacts/loki-template-nested-*` and
`loki-template-field-chains-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:707` | `bccdf782b5bfbfff9aa23d08ec1d00d6f4ff7f02b255523a4dd6fa8c72585fd4` |
| Go 1.27.3 `text/template/exec_test.go:720–722` | `de7b5095bf8c0e8a36adf3c27732d6083759af2978cef57c600341197eeb68ad` |
| Complete Go 1.27.3 `text/template/exec_test.go` | `c05cc4721d16f543b78431711c985918fbeffeb5fb4f99e48d2e92655e4d7d0b` |

## Bounded template iteration continuation

Two HTTP tests tagged `:template_range_batch` retain 26 successful controls, ten
runtime failures, nine constructor rejections and the existing 64/65-depth
boundary. They exercise label-map and integer/uint8 iteration, lexical key
ordering, single/two-variable bindings, empty/nil else paths, dot/root scoping,
body-local declarations, outer assignments and nested `break`/`continue`.
Integer ranges reject two variables even when empty. Loop control in an empty
inner loop's else reaches the enclosing loop.

Ten unchanged Go 1.27.3 `text/template/parse/parse_test.go:222–241` templates
retain their original syntax/admission assertions. Their struct/slice fixtures
are not invented as label values or claimed as runtime oracles. An independent
Go AST reader verifies all ten against the source's `noError` rows. These are
supplementary BSD-3-Clause Go parser cases (Go-LICENSE), not additional Loki
formatter-source evaluations; the latter total remains 65.

Rendering first changed to explicit output/state/control threading; all 215
existing HTTP/budget tests passed unchanged (seed497). The two new HTTP tests
then failed on unsupported range admission (seed498), and passed with the
implementation (seed499). Authenticated GET/form POST replay covers both
formatters, metric error selection, native Parquet and loopback S3. Earlier
intentional range fences now use genuinely malformed missing-expression syntax.
All **21,304 combined actual pinned Loki pipeline comparisons** agree, including
5388 runtime errors, excluding diagnostics. All 29 combined constructor
rejections agree with actual Loki. Cached pipelines span distinct entries and
stream/metadata contexts; this is not provider/deployed-client/capacity proof.

Every executed iteration spends one existing metric query-work unit or
request-local stream-template work unit, shared with regex counting/replacement.
Integer iteration streams without cardinality allocation; empty output is not
retained per iteration. The public budget regression covers empty-output and
continue loops, shared actions/entries/regex in both orders, explicit error
selection and int64-max iteration with immediate break. The output regression
also covers exact 16 MiB across iterations and fail-whole overflow. The targeted
batch passes four tests (seed500); full concurrent precommit passes **2295 tests**,
one skipped and the original 37 excluded, in 133.5s (seed501, eight cases without
trace). Strict Credo checks 446 files cleanly. Existing compaction timeout
diagnostics remain; this does not close load-sensitive reliability.

No native/storage/keyed-byte, dependency or ceiling change; upgrade query apps.
Generic JSON values, array/channel/function iteration, date/other functions,
byte-string propagation and exact diagnostics remain open. Normal replay needs
no Go or upstream service. Evidence is in `.amp/in/artifacts/loki-template-range-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/parse/parse_test.go:222–241` | `c1435e1b17d1200fc97effbd1361f4f240a06d7c9e6512d924ec3b45c05c6d8d` |
| Complete Go 1.27.3 `text/template/parse/parse_test.go` | `4e355963d9cf59595e91e032c3afada9a9448363e95d90042cdc6cf5747532ec` |

## Typed JSON template continuation

Two HTTP tests tagged `:template_json_batch` retain both complete Sprig v3.3.0
[`TestFromJson` oracles, `defaults_test.go:127–141`](https://github.com/Masterminds/sprig/blob/v3.3.0/defaults_test.go#L127-L141):
the unchanged `{"Input":"{\"foo\": 55}"}` fixture, templates and expected
`map[foo:55]`/`55` values. An independent Go AST reader verifies both complete
rows and their input against source. Copyright (C) 2013-2020 Masterminds, MIT;
the existing Sprig-LICENSE retains the notice. These are supplementary Sprig
unit oracles, not new Loki integration rows; the 65 Loki formatter-source
evaluations remain unchanged.

Another 29 successful controls and 19 runtime failures cover nested typed
objects/arrays, missing/null fields, arbitrary string keys, sequential indexing,
length/truthiness, float64 rounding/signed zero, duplicate replacement, malformed
documents, trailing-content rejection, surrogate pairs/replacement and iteration.
Both formatters replay through authenticated GET/form POST, native Parquet and
loopback S3, with metric error selection and preserved-input errors. Bare nil,
JSON object nil interfaces and root-label missing strings remain distinct.

The actual pinned Loki binary, compiled with Go 1.27.3 and its vendored Sprig
v3.3.0, agrees on **10,292 pipeline comparisons**, including **3874 runtime
errors**, excluding exact diagnostics. Inputs cover valid/malformed syntax,
whitespace, repeated keys, numeric precision/underflow/overflow, escaped Unicode,
container operations, field arguments and binding effects. Overflow-to-zero is
specifically the actual Go 1.27.3 decoder behavior; older Go versions may retain
nil instead. Neither table transcription nor a documentation example alone
establishes that runtime distinction.

Each executed decode spends its input byte count before normalization/term
expansion, sharing metric query work or request-local stream-template work with
loops and regex helpers. Input and normalized bytes cap at 16 MiB; decoded
containers reuse the 64-level cap. Normalization proves growth before allocation.
Two public budget tests cover shared actions/entries, invalid input work, loop/
regex combinations, 64/65 nesting, exact rendered output and raw/normalized
overflow. Exhaustion fails whole despite error selection or unused output.
These are Pulso admission bounds, not upstream allocation/capacity guarantees.

Both HTTP tests failed before implementation (seed502); all four new HTTP/budget
tests pass (seed505). The complete HTTP/budget suites pass **222 tests** (seed506).
Full concurrent precommit passes **2299 tests**, one skipped and the original
37 excluded, in 165.8s (seed507, eight cases without trace). Strict Credo checks
447 files cleanly. Compaction timeout/conflict diagnostics remain in the log;
this does not close load-sensitive reliability or provider/client/capacity parity.
Array slicing, methods, channel/function iteration, date/other functions,
non-UTF-8 byte propagation and exact diagnostics remain open. No dependency,
native/storage/keyed-byte change; upgrade query apps. Ordinary replay needs no
Go, network or upstream service. Evidence is in `.amp/in/artifacts/loki-template-json-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Sprig v3.3.0 `defaults_test.go:127–141` | `d00e5b069e69088efcb813a08c68622d9145d37b7d37fc6f81b556da47dfea67` |
| Complete Sprig v3.3.0 `defaults_test.go` | `b10e600ebab6bd492e10a528de3d2b524d7db693ed50b08d5c9f33c0be2075e5` |
| Complete pinned vendored Sprig `defaults.go` | `65893cbfa3ac451bf36593c39c5ed47bbbf23e986857528ddabda0772f043886` |

## Decoded array-slicing continuation

Both HTTP tests tagged `:template_array_slice_batch` retain the ten original
Go 1.27.3 `text/template/exec_test.go:544–553` slice templates and value/error
oracles. The Go `tVal.SI=[]int{3,4,5}` field becomes a decoded JSON `SI` array
with the same values. A surrounding `with (fromJson .payload)` selects that
object; the original template inside and its expected output are unchanged.
An independent standard-library Go AST reader verifies all ten templates,
success/error flags, expected strings and the complete original SI fixture.
These supplementary BSD-3-Clause Go rows do not increase the 65 Loki formatter
source evaluations; the existing Go-LICENSE retains the notice.

Another 18 successful and 13 failing controls cover default/exclusive/empty
bounds, all three bounds, nested views, backing values beyond current length,
reduced capacity, uint8 and wrong-type indices, sequential lookup, print/coercion,
iteration/index rebasing and skipped errors. Authenticated GET/form POST replays
both formatters through native Parquet/loopback S3, including ordinary metric
error rejection and explicit error selection. Two additional profile rejections
document the remaining allocator distinction: Pulso's initial array capacity
equals decoded length, never Go allocator spare cells. Third bounds cannot
reserve those cells either. This does not claim exact Go capacity parity.

Of **5984 actual pinned Loki pipeline comparisons**, **5980 match**, including
**4304 upstream runtime errors**, excluding diagnostics. Four comparisons (the
two profile controls through both formatters) deliberately reject spare-cell
access; there are no unexpected differences. The broad matrix sets an explicit
initial capacity, exercises asymmetric arrays of 0/1/3/5 elements and all valid
initial views, then both sides of nested length/capacity/index boundaries. Duplicate
template/input pairs are removed before replay.

The success regression fails before implementation (seed508). Representation
first changes without admitting slices, passing all four unchanged JSON HTTP/
budget tests (seed509); both slice HTTP tests then pass (seed510). The final
three-test HTTP/budget batch passes (seed511), including shared nested-loop
work, exact 16 MiB rendered arrays and fail-whole overflow on log/top-k/metric
paths despite error selection or later shrinking. Views retain the backing tuple
without copying; consumers inspect only visible elements and iteration streams.
Full concurrent precommit passes **2302 tests**, one skipped and the original
37 excluded, in 209.0s (seed512, eight cases without trace). Strict Credo checks
447 files cleanly. Existing compaction timeout/conflict diagnostics remain;
this does not close load-sensitive reliability or deployed/provider/capacity gaps.

No native/storage/keyed-byte/dependency/ceiling change; upgrade query apps.
Methods, channel/function iteration, dates/other functions, byte-string
propagation, exact diagnostics and broader formatter parity remain open.
Normal replay needs no Go, network or upstream service. Evidence is in
`.amp/in/artifacts/loki-template-array-slice-*`.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Go 1.27.3 `text/template/exec_test.go:544–553` | `256f041ce71f481da049e12f818df8ad9a629743624016466c9f7da31480e029` |
| Complete Go 1.27.3 `text/template/exec_test.go` | `c05cc4721d16f543b78431711c985918fbeffeb5fb4f99e48d2e92655e4d7d0b` |
| Go 1.27.3 `text/template/funcs.go:240–291` | `02cd47e75f193c6f66ae0cff8cb9226dda40da43f51438d96638183a148f51de` |

## Timestamp and Unix-time formatter continuation

The `:template_time_batch` checks port seven complete formatter rows and both
`TestInvalidUnixTimes` inputs from the pinned Loki revision above:

- [`fmt_test.go:361–369`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/fmt_test.go#L361-L369): line `timestamp_unix`.
- [`fmt_test.go:636–644`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/fmt_test.go#L636-L644): label `timestamp_unix`.
- [`fmt_test.go:728–772`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/fmt_test.go#L728-L772): all five `unixToTime` rows, retaining their original day/second/millisecond/microsecond/nanosecond strings.
- [`fmt_test.go:980–986`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/logql/log/fmt_test.go#L980-L986): both original invalid strings, `abc` and `464`.

The successful rows keep the original labels, templates, source nanoseconds
and lines. Their upstream helper variables and execution timestamp are at
539–546 and 775–780. A selector-only job label isolates each row. The five
`time.Local`-dependent expectations are frozen using Go's UTC `Time.String`
output; Pulso deliberately renders UTC on every instance, not a host-local
timezone. This is an explicit adaptation, not general timezone parity.
All seven successful rows run through GET/form POST on both query endpoints,
authenticated Loki ingestion and native Parquet/S3 reads.
The invalid inputs preserve runtime failure and input retention, not exact
Go error text. These are formatter/helper tables, not additional service suites.

One separate control wrapper checks both sides of a second/millisecond boundary,
source nanoseconds after line rewrites, saved typed times/with/equality,
floor-based pre-epoch conversion, byte-length units, signed-int64 wrapping,
epoch truthiness and numeric coercion. The error wrapper additionally checks
overflow/type/arity failures in both formatters, unchanged destinations/lines
and ordinary metric-error rejection versus explicit error selection.

The initial upstream wrapper fails before implementation with undefined
`__timestamp__` (seed626), then passes (seed627). All three final HTTP wrappers
pass (seed628). All 444 Loki upstream/LogQL checks pass (seed629); strict Credo
checks 447 files cleanly after dispatch is split into ordinary function clauses.
An independent temporary executable extracts the actual three Loki Unix helper
functions and Sprig v3.3.0 `unixEpoch`, then executes real Go `text/template`
with the original timestamp closure. Go 1.25.10 and Pulso's actual pipeline
agree on **28,292 formatter comparisons**, including **100 runtime failures**,
excluding exact diagnostics. Inputs cover signed-int64 source timestamps,
subsecond boundaries, all five byte-length units, signs/leading zeros, overflow,
wrong types/arity, scoped time variables and default UTC rendering. Both line
and label formatting run every comparison. This is helper/pipeline evidence,
not a full Loki process, distributed merge or deployed-client test.

Full precommit seed630 passes **2403 tests**, one skipped/37 excluded, in
239.3s, with four concurrent cases and ordinary timeouts. This includes the
complete Mimir/Loki/Tempo regression corpus; historical provider-capacity and
load-sensitive lifetime/timeout findings are not closed by a passing suite.

Normal regression replay uses Elixir/Rust alone, without Go or downloads.
No native/storage/keyed-byte/dependency/resource-limit change is required;
upgrade query applications. Arbitrary date layouts, named zones, time methods,
`now`, full diagnostics and broader formatter parity remain open.

| Source selection, including final newline | SHA-256 |
| --- | --- |
| Complete pinned Loki `pkg/logql/log/fmt.go` | `0a1830ef146924bdeff22d371fcb2489b8ffb44e8eb08503ee17c6c0f0418132` |
| Complete pinned Loki `pkg/logql/log/fmt_test.go` | `58955c60c2ff68f91a5f0777208e49180d194c290c3bbb36f3145911f1f22ff6` |
| `fmt.go:123–173` | `d25b17a7826c4171323681a8153b6163b6ede2d46f087e5dae5050b62ff1fb18` |
| Combined nine test selections above, in order | `e091966c1b9cd19113061890e3c90a22875aa88b7ec99bea021f353fe2e7e6db` |
| Combined `fmt_test.go:539–546,775–780` | `a2ff17deb9cd096ccf30c2b35054e5bc27354b61733bb240e14a00ccb7862c79` |
| Complete Sprig v3.3.0 `date.go` | `db6ac4b6f9a6445769b9ccaa36bb53f74b14f07bd04512a18dcd5d21b81d0540` |
| Sprig v3.3.0 `date.go:150–152` | `b08976360798c4a0bc7a2160ac308a468a1f71ec1682d2253a9f62fa7af7cfbd` |

## Replay

From the repository root, using the pinned Elixir/Rust toolchain:

```sh
ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mise exec -- mix test \
  test/pulso_web/controllers/loki*_test.exs \
  test/pulso_web/loki*_test.exs \
  test/pulso/loki/ test/pulso/logql/ --warnings-as-errors
```

Normal replay requires neither Go nor a Loki process/network download. To inspect
the source again, clone Grafana Loki and fetch/checkout the exact SHA above.
The original files have these SHA-256 hashes:

| Source file | SHA-256 |
| --- | --- |
| `integration/loki_micro_services_test.go` | `3d94850849d9a863c1c2c752d0838089e51cf9ce8c3e16941be512a845dd7cb5` |
| `integration/per_request_limits_test.go` | `6415acdc79d8eac2f14f91508b15d2f8953f1e2576f8056be647be0be92e0fdc` |
| `integration/labelaccess_test.go` | `413af218812a8e1041b9e3856aaa692c9a18887a4847587e951e3f492009d9e9` |
| `integration/loki_sample_query_dataobj_test.go` | `fc7803075eab9a89e44d0ceff3a672afea0eed48ecd740587b0bbb9427d40a57` |
| `integration/explore_logs_test.go` | `def1e19394da864cd335129a195f37ea7aac90033601a36609b370cc321f0837` |
| `integration/loki_sample_query_ordering_test.go` | `d8c47a4f78a2dfc0588c850ce3f18ba5f87b70608ba21a7875a79618f05459c2` |
| `integration/client/client.go` | `1689f725e14911c8d32f3f74948af77d979b5d957bce0ac020643401a2f2af68` |
| `pkg/logcli/client/client.go` | `9c587bfcb04bb6af81eddd5c24672749ff98235ca4bb0987ca2b491cb7fa402b` |
| `pkg/util/querylimits/propagation.go` | `63efbbbfba1864ea29c9f22885d659c7559c46a2c00eb035a0016ac6c9c788d1` |
| `pkg/util/querylimits/middleware.go` | `b37c4c5a02e5cbdf45350a87e2915cff9552ecfacf5f5b3f27aed5e816df0d31` |
| `pkg/util/querylimits/limiter.go` | `91db1c9eac2d8e636ff05482e780fb0cf2619209c90b09036be5959ae3cb3211` |
| `vendor/github.com/prometheus/common/model/time.go` | `67db3a4467353954b9df1de8deb2f041f596e29b90edcb0be8e575c49309eb48` |
| `docs/sources/configure/loki-limits.md` | `0d32f24101ae40ca3025ad22ce294262fe7f26d09ef7e57550f4625fecb97524` |
| `docs/sources/query/metric_queries.md` | `4ff8122d63c2fc6bc14c201b0419186d836dbe884faa620448450296cbcbabfe` |
| `pkg/logql/syntax/ast.go` | `87eb6811c68654e2b2f7d629d799d215dc79fb549e0f9d71a805a46bd51e1e69` |
| `pkg/logql/count_min_sketch_test.go` | `04b97ef240c41bd1063f8e3b1a0145edf4766cfae864fd57e81b9d141497891b` |
| `pkg/logql/approx_count_distinct_test.go` | `7067c3a1e8d992e4bef6f974e6f36b7b3781b544dd84f048130937ee81bf7377` |
| `pkg/logql/syntax/syntax.y` | `cc92e3cfd669d63fb4e987f0830ffefd85b75c26b2786e54885a103ae3a67a7a` |
| `pkg/logql/log/label_filter_test.go` | `2f2021388579402b82348acf1037c868aa0ad502b41cc670e1db7feb872006bf` |
| `pkg/logql/syntax/extractor.go` | `631842bfc26139802c3623c2117b0e75b548dc2b209e6b6b9e70c5194b7f4411` |
| `pkg/logql/log/metrics_extraction.go` | `6118400c0a9cbd491b7843b45f54932cf8ec47ae6726e33f27b222318d802214` |
| `pkg/logql/internal/logqltest/testdata/functions.logqltest` | `38c92a4989e2f7f19abdba06f5b789d0ef3c4d7cdb5850aab729a85f63bf3d66` |
| `pkg/logql/internal/logqltest/testdata/binary_operations.logqltest` | `590d52f764e989b1cad7eb98d2933231644cec28decd91ca6d4f631a827b6d81` |
| `pkg/logql/internal/logqltest/testdata/range_aggregations.logqltest` | `980fd537de60238fa05d50d7bed25848b36dc5e4628685ddbccfaa4122ab83af` |
| Counter selection, exact lines 588–739 including final newline | `0d683bccd991c437cc4636ca7d55e54acf3d8a01432d1adcbc2b69a288e799b7` |
| `pkg/logql/internal/logqltest/README.md` | `ed4c0b5b9c129c970e7e7031d5d092b7534b9559d53690ba17c7c5d5add76097` |
| `LICENSE` | `0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0` |
