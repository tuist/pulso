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

There are **28 upstream-derived ExUnit checks**: 27 preserve the selected public
request/response semantics, and one metric-range check has an explicit resolution
adaptation described below. They are not 28 complete upstream integration suites.
Twenty-one additional HTTP/S3 controls in the same file are Pulso-authored, not
upstream ports; two pipeline/evaluator controls live in the existing LogQL test files.

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

These cover nine upstream fixtures: the four `lineA`–`lineD` pushes, their separate
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

Pulso implements only `maxQueryLength`, as an additional request-local ceiling
on the parsed/defaulted `end - start`, without narrowing or truncating the
request. Exact equality passes; a single excess nanosecond rejects before any
storage access. This covers range/finite instant queries, labels, label values,
series, index stats, both volume aggregations and volume endpoints, detected
labels/fields/field-values and patterns, with GET/form POST agreement. Default
windows are checked too. Instant metric evaluation has a zero-length evaluation
window; range selectors and offsets are not capped by this field (the separate
`maxQueryInterval` policy is unsupported). An instant log read with a positive
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
selection), ignores unknown fields, and logs/ignores malformed policies. Pulso
does **not** implement `maxQueryInterval`, `maxQueryLookback`,
`maxEntriesLimitPerQuery`, `maxQueryTime`, `requiredLabels`,
`minimumLabelsNumber`, `maxQueryBytesRead`, or `tsdbShardingStrategy`.
Missing header, `{}` and valid zero durations add no restriction; they never
disable existing admission, deadline, scan, result, work or heap budgets.

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

## Unported boundaries

This selection does not establish full Loki compatibility. It omits enabled
LBAC/aggregated metric policies, deletion APIs, multi-tenant federation,
per-request limits other than `maxQueryLength`, unselected WebSocket tail
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
| `pkg/logql/syntax/extractor.go` | `631842bfc26139802c3623c2117b0e75b548dc2b209e6b6b9e70c5194b7f4411` |
| `pkg/logql/log/metrics_extraction.go` | `6118400c0a9cbd491b7843b45f54932cf8ec47ae6726e33f27b222318d802214` |
| `pkg/logql/internal/logqltest/testdata/functions.logqltest` | `38c92a4989e2f7f19abdba06f5b789d0ef3c4d7cdb5850aab729a85f63bf3d66` |
| `LICENSE` | `0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0` |
