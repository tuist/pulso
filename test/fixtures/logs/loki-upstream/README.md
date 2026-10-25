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

There are **22 upstream-derived ExUnit checks**: 21 preserve the selected public
request/response semantics, and one metric-range check has an explicit resolution
adaptation described below. They are not 22 complete upstream integration suites. Six
additional HTTP/S3 controls in the same file are Pulso-authored, not upstream
ports; two pipeline/evaluator controls live in the existing LogQL test files.

| Upstream source | Selected cases | Checks |
| --- | --- | ---: |
| [`integration/loki_micro_services_test.go`, `TestMicroServicesIngestQuery`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L110-L159) | `query`, `label-names`, `label-values`, `series`, `series error`, `stats error` | 6 |
| [`integration/labelaccess_test.go`, `TestLabelAccessTestCases/disabled`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L78-L112) and its [ingest](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L297-L305) / [assertion helpers](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/labelaccess_test.go#L385-L470) | `logs-query`, `label-values-query`, `series-query`, `metrics-query`, `metrics-range-query` | 5 |
| [`integration/loki_micro_services_test.go`, `TestCategorizedLabels`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_micro_services_test.go#L1070-L1291) | `no header - no parser`, `no header - with parser`, `with header - no parser`, `with header - with parser` | 4 |
| [`integration/loki_sample_query_dataobj_test.go`, `TestSampleQueryStreamDataObjEquivalence`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_dataobj_test.go#L149-L213) and [public result checks](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_dataobj_test.go#L276-L327) | Counts `a=3,b=2`, unwrapped sums `a=60,b=3`, line-filter count `a=3` | 3 |
| [`integration/explore_logs_test.go`, `Test_ExploreLogsApis/detected_fields`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/explore_logs_test.go#L117-L223) | All six log lines; `non-split queries` (`now-1m` to `now+1m`, foo string/1, color string/3); `split queries` (`now-24h` to `now+1m`, foo string/1, color string/4) | 3 |
| [`integration/loki_sample_query_ordering_test.go`, `TestSampleQueryStreamOrderingEquivalence`](https://github.com/grafana/loki/blob/5aa4b9212d64d723f63b2770452054aa23df114d/integration/loki_sample_query_ordering_test.go#L23-L105) | Public `sum by (cluster, job, app) (count_over_time({cluster="prod"}[1h]))` result with all three labels and counts `a=3,b=2,c=1`, counted once | 1 |

These cover six upstream fixtures: the four `lineA`–`lineD` pushes, the four
`line1`–`line4` pushes with missing/classified environment labels, the four
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
  client, encoder and manifest CAS. Setup checks the expected four/five/six segment
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
(seed 674624). The complete integration file now contains 28 checks: 22 selected
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

## Unported boundaries

This selection does not establish full Loki compatibility. It omits enabled
LBAC/aggregated metric policies, deletion APIs, multi-tenant federation,
per-request limit headers, websocket tailing, OTLP
promotion/drop policies, probabilistic/approximate queries, ruler evaluation,
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
ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mix test \
  test/pulso_web/controllers/loki_upstream_integration_test.exs \
  test/pulso_web/controllers/loki_query_controller_test.exs \
  test/pulso/logql/pipeline_correctness_test.exs \
  test/pulso/logql/evaluator_test.exs --warnings-as-errors
```

Normal replay requires neither Go nor a Loki process/network download. To inspect
the source again, clone Grafana Loki and fetch/checkout the exact SHA above.
The original files have these SHA-256 hashes:

| Source file | SHA-256 |
| --- | --- |
| `integration/loki_micro_services_test.go` | `3d94850849d9a863c1c2c752d0838089e51cf9ce8c3e16941be512a845dd7cb5` |
| `integration/labelaccess_test.go` | `413af218812a8e1041b9e3856aaa692c9a18887a4847587e951e3f492009d9e9` |
| `integration/loki_sample_query_dataobj_test.go` | `fc7803075eab9a89e44d0ceff3a672afea0eed48ecd740587b0bbb9427d40a57` |
| `integration/explore_logs_test.go` | `def1e19394da864cd335129a195f37ea7aac90033601a36609b370cc321f0837` |
| `integration/loki_sample_query_ordering_test.go` | `d8c47a4f78a2dfc0588c850ce3f18ba5f87b70608ba21a7875a79618f05459c2` |
| `integration/client/client.go` | `1689f725e14911c8d32f3f74948af77d979b5d957bce0ac020643401a2f2af68` |
| `LICENSE` | `0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0` |
