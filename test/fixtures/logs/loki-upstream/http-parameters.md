# Original Loki HTTP request-parameter tables

`http-parameters.json` freezes all requested parameter tests from Grafana Loki
revision [`5aa4b9212d64d723f63b2770452054aa23df114d`](https://github.com/grafana/loki/tree/5aa4b9212d64d723f63b2770452054aa23df114d/pkg/loghttp).
Test-derived inputs and assertions are copyright Grafana Labs contributors,
AGPL-3.0-only; the adjacent `LICENSE` applies.

The producer imports only the Go standard library and never executes Pulso or Loki parsers.
Ordinary replay can decode the checked-in JSON with the existing Elixir/Rust
toolchain; Go is optional provenance/regeneration tooling, not a replay dependency.

## Pulso replay and qualifications

All 87 selected rows have replay ownership, without omitting failures:

- `test/pulso/loki/http_params_test.exs` replays 56 rows from nine complete
  query/component tables, including 25 rejection expectations, normalized opts,
  nanosecond values, original error fragments and deterministic clocks.
- `loki_upstream_integration_test.exs`, tag `http_parameters_batch`, exercises
  51 request rows through authenticated HTTP and native Parquet against owned
  loopback S3. Twenty overlap the component replay; 31 cover the other six
  complete tables. There are 82 read attempts, including five WebSocket tail
  handshakes. Query/label/series rows use GET and form POST; patterns/volume use
  GET. Accepted rows check nonempty results against bounds, selector unions,
  grouping or buckets; all 36 rejected HTTP attempts prove no provider request.
- The separate `http_parameter_controls` tag covers malformed supplied values,
  global interval-before-limit selection in both directions, decimal timestamps,
  `since`, fractional volume steps, fail-whole sample/work ceilings and ordinary
  zero-interval top-k. These are authored controls, not more source rows.

The source tests compare Go request structs. HTTP replay instead asserts their
observable effects, not an echo of an internal AST. In particular, volume's
partial aggregation assertions remain partial source oracles, with separately
authored byte/group controls. Go's year-one values returned alongside errors
map to Elixir error tuples without a value, not explicit accepted timestamps.
The symbolic tail plan is exercised by parsing/filtering the original query;
accepted `delay_for` still does not delay Pulso delivery. This is not complete
tail scheduling or client parity.

Primitive uint32 max values remain accepted independently of endpoint limits.
Detected-field and volume caps of 5000, configured tail caps, scan/work/heap
ceilings and metric inclusive-point ceilings remain authoritative. Explicit
timestamps must fit signed int64 nanoseconds; nonfinite/nonportable float
conversions reject. Instant raw-log queries and their optional start/end extension
remain Pulso behavior, not proof of Loki engine admission parity. General exact
HTTP error bodies/status mapping, duplicate scalar precedence, exhaustive RFC3339
grammar and deployed/provider capacity remain outside this selection.

```sh
ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mise exec -- mix test \
  test/pulso/loki/http_params_test.exs
ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mise exec -- mix test \
  test/pulso_web/controllers/loki_upstream_integration_test.exs \
  --only http_parameters_batch:true --only http_parameter_controls:true
```

## Complete selection

There are **87 original rows/assertion groups across 15 tests**: **35 original
error expectations and 52 remaining value/success rows**. The latter include
three `defaultQueryRangeStep` rows whose function has no error result and four
`interval` success-branch rows whose nil error is not actually asserted.
All seven tests in `params_test.go` are included. The two volume functions are
imperative tests: each contributes its unnamed top-level assertion group plus
the original `aggregate by/labels` and `aggregate by/invalid` subtests. The
`aggregate by` container itself issues no request and is not counted as a row.

All paths below are under `pkg/loghttp/` at the pinned revision; ranges are
inclusive. Complete selected function text, range, row count and SHA-256 are
also embedded in `selections`.

| Source | Complete test range | Test | Rows | Error expectations |
| --- | --- | --- | ---: | ---: |
| `query_test.go` | 18–90 | `TestParseRangeQuery` | 10 | 9 |
| `query_test.go` | 92–133 | `TestParseInstantQuery` | 4 | 3 |
| `query_test.go` | 278–334 | `Test_ParseVolumeInstantQuery` | 3 | 1 |
| `query_test.go` | 336–394 | `Test_ParseVolumeRangeQuery` | 3 | 1 |
| `params_test.go` | 17–47 | `TestHttp_defaultQueryRangeStep` | 3 | 0 |
| `params_test.go` | 49–147 | `Test_uint32QueryParamBounds` | 8 | 8 |
| `params_test.go` | 149–207 | `Test_uint32QueryParamValidValues` | 4 | 0 |
| `params_test.go` | 209–295 | `TestHttp_ParseRangeQuery_Step` | 6 | 0 |
| `params_test.go` | 297–348 | `Test_interval` | 5 | 1 |
| `params_test.go` | 350–381 | `Test_parseTimestamp` | 7 | 1 |
| `params_test.go` | 383–522 | `Test_determineBounds` | 9 | 3 |
| `labels_test.go` | 15–59 | `TestParseLabelQuery` | 4 | 2 |
| `series_test.go` | 13–102 | `TestParseAndValidateSeriesQuery` | 7 | 0 |
| `tail_test.go` | 15–59 | `TestParseTailQuery` | 5 | 4 |
| `patterns_test.go` | 14–122 | `TestParsePatternsQuery` | 9 | 2 |

Excluded as requested: `TestStreams_ToProto`, `Test_QueryResponseUnmarshal`
and `TestLabelsMap`. No requested parameter row is omitted, deduplicated,
rewritten to pass, or replaced with an authored substitute. Source typos remain,
including `good no name in the pah`, `should not be lower then 1s`, and the
`negative interval` URL containing the literal `step=1,interval=-1` rather than
an ampersand. The latter is only an error-presence oracle; it does not prove
which component rejected that request.

## JSON schema, version 1

The envelope contains:

- `schema_version`, `revision`.
- `sources`: six complete source file paths and SHA-256 hashes. Regeneration
  refuses changed bytes, including changes outside selected tests.
- `selections`: complete original selected function text (`original_go`, with
  one trailing newline), its SHA-256, source range, table name and row count.
- `rows`: source-order table entries/assertion groups, including failures.

Each row contains:

- `table`, `name`, `source`, `lines`, `original_go`. Names retain original
  spelling and spaces, without Go testing's display-name space replacement.
  The two unnamed volume groups use `name: ""`; nested names join original
  `t.Run` components with `/`. Volume group source text covers its original
  block, so the top-level block also contains the nested subtests.
- `input`: original request/component inputs. Request literals appear as
  `r: {type: "http.Request", fields: {URL: "..."}}`; route variables, when
  present, are `fields.path_variables`. `path`/`reqPath` are original request
  target strings. Series `input.input` is the original pre-parsed `url.Values`
  map with arrays preserving repeated values and `match` versus `match[]`.
  No URL is escaped, corrected or round-tripped through a URL serializer.
  Component inputs are `value`/`def_ns`, `start_ns`/`end_ns`, or `args` with
  `now_ns`, `startString`, `endString`, `sinceString`. The uint32 rows also
  identify their original `component`: `limit`, `lineLimit`,
  `detectedFieldsLimit`, or `tailDelay`.
- `expected`: independently extracted original `want`/`expected`, never an
  observed parser result. Go structs use `{type, fields}` and retain their
  original field names, with `_ns` added for times and durations. Omitted Go
  struct fields are still their original implicit zero values, not unknowns;
  they are not populated with inferred runtime defaults. Scalars and arrays
  retain their types. `expected_unit` describes scalar time/duration results.
- `expected_asserted` and `expectation_scope`: distinguish actual value
  assertions from discarded/unasserted values. A null expected value with
  `expected_asserted: true` is the original nil-result oracle. The eight uint32
  error rows discard the returned value. `interval`'s error row retains its
  literal expected zero but marks it unasserted. Each volume `labels` subtest
  asserts **only** `AggregateBy`, so its expectation is just
  `{"AggregateBy":"labels"}`, not an invented full query result. Its `invalid`
  subtest asserts only the error, not a nil result.
- `error`: `reject`, `asserted`, original assertion kind, and optional
  `contains` or `equals`. Boolean error tables preserve error presence only;
  no exact diagnostic is inferred. The uint32 failure strings and all three
  `determineBounds` callback substrings are preserved; the two volume failures
  preserve exact `invalid aggregation option`. `asserted: false` prevents the
  informational `reject: false` from becoming a new error oracle for the
  three no-error-result rows or four unasserted interval success errors.
- `clock`, where applicable, explicitly records the only deterministic
  replacement for an original dynamic clock.

Times and durations are **decimal nanosecond strings**, not JSON floating-point
numbers. Absolute times are Unix-relative. `time.Time{}` is represented exactly
as `"-62135596800000000000"` (year 1), not Unix epoch zero or an overflowing
`UnixNano()` result. The producer uses `big.Int` for that conversion. The
patterns request's native millisecond `Step` is converted back to `Step_ns`;
`defaultQueryRangeStep`'s integer-second result is multiplied by 1e9. Original
expressions and units remain in `original_go`. Non-duration protocol integers
remain integers: limits and `TailRequest.DelayFor` (seconds), including the
primitive `tailDelay` uint32 maximum. Go time location/monotonic implementation
details and pointer identities are intentionally not JSON values.

### Clocks and original constructors

`Test_parseTimestamp` declares `now := time.Now()`. Regeneration substitutes
**2022-12-18T00:00:00Z**, `1671321600000000000` Unix nanoseconds, for that single
binding. Every original default and the default row's expected `now` share it.
This date is a deterministic extraction choice, **not** a claim that upstream
fixed that clock; `clock.original` and complete selected source preserve the
distinction. `Test_determineBounds`'s nine original explicit clocks are retained
individually and are not replaced. Other parser tests do not supply a fixed
wall clock; no clock-dependent expected fields are invented for them.

The original series helper `mkSeriesRequest` (`series_test.go:109–122`) builds
expected bounds through `bounds`. For its explicit decimal-second `1000`/`2000`
inputs, the producer uses only `strconv.ParseInt` plus `time.Unix`; it does not
ask the parser under test for expected values. Original expected matcher arrays
are copied verbatim, including empty arrays. `withForm` at lines 104–106 is
represented as its pre-parsed form map. `timePtr`/`requestWithVar` in
`labels_test.go:77–85` are represented as the pointed-to time and route variables.

The tail row's original `Plan: testutil.MustPlan(...)` is retained symbolically
as `{constructor: "testutil.MustPlan", expression: "{foo=\"bar\"}"}`. This
freezes the original constructor input, **not** a synthesized Loki query-plan
AST/protobuf or an assertion that Pulso produces an identical internal plan.

## Admission versus HTTP limits

These are original **parser/component-level** oracles, not HTTP acceptance,
query execution, storage, scan budgets or result-capacity evidence. In particular:

- `tailDelay` accepts max uint32 at the component level; `ParseTailQuery`
  independently rejects the original 20-second delay row. Do not route the
  component maximum through the endpoint and relabel the resulting HTTP
  rejection as a contradictory parser oracle.
- Max uint32 limits, long windows, five-day steps, `{}` selectors and retained
  source timestamps are not permission to bypass Pulso resource ceilings or
  unsupported-feature checks. A replay must report such differences separately,
  without rewriting inputs, narrowing windows, raising limits or changing wants.
- No response/proto conversion, label-map behavior, deployed-client parity or
  full Loki HTTP compatibility is claimed. No original table is certified by
  running it against Pulso in this change.

## Optional offline regeneration and checks

Run from the Pulso repository root with an existing pinned Loki checkout:

```sh
cp test/fixtures/logs/loki-upstream/http-parameters-producer.go.txt /tmp/http-parameters-producer.go
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go build \
  -o /tmp/http-parameters-producer /tmp/http-parameters-producer.go
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off go vet /tmp/http-parameters-producer.go
/tmp/http-parameters-producer /tmp/amp-librarian/grafana__loki > /tmp/http-parameters.json
cmp /tmp/http-parameters.json test/fixtures/logs/loki-upstream/http-parameters.json
jq '{rows:(.rows|length), errors:([.rows[]|select(.error.reject)]|length), selections:(.selections|length)}' \
  test/fixtures/logs/loki-upstream/http-parameters.json
```

Go 1.25.10 successfully built and vetted the standard-library-only producer.
Independent invocations regenerated byte-identical JSON (`cmp` exit 0):
87 rows, 35 error expectations, 15 selected tests. No Go module downloads,
Python, parser execution or remote writes were needed.
Standalone Elixir `JSON.decode!` also validated all 87 unique table/name
identities and all 15 embedded selection hashes and per-table row counts,
without compiling or starting the Pulso application.

| Complete source/artifact, including final newline | SHA-256 |
| --- | --- |
| `pkg/loghttp/query_test.go` | `ed3ebc61ce82f3118955ecb70b5010687352c4a7e9ecb33ea3b698a8656af96c` |
| `pkg/loghttp/params_test.go` | `00ad859e9cb323df72f8c5b16b495f7e3dd64b9b918c2823ed2a36b6c25176be` |
| `pkg/loghttp/labels_test.go` | `a1c156f03b2b722b580522db64445ed87272fc61dbd0203c548439c0cc36f958` |
| `pkg/loghttp/series_test.go` | `a5b0b428c8b6d5f7987620c442472994904a7182c4f28d412852ec5cfbd9d48b` |
| `pkg/loghttp/tail_test.go` | `cd360528565522af9acbd35b22ad2d853033e6cad94d8ca231edb1fffd8cb7f0` |
| `pkg/loghttp/patterns_test.go` | `a04cf415adefb5a93488f5406d17ba9c2353865710f208287390a1fee89945df` |
| `http-parameters.json` | `c69285c2b8975e6ddf9a67ac8af315b7f4104e65af3abb22d8769fe77bef262b` |
| `http-parameters-producer.go.txt` | `840c24ba78ea64a71633778c9fc65af6d0421a0a65442c59a7dcd8a2bc9472a2` |
