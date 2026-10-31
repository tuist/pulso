# Grafana Mimir math and range-function integration cases

These are **1590 selected query-engine integration cases in 85 fixture files**, copied without
changing their input samples, expressions or expected values from
[grafana/mimir at dd039cdd4ba466adb5d28280bf1b8a374a98432e](https://github.com/grafana/mimir/tree/dd039cdd4ba466adb5d28280bf1b8a374a98432e).
This is not Mimir's complete test suite or its Docker/service integration suite.

The upstream entry points are `TestUpstreamTestCases` and `TestOurTestCases` in
[`pkg/streamingpromql/comparisons/comparisons_test.go`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/pkg/streamingpromql/comparisons/comparisons_test.go).
They execute the DSL against real query engines; Mimir's own cases also run
against Prometheus. The imported files cover the six basic math functions,
`clamp`/`clamp_min`/`clamp_max` with changing bounds and missing scalar inputs,
histogram omission, arithmetic/nested calls, NaN/infinities, signed zero,
metric-name collisions, and merging disjoint series lifetimes in range queries.
They also cover `present_over_time` with empty/nonempty windows, NaN,
explicit-resolution subqueries, mixed float/histogram lifetimes and collisions.
`last_over_time` adds name preservation, newest-sample type selection, sparse
data and initially empty ranges, with complete histogram outputs.
`first_over_time` checks earliest-sample selection over the same upstream
float/histogram, sparse and NaN inputs, preserving names and complete histograms.
`ts_of_first_over_time`/`ts_of_last_over_time` add source timestamp selection,
metric-name removal, mixed-type ordering and fractional-second observations.
`changes`/`resets` add chronological transition counts, float/NaN behavior,
exponential/custom native histogram resets, zero buckets and moving windows.
`deriv` adds least-squares gauge slopes, mixed-window info annotations and
silent histogram-only omission, with related `rate`/`predict_linear` controls.
`mad_over_time` adds median absolute deviation over sparse, even/odd, constant,
NaN and mixed float/histogram inputs.
`ts_of_min_over_time`/`ts_of_max_over_time` add float-extremum source timestamps,
newest-tie selection, mixed-window annotations and fractional observations.
`label_join` adds ordered/missing sources, destination overwrite/removal,
metric-name rewriting, UTF-8 label names, complete histogram preservation and
disjoint/overlapping result identities.
`quantile_over_time` adds interpolated ranks, per-step scalar quantiles,
NaN/infinity observations, exact-ranked infinity boundaries, invalid-quantile
warnings and mixed-histogram notices.
`double_exponential_smoothing` and its `holt_winters` alias add positive/negative
trends, moving windows, per-step scalar factors, nonfinite observations,
mixed-histogram notices and invalid-factor errors.
`deg`/`rad` add angular conversions, large finite inputs and metric-name
collisions; `pi()` adds scalar instant results and constant range matrices.
`asin`/`acos`/`atan` add inverse trigonometry, per-sample domain NaNs, changing
range inputs and metric-name collisions.
`sin`/`cos`/`tan` add circular functions of radian inputs, NaN propagation and
metric-name collisions.
`sinh`/`cosh`/`tanh` add hyperbolic functions, NaN propagation and metric-name
collisions.
`asinh`/`acosh`/`atanh` add inverse hyperbolic functions, per-sample domain NaNs,
changing range inputs and metric-name collisions.
The eleven-feature calendar/operator batch adds eight UTC calendar functions (`year`,
`month`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`
and `minute`), `sort_by_label`/`sort_by_label_desc` and binary `atan2`.
Its seven files add 126 evaluations, retaining existing `time()` and arithmetic
controls as well as range-query sort warnings. Calendar cases cover default and
explicit inputs, leap/calendar boundaries, changing values and histogram omission;
label sorts cover natural ordering, full-label ties, complete histograms and
range-query ineffectiveness; `atan2` covers scalar/vector inputs, NaN and
histogram omission with info annotations.

The scalar-context/extrema batch adds `start`, `end`, `step`, `range`, `min_of`
and `max_of`: 51 evaluations in three files, preserving scalar outputs, range
grids and composed arithmetic. Mimir folds query-context calls through its
vendored Prometheus preprocessing before constructing streaming operators;
these are not unsupported factory names. Pulso's extra controls cover outer
context through nested subqueries, non-grid-aligned ends, equal-bound versus
one-step ranges, per-source isolation and all 98 extrema special-value pairs.

The aggregation batch adds `stddev`, `stdvar`, `quantile` and `count_values`:
88 evaluations in five files. They retain population-dispersion precision and
nonfinite regressions, quantile grouping/ranges/empty-input warnings, complete
negative/native histogram value counts, fixed float labels, signed zero and
grouping-label overwrite collisions. Existing sum/avg controls remain in the
initial Mimir grouping block rather than rewriting it around the new operator.

The time-modifier batch adds negative offsets, unitless durations and numeric,
`start()`/`end()` `@` anchors: 95 evaluations in four files. It includes the
complete 341-line upstream `at_modifier.test`, seven fixed-subquery cases with
varying scalar parameters, and selector/reset controls around the newly
supported negative-offset and unitless-window cases. Source timestamps,
evaluation timestamps and scalar argument times remain distinct. No selected
query or expectation is rewritten, including duration-valued `@` arguments.

The sampling batch adds `limitk` and `limit_ratio`: the complete upstream and
Mimir `limit.test` files, with 130 evaluations. They cover grouped subset
cardinality, original labels/names, complete native histograms, mixed sample
lifetimes, missing observations, NaNs, changing scalar parameters, positive and
negative ratio complements, zero/all selection and capping warnings. These are
complete files, not selected happy paths; their duplicate commands remain.

The duration-expression batch adds the complete upstream `duration_expression.test`:
59 evaluations covering all six arithmetic operators, mixed units, parentheses,
subquery windows/resolutions, signed-offset precedence and nested `step()`/`range()`/
`min_of`/`max_of`. No query or expectation is rewritten. Unit-bearing scalar
arguments use the same duration vocabulary.

The grammar batch adds 14 verbatim evaluations: the initial name-removal block,
including `last_over_time(abs(metric)[10m:])`, and both empty absent/present
omitted-resolution subqueries. Numeric time spellings, UTF-8 grouping/matching
and `label_replace` destination names use source-derived Pulso controls, not
invented upstream integration fixtures. The original service-integration UTF-8
ingestion/selector cases remain separately mapped in `services/README.md`.

The result-type batch adds ten verbatim instant evaluations: five string
results (including parentheses and empty strings), and five raw range matrices
with original timestamps, native histograms, stale markers and empty windows.
Their expressions are additionally sent to the range endpoint and must reject,
even with equal bounds; no invalid range request is silently omitted. Histogram
reset hints are loaded, but the public response cannot expose them, so this
does not establish wire-level reset-hint parity. Separate Pulso controls cover
offsets, anchors, subqueries, mixed types, exact boundary/zero signs, form POST,
tenant isolation and sample/work/intermediate/output quotas.

The histogram-annotation batch adds twelve verbatim evaluations. Four retain
decreasing classic buckets, missing/malformed `le` labels and classic/native
identity collisions; eight retain native NaN-observation quantile/fraction
results and their distinct info notices. They replay as instant and equal-bound
range HTTP requests against memory and native Parquet/S3. Expected diagnostics
omit only upstream AST positions; the monotonicity regex remains permissive as
authored upstream. Pulso currently emits its base monotonicity notice without
Mimir's additional metric/bucket/time/sample summary. Supplementary controls
cover NaN/overflow/escaped bounds, full input identities, native `le` labels,
tiny changes in either direction, duplicate numeric bounds, missing `+Inf`,
stale transitions, form POST and source-local notices.

Additional source-derived controls cover overflowing sums of finite counts,
subnormal changes with zero and nonzero operands, and the strict absolute-floor
boundary. The pinned `vendor/github.com/prometheus/prometheus/util/almost/almost.go`
uses a minimum-normal absolute floor and a maximum-float-capped relative scale;
its SHA-256 is `27c031c9b1efde4ed7d4cf912246b339623e62f7e84290f05a7ed0a90bacbda8`.
The actual pinned `promql/quantile.go` `BucketQuantile`, coalescing and repair
functions (source SHA-256
`47b623317e823b5e2e2bb601f25d86c71d25917fcab6596723ee465ac1e4ac41`)
agree across 10,030 numerical results and repair flags: 10,010 results are
numerically exact and 20 differ by less than `1e-12` relative error. All repair
flags agree exactly. These are primitive validation comparisons, not imported
engine evaluations; the six public regression controls also run through both
HTTP endpoints and adapters without requiring Go.

The plural-quantile continuation adds the complete five-evaluation Mimir
`histogram_quantiles.test` and four selected upstream native evaluations. They
retain multiple/per-step scalar quantiles, sparse output labels, duplicate-step
failures, NaN info, mixed classic/native omission and a composed native
expression. `expect fail` maps to Pulso's exact duplicate-series execution
error; regex warnings omit only the source-position suffix. Extra Pulso controls
cover OpenMetrics zero/integer/exponent/nonfinite labels, UTF-8 destinations,
overwrites/name removal, form POST and batch-shared intermediate/bucket/label
work.

Classic `histogram_fraction` adds three unchanged evaluations, including both
singular/plural NaN-quantile controls from their shared source block. Public
controls cover interpolation across positive/negative/infinite bounds,
duplicate buckets, nonmonotonic counts, changing scalar bounds, malformed labels,
mixed classic/native collisions, source-local notices and shared work limits.
Fractions do not inherit quantile monotonicity repair: decreasing counts may
produce negative fractions without repair notices. Another 14,400 comparisons
against the actual pinned Go `BucketFraction` and `coalesceBuckets` functions
match numerically, including nonfinite counts/bounds and asymmetric random inputs.

Eight further unchanged evaluations cover multi-bucket native NaN sums, matching
fractions and the ±Inf-observation case with no NaN observations. These replay
through instant and equal-bound range HTTP over both adapters. The native
primitive now interpolates within the selected bucket, intentionally replacing
Prometheus 3.5.0's last-bucket quirk with the fix in the pinned Mimir source.
Supplementary primitive controls cover negative, zero and custom buckets.
This is not a wholesale native-arithmetic upgrade or full histogram parity.

The same selection now also retains all twelve classic/custom-native paired
NaN-sum controls. NaN observations are represented in their `+Inf` bucket, so
linear quantiles/fractions produce the original finite values without the
exponential-native NaN notices. The original no-NaN controls remain alongside
both NaN cases; no expected value or notice policy is adapted.

Eight further unchanged evaluations check count-based zero-point extrapolation
for native counter `increase`, alongside identical float-counter inputs. Aligned
and unaligned ends distinguish zero points inside/outside the window. Counts
bound one shared factor, not individual buckets or sums. Public controls cover
`rate`, reset-discarded first observations, zero starts, flat counts, negative
sums and positive/negative/zero/custom buckets; `delta` and `irate` stay unchanged.

The complete ten-evaluation `idelta` block retains positive/negative float deltas,
NaNs, last-pair native selection, short windows, gauge/mixed-type/schema warnings
and mismatched custom-bound subtraction with its info notice. The fixture adapter
preserves gauge hints in count/sum ramps and asserts custom bucket geometry
independently of Pulso's formatter. Only AST-position suffixes are omitted from
diagnostics. Public controls add decreasing gauges, disjoint/asymmetric common
bounds, nanosecond observations, anchors/subqueries, form POST, tenant/source
isolation and shared budgets. Another 4,983 executions of the actual pinned
`intersectCustomBucketBounds`/`addCustomBucketsWithMismatches` and Kahan helpers
match Pulso's custom subtraction: exact geometry and bit-exact finite counts,
including sparse layouts, negative/subnormal counts, NaNs and infinities.

The complete eleven-evaluation `irate` block retains float resets/NaNs, last-pair
selection, gauge/mixed-type warnings, schema-family resets and differing custom
bounds without a reset. Reset detection compares plain source-ordered interval
totals at common custom boundaries, including the final +Inf interval; changing
bounds alone is not a reset. Another 9,950 comparisons against the actual pinned
`FloatHistogram.DetectReset` agree, including sparse buckets, rounded/subnormal
counts and explicit reset/gauge hints. Public HTTP controls distinguish finite/
infinite/missing-bucket decreases, forced hints, separate Parquet objects and
batch-local notices. Reset branches do not report unexecuted subtraction notices.

The complete four-evaluation `delta` block preserves float decreases, native
gauge outputs, non-gauge warnings and mixed-window omission. The adapter expands
same-layout bucket-bearing histogram ramps independently by repeated field
addition, preserving hints. Public controls distinguish whole-window type checks
from endpoint-only schema/gauge checks, ignored middle schemas/hints, short
windows, custom reconciliation, extrapolated bucket values and batch isolation.
All three histogram-difference functions charge endpoint bucket/bound work.

The complete eleven-evaluation custom-bound arithmetic block retains three-step
`sum`/`avg`, counting/grouping/sampling/set controls, binary `+`/`-`, missing
points, complete histogram outputs and binary reconciliation info. Addition now
coarsens mismatched custom layouts with the same source-ordered compensated
mapping as subtraction. Another 9,950 comparisons against the actual pinned
`FloatHistogram.Add` agree on compacted geometry, finite bucket/count/sum bits
and reset hints. Public controls cover asymmetric layouts, both operand orders,
instant/range aggregates, subtraction gauge preservation through a subquery,
source-local info and shared binary bucket/bound work.

Two complete range/group histogram aggregation blocks add 21 evaluations,
retaining full histogram outputs, six group types, both evaluation windows and
last/count/presence/change/delta/rate/increase/reset controls. Native sum/avg and
their over-time forms report custom reconciliation, incompatible families,
mixed inputs and original reset-hint conflicts. Explicit Pulso controls cover
gauge/unknown hints between conflicting reset hints, notices before/after a
failed family, cross-object windows, source names, stale inputs, query-batch
isolation and shared bucket/bound work.

The complete seven-evaluation counter-annotation block retains two-point and
longer gauge rates, invalid middle families, nested histogram-count calls and
successful first-pair family resets. Rate/increase notices now come from actual
endpoint subtraction and reset-recovery additions, using a constant-size set.
Separate Pulso controls cover differing middle layouts with no executed
reconciliation, addition-only/both reconciliation events, reset-discarded first
layouts, original gauge hints, middle-versus-endpoint error precedence,
mixed/stale/short windows, per-source notices and shared native work ceilings.
Implicit iterator-generated reset hints and complete aggregation
precision/annotation parity remain open; this batch does not establish those
contracts.

The complete histogram precision blocks add 26 evaluations for sum/avg,
sum_over_time/avg_over_time, cancellation, overflow-triggered incremental means,
negative/zero buckets and singleton histograms. Commented-out upstream tests
remain comments, including their order/precision limitations. No disabled test
was counted as passing. Public Pulso controls independently exercise compensation
through resolution, custom-bound and zero-threshold changes, and bucket/zero
overflow without count overflow. An additional 10,000 chains / 55,112 calls
against the actual pinned Go `FloatHistogram.KahanAdd` validate compacted
geometry, finite count/sum/zero/bucket bits and nonfinite classes; those are
primitive checks, not more imported engine evaluations. Normal fixture replay
needs neither Go nor downloads.

The complete multiplication/division and mixed histogram/float arithmetic
blocks add 25 unchanged evaluations. They retain scalar/vector operand order,
unary negation, negative/zero factors, complete positive/negative bucket results,
division-by-zero infinities/NaNs and unsupported-operator info notices. The
HTTP adapter independently applies the pinned iterator's zero-bucket rule:
only strictly positive zero counts (including +Inf) are serialized as buckets;
negative/NaN zero counts are omitted, without changing the raw fixture oracle.
Public Pulso controls exercise gauge identity through double negation and
nested multiplication/division, with delta/rate warnings and a positive-scaling
counter control. Another 26,000 actual pinned Go Mul/Div calls agree on full
geometry, finite field bits, nonfinite classes and hints, including signed zero,
infinity and NaN factors. These are primitive checks, not additional engine cases.

The complete native histogram trimming block adds 124 unchanged evaluations
for `</` and `>/`: scalar/vector cutoffs, matching/grouping, chained trims,
linear/custom and exponential interpolation, biased zero buckets, infinite
bounds, empty histograms and histogram-fraction identities. No-op trims retain
the original count/sum; changed populations recompute them in source order.
Public Pulso controls cover operand-type notices, admission, form POST, hint
preservation, immutable source reuse, tenant/batch isolation and work/scan/sample/
result ceilings. Scalar-only trims reject at admission; upstream admits syntax
that its scalar execution path cannot execute.

Another 49,840 actual pinned Go `TrimBuckets` calls match all finite field bits,
signed zeros, nonfinite classes, layouts and reset hints. These use Go 1.25.10
with `-gcflags=all=-d=fmahash=n`, matching Pulso's existing unfused histogram
arithmetic profile. Default arm64 fused arithmetic differs in rounding and can
retain finite cancellation where unfused products overflow; this is not a claim
of bit parity across Go architectures/compiler modes. Normal fixture replay
uses unchanged upstream expectations and requires no Go or network access.

The complete empty/property/dispersion and bucket-retention blocks add 37
unchanged evaluations. They retain empty exponential/custom histograms, count/
sum/average, geometric-midpoint variance/deviation, sparse positive/negative
buckets and NaN/infinite sums. Equality, inequality, changes and distinct-value
counts must inspect buckets even beneath count/sum/timestamp/absence wrappers.
These already-supported numerical results now have HTTP/Parquet/S3 regression
coverage; they are not newly implemented functions. Separate Pulso controls
close missing query-work charges for variance/deviation and histogram equality:
empty bucket slots and sparse custom bounds spend work before traversal, while
constant-time count/sum/average do not. Repeated steps and query-batch sources
share the ceiling; false comparisons and `bool` cannot evade it. These controls
are not additional upstream evaluations.

Another 65 unchanged evaluations cover repeated and accumulating histograms,
resolution changes, positive/negative/zero/custom buckets, newest-sample
selection, `sum(last_over_time(...))`, rates and bucket-only resets despite
growing total counts. The test-only ramp loader merges disjoint sides/offsets
and finer-schema increments without calling Pulso's histogram arithmetic.
All seven source ramps (149 generated samples) match actual pinned Go
`Copy().Add()` using `Equals` and reset-hint comparison. This validates the
fixture loader, separately from the HTTP engine replay; it does not close
implicit storage-iterator reset hints or establish provider/client acceptance.

The complete positive/negative/mixed quantile and fraction interpolation blocks,
plus constant-histogram rates, add 147 unchanged evaluations. They cover biased
and symmetric zero buckets, multiple resolutions, empty/infinite/NaN endpoints,
quantile/fraction inverses and zero-rate downstream functions. These numerical
paths already existed. Separate Pulso controls close missing traversal-work
charges for multiplication, nonzero division and unary negation, including
scalar/vector operand orders, multiplication by zero, repeated steps and shared
batches. Unary plus, zero division and unused custom bounds do not incur bucket
traversal charges. These controls are not counted as upstream evaluations.

The complete native/mixed binary-operator blocks add 25 unchanged evaluations:
matching, scalar/vector scaling, mixed float/histogram timestamp grids,
unsupported operand-type info notices, incompatible-family warnings and
custom-bound reconciliation info. Binary addition/subtraction now publish the
missing incompatible-layout warning rather than silently omitting a pair.
Separate source-derived Pulso controls cover reset/no-reset collision warnings,
preserved successful values, noncollision hints, schema-error precedence and
query-batch notice isolation in both source orders. These controls are not
additional upstream evaluations; AST positions and implicit iterator hints
remain outside the claimed scope.

Separate Pulso reset controls check empty sides, zero-prefixed missing tails,
custom +Inf buckets, all-zero nonreset tails and explicit hint precedence
through resets/irate/rate/increase on both adapters. Another 50,000 actual pinned
Go DetectReset calls agree across custom/exponential layouts, sparse sides,
zero/schema changes, hint overrides and both reset/nonreset outcomes. No upstream
engine evaluation count includes those controls or primitive comparisons.

## Exact source selection

Paths below are relative to Mimir's `pkg/streamingpromql/testdata/` at that
revision. Each local file concatenates the listed inclusive source line ranges,
in order. Exclusions are described below; expectations are not regenerated from
Pulso.

| Local file | Source | Inclusive ranges | Evaluations |
| --- | --- | --- | --- |
| `upstream-math.test` | `upstream/functions.test` | 1–5, 2017–2030, 2042–2052, 2270–2358 | 21 |
| `mimir-math.test` | `ours/functions.test` | 1–2, 74–77, 90–92, 96–97, 406–412, 437–439, 452–454, 476–484, 503–504 | 7 |
| `collision.test` | `ours/collision.test` | 1–17 | 2 |
| `upstream-clamp.test` | `upstream/functions.test` | 1–5, 615–708, 2651–2702 | 27 |
| `mimir-clamp.test` | `ours/functions.test` | 1–2, 515–558 | 9 |
| `upstream-presence.test` | `upstream/functions.test` | 1–5, 2211–2219, 2222–2270 | 19 |
| `mimir-presence.test` | `ours/functions.test` | 1–2, 98–99, 115–120, 404–412, 485–487 | 3 |
| `upstream-last.test` | `upstream/functions.test` | 1–5, 1918–1938, 1979–1990 | 1 |
| `mimir-last.test` | `ours/functions.test` | 1–2, 98–99, 109–114, 767–780 | 3 |
| `upstream-first.test` | `upstream/functions.test` | 1–5, 1918–1938, 1991–2002 | 1 |
| `upstream-endpoint-timestamps.test` | `upstream/functions.test` | 1–5, 1655–1695 | 9 |
| `mimir-endpoint-timestamps.test` | `ours/functions.test` | 1–2, 1391–1423 | 6 |
| `upstream-transitions.test` | `upstream/functions.test` | 1–22, 28–117 | 20 |
| `mimir-transitions.test` | `ours/functions.test` | 1–2, 583–622 | 1 |
| `upstream-regression.test` | `upstream/functions.test` | 1–5, 365–425, 449–453, 464–474 | 11 |
| `mimir-regression.test` | `ours/functions.test` | 1–2, 624–658 | 1 |
| `upstream-mad.test` | `upstream/functions.test` | 1–5, 1601–1643 | 7 |
| `mimir-mad.test` | `ours/functions.test` | 1–2, 1490–1534 | 8 |
| `upstream-extremum-timestamps.test` | `upstream/functions.test` | 1–5, 1644–1654 | 2 |
| `mimir-extremum-timestamps.test` | `ours/functions.test` | 1–2, 1349–1390 | 8 |
| `upstream-label-join.test` | `upstream/functions.test` | 1–5, 559–600, 2396–2400, 2410–2419, 2421–2425, 2428–2430 | 10 |
| `mimir-label-join.test` | `ours/functions.test` | 1–2, 263–342 | 12 |
| `upstream-range-quantile.test` | `upstream/functions.test` | 1–5, 1696–1800 | 13 |
| `mimir-range-quantile.test` | `ours/functions.test` | 1–2, 1119–1154 | 2 |
| `upstream-smoothing.test` | `upstream/functions.test` | 1–5, 923–971 | 4 |
| `mimir-smoothing.test` | `ours/functions.test` | 1–2, 1290–1346 | 6 |
| `upstream-angles.test` | `upstream/trig_functions.test` | 1–5, 8–12, 73–112 | 9 |
| `mimir-angles.test` | `ours/functions.test` | 1–2, 183–187, 406–412, 449–451, 488–490 | 3 |
| `upstream-inverse-trig.test` | `upstream/trig_functions.test` | 1–5, 8–12, 28–42 | 3 |
| `mimir-inverse-trig.test` | `ours/functions.test` | 1–2, 74–77, 81–86, 404–412, 416–418, 422–424, 428–430 | 5 |
| `upstream-trig.test` | `upstream/trig_functions.test` | 1–5, 8–27 | 3 |
| `mimir-trig.test` | `ours/functions.test` | 1–2, 404–412, 440–442, 497–499, 509–511 | 3 |
| `upstream-hyperbolic.test` | `upstream/trig_functions.test` | 1–5, 8–12, 43–57 | 3 |
| `mimir-hyperbolic.test` | `ours/functions.test` | 1–2, 404–412, 443–445, 500–502, 512–514 | 3 |
| `upstream-inverse-hyperbolic.test` | `upstream/trig_functions.test` | 1–5, 8–12, 58–72 | 3 |
| `mimir-inverse-hyperbolic.test` | `ours/functions.test` | 1–2, 74–77, 87–89, 404–412, 419–421, 425–427, 431–433 | 4 |
| `upstream-calendar.test` | `upstream/functions.test` | 1–5, 1801–1904 | 32 |
| `mimir-calendar.test` | `ours/functions.test` | 1–2, 780–919 | 34 |
| `upstream-label-sort.test` | `upstream/functions.test` | 1–5, 767–922 | 11 |
| `mimir-label-sort.test` | `ours/functions.test` | 1–2, 1010–1089 | 8 |
| `upstream-atan2.test` | `upstream/operators.test` | 1–5, 527–547 | 4 |
| `mimir-atan2.test` | `ours/binary_operators.test` | 1–2, 8–34, 187–274, 414–444 | 32 |
| `upstream-sort-warnings.test` | `upstream/range_queries.test` | 1–5, 114–127 | 5 |
| `upstream-query-context.test` | `upstream/functions.test` | 1–5, 2431–2553 | 33 |
| `upstream-scalar-extrema.test` | `upstream/functions.test` | 1–5, 2570–2619 | 14 |
| `mimir-scalar-extrema.test` | `ours/functions.test` | 1–2, 1585–1597 | 4 |
| `upstream-dispersion.test` | `upstream/aggregators.test` | 1–14, 188–214, 837–1008 | 36 |
| `upstream-value-count.test` | `upstream/aggregators.test` | 1–5, 439–488, 1010–1073 | 10 |
| `mimir-value-count.test` | `ours/aggregators.test` | 1–2, 666–706 | 4 |
| `upstream-aggregate-quantile.test` | `upstream/aggregators.test` | 1–5, 490–598 | 11 |
| `mimir-aggregate-quantile.test` | `ours/aggregators.test` | 1–70, 170–176, 184–187, 229–234, 242–245, 247–252, 263–266, 268–273, 284–287, 289–296, 307–310, 708–735, 750–761 | 27 |
| `upstream-time-anchors.test` | `upstream/at_modifier.test` | 1–341 | 78 |
| `upstream-time-duration.test` | `upstream/functions.test` | 1–27 | 3 |
| `upstream-time-offset.test` | `upstream/selectors.test` | 1–48 | 7 |
| `upstream-time-subquery.test` | `upstream/subquery.test` | 1–5, 168–195 | 7 |
| `upstream-limit.test` | `upstream/limit.test` | 1–179 | 37 |
| `mimir-limit.test` | `ours/limit.test` | 1–342 | 93 |
| `upstream-duration-expressions.test` | `upstream/duration_expression.test` | 1–256 | 59 |
| `upstream-default-subquery.test` | `upstream/name_label_dropping.test` | 1–57 | 12 |
| `upstream-default-subquery-empty.test` | `upstream/functions.test` | 1–5, 2143–2145, 2220–2221 | 2 |
| `upstream-string-results.test` | `upstream/literals.test` | 1–5, 66–79 | 5 |
| `upstream-range-results.test` | `upstream/range_queries.test` | 1–5, 78–112 | 5 |
| `upstream-histogram-annotations.test` | `upstream/native_histograms.test` | 1–5, 2032–2057 | 4 |
| `upstream-histogram-nan.test` | `upstream/native_histograms.test` | 1–5, 1615–1630, 1636–1661 | 8 |
| `upstream-histogram-nan-multi.test` | `upstream/native_histograms.test` | 1–5, 1665–1801 | 20 |
| `upstream-histogram-quantiles.test` | `upstream/native_histograms.test` | 1–5, 37–40, 70–74, 1615–1620, 1631–1635, 1663–1664, 2033–2045, 2059–2062, 2122–2124, 2131–2134 | 4 |
| `upstream-histogram-fractions.test` | `upstream/histograms.test` | 1–5, 1281–1303 | 3 |
| `upstream-histogram-extrapolation.test` | `upstream/native_histograms.test` | 1–5, 1802–1861 | 8 |
| `upstream-idelta.test` | `upstream/functions.test` | 1–5, 311–363 | 10 |
| `upstream-irate.test` | `upstream/functions.test` | 1–5, 222–280 | 11 |
| `upstream-delta.test` | `upstream/functions.test` | 1–5, 282–310 | 4 |
| `upstream-custom-histogram-arithmetic.test` | `upstream/native_histograms.test` | 1–5, 1224–1294, 2152–2180 | 17 |
| `upstream-histogram-range-aggregation.test` | `upstream/native_histograms.test` | 1–5, 1294–1387 | 20 |
| `upstream-histogram-group-aggregation.test` | `upstream/native_histograms.test` | 1–5, 1203–1223, 1389–1410 | 3 |
| `upstream-histogram-counter-annotations.test` | `upstream/native_histograms.test` | 1–5, 1096–1147 | 7 |
| `upstream-histogram-precision.test` | `upstream/native_histograms.test` | 1–5, 1412–1614, 2136–2151 | 32 |
| `upstream-histogram-scaling.test` | `upstream/native_histograms.test` | 1–5, 955–1060 | 25 |
| `upstream-histogram-trimming.test` | `upstream/native_histograms.test` | 1–5, 2181–2678 | 124 |
| `upstream-histogram-properties.test` | `upstream/native_histograms.test` | 1–35, 321–415 | 22 |
| `upstream-histogram-bucket-retention.test` | `upstream/native_histograms.test` | 1–5, 2680–2735 | 15 |
| `upstream-histogram-fundamentals.test` | `upstream/native_histograms.test` | 1–5, 36–319, 1062–1094 | 65 |
| `upstream-histogram-interpolation.test` | `upstream/native_histograms.test` | 1–5, 417–953, 1147–1202, 2117–2135 | 150 |
| `mimir-histogram-binary.test` | `ours/binary_operators.test` | 1–2, 189–345 | 25 |
| `mimir-histogram-quantiles.test` | `ours/histogram_quantiles.test` | 1–67 (complete) | 5 |
| `upstream-classic-histograms.test` | `upstream/histograms.test` | 1–1327 (complete) | 183 |

For example, reproduce the first extraction from a checkout of that revision:

```sh
sed -n '1,5p;2017,2030p;2042,2052p;2270,2358p' \
  pkg/streamingpromql/testdata/upstream/functions.test
```

Reproduce the clamp extractions from the same pinned checkout:

```sh
sed -n '1,5p;615,708p;2651,2702p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-clamp.test
sed -n '1,2p;515,558p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-clamp.test
```

Reproduce the presence extractions from the same pinned checkout:

```sh
sed -n '1,5p;2211,2219p;2222,2270p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-presence.test
sed -n '1,2p;98,99p;115,120p;404,412p;485,487p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-presence.test
```

The original presence selection excludes the omitted-resolution subquery at
upstream lines 2220–2221; it is now replayed unchanged in
`upstream-default-subquery-empty.test`. Mimir's multi-instrument load block contributes only the queried
`some_metric_count` row; unused source rows 100–101 are excluded. Every selected
query, its input samples and its expected output remain unchanged.

Reproduce the last-sample extractions from the same pinned checkout:

```sh
sed -n '1,5p;1918,1938p;1979,1990p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-last.test
sed -n '1,2p;98,99p;109,114p;767,780p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-last.test
```

The last-sample selection also excludes unused load rows 100–101. It preserves
the upstream split-load ordering workaround and `expect no_info` assertion.
The count-only histogram sequence `{{count:5}}+{{count:5}}x4` expands using
upstream's zero defaults and inclusive sequence length. Bucket-bearing histogram
arithmetic is not implemented by the test adapter and fails rather than being
silently approximated.

Reproduce the first-sample extraction from the same pinned checkout:

```sh
sed -n '1,5p;1918,1938p;1991,2002p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-first.test
```

This preserves the same split-load workaround and `expect no_info` assertion.

Reproduce the endpoint-timestamp extractions from the same pinned checkout:

```sh
sed -n '1,5p;1655,1695p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-endpoint-timestamps.test
sed -n '1,2p;1391,1423p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-endpoint-timestamps.test
```

All fifteen evaluations and their load directives are retained. The test adapter
parses compound `10s53ms` intervals into integer nanoseconds before multiplying
by the sample index; timestamps are not rounded to whole seconds or accumulated
through floating-point additions. Indented comments, including `#empty`, are
removed as DSL comments; an empty expectation still asserts an empty result.

Reproduce the transition-count extractions from the same pinned checkout:

```sh
sed -n '1,22p;28,117p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-transitions.test
sed -n '1,2p;583,622p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-transitions.test
```

The transition selection excludes upstream lines 23–27, whose unitless `[600]`
window is now replayed separately in `upstream-time-duration.test`. No query
is rewritten to hide that earlier exclusion. All selected load rows and expectations
remain unchanged, including the complete sixteen-series Mimir range evaluation.
The adapter maps upstream `z_bucket_w`, `z_bucket` and `custom_values` directly
to zero threshold, zero count and custom bounds, following the upstream
histogram parser; bucket counts remain absolute with default offset zero.
Unsupported histogram fields still fail rather than being ignored. `anchored`
modifiers remain unsupported; these are ordinary range-window cases.

Reproduce the least-squares regression extractions from the same pinned checkout:

```sh
sed -n '1,5p;365,425p;449,453p;464,474p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-regression.test
sed -n '1,2p;624,658p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-regression.test
```

The selection preserves every `deriv` evaluation in upstream lines 365–474 and
the complete sixteen-series Mimir range case. It also retains three supported
numeric-horizon predictions, one rate control and the prediction/derivative
identity. Duration-valued scalar arguments and `@` modifiers in upstream lines
426–448 and 454–463 are excluded, not rewritten. Selector `@` modifiers are
now covered by the separate time-modifier fixtures; duration-valued scalar
arguments are now supported and checked by the duration-expression batch's
scalar and parser controls, but these excluded prediction rows are not counted
as imported evaluations.
All selected load rows, expressions, expected values and annotation directives
remain byte-identical. The adapter checks every `expect info`, `expect no_info`,
`expect no_warn` and `expect info regex:` directive, before or after sample rows,
including multiple directives around an empty result. Unknown directives fail
rather than being skipped.
As upstream does for these selected cases, `expect info` requires a nonempty
info array, not exact text or per-series counts. Regex directives require every
pattern to match an info and every info to match a pattern. Mixed-range infos
include the original metric name, JSON-quoted, and deduplicate by name within
each query. Expression positions, arbitrary-name Go quoting parity, complete
annotation taxonomy and deployment/capacity acceptance are not claimed.

Reproduce the median-absolute-deviation extractions from the same pinned checkout:

```sh
sed -n '1,5p;1601,1643p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-mad.test
sed -n '1,2p;1490,1534p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-mad.test
```

All fifteen selected evaluations, complete load rows and annotation assertions
are byte-identical to upstream. This includes Mimir lines 1529–1532 and their
unchanged mixed-window diagnostic regex; that case is no longer excluded.

Reproduce the extremum-timestamp extractions from the same pinned checkout:

```sh
sed -n '1,5p;1644,1654p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-extremum-timestamps.test
sed -n '1,2p;1349,1390p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-extremum-timestamps.test
```

All ten evaluations preserve their inputs, expressions, outputs and annotation
regexes unchanged, including trailing directives. The odd scrape interval checks
fractional timestamps; Mimir's mixed rows distinguish float selection from
histogram selection and newest from oldest equal extrema. No runtime operator
implementation from Mimir is copied.

Reproduce the label-join extractions from the same pinned checkout:

```sh
sed -n '1,5p;559,600p;2396,2400p;2410,2419p;2421,2425p;2428,2430p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-label-join.test
sed -n '1,2p;263,342p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-label-join.test
```

All 22 evaluations retain their exact inputs, queries and directives; unrelated
`label_replace` evaluations in the shared upstream load blocks are excluded.
The Mimir block is complete, including four invalid-name cases and two deliberate
duplicate-label failures without delayed metric-name removal. The test adapter
accepts quoted UTF-8 expected label keys, without changing the fixture labels.

Four invalid-name regex directives explicitly map to Pulso's exact HTTP 400
`bad_data` response: empty/invalid-UTF-8 source or destination names reject during
parsing instead of panicking during evaluation. Only those four diagnostic
mappings are accepted; unknown directives still fail. Duplicate-label directives,
including `eval_fail` with no diagnostic body, assert Pulso's existing exact HTTP
422 duplicate-series error. These are disclosed error-diagnostic adaptations,
not exact upstream error-message or status parity. Successful label sets, values,
histograms, timestamp grids and omissions are not adapted. No Mimir runtime
implementation is copied.

Reproduce the range-quantile extractions from the same pinned checkout:

```sh
sed -n '1,5p;1696,1800p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-range-quantile.test
sed -n '1,2p;1119,1154p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-range-quantile.test
```

All fifteen evaluations retain their load blocks, queries, expected samples and
annotation directives unchanged. These are complete contiguous quantile sections,
including three invalid-quantile warning cases and both Mimir edge-case and
changing-scalar evaluations. The adapter now asserts `expect warn` as well as
`expect no_warn`; warnings are not silently discarded. No runtime operator
implementation from Mimir is copied. Diagnostics omit upstream expression
positions and do not claim arbitrary-number formatting parity.

Reproduce the smoothing extractions from the same pinned checkout:

```sh
sed -n '1,5p;923,971p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-smoothing.test
sed -n '1,2p;1290,1346p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-smoothing.test
```

All ten evaluations preserve the complete contiguous smoothing sections,
including the legacy alias, scalar ramps, annotations and both failure messages.
The adapter asserts those exact messages as HTTP 422 execution errors, not any
failed request; upstream HTTP status parity is not established by engine fixtures.
Scalar ramps now support negative increments and use repeated addition, matching
the pinned vendored Prometheus `promql/parser/generated_parser.y` lines 943–950.
Index multiplication would round `0.1+0.1x9` to exactly 1 at its last observation
instead of the producer's slightly smaller value, incorrectly rejecting a valid
fixture. No expected input or output is rewritten and no runtime implementation
from Mimir is copied.

Reproduce the angular extractions from the same pinned checkout:

```sh
sed -n '1,5p;8,12p;73,112p' \
  pkg/streamingpromql/testdata/upstream/trig_functions.test > upstream-angles.test
sed -n '1,2p;183,187p;406,412p;449,451p;488,490p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-angles.test
```

All twelve evaluations keep the source samples, expressions and expectations
unchanged. The trig selection retains the common load block and complete
`deg`/`rad`/`pi` sections; inverse trigonometry is covered separately below.
Mimir's selection includes the complete `pi()` range case, its shared collision
load block and both angular collision queries. Existing exact HTTP collision
diagnostic mapping applies. The adapter recognizes the upstream scalar expected
line without adding vector labels; it asserts the scalar instant envelope, then
replays it as an empty-label one-step range matrix. No runtime implementation
from Mimir or Prometheus is copied.

Reproduce the inverse-trigonometric extractions from the same pinned checkout:

```sh
sed -n '1,5p;8,12p;28,42p' \
  pkg/streamingpromql/testdata/upstream/trig_functions.test > upstream-inverse-trig.test
sed -n '1,2p;74,77p;81,86p;404,412p;416,418p;422,424p;428,430p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-inverse-trig.test
```

All eight evaluations preserve their original loads, expressions and expected
values. The complete upstream `asin`/`acos`/`atan` section includes both valid
and out-of-domain samples in the same response. Mimir contributes both inverse
circular range sections and all three corresponding collision cases, using the
existing exact HTTP collision-diagnostic mapping. No upstream runtime code or
test runner is copied. Direct circular functions are covered separately below;
inverse hyperbolic functions have their own selection below.

Reproduce the circular-trigonometric extractions from the same pinned checkout:

```sh
sed -n '1,5p;8,27p' \
  pkg/streamingpromql/testdata/upstream/trig_functions.test > upstream-trig.test
sed -n '1,2p;404,412p;440,442p;497,499p;509,511p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-trig.test
```

All six evaluations preserve the source loads, expressions and expectations.
The complete upstream `sin`/`cos`/`tan` section covers radian inputs and NaN;
Mimir contributes all three metric-name collisions. Existing exact HTTP
collision-diagnostic mapping applies. Runtime calls platform math, as the pinned
Mimir operators call Go math; no upstream runtime or test runner is copied.

Reproduce the hyperbolic extractions from the same pinned checkout:

```sh
sed -n '1,5p;8,12p;43,57p' \
  pkg/streamingpromql/testdata/upstream/trig_functions.test > upstream-hyperbolic.test
sed -n '1,2p;404,412p;443,445p;500,502p;512,514p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-hyperbolic.test
```

All six evaluations retain the source loads, expressions and expectations,
including the upstream approximate `sinh(10)` expectation. Mimir contributes
all three metric-name collision cases, with the existing exact HTTP diagnostic
mapping. Separate controls below use stricter independent Go expectations;
imported expectations are not regenerated or tightened. No upstream runtime
or runner is copied.

Reproduce the inverse-hyperbolic extractions from the same pinned checkout:

```sh
sed -n '1,5p;8,12p;58,72p' \
  pkg/streamingpromql/testdata/upstream/trig_functions.test > upstream-inverse-hyperbolic.test
sed -n '1,2p;74,77p;87,89p;404,412p;419,421p;425,427p;431,433p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-inverse-hyperbolic.test
```

All seven evaluations preserve the source loads, expressions and expectations.
The complete upstream inverse-hyperbolic section includes valid and invalid
`atanh` inputs in one result; Mimir adds its changing-range `atanh` case and
all three metric-name collisions. Existing exact HTTP collision-diagnostic
mapping applies. No upstream runtime implementation or runner is copied.

Reproduce the calendar extractions from the same pinned checkout:

```sh
sed -n '1,5p;1801,1904p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-calendar.test
sed -n '1,2p;780,919p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-calendar.test
```

All 66 evaluations preserve the complete calendar sections, including existing
`time()` controls, scalar expected lines, histogram-only omissions and Mimir's
gaps, mixed lifetimes and composed input values. The calendar compatibility
contract freezes Go 1.25 arm64's NaN-to-Unix-epoch and saturating signed-int64
overflow conversions, including infinities; it does not claim cross-architecture
nonfinite conversion parity. Those edge-case semantics are Pulso compatibility
choices, not additional expectations in these imported calendar sections.

Reproduce the label-sort extractions from the same pinned checkout:

```sh
sed -n '1,5p;767,922p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-label-sort.test
sed -n '1,2p;1010,1089p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-label-sort.test
```

All nineteen evaluations retain their original inputs, output rows and ordering
directives. Natural sorting uses ASCII digit/nondigit chunks with bounded int64
numeric conversion and lexical comparison for chunks outside that range.
Equal requested label strings fall back to full-label lexical comparison;
descending reverses that fallback too. With no requested labels, full-label
lexical comparison applies directly. There is no guaranteed relative order for
unequal strings that are naturally equivalent (such as differently zero-padded
numbers) or empty-string comparisons: the pinned comparator is ambiguous for
these inputs. The imported cases do not establish a total-order guarantee there.
Histograms retain their complete outputs; ordering is asserted only for instant
vectors, not range matrices.

Reproduce the `atan2` extractions from the same pinned checkout:

```sh
sed -n '1,5p;527,547p' \
  pkg/streamingpromql/testdata/upstream/operators.test > upstream-atan2.test
sed -n '1,2p;8,34p;187,274p;414,444p' \
  pkg/streamingpromql/testdata/ours/binary_operators.test > mimir-atan2.test
```

All 36 evaluations preserve their load blocks, queries, expected values and
annotation directives. Mimir's selection deliberately retains its surrounding
arithmetic controls, including histogram addition/subtraction/scaling, unsupported
histogram-pair omissions and scalar arithmetic; it is not 32 new `atan2` cases.
No runtime operator implementation or test runner is copied.

Reproduce the sort-warning extraction from the same pinned checkout:

```sh
sed -n '1,5p;114,127p' \
  pkg/streamingpromql/testdata/upstream/range_queries.test > upstream-sort-warnings.test
```

All five empty-result evaluations and their `expect warn msg:` directives are
unchanged, including nested `sum(sort(...))`. The adapter maps the known warning
message exactly after removing upstream expression positions such as `(1:1)`
and `(1:5)`; it does not accept an arbitrary warning or claim source-position
parity. The warning is required even without matching series.

Reproduce the scalar-context/extrema extractions from the same pinned checkout:

```sh
sed -n '1,5p;2431,2553p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-query-context.test
sed -n '1,5p;2570,2619p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-scalar-extrema.test
sed -n '1,2p;1585,1597p' \
  pkg/streamingpromql/testdata/ours/functions.test > mimir-scalar-extrema.test
```

All 51 evaluations preserve complete commands and expected rows. The separate
selector `@ start()`/`@ end()` section at upstream lines 2556–2567 is excluded
from this scalar-function selection; the subsequent time-modifier batch covers
these anchors independently. No selected query is rewritten to bypass an
unsupported construct. `step()` is zero when
start equals end, including the runner's one-step replay of instant cases.
The four context functions use outer query settings, not subquery settings.
Pulso keeps its existing nanosecond timestamp precision, unlike upstream's
millisecond boundaries, and saturates `range()` at Go's int64 duration limit.
Scalar extrema use Go `math.Min`/`math.Max`, not NaN-ignoring aggregations or
Go's built-in `min`/`max`: the selected infinity wins even over NaN. The full
NaN/infinity/signed-zero matrices and sub-millisecond behavior are separate
Pulso controls, not additional imported cases.

Reproduce the aggregation extractions from the same pinned checkout:

```sh
sed -n '1,14p;188,214p;837,1008p' \
  pkg/streamingpromql/testdata/upstream/aggregators.test > upstream-dispersion.test
sed -n '1,5p;439,488p;1010,1073p' \
  pkg/streamingpromql/testdata/upstream/aggregators.test > upstream-value-count.test
sed -n '1,2p;666,706p' \
  pkg/streamingpromql/testdata/ours/aggregators.test > mimir-value-count.test
sed -n '1,5p;490,598p' \
  pkg/streamingpromql/testdata/upstream/aggregators.test > upstream-aggregate-quantile.test
sed -n '1,70p;170,176p;184,187p;229,234p;242,245p;247,252p;263,266p;268,273p;284,287p;289,296p;307,310p;708,735p;750,761p' \
  pkg/streamingpromql/testdata/ours/aggregators.test > mimir-aggregate-quantile.test
```

All 88 commands and their expectations are unchanged, including parenthesized
strings, invalid UTF-8 label rejection and the NaN quantile warning regex.
The warning mapping removes only its source-location suffix because Pulso's
AST does not retain positions. Invalid label UTF-8 maps explicitly to Pulso's
parse-time HTTP400, not an arbitrary engine error. The runner handles negative
histogram input buckets and braces inside quoted histogram value labels; it
does not discard them to obtain passing float-only cases. No unported sum/avg
mixed-type warning block is claimed by selecting only its independent quantile
commands. Histogram-only, mixed float/histogram, NaN/infinity, exact-ranked
infinity, dynamic quantile and destination-overwrite cases remain included.
Mimir's group-then-set count_values implementation retains the generated label
even if `without` names it, unlike its vendored Prometheus engine; a separate
Pulso control freezes the explicitly documented Mimir choice. Other local
controls check smallest-subnormal/fixed decimal labels, custom and zero-overlap
histogram text, selected-step staleness, budgets and source-local notices.

Reproduce the time-modifier extractions from the same pinned checkout:

```sh
sed -n '1,341p' \
  pkg/streamingpromql/testdata/upstream/at_modifier.test > upstream-time-anchors.test
sed -n '1,27p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-time-duration.test
sed -n '1,48p' \
  pkg/streamingpromql/testdata/upstream/selectors.test > upstream-time-offset.test
sed -n '1,5p;168,195p' \
  pkg/streamingpromql/testdata/upstream/subquery.test > upstream-time-subquery.test
```

All 95 evaluations retain complete load/query/expectation blocks, including
negative offsets, duration-valued anchors and nested fixed subqueries with
time-varying prediction horizons, quantiles and smoothing factors. Only data
windows are pinned: scalar parameters and prediction reference times follow
the outer evaluation step. `start()`/`end()` anchors use the requested outer
boundaries, not the last grid point or an inner subquery clock. Pulso retains
nanosecond precision rather than upstream millisecond quantization; decimal
and exponent seconds normalize exactly, with the existing one-year window/
absolute-offset ceiling and signed-int64 anchor bound. Hexadecimal/underscore
time literals and omitted subquery resolutions are covered by the grammar batch
below; `anchored`/`smoothed` modifiers remain unsupported. Fixed windows still spend
query work per step; this adds no upstream step-invariant operator cache.

Separate memory/S3 controls check exact source-time and left-open window
boundaries, stale/native-histogram selection, negative query anchors with future
offsets, form POST, tenant isolation, nanosecond outer boundaries at contemporary
epochs, unaligned ends, nested fixed clocks, varying scalar parameters and
per-source batch anchors. Sample/work/result and cumulative batch limits remain
enforced, with physical footer-row limits checked on S3. Shifted windows outside
int64 intersect the storage domain without wrapping or clamping observations;
generated subquery grids outside int64 reject. Negative source-time storage is
still outside S3's object-key contract, so only memory stores that extra edge
sample. These are Pulso controls, not additional upstream evaluations or
provider/capacity/client acceptance.

Reproduce the duration-expression fixture from the same pinned revision:

```sh
git show dd039cdd4ba466adb5d28280bf1b8a374a98432e:pkg/streamingpromql/testdata/upstream/duration_expression.test \
  > upstream-duration-expressions.test
```

The complete file has 30 instant and 29 range evaluations, or 89 requests per
adapter. Pinned `vendor/github.com/prometheus/prometheus/promql/durations.go`
lines 24–174 and `parser/generated_parser.y` lines 1222–1409 define the
outer-context evaluation and offset binding contracts. Pulso resolves durations
once per source before storage reads, not on nested grids. Only literals,
`step()`, `range()`, `min_of`, `max_of`, arithmetic and parentheses are admitted.
Invalid divisions, nonfinite final values and nonpositive windows/resolutions
reject even on empty data. Duration ASTs share the 256-node/64-depth query
ceiling, and their evaluation spends work from the existing shared budget.

Pulso deliberately retains its one-year literal/result ceiling and rounds the
completed expression to nanoseconds, ties away from zero. Upstream allows wider
durations and truncates calculated durations to milliseconds. Bare decimal time
literals keep exact existing parsing; expression operands retain subnanosecond
fractions until final rounding. This corpus is exact on its supplied values,
not evidence of submillisecond or full duration-range parity. The extra instant
control translates Mimir `engine_test.go` lines 5710–5743
(`TestInstantQueryDurationExpression`); it uses the original 1ms load and 5ms
query with an independently asserted count of one. Other local controls cover
rounding boundaries, histogram selection, nested clocks, non-grid ends,
GET/form POST, tenant/source isolation and sample/work/intermediate/result/scan
and cumulative batch budgets. They are not counted as imported DSL evaluations.

Reproduce the default-resolution selections from the same pinned checkout:

```sh
sed -n '1,57p' pkg/streamingpromql/testdata/upstream/name_label_dropping.test \
  > upstream-default-subquery.test
sed -n '1,5p;2143,2145p;2220,2221p' pkg/streamingpromql/testdata/upstream/functions.test \
  > upstream-default-subquery-empty.test
```

All 14 evaluations are instant queries, replayed as both instant and one-step
range requests: 56 HTTP requests over both adapters. The minute default is
pinned by Mimir `pkg/streamingpromql/config.go:304` and vendored Prometheus
`promql/promqltest/test.go:109`; it does not depend on the outer step or window.
The pinned parser's `generated_parser.y:400–435,693–727,1129–1150` defines quoted
grouping/matching labels, omitted resolutions and numeric duration leaves;
`lex.go:1023–1108` defines numeric spelling. Fractional hexadecimal/binary-exponent
literals and separators in unit-bearing durations remain unsupported. UTF-8
destination validation follows Mimir `operators/functions/label.go:79–82`.

Separate Pulso HTTP controls assert exact minute-grid sums with unaligned
25-second outer steps, nested grids, fixed/future source clocks, empty grids,
outer `step()`/`range()`, tenant isolation, form POST and unchanged budgets.
Hexadecimal/separator windows, offsets, anchors and duration arithmetic are
checked against explicit independently derived values, including adjacent
contemporary-epoch nanoseconds. Label controls check aggregation, set matching,
both group orientations and destination rewriting. Empty, malformed, duplicate
and overlapping decoded names reject; GET/form-POST formatting preserves ASTs.
These controls are not counted as imported upstream evaluations. Query limits,
HTTP parameter grammar, native ABI, stored formats and keyed bytes are unchanged.

Sampling reads these pinned contracts:

- `operators/aggregations/limitklimitratio/{operator,step_arguments}.go`
  under `pkg/streamingpromql/` for per-step validation, group counts, full-label
  hashing and first warning per capping direction.
- Vendored Prometheus `model/labels/labels_stringlabels.go`: the default
  `!slicelabels && !dedupelabels` representation hashes sorted names/values with
  XXH64 seed zero. Each UTF-8 string has a one-byte byte-length below 255;
  otherwise byte 255 precedes three little-endian length bytes. This is not
  separator-based storage `StableHash`. Upstream explicitly makes no
  cross-build/version guarantee for `Labels.Hash()`.
- Vendored Prometheus `promql/engine.go` for ratio thresholds: float-converted
  uint64 hash divided by float-converted MaxUint64 (rounded to 2^64), `< r`
  for nonnegative ratios and `>= 1+r` for negative ratios.

Pulso's `limitk` picks the lowest full-label hashes per group, independently of
sample values/types. Mimir picks its input iteration order; exact chosen
identities when k binds are deliberately **not** claimed identical. Imported
cases assert cardinality and membership there. Ratio selection uses the pinned
default hash, not a claim for alternative label builds or arbitrary versions.
Grouping preserves complete labels and does not change a ratio's membership.
NaN parameters fail even on empty input; k rejects infinities/out-of-int64 values,
truncates finite fractions toward zero and maps remaining negative k to zero.
Out-of-range ratios, including infinities, cap with warnings. No position is
invented for Pulso diagnostics; the replay maps upstream warning suffixes as
documented above.

Independent disposable Go 1.25.10 / `cespare/xxhash/v2` v2.3.0 validation checked
1031 label sets, including the empty set, all payload lengths 0–1027, Unicode
and a 255-byte label name. Fourteen retained public-HTTP controls use fixed Go
hashes to distinguish exact threshold exclusion from inclusion at the adjacent
representable ratio; payload lengths 254/255/256 exercise the prefix boundary.
They also check full-label `limitk` identities, complete float/histogram sample
preservation, rewritten-label hashing, complementary sets, GET/form POST, tenant
isolation, empty-input failures, per-source warning isolation and independent/
cumulative sample/work/result bounds. These controls are not imported cases.
Normal replay requires no Go or network, and no query hash changes storage,
keyed bytes or the native ABI. This is not provider/capacity/client acceptance.

Reproduce the result-type extractions from the same pinned checkout:

```sh
sed -n '1,5p;66,79p' \
  pkg/streamingpromql/testdata/upstream/literals.test > upstream-string-results.test
sed -n '1,5p;78,112p' \
  pkg/streamingpromql/testdata/upstream/range_queries.test > upstream-range-results.test
```

Reproduce the histogram-annotation extractions from the same pinned checkout:

```sh
sed -n '1,5p;2032,2057p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-annotations.test
sed -n '1,5p;1615,1630p;1636,1661p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-nan.test
sed -n '1,5p;1665,1801p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-nan-multi.test
sed -n '1,5p;1281,1303p' \
  pkg/streamingpromql/testdata/upstream/histograms.test > upstream-histogram-fractions.test
sed -n '1,5p;1802,1861p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-extrapolation.test
sed -n '1,5p;311,363p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-idelta.test
sed -n '1,5p;222,280p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-irate.test
sed -n '1,5p;282,310p' \
  pkg/streamingpromql/testdata/upstream/functions.test > upstream-delta.test
sed -n '1,5p;1224,1294p;2152,2180p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-custom-histogram-arithmetic.test
sed -n '1,5p;1294,1387p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-range-aggregation.test
sed -n '1,5p;1203,1223p;1389,1410p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-group-aggregation.test
sed -n '1,5p;1096,1147p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-counter-annotations.test
sed -n '1,5p;1412,1614p;2136,2151p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-precision.test
sed -n '1,5p;955,1060p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-scaling.test
sed -n '1,5p;2181,2678p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-trimming.test
sed -n '1,35p;321,415p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-properties.test
sed -n '1,5p;2680,2735p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-bucket-retention.test
sed -n '1,5p;36,319p;1062,1094p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-fundamentals.test
sed -n '1,5p;417,953p;1147,1202p;2117,2135p' \
  pkg/streamingpromql/testdata/upstream/native_histograms.test > upstream-histogram-interpolation.test
sed -n '1,2p;189,345p' \
  pkg/streamingpromql/testdata/ours/binary_operators.test > mimir-histogram-binary.test
cp pkg/streamingpromql/testdata/upstream/histograms.test upstream-classic-histograms.test
```

SHA-256 hashes preserve original line endings, including the missing final
newlines in `mimir-limit.test` (342 logical lines, 341 newline bytes) and
`upstream-string-results.test`:

| Artifact | SHA-256 |
| --- | --- |
| Complete upstream `upstream/functions.test` | `ece0b612aefb95b029fec6685d6655827c6985ee29ab64790e764fcb67f45b71` |
| Complete upstream `ours/functions.test` | `826775530156676094dacdedec4981a93c26b1228c8e60ca9a7acf553cab3272` |
| Complete upstream `upstream/trig_functions.test` | `856ef7ea78551f4b847c1092cc21f2a9d5691e5b1038bc92a4def2da3ad71c68` |
| Complete upstream `upstream/operators.test` | `5aefc0ee394d20bc1308a64778d833e06a3b424ce725a46cf553c8c46d78487c` |
| Complete upstream `ours/binary_operators.test` | `8878cd8febe276238e02eadb6797cb8361044dbe1d8b0376facb2350c330871b` |
| Complete upstream `upstream/range_queries.test` | `9172b1fb7af71251807e921af38d7210b6718ff7e94db7384ec6befb28e5d2a8` |
| Complete upstream `upstream/aggregators.test` | `92b028897b5da1858097c485b6332fd9f7a227a046467dc8b9c1796b889214a2` |
| Complete upstream `ours/aggregators.test` | `4a7dcceca5973f90cb2e930902546f33174965b1c1e0f23632ab467c63142792` |
| Complete upstream `upstream/at_modifier.test` | `7b702dafee448eb2f0a26beed71c22ec0ab67916605a090cd2448a32509f3d43` |
| Complete upstream `upstream/selectors.test` | `92b0771305a6d315d7b5a0f846301914df10d5b1c1e926522ea6784bda342c6e` |
| Complete upstream `upstream/subquery.test` | `898e3ae15d77a65bc56a3ee3a8980e53b098bc6fabc322a0c8b17c32d791fe1d` |
| Complete upstream `upstream/limit.test` | `31e6224016d5d92988a48ccb3ccc6732708354036294a4f00711ff0524af5312` |
| Complete upstream `ours/limit.test` | `c4c6602c4ac8022522b628a8bdce452832f0a62739c86e98d30f3dcd094dfee3` |
| Complete upstream `upstream/duration_expression.test` | `90405f86486b84e2e12f110f1c486411c99f213e581001a344129a7b087fb792` |
| Complete upstream `upstream/name_label_dropping.test` | `8b6358520aa08cb65c87f204a3e175067ce12590322eb78d593a865d04fbf1d6` |
| Complete upstream `upstream/literals.test` | `6b3f622601de40f019f006b16cdc997f2101ba09d62c540a8077fea5293eba92` |
| Complete upstream `upstream/native_histograms.test` | `6f7d52eea5787cf1f96dba6eceb8adc124177179aa066811cc6eb2a574d399cd` |
| Complete upstream `upstream/histograms.test` | `1eadbc6129d601f4135da5734c49ec223f5ed52aed861897126ba8fb8bdd375a` |
| `upstream-clamp.test` | `6b33f7d8d285983621c269fd088b6744a8413cdeb148f734d809179a7a2330e5` |
| `mimir-clamp.test` | `c91ea2449d23b7da863156481ffc9d6e838a88e9327a068cd62fc88a3baa0f7f` |
| `upstream-presence.test` | `a8f5c9c63568ddd3cd978127babc3d12507d1fc58f2b50dcac94d806903dbd2e` |
| `mimir-presence.test` | `11e1fcc0820452044f0bac53427c960c880d1810f1d69f3cd08ba90cb44796f1` |
| `upstream-last.test` | `c8c0aec8e56c50203420f02609ded52c34db239295211b3c194d850b4a53161f` |
| `mimir-last.test` | `708f2d13216069f8316bd996da0b0b9ca2c6c3935e61185c6c437db39d82708d` |
| `upstream-first.test` | `6952f98017c5122e4df706829f2e3a6ada78e1f7fec2897eda6ab32d2ae66ef8` |
| `upstream-endpoint-timestamps.test` | `ea90f6c882a7c9a3e9cade71d2dbce875c505c5fd5b91b39c83b74a3e6218b5a` |
| `mimir-endpoint-timestamps.test` | `6014c330acb8229f49404c622a9e687b82855383d401c39c92a897af54a2fbff` |
| `upstream-transitions.test` | `afded9bc27127366f0349ea95737a4fb9a972823cd4e3e2530cc9025a2392578` |
| `mimir-transitions.test` | `157bb3b5a93e1174f3ec038ae5518bd4a15bb4788c1f0ae696dee9e40b36d371` |
| `upstream-regression.test` | `d4e1c6a72fc624110fb618880acd93db569b1081c33c683bfbad26df12502268` |
| `mimir-regression.test` | `7e3a378b8700d9f1740d8138359eca4e0d8fe47f7cfa9c76765fc34eb4395513` |
| `upstream-mad.test` | `5c86a4b06e0a60876226991e3d9bc0814ac09e44a9163833c10be51c44b497c5` |
| `mimir-mad.test` | `a3bd2325323c555f91bac2e50db96f2b8fc37a02d21a1583da76300a623faa37` |
| `upstream-extremum-timestamps.test` | `4d3244736aac9c546b1d1613b3d35362b401e38aabf6665c384f9ebec1fc8517` |
| `mimir-extremum-timestamps.test` | `7f566f497a3ae7f7b289fa193ef37f093f383d9d5e76768f6f1c7e68f7c78758` |
| `upstream-label-join.test` | `7327afb5243aa0e73093b680632751e3c02ed63dea64853d0ceb4a72ac9588eb` |
| `mimir-label-join.test` | `b34987b98d5e19ad6b1e6085c2f6195e334a38a046f1f5d97fd09572edf4faf3` |
| `upstream-range-quantile.test` | `d9359dcfda4548e775939917b05498ea7cd5890c3d4eeba95c1b50fe7d839c49` |
| `mimir-range-quantile.test` | `f28effeb7c332397c9c934825ef02d243229e09c9b4539eb8b15d3d4b0582144` |
| `upstream-smoothing.test` | `f8ff38c664b8ad3028d611f3bcc59dd1407f04f32c74afbcb28866a68c5dd507` |
| `mimir-smoothing.test` | `44a0376d5075c9a1c3932a262b3af1cc24e532ef485b11460b1450b0971a3d25` |
| `upstream-angles.test` | `23bba411365ab0376cbb35c5cd9c5f0b47788b5f78976d782b1beb51f92811cd` |
| `mimir-angles.test` | `3823a5204f0c830e27a2c03c3fa21c58d991b6dda698c94907d4cb74cd724d10` |
| `upstream-inverse-trig.test` | `08e3a1a2c1752b83b8a016a63260eeb79939abb3f49ee4caf2f100792075fe74` |
| `mimir-inverse-trig.test` | `c41c613ff7a6f19d66cf227b028d2fff7e5c3fb491d82cf30056f7973fd5c4a4` |
| `upstream-trig.test` | `a19f7fc97c4d682afef4fc1755f10e6bd2cda50f670ce870a8384040fca72b4b` |
| `mimir-trig.test` | `7e3adadc6a83a7d6d3a98c8d40317e91287d859e2170d595935f6fd70f628da7` |
| `upstream-hyperbolic.test` | `b6ee8a2e053aed90707cbf7884b933762720334278136a03277c6939b1d5f435` |
| `mimir-hyperbolic.test` | `3ca8741052611ed9256171c0c9845f31e18230d4a4899798bb1e018297b91264` |
| `upstream-inverse-hyperbolic.test` | `f2b1990180e7f116a85725ff2cbae4ed1029b3aab9c80d6b558bc5261d177ff4` |
| `mimir-inverse-hyperbolic.test` | `4c01467f3a12b187e0b9dc0cedf8f0e9b2f09b55198540a41cba2d0557cc95a9` |
| `upstream-calendar.test` | `438d4e32dd775b20396dd3b2b3fd3bd828067137d4782eb412f7cdbef221848c` |
| `mimir-calendar.test` | `87f44861bc4ced75436914cf051dbb7060ae36b52d0c20ce9df521d74617cf76` |
| `upstream-label-sort.test` | `9c3a03a6fcf46360fb09d6e43e274ed72dedac5db8f2cc12787225cc5b9c6d70` |
| `mimir-label-sort.test` | `317a8a89eb000a6bf47062105237702cce538a370c6fef89d6953546a4f7e008` |
| `upstream-atan2.test` | `438e8607120cbb844a505ae2b9fe9e45fb4d4a45e3ffe8a48bed9899534465b6` |
| `mimir-atan2.test` | `86b647ba879a1174f61b8e1aed0a71e71c0892f5e29d5e82a4314a5cb7d1bef3` |
| `upstream-sort-warnings.test` | `b98a8e9f19ce5dfffb9e67aa918a6c6b815594a730e15829062aded9b731597f` |
| `upstream-query-context.test` | `b2213af3ecbf8ce47c7bceb0860da7089726d7bb57121ea441aaababda025547` |
| `upstream-scalar-extrema.test` | `cde0efc7590f0d3b4eebe7c35b0412970d774b0be3c98f2b77ad61e8aabb4d5d` |
| `mimir-scalar-extrema.test` | `d837e8ae8219c45262b3da0fcdef970f768e7357a2395a5d73f0f000775ffe62` |
| `upstream-dispersion.test` | `12a845c9890ba823ef932b41e03d8238d2f296adafb5928d8ee516d936e9ae42` |
| `upstream-value-count.test` | `c061bff5c769c2312894b17a1c7d988bdfdc34bd4bf041cb7ed952a15a8de905` |
| `mimir-value-count.test` | `822e203089deb2a749d733a3644cd048e5d29f52bf9c1649051be21974423918` |
| `upstream-aggregate-quantile.test` | `0f4124c8955858e11a1e358161f4413ea84b386b7c20e58c97a96dfbe3a5eb23` |
| `mimir-aggregate-quantile.test` | `812fbae4a629e578f0fa8e12c5ac40c779ec740481c4224e34c1ed40e82093ae` |
| `upstream-time-anchors.test` | `7b702dafee448eb2f0a26beed71c22ec0ab67916605a090cd2448a32509f3d43` |
| `upstream-time-duration.test` | `a997a6e557809ea296b3c5848d79b02f35c8de6ad32df269bc2183114a708c22` |
| `upstream-time-offset.test` | `737fe81d1aad5d7c206b9bd98a04d4fd51c7956ceee47c67968b51e68dee1e10` |
| `upstream-time-subquery.test` | `d1e0de4c55150a743fd08673eed7b7db1c63d34fae34cf257d828b706d817065` |
| `upstream-limit.test` | `31e6224016d5d92988a48ccb3ccc6732708354036294a4f00711ff0524af5312` |
| `mimir-limit.test` | `c4c6602c4ac8022522b628a8bdce452832f0a62739c86e98d30f3dcd094dfee3` |
| `upstream-duration-expressions.test` | `90405f86486b84e2e12f110f1c486411c99f213e581001a344129a7b087fb792` |
| `upstream-default-subquery.test` | `0b09cdbc4b620cd780a2609b4c9160804265130032ccceaad7602290a8bbb5f0` |
| `upstream-default-subquery-empty.test` | `428ec23fc7274cabb0f5b121dd2d9f6c0d43711f34f1fd4085b37c9180db88e6` |
| `upstream-string-results.test` | `2edb6afb1ebb9aa7cf4d1b7c18dd1986f602fc89ac5ee8935d85ce56b936a0ee` |
| `upstream-range-results.test` | `6718999eb5069565dc320dc34baf1f44062dee60bf4c59be976b0e0bab388ba4` |
| `upstream-histogram-annotations.test` | `fc7649a62f314e0c1bf1011769020e7fc3eb1c473f94c7af9573151dfa3dfa57` |
| `upstream-histogram-nan.test` | `04281a5d79911b7c8bb880aa4422cfe6ca4d6646d915ff5f3efbc4cb335a91f5` |
| `upstream-histogram-nan-multi.test` | `823e4e4d79b1d92d0261587757c3117bb4ff8fa7efca95d73785fda685b19262` |
| `upstream-histogram-quantiles.test` | `b2d755316c0c81aa3ca61388122283b415105efd4c7cac9a6e373eab41def9d6` |
| `upstream-histogram-fractions.test` | `2b8dba217acf0d2b9128fbca5ea98db7ab42569b843c3ee14efd624608d65706` |
| `upstream-histogram-extrapolation.test` | `222f5acc5ab05c24ee21101b8da809b648a9c681f283016839e4a88ac8660755` |
| `upstream-idelta.test` | `73fffb079a7393b7e7d216193c8ae9e49c8d4a33359ebd70dbbab507a77433ef` |
| `upstream-irate.test` | `c7b0a6d060b2e9bac5cfba52259f865970a0cf31acc6084874fa1a5e8409b62d` |
| `upstream-delta.test` | `0543992eda787fbbaa84b6daad47c1934d39266fd5c48b1dfc768c1590d10d67` |
| `upstream-custom-histogram-arithmetic.test` | `a2f6751d6614c9f0f866dbb96296341241f1f049f8e4c860711626502a81fd70` |
| `upstream-histogram-range-aggregation.test` | `f36ed5dcf1ad09f2863a59c43d6054e4f6b852a5c273ca2e1062f6d3df7dc3b8` |
| `upstream-histogram-group-aggregation.test` | `8140233809e6112aec21df2caf1d6134164c86ab6689bc1995c36ae91b2c6ea5` |
| `upstream-histogram-counter-annotations.test` | `57ce46ac10e5b3db1431d175bcc5ef5c6f3223ed4fa478be905ef23e7eed84a0` |
| `upstream-histogram-precision.test` | `2209c0d7fc2e55cdf5ac10f7a6ff68ae48d3b340ae6eabd9d16a1b671e41d296` |
| `upstream-histogram-scaling.test` | `38467b9f6d08ea92197d344764acc0ec62283e6813726d23d3689b83d6ae60a2` |
| `upstream-histogram-trimming.test` | `272f86d1d23909be4e966f0539adc5704640c0e462c85367eeac9df51bb27e50` |
| `upstream-histogram-properties.test` | `6577f148e68b71866b044ff0ebfdbe59cb47f5027d6a87a74782f4e225fff58e` |
| `upstream-histogram-bucket-retention.test` | `fa4fd2c586349ce4a023acff9e6a12e14cf2196b5086f6f93d0d4814018464a1` |
| `upstream-histogram-fundamentals.test` | `9471bbca7793619c9a26cd179d8c066c351c9e9698c805aad5f3a6eea4ba1e6f` |
| `upstream-histogram-interpolation.test` | `83ff169181dd010bc14c5f984a2e2e85716e92ef08eb522f3d7d05a8687fc234` |
| `mimir-histogram-binary.test` | `9b4fc6752be89e505514284ffddd0ad31a3de7d0ddfed39dcd79d12e7b070b9c` |
| `mimir-histogram-quantiles.test` | `b43d11cc529f07fb2624f3f3c398536fb973f7fac0cf1852045d680fa86ddfcc` |
| `upstream-classic-histograms.test` (also complete upstream source) | `1eadbc6129d601f4135da5734c49ec223f5ed52aed861897126ba8fb8bdd375a` |

The 36 clamp evaluations preserve every assertion in the selected blocks,
including already-supported `clamp_min`/`clamp_max` controls. Histogram bucket
arrays use the upstream parser's absolute counts and explicit/default-zero
offsets, not integer deltas. All original inputs and expected outputs remain unchanged.

## Offline Pulso replay

```sh
mix test test/pulso_web/controllers/prometheus_math_conformance_test.exs
```

The test-only adapter interprets the selected `load`, `load_with_nhcb`, `clear`,
`eval instant`, `eval range`, `eval_fail` and `eval_ordered instant` forms,
plus `expect ordered`.
`expect string` and `expect range vector` assert complete decoded strings and
source-timestamp matrices, respectively.
Ordered directives assert the exact instant-vector row order only; their one-step
range replays compare label identities and samples without imposing vector order
on a matrix. Unsupported syntax fails rather than being silently skipped.
Every evaluation runs through Pulso's HTTP instant/range query endpoints, using
both memory storage and the real native S3/Parquet adapter against an owned
loopback S3 stub. The corpus has 1295 instant evaluations, each also replayed as
a one-step range, and 295 range evaluations, each requested once: 2885 HTTP
requests per adapter, 5770 total across 170 fixture wrappers (85 files per
adapter). The three-file scalar-context/extrema batch contributes 51 evaluations
and 72 requests per adapter; the five-file aggregation batch contributes 88
evaluations (59 instant, 29 range) and 147 requests per adapter. The four-file
time batch contributes 95 evaluations (73 instant, 22 range) and 168 requests
per adapter. The two-file sampling batch contributes 130 evaluations (71 instant,
59 range) and 201 requests per adapter. The duration-expression file contributes
59 evaluations (30 instant, 29 range) and 89 requests per adapter. The two-file
default-resolution selection adds 14 evaluations and 28 requests per adapter.
The result-type selection adds ten evaluations and 20 requests per adapter,
including ten deliberate range-endpoint rejections.
These are corpus/replay counts, not a report of a
successful test run. Separate Pulso controls are not counted as imported
evaluations or requests. Default tests require no Go executable, running Mimir,
Docker, external store or downloaded fixtures.

The complete classic-histogram file adds 183 unchanged evaluations, including
paired classic/custom-native quantiles and fractions, zero-crossing and
infinite-width buckets, rates, aggregation, duplicate numeric bounds, empty
distributions and resets. Its 34 `load_with_nhcb` blocks retain the classic
samples and add custom-native inputs from cumulative bucket differences.
An independent check compared all 419 generated inputs with the pinned
`vendor/github.com/prometheus/prometheus/util/convertnhcb/convert.go`
`TempHistogram.Convert` implementation (source SHA-256
`6b9f4b56c6f926be632c5c2b1720d785e3d8468663fb13c1bdfea5b4b577711a`),
including compacted bounds, populations, geometry and reset hints. No production
converter supplies test inputs or expectations. Commented upstream tests remain
comments and are not counted. Three NaN-boundary evaluations duplicate the
earlier fractions selection; corpus counts are replay counts, not unique queries.

The NaN-pair extension adds another six custom-native inputs: all 425 generated
inputs across 35 blocks match the same pinned converter, including NaN sum
classification. Further unchanged blocks add cross-schema subtraction with
negative populations, `@ start()`-anchored `last_over_time` subqueries, mixed float/native
range ordering and lifetimes, repeated shared-source property/quantile queries,
and custom-bound `irate` reconciliation versus bucket-only resets. The repeated
plural-quantile case duplicates a prior selection; replay counts include it.
These blocks add regression coverage for existing behavior, not new storage
iterator optimization or complete annotation parity.

Classic repair notices deliberately retain the generic message used by the
pinned Mimir streaming engine, not its vendored Prometheus engine's expanded
time/bucket/difference summaries. In
`pkg/streamingpromql/operators/functions/histogram_function.go:730–743,1053–1064`,
both quantile implementations pass four zero summary values with an explicit
comment deferring annotation merging. The complete source SHA-256 is
`9e64ef69c5acac0100385147845fc4f5ec78a07aa4e1f234e76fa320f626b3e2`.
The public repair control asserts the exact generic message through memory and
native Parquet/S3; it omits upstream AST positions, which remain a gap. The
histogram-annotation batch passed 174 tests with seed631 and ordinary timeouts.

Five further unchanged range evaluations preserve custom-bucket equality and
inequality over a changing complete timestamp grid, unsupported ordering info,
and sum/average results at pure-schema steps while omitting the mixed
exponential/custom step with a warning. These extend the existing arithmetic
and grouped-aggregation fixtures; their input samples and output expectations
are unchanged upstream data.

Histogram replay checks count, sum, populated bucket counts/bounds/inclusion
rules, output type and timestamps. Expected histogram outputs include both
exponential sides, zero and custom buckets; bounds derive from their schema or
explicit custom bounds independently of Pulso's histogram formatter. Other
layouts reject rather than receiving incomplete assertions. Mixed range outputs
must preserve the complete float/histogram timestamp grid. No histogram
assertion is reduced to only its sum or count.

Series identities, timestamp grids, sample counts and omission are exact.
Finite values use upstream's relative epsilon of `1e-6`; signed zeros compare
equal and NaN signs normalize, as in upstream's comparator. The separate
Prometheus oracle corpus checks zero signs and nonfinite classifications with
exact boolean predicates, and finite replay within one ULP. Pulso translates
upstream's duplicate-label error into its existing HTTP execution diagnostic;
the adapter asserts that exact diagnostic, not an arbitrary failure status.
Error wording is not claimed identical to Mimir.

Separate inverse-hyperbolic controls use independent Go 1.25.10 arm64 math
expectations. They distinguish valid `acosh` at/above 1 from invalid inputs
below 1 and distinguish finite `atanh` immediately inside ±1, signed infinite
endpoints and NaNs immediately outside. Signed zeros, smallest subnormals and
nonfinite classifications are exact; other selected finite values allow one
ULP, without a global or cross-platform bit-identity claim. Asymmetric signed
inputs, the immediate neighbors of Go's large-input branch at 2**28, ±1e308
and largest finite values catch unstable textbook formulas that overflow or
lose tiny inputs. Runtime uses platform inverse-hyperbolic math with explicit
domain/nonfinite handling, not copied Go algorithms. Memory/S3 checks also
cover histogram/stale omission, labels/types/timestamps, form POST, tenant
isolation, changing range inputs, nested composition, selector/subquery offsets
and independent/cumulative query budgets. Permanent tests need no Go or
downloads; these are not imported evaluations, provider, capacity or deployed-
client acceptance.

Separate hyperbolic controls use independent Go 1.25.10 math expectations on
arm64. Signed zeros, smallest subnormals, NaN/infinity classes and tangent
saturation are exact; other selected finite values allow one ULP, without a
cross-platform bit-identity promise. Asymmetric ±0.5/±2.1 and `sinh(10)`
distinguish signs and sine/cosine behavior. Immediate neighbors around 21 and
709.782712893384 distinguish the Go exponential path and overflow boundary:
`sinh(710)` and `cosh(-710)` overflow here even though platform libraries may
return finite values. The implementation reuses Pulso's existing Go-compatible
exponential scaling for magnitudes above 21; other finite math uses the platform
library. See Go's `math/sinh.go` and `math/tanh.go` at the producer version.
Largest finite values, histogram/stale omission, labels/types/timestamps, form
POST, tenant isolation, changing range inputs, scalar composition, selector/
subquery offsets and independent/cumulative budgets run through memory and S3.
Permanent tests need no Go or downloads. These are Pulso controls, not imported
evaluations or provider, capacity, deployed-client or global precision acceptance.

Separate circular-trigonometric controls check signed zero, smallest subnormals,
asymmetric quadrants, π, both immediate neighbors of the binary64 value nearest
π/2, large angles and the largest finite binary64 value. Independent Go 1.25.10
expectations on arm64 use exact tiny values/zero signs and `1e-14` relative
tolerance for other finite results, tighter than the upstream replay's `1e-6`.
Platform argument reduction is not bit-identical to Go: these selected probes
differ by up to 26 ULP, so no global one-ULP guarantee is claimed. The tests
detect losing near-zero residuals, inventing exact poles or naively reducing
huge inputs modulo a rounded 2π. NaN/infinity classification, histogram/stale
omission, labels/types/timestamps, form POST, tenant isolation, changing range
inputs, nested angular expressions, selector/subquery offsets and independent/
cumulative budgets run through memory and S3. Permanent tests require no Go
or downloads. These are Pulso controls, not additional imported evaluations or
provider, capacity, deployed-client or cross-platform precision acceptance.

Separate inverse-trigonometric controls check both endpoints of [-1, 1] and
their immediate binary64 neighbors, asymmetric finite inputs, exact signed zero
and smallest subnormals, NaN/infinities and huge finite `atan` saturation.
Expectations come from independent Go 1.25.10 math functions on arm64; NaN
classifications and tiny signed values are exact, while other selected finite
values allow one ULP. Permanent tests do not invoke Go, and this is not a
bit-identical guarantee across platforms. Histogram omission, stale-history
hiding, result labels/types/timestamps, form POST, tenant isolation, angular/
scalar composition, positive selector/subquery offsets, independent sample/
work/result rejection and cumulative batch sample limits run through memory
and S3. These controls are not additional imported evaluations.

Separate angular controls assert exact binary64 results for signed zero,
subnormal inputs/results, underflow and large finite conversions, plus signed
overflow, NaN/infinity, histogram omission and stale-history hiding. Expected
bits were independently checked with Go 1.25.10 standard-library constant
expressions on arm64; the permanent Elixir tests do not invoke Go. Scalar/vector
composition, offset selectors/subqueries, fractional timestamp grids, form POST,
tenant isolation, independent sample/work/result limits and cumulative batch
sample/result limits run through memory and S3. These are local controls, not
additional upstream evaluations or full trigonometric/architecture acceptance.

Separate smoothing controls distinguish initialized and updated trends from
moving averages, last-sample selection and timestamp-based regression. They
cover asymmetric values, overflowing finite differences, NaN/infinity/zero,
unused NaN trend factors on two observations, stale bridges, single-float mixed
notices and histogram-only validation. Exact nanosecond boundaries, moving
expiry, outer-time factors with range/subquery offsets, form POST, tenant
isolation, name collisions, independent sample/work/result limits and cumulative
batch work are checked through memory and S3. These are not additional imported
evaluations or upstream precision/provider/capacity evidence.

Separate range-quantile controls distinguish NaN ordering from unconditional
propagation, exact ranks from interpolation, and endpoint weighting from an
overflowing difference formula. They cover exact negative zero, out-of-order
ingestion, mixed/stale native histograms, nanosecond window boundaries, moving
expiry, outer-time scalar evaluation with range/subquery offsets, epoch-aligned
subquery observations, form POST, tenant isolation and name collisions. Nested
warning deduplication, per-source annotation isolation, invalid quantiles over
empty/histogram-only windows, sample/work/result limits, sorting-specific work
and cumulative batch-work admission/rejection are Pulso controls, not additional
upstream evaluations.

Separate label-join controls check actual Unicode source values/names and
separators, repeated sources and destination-as-source reads from the original
labels, metric-name deletion, exact signed zero and complete native histogram
preservation, staleness, GET/form POST and tenant isolation. Independent sample,
work and result limits reject whole; lookup-sensitive, repeated-source-byte,
separator-byte and cumulative batch-work controls prevent joins from bypassing
the existing work ceiling. These are not additional imported evaluations.

Separate Pulso controls check strict scalar/vector signatures, exact signed-zero
results, NaN/infinity combinations, duplicate labels and budget rejection even
for inverted bounds. `clamp` follows the [pinned Mimir operator](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/pkg/streamingpromql/operators/functions/math.go#L67-L91)'s built-in Go
`max(minVal, min(maxVal, value))`, where NaN always propagates. This differs from
older Prometheus engines' `math.Max`/`math.Min` on some NaN/infinity combinations;
these edge cases are not claimed identical across all versions. The existing
basic-math Prometheus oracle corpus is unchanged.

Separate memory/S3 presence controls cover zero/NaN/infinities, custom native
histograms, mixed windows, scalar/histogram staleness, exact left-open/right-closed
bounds, offsets, histogram subqueries, aggregation, tenant isolation, form POST
and independent sample/work/result-budget rejection. These are Pulso controls,
not additional upstream cases.

Separate first/last-sample controls cover chronological selection rather than
minimum/maximum or the opposite endpoint, moving-window expiry, name preservation
across distinct metrics, stale scalar/custom
histogram fallback, mixed-type order, exact custom-bucket output, NaN/infinity/
signed zero, nanosecond boundaries, offsets, histogram subqueries, tenant
isolation, GET/form POST and independent sample/work/result-budget rejection.
These are not additional upstream cases.

Separate source-timestamp controls check exact sub-millisecond result strings,
epoch-zero timestamps, stale scalar/histogram samples, nonfinite values,
both mixed-type orders, metric-name removal and duplicate-label rejection.
Offsets must not shift returned source times; subqueries must return inner-grid
timestamps instead of underlying row times. Moving-window expiry, one-nanosecond
boundaries, GET/form POST, tenant isolation and sample/work/result-budget
rejection are also checked. These are Pulso controls, not additional upstream
cases or precision parity beyond upstream's millisecond storage.

Separate extremum-timestamp controls check newest ties, signed-zero ties,
leading/trailing/all NaNs, infinities, huge finite values, mixed and single-float
windows, histogram omission and staleness. Exact nanosecond timestamp values,
epoch zero, moving left-open/right-closed bounds, offsets and subquery grids
distinguish source time from evaluation time. Per-metric and nameless-expression
infos, sparse-window notice absence, collisions, tenant isolation, GET/form POST
and independent sample/work/result-budget rejection are also covered. These
are Pulso controls, not additional upstream evaluations or timestamp precision
parity beyond upstream's millisecond storage.

Separate transition controls distinguish changes from resets, interleave floats
and histograms in both orders, compare NaN/infinity/signed-zero values, and check
sum-only histogram changes, reset hints and histogram signed-zero equality.
Scalar/histogram stale markers cannot break comparisons between valid samples.
Single/empty windows, exact nanosecond boundaries, offsets, explicit subquery
grids, name removal/collisions, GET/form POST, tenant isolation and independent
sample/work/result-budget failures are checked through memory and S3. These are
Pulso controls, not additional upstream cases.

Separate regression controls distinguish least-squares slope from endpoint
differences, sample-index slopes and reset-aware rates using irregular times and
asymmetric values. They cover huge finite constants, positive zero slopes,
stale bridges, large epochs, exact window edges, offsets and float/mixed
subqueries. Notices are required for mixed windows even with insufficient floats,
but forbidden for histogram-only selections, stale histograms or histograms fetched
between sparse evaluation windows. GET/form POST, name collisions, tenant
isolation, independent budgets and query-batch notice isolation are checked
through memory and S3. These are Pulso controls, not additional upstream cases.

Separate median-deviation controls distinguish the median from mean-based or
scaled deviations on asymmetric inputs. They cover infinite medians/deviations,
NaN, huge finite values whose unweighted midpoint would overflow, overflowing
individual deviations, positive zero, stale bridges, single-float mixed windows,
histogram-only silence, boundaries, offsets and float/mixed subqueries. Sparse
range steps do not report histograms fetched only between evaluations. GET/form
POST, name collisions, tenant and batch-notice isolation, sample/result limits,
sort-sensitive work limits and accumulated batch work are checked through memory
and S3. These controls are not additional upstream cases.

The local `:regex_literals_batch` wrapper replays 30 quoted/octal regex patterns
over 15 ingested label values, using the independent substring/whole-label Go
oracles in [`regex/go-controls.json`](../../regex/go-controls.json).
Remote write, native Parquet/S3, positive/negative selectors and GET/form POST
are exercised, with separate `label_replace`, discovery and exemplar controls
for omitted terminal `\E`. The original five quote-parser patterns and octal
pattern retain their provenance in the shared fixture README. These controls
use the Mimir integration harness but are not additional upstream integration
rows, full regex conformance or provider/client/capacity acceptance.

## License and attribution

The `.test` files retain their original headers, including Mimir's
**AGPL-3.0-only** SPDX headers where supplied. `mimir-limit.test` has no header
in the source; it remains covered by Mimir's root license rather than adding
invented provenance to a verbatim file. `LICENSE` is copied from Mimir's root
license. The `upstream-*.test` files
retain their embedded **Apache-2.0** Prometheus provenance and copyright notice;
`LICENSE-Prometheus` and `NOTICE-Prometheus` are copied verbatim from Mimir's
vendored Prometheus.
These copied test fixtures are not relicensed under Pulso's MIT license. No
Mimir implementation or test-runner code is copied into Pulso's runtime.
