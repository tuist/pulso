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
| Prometheus queries/discovery | Documented bounded PromQL subset, including instant-query string and raw range/subquery matrix results, signed offsets, unitless decimal/hexadecimal/separator time literals, duration-valued scalars and bounded duration arithmetic with outer `step()`/`range()` and `min_of`/`max_of` in windows/resolutions/offsets, numeric/`start()`/`end()` `@` anchors on selectors and explicit/default-minute-resolution subqueries, quoted UTF-8 selector/grouping/matching names and UTF-8 `label_replace` destinations, UTF-8 `label_join` with ordered/missing/repeated sources and name rewriting, `present_over_time`, name-preserving `first_over_time`/`last_over_time`, source-timestamp `ts_of_first_over_time`/`ts_of_last_over_time`, newest-tie float-extremum timestamps `ts_of_min_over_time`/`ts_of_max_over_time`, and chronological `changes`/`resets` for floats/native histograms/mixed windows, least-squares gauge `deriv`, median absolute deviation `mad_over_time`, interpolated `quantile_over_time` with per-step quantiles/invalid-quantile warnings, and `double_exponential_smoothing` (`holt_winters` alias) with per-step factors and fail-whole factor validation; metric-named mixed-histogram info annotations (also for `predict_linear` and extremum timestamps), three-argument `clamp` with per-step scalar bounds and Mimir NaN semantics, basic math (`ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`), angular `deg`/`rad`, scalar `pi()`, inverse trigonometric `asin`/`acos`/`atan` with per-sample domain NaNs, circular `sin`/`cos`/`tan` with radians and nonfinite-input NaNs, hyperbolic `sinh`/`cosh`/`tanh` with Go-compatible overflow classification, and inverse hyperbolic `asinh`/`acosh`/`atanh` with domain NaNs and signed endpoint infinities; native histograms (Prometheus 3.5.0 base with Mimir's corrected NaN-sum quantiles), plural quantile labels, classic/native fractions, `/api/v1/query_exemplars`, instant/range HTTP APIs, sample-backed label/series discovery, build info, bounded single-line query formatting, `/api/v1/metadata` from retained remote-write metadata, and MCP queries | Full language/function/modifier coverage (including `anchored`/`smoothed`), full warning/info annotation parity, delayed metric-name removal, exact multiline printer parity, OTLP-sourced metadata, and full TSDB/client parity |
| Loki ingestion/queries | JSON/Snappy protobuf push and OTLP at `/otlp/v1/logs`; documented bounded LogQL queries, including scalar instant envelopes, scalar comparisons, decimal/hex-float literals and zero-divisor NaNs, instant/range reset-aware `rate_counter`, missing/empty-field skipping for all `unwrap` aggregations (log codec `{1,1,7}`), post-unwrap label/error filtering and explicit error preservation, typed-filter `LabelFilterErr` generation with first-error preservation, Go-profile signed-int64 runtime duration/unsigned-uint64 byte conversions with bounded fractional rounding and overflow rejection, signed/fractional duration thresholds and offsets, uint64 byte thresholds and separate ordered integer bracket windows, float64 numeric filtering and eager-AND/short-circuited-OR error propagation, pinned Loki double-quoted/raw string decoding with exact byte escapes, logfmt strict byte-position errors, non-strict recovery, empty-value flags, first normalized duplicates and quoted Unicode decoding; stream/metadata collision ownership, released stream names after drops and first-extraction history across parsers; instant-only `approx_topk` with exact ranking and instant/range `approx_count_distinct` with exact string counting under existing budgets; request-local `X-Loki-Query-Limits` maxQueryLength for finite reads; complete stream label, value and series discovery; index stats and volume (`aggregateBy=series` and bounded selector-only `aggregateBy=labels`, using actual line bytes rather than Loki chunk estimates); build info; WebSocket live tail; detected labels and fields; query-time patterns and `|>`/`!>` filters; MCP log reads | Full LogQL, including broader JSON/parser grammar, complete parser-error generation and hint-driven early exits; nondeterministic shared-source alias choice; pinned scanner/test contradictions such as accepted `0B`; probabilistic sketches/distributed merging/high-cardinality acceleration; other per-request policy fields; index-backed volume cost/estimate parity; full client parity |
| Tempo compatibility | Experimental bounded v1/v2 exact-ID export with V2 same-span filters and ancestor/descendant depths; separately opt-in retained-context search and discovery; documented typed predicates; TraceQL metrics with `topk`/`bottomk`, core `span:id` grouping (`rate`, `count`/`min`/`max`/`sum`/`avg_over_time`, `quantile_over_time`, `histogram_over_time`), bounded scalar/vector `+ - * /` expressions, typed ordered-label matching/ungrouped broadcasting and value-preserving scalar filters; echo and build info | Full TraceQL including structural/spanset operations and `compare()`, exemplars, unlimited-series overrides, full indexed planning, discovery parity and deployed-client acceptance |
| Alerting | Native bounded thresholds, authored mixed-source expressions, experimental recordings and delivery | Full Grafana imported-rule execution, lifecycle, routing, notification and template semantics |
| MCP | Stateless 2026-07-28 tools, capability-scoped Pulso writes and request-scoped subscriptions | Legacy initialize/session protocols; infrastructure remediation is deliberately a separate server |

PromQL, LogQL and trace predicates share a bounded regex profile accepting
braced hexadecimal escapes followed by repetition, literal quoting (`\Q...\E`,
including an omitted terminal `\E`) and Unicode-valued octal escapes. Quoting
inside character classes and backreferences still reject. Whole-value matching
remains anchored even with an unfinished quote. POSIX classes preserve ASCII
membership and Unicode simple case folding, including their complements.
Malformed escapes and actual nested/possessive repetition reject, including
extra lazy suffixes (`a???`, `a{2}??`); valid `a??` remains supported.
Counted bounds and nested products follow Go's 1,000-copy admission limit;
leading-zero and malformed count spellings remain literal braces. Invalid
inner counts cannot hide under an outer zero. Independent admission and
matching oracles retain eight original Go parser-table patterns plus local
controls; Mimir/Loki/Tempo HTTP tests reject invalid patterns before storage.
Repeated `&&`/`~~` inside classes are literal members, not Rust set operators,
including with negation, case folding, named classes and adjacent ranges.
Literal opening brackets and left-to-right scalar hyphen ranges also follow
Go: `[[]` matches `[`, `[--x]` spans `-` through `x`, and `[a--b]` rejects.
Leading Unicode-property carets toggle negation (`\p{^Latin}` / `\P{^Latin}`),
including inside classes, with folding and before quantifiers. Property names
and scalar membership use Go 1.27.3's Unicode 17 categories/scripts, category
aliases and Any/ASCII/Assigned, with loose ASCII case/space/underscore/hyphen
spelling. ISO script abbreviations, binary properties, Unicode whitespace and
property=value forms reject as in the pinned parser. Property-specific folds
precede negation; this does not update literal case folding or prove full Go
regex parity. Property preparation has a separate 1 MiB logical charge before
union/zero-repeat simplification and bounds emitted text independently, alongside
existing program/schema/work caps.
Raw capture/modifier headers validate before literal normalization: invalid
name bytes, quoted/escaped header repairs and unsupported flags reject.
Empty and repeated inline flags preserve scope, atom binding and greediness.
Independent Go matching oracles and local HTTP/Parquet/S3 controls cover
all three protocols; this does not establish full Go regex, protocol or client
parity. Named `regexp` parser admission now inspects native capture metadata,
without imposing PCRE's different directive/quantifier grammar. Eight independent
Go flag/capture cases and a Unicode-property capture replay through log and
metric HTTP/Parquet/S3 queries; original Loki constructor rejections remain
covered. Original capture schemas preserve erased groups as empty values,
including trailing numeric groups, impossible alternatives and first-parser
ownership. Executed Go oracles and local HTTP/Parquet/S3 controls cover these
forms. Predicate/parser/indexed-extraction/template-helper schemas reject above an independent
1 MiB logical ceiling (32 bytes per group plus raw name bytes), even when
optimization removes every group; counting does not allocate capture names.
Flag-separated quantifiers nest the preceding quantified atom, preserving
capture indices, earlier atom flags, subsequent scope and each repetition's
U/lazy greediness. Counted products still reject above 1,000; directly adjacent
quantifiers remain invalid. Executed Go/Loki-fork oracles and local
Mimir/Loki/Tempo HTTP/Parquet controls cover admission and matching; Loki
parser controls cover captures and metric filtering. Numeric/repeated ASCII
names work in selectors and `label_replace`, with original numeric indices and
first-participating named expansion. Loki regexp parsers reject repeated raw
names but accept numeric destinations, normalized before collision suffixing;
first declared captures own normalized collisions, even when empty. Executed
Go/fork oracles and local Mimir/Loki/Tempo HTTP/Parquet/S3 regressions cover
these contracts, not full regex, client or capacity acceptance.
Negated scalar classes correctly exclude separately listed U+D7FF/U+E000
members across the surrogate hole, including raw/hexadecimal/range forms,
folding, captures and repetition. Independent Go/fork oracles and the same
local HTTP harnesses cover matching and rejection before storage.
Upgrade all query-serving app/native pairs to log tuple `{1,1,16}`; trace/metric
tuples, storage and keyed bytes are unchanged. See
[query regex semantics](querying.md#shared-query-regex-semantics).

Explicit LogQL logfmt extraction supports last-definition duplicate destinations,
ordered source remapping and destination-key capture, and empty values for
replacement-character input. Shared-source aliases select one lexical destination
instead of Loki's nondeterministic map choice. Hint-dependent early exits and
broader JSON/parser grammar remain incomplete; see
[query semantics](querying.md#parser-labels-and-quantities).

Loki request-local policies also support `maxQueryInterval`,
`maxEntriesLimitPerQuery`, `requiredLabels` and `minimumLabelsNumber`.
Twenty-five pinned engine/middleware admission cases replay through 64 HTTP
requests, with additional nonempty Parquet/S3 and pre-read rejection controls.
Parenthesized log pipelines before range brackets are accepted. These are
optional client restrictions with existing server budgets, not configured
operator quotas. `maxQueryTime` also tightens the query-worker timeout on all
finite reads without raising the server cap. Blocked-adapter GET/form-POST
controls cover all 15 finite request shapes, and an actual blocked S3 GET checks
timeout recovery. Already-dispatched native/provider work may outlive callers.
Lookback, byte and sharding policies remain
unsupported; see [query limits](querying.md#limits-and-metric-evaluation).

LogQL range pipelines may precede or follow the bracket/offset, and complete
ranges may be parenthesized recursively. Seventeen original first/last engine
oracles replay in six equivalent placements; split pipelines and invalid
post-unwrap stages still reject before storage. Existing depth/budget ceilings
apply. This closes range placement, not general parser-error or hint parity.

LogQL `label_replace` rewrites final metric labels in instant/range queries,
with anchored captures, missing-source defaults and destination deletion.
Loki's non-dot-all regex default is preserved. All 19 evaluations from the
pinned engine block retain their original 64 loaded entries and output oracles
through HTTP GET/form POST and Parquet/S3; local controls cover nested policies,
capture expansion, newline flags and nonaggregating instant collisions. This
does not establish general parser-error, distributed or deployed-client parity.

Regexp/pattern parser admission, short-line pattern captures and string-only
`unpack` with `_entry` line replacement replay all 30 evaluations from the
remaining pinned parser-engine block through HTTP GET/form POST and Parquet/S3.
Constructor tables and nine unhinted unpack rows add regressions; 528 actual
pinned runtime pipeline comparisons agree. Malformed-input diagnostic and
hint-driven early-exit parity remain incomplete. Existing resource ceilings and
native/storage/keyed bytes are unchanged; upgrade query-serving applications.

`label_format` validates unique/non-error destinations and preserves ordered
renames, self-rename deletion and first-template label snapshots. The complete
three-row upstream validation table and four basic assignment rows replay
through HTTP/Parquet/S3, with stream/metadata/parsed and chained-stage controls.
Unicode literal prefixes preserve byte offsets in both line and label templates.
All 723 additional actual pinned pipeline comparisons agree.

Bounded string templates additionally support pipelines, parentheses, simple
Unicode case conversion, historical Go title casing, Unicode cutset trimming,
literal/counted replacement, repetition, indentation, Unicode-scalar alignment,
byte-based truncation/slicing, signed-int64 constants, query-form URL escaping,
Base64 encoding/decoding, defaulting and current-line lookup. Conditional
`if`/`else if`/`else`, lazy `and`/`or`, `not`, typed comparisons and byte `len`
are supported; all branches validate before storage, but only selected branches
execute and share one output cap. Sixty-five adapted upstream formatter evaluations replay
through HTTP/Parquet/S3. Separate runtime comparisons cover every Unicode scalar
under upper/lower and title/boundary contexts. Expanding functions enforce the
16 MiB intermediate-output ceiling before allocation, even under a later trim.
Base64 corruption returns ordinary Go-compatible byte-offset diagnostic strings.
Another 32,298 actual pinned runtime comparisons cover conditional/logical/
comparison behavior and admission, excluding exact runtime error details.
Literal ASCII-whitespace trim markers and non-nesting template comments follow
Go delimiter rules, without trimming rendered values or Unicode spaces.
Fifteen Go lexer/parser rows replay through the Loki HTTP/storage path, with
AST/token expectations adapted to rendered output or rejection; they are not
additional Loki source rows. Another 29,826 actual pinned Loki comparisons agree
on trim/comment admission, output, errors and label categories, excluding exact
runtime error details. Existing output ceilings remain authoritative.
Integer helpers `int`, `add`, `sub`, `mul`, `div`, `mod`, `min` and `max` support
string/boolean conversion, exact decimal truncation, base-zero parsing and
signed-int64 wrapping. Invalid/overflowing conversions return zero; zero division
preserves inputs with `TemplateFormatErr`. Another 20,826 actual pinned pipeline
comparisons agree, including 1,458 runtime errors, excluding exact error details.
Quantity helpers `bytes`, `duration` and `duration_seconds` retain typed float64
results with Go general-format rendering, numeric-zero defaults/truthiness,
float comparisons and in-range truncation for integer helpers. Invalid quantities
preserve inputs with `TemplateFormatErr`. Of 66,570 actual pinned pipeline
comparisons, 66,438 agree exactly (excluding error details) and 132 deliberately
reject out-of-range float-to-integer casts instead of selecting Go's
implementation-dependent overflow value. No unexpected differences remain in
this batch; those nonportable casts are not an exact upstream-parity claim.
Regex `count` supports integer non-overlapping counts with Unicode-scalar empty
matches, ASCII Perl classes/word boundaries and Unicode script properties.
Non-backtracking native matching retains no match list and charges each match
plus a final probe to shared metric work or request-local stream work across
entries/actions. Invalid patterns preserve inputs with `TemplateFormatErr`;
work exhaustion fails whole even under explicit error selection. Upgrade
query-serving apps/native artifacts to log tuple `{1,1,10}`; storage/keyed bytes
are unchanged. Another 29,952 actual pinned Loki pipeline comparisons agree,
including 3,120 runtime errors, excluding exact error details.
`regexReplaceAll` expands Go-style whole-match/numbered/named captures and
literal dollar escapes; `regexReplaceAllLiteral` inserts replacement text
verbatim. Both share `count`'s native regex profile and request work budget.
Output admission and rendering each charge probes and dollar-expansion work,
including capture-name bytes even when an unknown reference emits nothing.
Without retaining matches, the complete result fits 16 MiB before allocation,
even under later truncation. Another 107,520 actual pinned Loki pipeline
comparisons agree, including 15,360 runtime errors, excluding exact error
details. All 87 Go replacement table rows retain their original oracles through
135 successful mode evaluations, including ten formerly rejected repeated-name
cases. Template helpers accept numeric/repeated ASCII names in both Go spellings
and literal `\Q...\E` runs, including EOF-terminated runs. Named expansion picks
the first participating capture, including an empty match; numeric references
preserve Go's leading-zero and one-billion cutoff rules. Additional duplicate
lookups spend shared work even when they emit nothing. Selector/regexp-parser
admission and full Go regexp syntax/capacity parity remain separate gaps.
Template helpers also accept Go octal escapes in patterns, consuming up to
three digits with Unicode-character semantics through 0777. Nonzero single
digits remain unsupported backreferences. Classes, quantifiers and literal
quoting keep their meanings; these escapes do not expand selector/parser syntax.
Typed JSON and object/array iteration are covered by the continuation below.
Timestamp/Unix helpers are covered by the continuation below. Channel/function
iteration, remaining date functions, non-UTF-8 decoded/sliced values,
exact template errors and general
pipeline-error ownership remain incomplete; this is not full formatter parity.

Finite template float literals preserve decimal/scientific/hexadecimal syntax,
signed zero and Go's direct-constant versus typed-pipeline integer argument rules.
All ten original non-complex Go float-number oracles replay through HTTP/Parquet/S3.
Of 6,912 actual pinned Loki pipeline comparisons, 6,874 agree exactly excluding
error details, and 38 deliberately reject nonportable out-of-range integer casts;
there are no unexpected differences. These are not full Go-template/client or
capacity acceptance. Upgrade query-serving apps; no native/storage/keyed-byte change.

Finite complex template constants preserve both components, signed zeros,
truthiness, complex-only equality, generic numeric coercion to zero and
direct-constant versus typed-pipeline numeric arguments. Seven original Go
number rows and separate controls replay through HTTP/Parquet/S3; 8,240 actual
pinned Loki pipeline comparisons agree, including 4,744 runtime errors,
excluding exact diagnostics. These are supplementary Go number oracles, not
additional Loki formatter-source rows. Malformed/overflowing components reject
before storage. Existing nonportable integer-cast exclusions, resource ceilings
and native/storage/keyed bytes are unchanged; upgrade query-serving apps.

Template root maps and `index` support arbitrary string-key label lookup,
missing-key empty values, sorted rendering, map length/truthiness and successive
string-byte indexing. Unsigned byte values preserve comparison/conversion and
typed-argument distinctions. Two original Go comparison regressions replay
through HTTP/Parquet/S3; 7,638 actual pinned Loki pipeline comparisons agree,
including 4,442 runtime errors, excluding exact diagnostics. Root-map output
overflow fails whole even before later drops or explicit error selection.
This is not generic array/JSON-object/field-chain or full built-in parity.
Existing resource ceilings and native/storage/keyed bytes are unchanged;
upgrade query-serving apps.

Template `slice` supports string byte ranges with default indices, exclusive
ends, empty results and integer/uint8 indices; invalid bounds/types fail with
`TemplateFormatErr`. All six original Go string cases replay through
HTTP/Parquet/S3, with separate pipeline/conversion/Unicode controls. Of 4240
actual pinned Loki pipeline comparisons, 4076 agree excluding exact diagnostics
and 164 deliberately reject non-UTF-8 intermediate byte strings; no unexpected
differences. Array slicing and byte-string propagation remain open. Existing
resource ceilings and native/storage/keyed bytes are unchanged; upgrade query apps.

Template `print`, `println`, `html` and `urlquery` support variadic typed values,
Go argument spacing/newlines, explicit HTML escaping and query-form escaping.
Seven original Go cases and seventeen controls replay through HTTP/Parquet/S3;
8504 actual pinned Loki pipeline comparisons agree, including 1488 runtime
errors, excluding exact diagnostics. Combined text/escaped output retain the
16 MiB fail-whole cap; dense HTML escaping stays within the worker heap.
`printf`, typed-interface/pointer values and byte-string propagation remain
open. No native/storage/keyed-byte change; upgrade query-serving applications.

Template `js` adds Go JavaScript text escaping, including variadic typed/nil
joining, ASCII controls and Unicode 17 printability. All eight upstream Go
escaping rows plus fourteen successful controls and three nested runtime errors
replay through HTTP/Parquet/S3. All 19060 actual pinned Loki pipeline comparisons
agree, including 304 runtime errors, excluding diagnostics; grouped Unicode
inputs cover all 1112064 valid scalars. Exact 16 MiB output and fail-whole
intermediate overflow replay through log/top-k/metric paths. This preserves Go's
minimum-width escapes, not JSON string quoting or context-aware sanitization.
Existing ceilings and native/storage/keyed bytes are unchanged; upgrade query apps.

Template `with`/`else with` actions change dot only inside successful branches;
false branches and later actions retain it. `$` preserves the formatter's root
label snapshot, including across label-format destinations, refreshed by a later
stage. Ten original Go cases plus nested/typed/error/admission controls replay
through HTTP/Parquet/S3. All 2387 actual pinned Loki pipeline comparisons agree,
including 516 runtime errors, excluding diagnostics; six syntax/function
rejections also match actual Loki. Exact 16 MiB and accumulated outer/branch
overflow checks retain existing fail-whole policies. Generic object iteration
remains unsupported. Existing ceilings/native/storage/keyed bytes are unchanged;
upgrade query-serving applications.

Template action and `if`/`with` header variables support declarations,
nearest-binding assignments, shadowing/restoration, typed values and saved-map
lookup. Bindings reset across templates/destinations/entries; `$` reassignment
does not mutate the root label snapshot. Three original Go assignment rows,
26 successful controls, ten runtime failures and admission/budget controls replay
through HTTP/Parquet/S3. All 6198 actual pinned Loki pipeline comparisons agree,
including 1454 runtime errors, excluding diagnostics. Eight upstream admission
failures agree. The nested-binding continuation removes the former stricter
parenthesized-pipeline rejection: arguments thread bindings left to right,
lazy operators skip side effects and forward references still reject before
storage. Its 16,622 actual pinned Loki comparisons agree, including 3764 runtime
errors, excluding diagnostics, with unchanged accumulated/intermediate caps.

Template field chains support label-map variables, functions and parenthesized
results, retaining Go's distinct missing-string/nil behavior and skipping field
arguments on nil receivers. Four unchanged Go source rows plus 14 successful,
ten runtime-error and seven admission controls replay through HTTP/Parquet/S3.
The combined 18,838 actual pinned Loki comparisons agree, including 5278 runtime
errors, excluding diagnostics. The JSON continuation below adds typed containers;
methods and full formatter/client/provider/capacity parity remain open.
Existing ceilings/native/storage/keyed bytes are unchanged; upgrade query-serving
applications.

Template `range` supports lexical label-map and integer/uint8 iteration,
single/two-variable bindings where applicable, empty/nil else paths and nested
`break`/`continue`. Dot/root and per-iteration/block scope match the current Go
value profile. Ten unchanged Go parser admission rows, 26 successful controls,
ten runtime errors and syntax/budget controls replay through HTTP/Parquet/S3.
The combined 21,304 actual pinned Loki pipeline comparisons agree, including
5388 runtime errors, excluding diagnostics; 29 constructor rejections also agree.
Each executed iteration shares metric query work or request-local stream-template
work with regex helpers, even without output. Work/output exhaustion fails whole;
early exit charges actual iterations without allocating integer ranges. The
64-expression and 16 MiB caps remain. The JSON continuation below adds object/array
iteration. Channel/function iteration, date/other functions and exact diagnostics
remain open. Upgrade query applications; native/storage/keyed bytes and dependencies
are unchanged.

Template `fromJson` supports typed nested objects/arrays, float64 numbers,
booleans, strings and nil. Field/key/index lookup, container printing/length/
truthiness, numeric coercion, iteration and duplicate-key replacement preserve
the pinned runtime's distinctions from string-valued root labels. Both complete
Sprig v3.3.0 `TestFromJson` oracles plus 29 successful controls and 19 runtime
failures replay through authenticated GET/form POST and native Parquet/S3.
All **10,292 actual pinned Loki pipeline comparisons** agree, including 3874
runtime errors, excluding exact diagnostics. Overflow-to-zero behavior is pinned
to Loki's actual Go 1.27.3 runtime, not an assertion about older Go JSON decoders.
Malformed documents return nil; lone escaped surrogates become replacement
characters. These supplementary Sprig rows are not additional Loki integration
rows, and the comparisons do not establish provider/client/capacity parity.

Each executed decode charges input bytes before expansion to metric query work
or request-local stream-template work, shared with loops/regex helpers. Input
and normalized bytes cap at 16 MiB; containers reuse the 64-level nesting cap.
Growth is checked before allocation and work/size/depth exhaustion fails whole,
including explicit error selection. Public budget controls cover exact output,
raw/normalized overflow, shared entries/actions and nesting. Methods,
channel/function iteration, date/other functions, non-UTF-8 byte
propagation and exact diagnostics remain open. Upgrade query-serving apps;
native/storage/keyed bytes and dependencies are unchanged.

Template array `slice` supports default/exclusive/three-bound indices, retained
decoded backing elements and explicit reduced capacity without copying arrays.
Indexing, iteration and other consumers respect the current view. Ten original
Go slice oracles plus 18 successful/13 failure controls and two explicit
spare-capacity rejections replay through HTTP/Parquet/S3. Of 5984 actual pinned
Loki comparisons, 5980 match, including 4304 runtime errors; four deliberately
reject Go allocator spare cells. No unexpected differences, excluding diagnostics.
Initial capacity is the decoded element count, not Go allocator capacity;
spare-cell access remains a compatibility gap. Shared work and exact 16 MiB/
overflow controls cover ordinary/top-k/metric paths despite error selection or
later shrinking. No native/storage/keyed-byte/dependency/ceiling change; upgrade
query apps. This does not establish full formatter/client/provider/capacity parity.

Template `__timestamp__`, `unixToTime`, `unixEpoch`, `unixEpochMillis` and
`unixEpochNanos` retain typed times and exact source nanoseconds through both
formatters. Unix parsing follows byte-length units, signed-int64 admission and
wrapping; epoch seconds/milliseconds round down before zero. Seven original
Loki formatter rows and both invalid-time inputs replay through authenticated
HTTP and native Parquet/S3, with separate source-time, boundary, scope, wrapping
and error-selection controls. All 28,292 pipeline comparisons against pinned
Loki/Sprig helpers through Go text/template agree, including 100 runtime errors,
excluding diagnostics. Pulso renders time values in UTC, not the host-local
timezone used by Loki. Arbitrary date layouts, named zones, time methods and
`now` remain open. Existing budgets/native/storage/keyed bytes/dependencies are
unchanged; upgrade query applications. This is not full date/client parity.

Template untyped `nil` supports false truthiness/defaults, equality, zero numeric
coercion, function-result `<no value>` rendering and distinct print/escape
representations and spacing. Bare nil commands and typed-argument failures remain
runtime `TemplateFormatErr`, not admission errors; missing labels stay empty
strings. Three original Go cases plus successful/error/admission controls replay
through HTTP/Parquet/S3. All 7528 actual pinned Loki pipeline comparisons agree,
including 812 runtime errors, excluding exact diagnostics. Existing budgets and
native/storage/keyed bytes are unchanged; upgrade query-serving applications.

Template `float64`, `minf`, `maxf`, `ceil`, `floor` and `round` support coercion,
signed zero/nonfinite runtime values, typed float comparisons and integer
precision/float thresholds. Three original Loki formatter rows replay through
HTTP/Parquet/S3; 27,928 actual pinned parser/pipeline comparisons agree, including
30 runtime errors, excluding exact diagnostics. Rounding preserves the pinned
Go 1.27.3 fused-residual profile, not a cross-compiler/platform bit-parity claim.
An independent control records that this runtime rounds `123.5555` to `123.555`
at three places, unlike Sprig's static `123.556` test expectation; this is not
claimed as an unchanged Sprig table replay. Nonportable float-to-integer casts
still deliberately reject. Existing resource ceilings and native/storage/keyed
bytes are unchanged; upgrade query-serving apps.

Template `addf`, `subf`, `mulf` and `divf` use float64 coercion followed by exact
shortest-decimal arithmetic, rounding once on function return. Division rounds
each step to 16 decimal places, halfway away from zero. Two original Loki rows
replay through HTTP/Parquet/S3; all 14,012 actual pinned pipeline comparisons
agree, including 1,524 runtime errors, excluding exact diagnostics. Controls
distinguish decimal cancellation and variadic order from binary arithmetic,
input precision loss, underflow/overflow, nonfinite/zero-division failures and
preserved metric errors. Existing ceilings and native/storage/keyed bytes are
unchanged; upgrade query-serving apps. This is not full formatter/client or
capacity acceptance.

Template rune constants retain integer code-point values, including hex/octal
byte escapes without creating byte strings. Eight original Go number oracles,
three original rejections and all seven selected lexer character tokens replay
through HTTP/Parquet/S3, separately from the 65 Loki formatter evaluations.
Malformed constants reject even in skipped branches; type errors preserve inputs
and existing metric-error selection. Another 10,400 actual pinned Loki pipeline
comparisons agree, including 2,600 runtime errors, excluding exact error details.
Upgrade query-serving applications; no additional native/storage/keyed-byte change.

The Loki conversion-diagnostic gap now excludes numeric, duration and byte errors:
filters and unwrap expose Go-compatible details, including duration byte-oriented
escaping, byte numeric-prefix errors, normalized unknown suffixes and overflow
precedence, and numeric syntax/range failures with Unicode 17 Go quoting.
Exact-detail selectors work through HTTP without changing
error-preservation policies or budgets. Quoted/raw strings now follow Loki's
scanner and Prometheus decoder, including raw byte escapes, literal carriage
returns, surrogate replacement and signed-int32 escape overflow. Single quotes,
unknown/incomplete escapes, literal NUL and invalid UTF-8 source reject.
The 104 frozen cases and HTTP/Parquet/S3 regressions close literal decoding,
not exact parser-error text/positions, full LogQL or client acceptance. See
the [query contracts](querying.md#parser-labels-and-quantities) and
[string-literal fixtures](../test/fixtures/logs/loki-upstream/README.md#string-literal-continuation).

Logfmt log-line decoding is a separate covered slice: default recovery, strict
`LogfmtParserErr` byte positions, empty-value policy, normalized first duplicates
and quoted Unicode/surrogate decoding replay all 60 selected upstream
unit/engine evaluations through authenticated HTTP and native Parquet/S3.
Explicit-field source grammar/admission, normalized-key lookup and absent-source
defaults add 15 frozen unit-table rows and separate HTTP controls. One row's
ingest-promoted `detected_level` is asserted separately from Loki's unit oracle;
the empty-expression rejection follows the pinned parser rather than a nil-error
assertion in that test table. The three original expression engine evaluations
remain in the 60-case replay, not counted again.
Thirteen formerly omitted table/engine evaluations now cover structured-metadata
collisions, dropped stream names and categorized output through the same stack.
Additional controls cover repeated parsers, retained extraction history after
drops, explicit JSON overwrites and explicit-logfmt suffix halting. Another
1512 actual pinned pipeline comparisons agree. These do not establish full JSON
parsing or hint-driven early exits. Upgrade every
query-serving app/native pair to log tuple `{1,1,10}`; storage/keyed bytes and
resource ceilings are unchanged. See the
[logfmt fixtures](../test/fixtures/logs/loki-upstream/README.md#logfmt-decoding-continuation).

Plain JSON parsing now preserves source order, first duplicate/sanitized-target
ownership, nested object flattening, scalar numeric spelling and array/null
omission. Explicit JSON missing-source defaults preserve existing destinations,
unlike present nulls. All 16 upstream parser-table rows and 42 engine evaluations
replay through authenticated GET/form POST and native Parquet/S3, including
error-preserving grouped metrics and categorized output. The hinted table row's
public output is checked without exercising Loki's hint object. An additional
1216 actual pinned pipeline comparisons agree. This does not close malformed-input
partial extraction/diagnostic parity or hint-driven early exits. Evaluation remains bounded and outside native pushdown;
the log tuple, storage/keyed bytes and limits do not change further. See
[JSON fixtures](../test/fixtures/logs/loki-upstream/README.md#json-parser-continuation).

Explicit JSON paths now include dot/bracket keys, UTF-8 quoted keys, root/nested
array indices, source-preserving objects/numbers and whole-array escape decoding.
All 29 original expression rows, five constructor rejections, 11 nonempty grammar
paths, six grammar rejections and five engine evaluations replay over HTTP and
Parquet/S3. The grammar table's empty/nil-path row has no upstream error assertion;
it remains provenance, not an acceptance oracle. Pulso rejects empty paths.
Separate controls cover nested duplicates, source-order destinations, shared
paths, arrays and pre-read rejection. Another 3072 actual pinned pipeline
comparisons agree. Exact constructor/whole-parser diagnostic text, general
malformed-input and hint parity remain open; see
[explicit JSON fixtures](../test/fixtures/logs/loki-upstream/README.md#explicit-json-path-continuation).

Loki `ip(...)` line and label predicates now support original IPv4/IPv6 address,
CIDR and inclusive-range matching, negation and embedded-address scanning.
Missing labels, prior errors, mapped families and candidate boundaries retain
the pinned Loki behavior. Fifteen original upstream unit-table rows replay
through authenticated HTTP/Parquet/S3, with separate local pipeline/budget
controls; they are not fifteen integration suites. Invalid forms fail before
provider reads. Existing resource bounds and native/storage formats are
unchanged; IP evaluation does not use native pushdown. See the
[IP fixture provenance](../test/fixtures/logs/loki-upstream/README.md#ip-filter-continuation).
This closes IP predicates, not full LogQL, exact parser-error messages or
deployed-client/provider/capacity acceptance.

Loki line-filter `or` alternatives now work for substring, regex and pattern
operators, including mixed string/IP alternatives under `|=`/`!=`. Positive
groups keep any match; negative groups reject any match. All alternatives
validate before storage, and adjacent stages preserve their authored order.
Two HTTP/Parquet/S3 wrappers replay 27 original upstream matcher-table queries
(121 line decisions) and 29 engine evaluations (26 successful, three rejected),
with a separate local admission/budget control. This removes the line-filter
alternative gap, not full LogQL or exact parser-error text. Evaluation stays in
the bounded Elixir pipeline, without native pushdown or format/limit changes.
See the [line-filter fixtures](../test/fixtures/logs/loki-upstream/README.md#line-filter-or-continuation).

Loki label-filter comma and implicit conjunctions now parse, including mixed
`or` expressions with the pinned parser's right-attaching behavior. Explicit
`and` still binds tighter than `or`; both keywords are case-insensitive.
One authenticated HTTP/Parquet/S3 wrapper preserves 36 original engine
evaluations (27 successful, nine rejected), with a separate truth-table,
error-propagation and admission control. These are engine-script ports,
not additional service integration suites. Existing evaluation paths, budgets,
native/storage formats and generic metric-error envelopes are unchanged.
See the [conjunction fixtures](../test/fixtures/logs/loki-upstream/README.md#label-filter-conjunction-continuation).
Complete parser-error and deployed-client/capacity acceptance remain open.

The PromQL subset also includes eight optional-vector UTC calendar functions
(`year`, `month`, `day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`,
`hour`, `minute`), natural `sort_by_label`/`sort_by_label_desc` with histogram
preservation, and quadrant-aware binary `atan2`. Range sorts bypass ordering
and emit query-local warnings; invalid histogram binary pairs emit operand-type
infos. Calendar exceptional-value conversions freeze Go 1.25 arm64 behavior;
natural-comparator empty/leading-zero ambiguities, cross-platform float bit
identity and upstream expression positions are not parity claims. The batch
adds 126 unchanged Mimir query-engine cases, replayed through HTTP with memory
and local S3/Parquet. See [query semantics and limits](querying.md) and
[fixture provenance](../test/fixtures/promql/mimir/README.md).

Six further scalar-function gaps are closed: `start`, `end`, `step`, `range`,
`min_of` and `max_of`. Query-context values retain requested outer boundaries
inside subqueries and remain source-local in batches; extrema preserve Go's
infinity/NaN precedence and signed-zero ties. Another 51 unchanged Mimir cases
run through HTTP on both storage adapters. Pulso retains nanosecond query-time
precision rather than upstream millisecond quantization. Selector `@` modifiers,
including `@ start()`/`@ end()`, are implemented by the separate time-modifier
batch below, not by these scalar calls alone.

Four aggregation gaps are also closed: population `stddev`/`stdvar`, per-step
`quantile` and float/native-histogram `count_values`. Another 88 unchanged Mimir
engine cases exercise grouping, changing membership, nonfinite values,
histogram omissions and complete histogram value labels over both storage
adapters. Invalid quantiles warn even on empty input. Generated value-label
overwrites follow Mimir's group-then-set contract, including retaining a label
named by `without`, where the pinned Prometheus engine differs. Sort and label
construction share query budgets; full language/annotation/client/capacity parity
remains open.

Histogram quantiles now warn for missing/malformed/NaN bounds, omit conflicting
classic/native identities per step, and distinguish real monotonic repairs from
silent floating-point adjustments. Native quantile/fraction NaN-observation
notices reach responses instead of being discarded. Twelve unchanged pinned
Mimir cases replay over both HTTP query endpoints and memory/Parquet/S3, with
separate collision, stale, numeric-boundary and source-isolation controls.
The plural `histogram_quantiles` function adds nine unchanged Mimir evaluations:
per-step quantile labeling, sparse range identities, duplicate-step failures,
mixed classic/native omission, NaN notices and composed native expressions.
Separate controls cover OpenMetrics labels, UTF-8 destinations, overwrites,
form POST and shared fan-out/bucket/label-work quotas. Intermediate fan-out is
reserved before construction; limits and native/storage bytes are unchanged.
Classic fractions add three unchanged Mimir evaluations and 14,400 actual pinned
Go comparisons: sorted/coalesced buckets, linear interpolation and no quantile
monotonicity repair. Another eight unchanged Mimir evaluations cover corrected
selected-bucket native NaN-sum interpolation and ±Inf sums without NaN-observation
notices. That correction intentionally supersedes Prometheus 3.5.0's last-bucket
quirk. Eight further unchanged Mimir evaluations cover count-based zero-point
extrapolation for native counter `increase`, including aligned/unaligned ends
and zero points inside/outside the requested range. One factor scales the whole
histogram; individual bucket increases need not stop at their final values.
Separate public controls cover `rate`, resets, negative sums, custom/negative/zero
buckets and unchanged `delta`/`irate`. Other native arithmetic and upstream
expression positions remain gaps. Generic classic-repair notices match the
pinned Mimir streaming engine; expanded summaries in its vendored Prometheus
engine are not a missing Mimir feature.

`idelta` adds the complete ten-evaluation Mimir engine block: last-pair float/
native subtraction, NaNs, short windows, gauge-type warnings, mixed sample/schema
omissions and custom-bound reconciliation. Native subtraction now coarsens
differing custom layouts to common bounds, matching 4,983 actual pinned Go
helper executions with bit-exact finite bucket results. Additional public controls
cover decreasing gauges, disjoint/asymmetric bounds, nanosecond observations,
anchors/subqueries, form POST, tenant/source isolation and shared budgets.
The complete eleven-evaluation `irate` block adds gauge/mixed-pair warnings,
schema-family resets and custom-bound changes without false resets. Custom
reset detection compares plain interval sums at common bounds including +Inf,
matching 9,950 executions of the actual pinned Go `FloatHistogram.DetectReset`.
Reset branches report no unexecuted subtraction notices. The complete four-case
`delta` block adds non-gauge/mixed-window warnings and full bucket outputs;
public controls distinguish whole-window float mixtures from endpoint-only
histogram schema/hint checks, custom reconciliation, extrapolation and isolated
batch notices. Eleven unchanged custom-arithmetic evaluations add mismatched-bound
`sum`/`avg`, sampling/counting/set controls and binary `+`/`-` with full outputs
and reconciliation info. Addition now uses common-bound compensated mapping,
matching another 9,950 executions of actual pinned Go `FloatHistogram.Add`
(compacted geometry, finite count/sum/bucket bits and reset hints). Binary
subtraction preserves gauge identity through nested queries; binary histogram
work spends the shared query/batch budget. Another 21 unchanged evaluations
cover complete range/group histogram aggregation blocks. Native sum/avg and
their over-time forms now report custom reconciliation, incompatible families,
mixed inputs and original reset-hint conflicts under shared bucket/bound work
limits. Public controls retain source names, stop notices at the first failed
family and isolate query-batch sources. Seven more unchanged counter-range
evaluations cover gauge/schema warnings, nested histogram-count calls and
first-pair layout resets. Rate/increase now report executed subtraction and
reset-addition reconciliation, preserve endpoint-versus-middle warning
precedence, and charge native bucket/bound work before traversal. Another 26
unchanged evaluations cover compensated histogram sum/avg and their over-time
forms, cancellation, overflow-driven incremental means, negative buckets and
singletons. Compensation survives resolution, zero-threshold and custom-bound
changes; public HTTP controls cover those transitions and bucket/zero-field
overflow independently of count overflow. An additional 55,112 actual pinned
Go `KahanAdd` executions across 10,000 chains agree on compacted geometry,
finite field bits and nonfinite classes. This is primitive validation, not
55,112 upstream engine tests. Implicit iterator-generated reset hints and
complete annotation parity remain gaps. Matching-layout/exponential reset
detection now traverses all remaining previous buckets, so leading zero buckets
cannot hide disappeared populations; zero widening uses the pinned compensated
traversal. Fifty thousand actual pinned Go `DetectReset` comparisons agree,
including custom/exponential layouts, both bucket sides, sparse tails,
zero/schema changes, nonreset controls and hint overrides. Public controls
exercise `resets`, `irate`, `rate` and `increase` through both storage adapters.

Another 25 unchanged evaluations cover the complete native multiplication/
division and mixed-arithmetic blocks. Negative scaling and unary negation now
produce gauge histograms, retained through nested arithmetic and double
negation. Public HTTP controls distinguish gauge `delta` from warned counter
`rate` and retain a positive-scaling counter control. Another 26,000 actual
pinned Go `Mul`/`Div` calls agree on full geometry, field bits/nonfinite classes
and reset hints, including signed zero, infinities and NaN factors. These are
primitive comparisons, not upstream engine tests or full native arithmetic
parity. Upgrade query applications; native/storage/keyed bytes are unchanged.

Histogram trimming `</` and `>/` now replays the complete pinned 124-evaluation
block through HTTP and native Parquet/S3: scalar/vector cutoffs, ordinary
matching/grouping, exponential/custom/zero interpolation, infinite endpoints,
empty histograms and fraction identities. Names/reset hints survive; no-op trims
retain original totals. Invalid matched types omit with info; `bool` and
scalar-only trims reject before storage. Shared work/scan/sample/result bounds,
tenant isolation and per-source notices remain enforced. Another 49,840 actual
pinned Go TrimBuckets calls match finite bits, signed zeros, nonfinite classes,
geometry and hints with Go FMA disabled (`-gcflags=all=-d=fmahash=n`), matching
Pulso's existing unfused arithmetic. This does not establish bit parity with
default arm64 FMA builds, which can differ even in overflow/cancellation classes.
Another 37 unchanged property/dispersion and bucket-retention evaluations cover
empty exponential/custom histograms, sparse signed buckets, NaN/infinite sums,
and bucket-sensitive equality/changes/distinct counts beneath scalar wrappers.
These numerical functions already existed; the new runtime correction charges
variance/deviation and histogram equality traversal to shared query/batch work,
including empty slots, sparse bounds and false/`bool` comparisons. Constant-time
count/sum/average remain exempt from traversal charges.
Another 212 unchanged evaluations cover repeated/accumulating histograms,
resolution changes, signed/zero/custom buckets, bucket-only resets, quantile/
fraction interpolation and inverses, and constant-histogram rates. These
already-supported numerical paths now have permanent HTTP/Parquet/S3 coverage.
Multiplication, nonzero division and unary negation also now reserve bucket work
before traversal; empty slots, multiplication by zero, scalar wrappers, repeated
steps and shared batches cannot evade it. Unary plus and zero division remain
constant-time; scaling does not charge unused custom bounds.
Another 25 unchanged native/mixed binary evaluations cover full timestamp grids,
matching, scaling and operand-type/layout/reconciliation notices. Binary `+`/`-`
now warn when incompatible families omit a pair and retain valid computed values
with a warning for conflicting explicit reset/no-reset hints. Schema rejection
precedes hint diagnostics; ordinary gauge/unknown hints do not create conflicts.
Request/batch notice isolation remains enforced; AST positions and implicit
iterator hints remain open.
The complete classic-histogram file adds 183 unchanged evaluations covering
paired classic/custom-native quantiles and fractions, grouping/rates, duplicate
boundaries, empty distributions, zero crossings and infinite-width buckets.
It exposed incorrect exponential zero-bucket clipping of custom fraction
buckets; custom bounds now remain intact. First negative-infinite buckets and
large/tiny linear interpolation also follow the pinned implementation.
Another 24 unchanged evaluations cover paired NaN sums, cross-schema
subtraction, anchored subqueries, mixed-type range ordering, shared-source
properties/quantiles and custom `irate` resets versus reconciliation.
All 425 generated native inputs match the actual pinned classic converter;
expectations remain the unchanged upstream test DSL.
Five more unchanged range evaluations cover custom-bucket equality/inequality,
unsupported ordering info and mixed-schema aggregation omission. Classic
quantile comparisons now also preserve upstream's subnormal absolute floor,
strict tolerance and capped relative scale, avoiding arithmetic failures on
large finite counts. Six public HTTP controls and 10,030 actual pinned Go
quantile/repair comparisons cover these numerical extremes separately from
the imported engine count.
The complete selected corpus now contains 1590 evaluations in 85 files, replayed
through 5770 HTTP requests across both adapters. Upgrade query applications;
native/storage/keyed bytes are unchanged. Remaining native annotation/iterator
and deployed-client/provider/capacity gaps are not closed by this batch.

The time-modifier batch closes signed offsets, unitless duration literals and
numeric/`start()`/`end()` `@` anchors on selectors and explicit-resolution
subqueries. Another 95 unchanged Mimir evaluations cover fixed data windows,
varying scalar parameters, nested clocks and true source timestamps. Bounds
remain outer-query/source-local and preserve admitted nanoseconds; existing
limits and stricter duration/time syntax are documented in [querying](querying.md).
This does not add negative-source-time S3 storage or upstream step-invariant
operator caching.

The sampling batch closes `limitk` and `limit_ratio`, adding the complete two
Mimir limit fixture files: 130 unchanged evaluations over both storage adapters.
They preserve full labels/names and float/native-histogram samples, support
per-step scalar parameters and ratio complements, and validate/cap parameters
even on empty input. Ratio membership uses the pinned default upstream label
hash; `limitk` uses deterministic hash ordering rather than promising Mimir's
iteration-order subset. Sampling spends existing scan/sample/work/result
budgets and is not indexed acceleration or capacity acceptance.

Duration arithmetic now covers range windows, subquery windows/resolutions and
signed offsets, including nested outer-context `step()`/`range()` and
`min_of`/`max_of`. Unit-bearing scalar arguments also work. The complete unchanged
59-case Mimir duration file adds 178 memory/S3 HTTP requests; separate controls
cover instant queries, nanosecond rounding, nested contexts and fail-whole
validation/budgets. Pulso retains its one-year ceiling and nanosecond rounding,
not upstream millisecond truncation. No native/storage/keyed-byte changes.

The grammar batch closes omitted subquery resolutions (fixed one-minute epoch
grids), hexadecimal/separator query time literals, quoted UTF-8 grouping and
matching/include labels, and UTF-8 `label_replace` destinations. Fourteen more
unchanged Mimir evaluations replay through memory and native S3/Parquet HTTP;
local controls distinguish outer clocks, nested grids, nanosecond boundaries,
grouped operand order and invalid names/literals. The selected corpus now has
815 evaluations in 60 files (2732 memory/S3 HTTP requests). Query budgets,
one-year/int64 time bounds and HTTP parameter grammar remain unchanged.

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
