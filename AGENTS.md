# Pulso

A headless, open-source observability backend: unified logs/metrics/traces access, built-in alerting, and a native Model Context Protocol (MCP) interface, built on Elixir/OTP with a Rust hot path for columnar data.

Pulso is agent-native from day one. It does not ship a UI. Consumers are humans through their own dashboards and, first-class, AI agents through MCP.

## Read this before any non-trivial design work

**[`docs/architecture.md`](./docs/architecture.md)** — source of truth for Pulso's architecture. Load it before you reason about ingest, storage, coordination, alerting, the Elixir/Rust split, or the MCP boundary. If you're about to add a database, a WAL, a leader-election protocol, or any cluster-visible mutable state, read the "Core bets" and "What Pulso deliberately does not have" sections first.

## Where the project is right now

Current trace search capability is **5**, tuple `{2,1,3,5,12}`, superseding the
search4 snapshot references below. Unscoped original-string/int64 search uses
span-key presence before resource fallback; sampled values may include both
owners. Omitted or paired-zero HTTP search bounds scan bounded retained context,
not an ingester-only tier; partial bounds reject. Discovery/window-local grammar,
storage formats and resource ceilings are unchanged. See `docs/trace-query-expressions.md`.

Trace metrics capability **3** adds bounded scalar/vector arithmetic and
value-preserving scalar filters. All unique leaves share one provider scan and
native series/point/selector-work/group-byte budgets; native decode repeats per
leaf, and arithmetic runs in the bounded BEAM query worker. Match ordered typed
group labels, broadcast only truly ungrouped series, omit NaNs and fail whole on
storage/budget errors without resetting scan budgets. Upgrade all serving
app/native pairs; trace/storage/keyed tuples are unchanged. See `docs/querying.md`.

Current log codec tuple is `{1,1,16}`, superseding the log API snapshots below.
Regexp-parser admission inspects native named-capture metadata instead of
compiling PCRE, preserving supported Go flag/quantifier grammar. Missing real
named captures still reject before storage. Upgrade all serving app/native pairs;
storage/keyed bytes are unchanged. Original capture names and indices survive
zero repetitions and impossible alternatives; erased groups extract empty values
without shifting later groups or relinquishing first-extraction ownership.
Predicate, template-helper and regexp/indexed-extraction schemas have an independent 1 MiB logical
ceiling (32 bytes per original group plus raw name bytes), even for tiny optimized
programs. Template replacements index aliases after shared schema admission;
counting does not allocate capture names.
Metadata is built once during existing validation; Boolean compiler paths do not
allocate names, log captures borrow input binaries, and native metric stages skip
copying values already owned by earlier extractions.
Shared query regexes accept leading-caret Unicode property negation, including
double negation, classes, folding and quantifiers. Flag-separated repetitions
nest the preceding quantified atom without adding captures or changing earlier
flags, subsequent scope or U/lazy greediness. Original counted-product and
program/schema/work limits still apply; directly adjacent quantifiers reject.
Normalization defers wrapper insertion to avoid repeated prefix copies; existing
compiled match loops and thread pools are unchanged. Numeric/repeated ASCII
names work in selectors and label_replace; Go-style expansion uses the first
participating named capture and gives decimal indices precedence over aliases.
Regexp parsers still reject repeated raw names; numeric labels gain an underscore
before collision suffixing, with first-declaration ownership after normalization.
Native preparation strips headers once without changing capture indices, and
aliases share detached metric values rather than copying them again. Property
escapes use Go 1.27.3 Unicode 17 category/script membership, aliases, ASCII/Any/
Assigned and loose ASCII case/space/underscore/hyphen spelling. Static native
ranges include property-specific folds before negation; preparation rejects
above a 1 MiB logical expansion charge (21 bytes/range plus 24/property) before
union/zero-repeat simplification, with separately bounded emitted text.
Non-property matching loops and thread pools are unchanged. Full regex/case-fold/
capacity parity remains open; property support does not update literal folding.
Negated scalar classes exclude both U+D7FF/U+E000 when separately listed or
used as range endpoints, without reintroducing them across the surrogate hole.
Preparation bridges only these already-present adjacent scalars; ordinary
classes stay borrowed, without extra class expansion or matching-time copies.
All unwrap aggregations skip missing/empty fields without charging retained
samples. Post-unwrap label filters run after conversion, before grouping;
explicit query error selection can retain failed conversions as zero-valued
error-labeled samples. These queries use the bounded Elixir evaluator, not the
native metric path. Selected scan/pipeline work and unpreserved pipeline-error
guards remain. Typed numeric/duration/byte filters retain malformed/empty values
as LabelFilterErr, preserving earlier errors; missing labels do not match.
Numeric comparisons use float64 including NaN/Inf and rounded large integers;
typed equality accepts =/==. AND evaluates both sides and threads error labels;
OR short-circuits. Native scans fall back for conversion/error handling.
Runtime duration/byte values follow Go signed-int64 duration and go-humanize
unsigned-uint64 byte profiles, including fractional rounding, Greek-mu units,
case-insensitive byte suffixes/spacing/commas and overflow rejection. Duration
unwrap splits seconds/remainders before float conversion. Signed bytes and
runtime days now reject. Query thresholds/offsets use model.Duration then Go
duration conversion, with signed hours/fractions but no fractional d/w/y;
byte thresholds use uint64 and runtime rounding under Loki's restricted suffix
runes. Bracket windows require ordered unrepeated integer y/w/d/h/m/s/ms;
former fractional/ns/us bracket extensions now reject. Source/evaluation times
retain nanosecond precision. Exact shared quantities stay unchanged. 0B remains
accepted despite a pinned scanner/test contradiction. Complete parser errors
remain open, not general quantity-converter or string-literal feature gaps.
Duration filters and unwrap expose Go-compatible invalid/missing/
unknown-unit diagnostics with byte-oriented hex quoting and unchanged first-error
and explicit error-preservation policies. Byte filters/unwrap expose go-humanize
numeric-prefix/unknown-suffix/overflow details and precedence, including its
rune-count/byte-slice diagnostics and simple Unicode suffix lowercasing.
Numeric filters/plain unwrap expose strconv.ParseFloat syntax/range diagnostics
with complete original values and Go Unicode 17 quoting; malformed overflow
retains syntax precedence. Missing/empty unwrap values still skip.
LogQL double-quoted/raw literals follow pinned Loki Scanner + strutil.Unquote,
not Go Unquote: named escapes, raw hex/octal bytes, fixed-width Unicode,
one replacement per escaped surrogate, literal CR/raw LF preservation and
signed-int32 U-escape overflow to low byte. Single quotes, unknown/incomplete
escapes, octal >255, literal NUL/invalid UTF-8 and positive out-of-range Unicode
reject. Regex backslashes require doubling or raw backticks. Frozen upstream
table inputs plus controls replay through HTTP/Parquet/S3; no full parser-error
or client acceptance claim. Upgrade query-serving apps for changed literals.
Upgrade every query-serving app/native pair; storage/keyed bytes are unchanged.

LogQL `ip(...)` line (`|=`/`!=`) and label (`=`/`!=`) filters accept IPv4/IPv6
addresses, host-bearing CIDRs and inclusive same-family ranges. Both scan for
embedded addresses with pinned Loki candidate boundaries, distinct mapped IPv6,
missing/empty-label and prior-error policies. Invalid patterns/operators reject
before storage. IP filters stay in the bounded Elixir pipeline, never native
pushdown; native/storage/keyed formats and resource ceilings are unchanged.
Upgrade query-serving apps. See `docs/querying.md` and Loki fixture provenance.

LogQL logfmt decoding skips malformed pairs and resumes by default; `--strict`
stops at the first error, retains earlier fields and publishes LogfmtParserErr
with one-based byte positions. Empty values/bare keys require `--keep-empty`
outside explicit field extraction; extract-all parsing keeps the first retained
value for each normalized key.
Quoted values decode JSON-style escapes plus escaped apostrophes and Unicode
surrogate pairs; replacement runes become spaces in extract-all parsing and
empty the entire value in explicit extraction. Native unflagged ASCII decoding
shares these semantics; Unicode falls back to the bounded Elixir pipeline.
All 60 selected upstream unit/engine evaluations replay through HTTP/Parquet/S3.
Explicit source grammar/admission, normalized-key lookup and missing-source
defaults add 15 frozen table rows and HTTP controls. Missing sources do not
invent empty suffixes for original stream labels. Duplicate destinations use
their final source definition after validating every expression; ordered source
remapping precedes destination-key capture, with last matching values retained.
Nineteen source-derived HTTP controls match the actual pinned Loki parser;
13,104 additional strict/non-strict runtime comparisons agree. Expression stages
stay in Elixir. Stream/metadata collisions use one suffix; drops release stream
names, parsed history survives drops, and ordinary parsers keep first extractions.
Explicit JSON overwrites; colliding explicit-logfmt suffix history halts scanning
before later strict errors. Shared-source aliases choose one lexical destination,
not exact Go map iteration. Thirteen formerly omitted upstream evaluations replay
through HTTP/Parquet/S3; 1512 additional actual-pipeline comparisons agree.
Hint-driven early exits and broader JSON/parser grammar remain open. Upgrade every
query-serving app/native pair to log tuple `{1,1,7}`; storage/keyed bytes and
resource ceilings are unchanged.

Plain LogQL JSON parsing preserves ordered nested scalar flattening, first
duplicate/sanitized-key ownership, original numeric spelling, null/array omission
and replacement-character values. Missing explicit sources preserve existing
destinations; present nulls extract empty values. All 16 pinned upstream table
rows and 42 engine evaluations replay through GET/form POST and native
Parquet/S3; 1216 additional actual-pipeline comparisons agree. This is not full
malformed partial-extraction/diagnostic or hint parity. Execution
stays in the bounded Elixir evaluator; no further tuple/storage/keyed-byte change.

Explicit JSON accepts dot/bracket keys, UTF-8 quoted keys, root/nested arrays and
int64 indices. Numbers/objects preserve source text; arrays unescape as whole
strings. First path matches and source-order repeated destinations retain
ownership/defaults; empty lines do not add defaults. Fifty-six original table,
grammar/admission and engine evaluations replay through HTTP/Parquet/S3; the
ambiguous empty-path grammar row stays provenance, while Pulso rejects it.
Another 3072 actual pinned pipeline comparisons agree. Complete parser-error,
malformed-value and hint parity remain open. No native/storage/keyed-byte change.

LogQL range pipelines can precede or follow brackets/offsets; source and recursive
whole-range parentheses retain pipeline/unwrap order. Split pipelines and invalid
post-unwrap stages reject before storage, with unchanged expression/depth limits.
Seventeen upstream first/last engine oracles replay in six placements; 336 actual
pinned parser admission comparisons agree. No native/storage/keyed-byte change.

LogQL `label_replace` supports instant/range metric label rewriting after
stream/metadata/parser ownership resolution, whole-value capture matching,
empty-source defaults and destination deletion. Regex dot is non-dot-all as
in Loki; values are never summed by rewriting. All 19 pinned engine evaluations
replay through HTTP/Parquet/S3, plus newline/capture/collision and nested-policy
controls. Existing budgets and native/storage/keyed bytes are unchanged; upgrade
query-serving applications. Full parser-error/distributed/client parity remains open.

Regexp/pattern parser admission, partial short-line captures and string-only
unpack with required string `_entry` replay all 30 remaining pinned parser-engine
evaluations through HTTP/Parquet/S3. Constructor tables, nine unhinted unpack
rows and 528 actual pinned pipeline comparisons cover duplicates, ownership,
Unicode and huge-number skipping. Complete malformed-input diagnostics and hint
early exits remain open. Existing budgets and native/storage/keyed bytes are
unchanged; upgrade query-serving applications.

LogQL label_format validates unique/non-error destinations before storage;
ordered renames delete self-renames, while templates share the first-template
label snapshot within a stage. Seven original validation/basic-assignment rows
replay through HTTP/Parquet/S3; 723 actual pinned pipeline comparisons agree,
including Unicode literal prefixes for line/label templates.
String templates additionally support bounded pipelines/parentheses, quoted
UTF-8 strings, booleans, signed-int64 literals, simple Go case conversion, trim/prefix/suffix predicates,
Unicode cutset trimming, literal/counted replacement, repetition, indentation,
Unicode-scalar alignment, byte-based truncation/slicing, historical Go title casing,
query-form URL escaping and Base64 encoding/decoding, default and current-line
lookup, with legacy aliases. Conditional if/else-if/else, lazy and/or, not,
typed comparisons and byte len validate all branches before storage while only
selected branches execute under one shared output cap. Another 32,298 actual
pinned runtime comparisons agree, excluding exact error details. Literal ASCII
whitespace trim markers and non-nesting comments follow Go delimiter rules;
rendered values/Unicode spaces never trim. Fifteen underlying Go lexer/parser
rows replay through HTTP/Parquet/S3, separate from the Loki source-row total;
29,826 actual pinned runtime comparisons agree, excluding exact error details.
Integer helpers int/add/sub/mul/div/mod/min/max coerce strings/booleans with
base-zero parsing and exact decimal truncation; invalid/overflowing conversions
return zero, arithmetic wraps signed64 and division by zero preserves inputs
with TemplateFormatErr. Another 20,826 actual pinned pipeline comparisons agree,
including 1,458 runtime errors, excluding exact error details.
Quantity helpers bytes/duration/duration_seconds preserve typed finite floats,
Go general rendering, numeric-zero truthiness/defaults and float comparisons.
Integer helpers truncate in-range float inputs; out-of-range float-to-int rejects
instead of choosing Go's implementation-dependent overflow. Of 66,570 actual
pinned pipeline comparisons, 66,438 agree exactly (excluding error details) and
132 deliberately reject those nonportable casts; no unexpected differences.
Regex count returns bounded integer non-overlapping match counts using the
non-backtracking native Go-style regex profile, including Unicode scalar
empty-match progress, ASCII word boundaries and Unicode script properties.
Invalid executed patterns preserve inputs with TemplateFormatErr. Accepted
matches plus one final probe spend shared metric work or request-local stream
work across entries/actions; work exhaustion fails whole, never as a preservable
formatting error. Upgrade query-serving apps/native artifacts to log tuple
{1,1,10}; storage/keyed bytes are unchanged.
Another 29,952 actual pinned Loki pipeline comparisons agree, including 3,120
runtime errors, excluding exact error details.
Regex replacement expands Go-style whole-match/numbered/named captures, dollar
escapes and malformed/unknown references; its literal variant never expands.
Both share counting work across entries/actions. Byte-admission and rendering
passes each charge probes/dollar scanning and capture-name bytes even for
unknown names, without retaining matches; complete
output fits 16 MiB before allocation even under later truncation. Work/output
exhaustion fails whole. Another 107,520 actual pinned Loki pipeline comparisons
agree, including 15,360 runtime errors, excluding exact error details. All 87 Go
replacement table rows retain their oracles across 135 successful mode evaluations,
including ten formerly rejected repeated-name cases. Template count/replacement
accept numeric/repeated ASCII capture names in both Go spellings and literal
\Q...\E runs, including EOF termination. Named expansion takes the first
participating capture, including empty matches; numbered references preserve
positions and Go's one-billion/leading-zero rules. Extra duplicate lookups spend
shared work even when they emit nothing. Selector/regexp-parser admission is
unchanged. Template helpers additionally accept Go octal escapes: up to three
digits, a required second digit after nonzero first digits, and Unicode scalar
rather than byte semantics through 0777. Classes, quantifiers, capture numbering
and quoted/escaped literals retain their meanings; lone nonzero backreferences
still reject. Full Go regexp syntax/capacity parity remains open.
Unknown functions and
unclosed actions reject before storage; argument failures attach TemplateFormatErr
and preserve inputs. Sixty-five source formatter evaluations replay through
HTTP/Parquet/S3. Formatting caps output at 16 MiB per entry/template and nesting
at 64; expanding functions check intermediate sizes before allocation, and output
overflow fails whole, never as a preservable error. Base64 corruption returns ordinary
Go byte-offset diagnostic strings. Decoded/sliced non-UTF-8 values become formatting
errors until byte-aware downstream handling exists. Generic JSON object/array
values, array/channel/function iteration, other functions, non-UTF-8 byte literals and
exact error text/positions/ownership remain open. Native/storage/keyed bytes
are unchanged; upgrade all query-serving applications. See docs/querying.md.

Template rune literals now retain integer Unicode code points, including Go
hex/octal byte escapes without creating non-UTF-8 strings. Eight original
number oracles, three original rejection rows and all seven selected lexer
tokens replay through HTTP/Parquet/S3; 10,400 actual pinned Loki comparisons
agree, including 2,600 runtime errors, excluding exact error details. Malformed
constants reject before storage even in skipped branches; existing type/error/
budget policies remain.
Upgrade query-serving apps; no additional native/storage/keyed-byte change.

Finite template float constants accept Go decimal/scientific/hexadecimal spelling,
separators and signed zero. Direct integral in-range literals satisfy integer
arguments; parenthesized/piped floats require explicit int conversion. Ten original
Go number oracles replay through HTTP/Parquet/S3. Of 6912 actual pinned Loki pipeline
comparisons, 6874 agree excluding exact error details and 38 deliberately reject
nonportable out-of-range casts (including rounded positive int64 literal boundary);
no unexpected differences. Existing budgets/native/storage/keyed bytes are unchanged.
Upgrade query-serving applications.

Finite complex template constants preserve signed components, truthiness,
complex-only equality, generic numeric coercion to zero and contextual typed
numeric arguments for zero-imaginary direct constants. Seven original Go
number oracles and separate controls replay through HTTP/Parquet/S3;
8,240 actual pinned Loki comparisons agree, including 4,744 runtime errors,
excluding exact diagnostics. Malformed/overflowing components reject before
storage. These are supplementary Go rows, not more Loki formatter-source rows.
Existing nonportable integer-cast exclusions, resource ceilings and native/
storage/keyed bytes are unchanged; upgrade query-serving apps.

Template root maps and index support arbitrary string-key label access,
missing-key empty strings, lexical map rendering/length/truthiness and
successive string-byte indexing. Bytes retain uint8 type boundaries for
comparisons, casts and typed arguments. Two original Go comparison regressions
replay through HTTP/Parquet/S3; 7638 actual pinned Loki pipeline comparisons
agree, including 4442 runtime errors, excluding exact diagnostics. Map output
overflow fails whole before later drops/error selection. Generic arrays/JSON
objects remain open; this is not full built-in parity. Existing
resource ceilings and native/storage/keyed bytes are unchanged; upgrade query apps.

Template slice supports default/exclusive string byte ranges, empty results
and integer/uint8 indices, with runtime type/bounds errors. Six original Go
string cases replay through HTTP/Parquet/S3; of 4240 actual pinned Loki pipeline
comparisons, 4076 agree excluding diagnostics and 164 deliberately reject
non-UTF-8 intermediate strings, with no unexpected differences. Array slicing
and byte-string propagation remain open. Existing resource ceilings and native/
storage/keyed bytes are unchanged; upgrade query-serving apps.

Template print/println/html/urlquery accept variadic existing value types,
with Go spacing/newlines and explicit HTML/query-form escaping. Seven original
Go cases and seventeen controls replay through HTTP/Parquet/S3; 8504 actual
pinned Loki pipeline comparisons agree, including 1488 runtime errors,
excluding diagnostics. Combined text and escaping preserve fail-whole 16 MiB
bounds; dense HTML replacement uses bounded chunks within the worker heap.
Printf, typed-interface/pointer values and non-UTF-8 byte propagation remain
open. Existing ceilings/native/storage/keyed bytes are unchanged; upgrade query apps.

Template js supports Go JavaScript text escaping with typed/nil joining, ASCII
controls and Unicode 17 printability. Eight original Go rows plus fourteen
successful controls and three runtime errors replay through HTTP/Parquet/S3.
All 19060 actual pinned Loki pipeline comparisons agree, including 304 runtime
errors, excluding diagnostics; grouped inputs cover all 1112064 Unicode scalars.
Exact 16 MiB output and fail-whole intermediate overflow work on log/top-k/metric
paths. Go's minimum-four-digit escapes are not JSON quoting or context-aware
sanitization. Existing budgets/native/storage/keyed bytes are unchanged; upgrade
query-serving applications.

Template with/else-with actions change dot only for successful branches, restore
outer dots afterward and preserve the stage's root label snapshot through $.
Ten original Go cases plus nested/typed/error/admission controls replay through
HTTP/Parquet/S3; all 2387 actual pinned Loki comparisons agree, including 516
runtime errors, excluding diagnostics. Six syntax/function rejections also
match actual Loki. Exact 16 MiB output and accumulated outer/branch overflow
retain fail-whole policies and the existing nesting ceiling. Generic object
iteration remains open. Existing ceilings/native/storage/keyed bytes are
unchanged; upgrade query-serving applications.

Template action and if/with-header variables support declarations, nearest
assignments, shadowing/restoration, typed values and saved-map fields. Fresh
bindings per template/destination/entry keep root reassignment local. Three
original Go rows plus 26 success/ten runtime-error and admission/budget controls
replay through HTTP/Parquet/S3. All 6198 actual pinned Loki comparisons agree,
including 1454 runtime errors, excluding diagnostics. Eight upstream admission
failures agree. Parenthesized-pipeline bindings now thread effects left to right,
with lazy skipping and forward-reference rejection: 16,622 actual pinned Loki
comparisons agree, including 3764 runtime errors, excluding diagnostics. Field
chains support saved label maps/functions/parenthesized results with distinct
empty-string/nil behavior and skipped nil-receiver arguments. Four unchanged Go
rows plus HTTP/Parquet/S3 controls replay; the combined 18,838 actual pinned Loki
comparisons agree, including 5278 runtime errors, excluding diagnostics. Generic
JSON objects/arrays are covered by the continuation below; methods and date/other
functions remain open.
Accumulated/intermediate caps and native/storage/keyed bytes are unchanged;
upgrade query-serving applications.

Template range supports sorted label maps and integer/uint8 values, scoped
single/two-variable declarations/assignments, empty/nil else and nested
break/continue. Ten unchanged Go parser rows plus 26 success/ten runtime-error
and admission/budget controls replay through HTTP/Parquet/S3. All 21,304 combined
actual pinned Loki comparisons agree, including 5388 runtime errors, excluding
diagnostics; 29 constructor failures agree. Every executed iteration charges
shared metric work or request-local stream-template work alongside regex helpers,
even without output; early break charges actual iterations, not cardinality.
Work/output exhaustion fails whole. Existing 64-depth/16 MiB caps and native/
storage/keyed bytes/dependencies are unchanged. The following JSON continuation
adds container values/iteration; channels/function iteration and date/other
functions remain open; upgrade query apps.

Template fromJson decodes typed nested objects/arrays, float64 numbers, strings,
booleans and nil. Missing/null object members retain interface-field errors,
distinct from empty string labels and nil function-result propagation. Printing,
index/field access, len/truthiness, numeric coercion and iteration preserve types;
duplicate keys replace earlier values. Both complete Sprig TestFromJson oracles
plus 29 successful/19 runtime-error controls replay through HTTP/Parquet/S3;
10,292 actual pinned Loki comparisons agree, including 3874 runtime errors,
excluding diagnostics. Numeric overflow-to-zero follows the actual pinned
Go 1.27.3 decoder, not older Go behavior. Input bytes spend shared metric or
request-local stream-template work before expansion; input/normalized bytes cap
at 16 MiB, containers at 64 levels. Growth/work/depth exhaustion fails whole,
including explicit error selection. Methods, channel/function
iteration, date/other functions, byte-string propagation and exact diagnostics
remain open. No native/storage/keyed-byte/dependency change; upgrade query apps.

Template array slice supports default/exclusive/three-bound indices, retained
decoded backing elements and explicit reduced capacity without array copying.
Ten original Go rows plus 18 success/13 error controls and two spare-capacity
rejections replay through HTTP/Parquet/S3. Of 5984 actual pinned Loki comparisons,
5980 agree, including 4304 runtime errors; four deliberately reject Go allocator
spare cells, with no unexpected differences excluding diagnostics. Initial
capacity equals decoded element count, not Go allocator capacity; spare-cell
access remains a compatibility gap. Public work/exact-16-MiB/overflow controls
cover log/top-k/metric paths despite error selection or later shrinking. Existing
ceilings/native/storage/keyed bytes/dependencies are unchanged; upgrade query apps.

Template timestamp helpers __timestamp__/unixToTime/unixEpoch/unixEpochMillis/
unixEpochNanos retain typed time and original source nanoseconds across line
rewrites, variables and scopes. Unix input units depend on byte length, with
signed-int64 admission/wrapping and floor-based epoch seconds/milliseconds.
Seven pinned Loki formatter rows and two invalid-time inputs replay through
HTTP/Parquet/S3; 28,292 comparisons against actual Loki/Sprig helpers through Go
text/template agree, including 100 runtime errors, excluding diagnostics.
Time renders in UTC deliberately, not the host-local timezone. Arbitrary date
layouts, named zones, time methods and now remain open. Existing native/storage/
keyed bytes, dependencies and ceilings are unchanged; upgrade query apps.

Template untyped nil supports false truthiness/defaults, equality, zero numeric
coercion, function-result <no value> rendering and distinct print/escape text and
spacing. Bare nil commands and wrong typed arguments fail at execution with
TemplateFormatErr; missing string labels remain empty, never nil. Three original
Go rows plus HTTP/Parquet/S3 controls replay; 7528 actual pinned Loki pipeline
comparisons agree, including 812 runtime errors, excluding diagnostics. Existing
budgets and native/storage/keyed bytes are unchanged; upgrade query-serving apps.

Template float64/minf/maxf/ceil/floor/round support coercion, runtime signed-zero/
nonfinite classes, integer precision and typed float thresholds. Three original
Loki rows replay through HTTP/Parquet/S3; 27,928 actual pinned pipeline comparisons
agree, including 30 runtime errors, excluding exact diagnostics. Go-template
gt/ge use inverted tests even for NaN. Rounding follows the pinned Go 1.27.3
fused fractional residual: round 123.5555 3 yields 123.555, unlike Sprig's static
test, not a cross-compiler/platform bit-parity claim. Nonportable float-to-int
casts still reject. Existing budgets/native/storage/keyed bytes are unchanged;
upgrade query-serving apps.

Template addf/subf/mulf/divf coerce float64 then fold shortest-decimal values,
rounding back to float64 only on function return. Division rounds every step to
16 decimal places, halfway away from zero. Two original Loki rows replay through
HTTP/Parquet/S3; all 14,012 actual pinned pipeline comparisons agree, including
1,524 runtime errors, excluding diagnostics. Exact zero loses signed zero;
nonzero results can underflow/overflow, but nonfinite operands/zero divisors
preserve inputs with TemplateFormatErr. Existing query/worker/output ceilings
and native/storage/keyed bytes are unchanged; upgrade query-serving apps.

Loki request-local query policies support `maxQueryLength`, `maxQueryInterval`,
`maxEntriesLimitPerQuery`, `requiredLabels`, `minimumLabelsNumber` and
`maxQueryTime`. Each
selector/range leaf validates before storage; entry ceilings reject requested
log limits or detected-field `line_limit`, never silently truncate results.
Required names are deduplicated, matcher minima count repeated names, and
pipeline/group labels do not satisfy selector policies. Parenthesized source
pipelines before range brackets work. Twenty-five pinned admission cases replay
through 64 HTTP requests, with separate nonempty Parquet/S3 controls. This is
optional client tightening, not operator quotas or selectivity proof. Existing
server budgets remain authoritative; positive request deadlines shorten every
finite read's worker timeout, while zero/omission preserves the server cap.
Blocked-adapter and native S3 GET controls cover timeout/admission recovery;
already-dispatched provider/native work can outlive callers. Lookback/byte/sharding fields reject.
Upgrade query applications; no further native/storage/keyed-byte change.

LogQL line filters accept case-insensitive `or` alternatives inheriting their
primary substring/regex/pattern operator; string/IP alternatives can mix under
`|=`/`!=`. Positive groups keep any match; negatives reject any match. All
branches validate before storage, including unreachable invalid alternatives;
adjacent filters and line-format ordering stay intact. Alternative groups use
the bounded Elixir pipeline without native pushdown. Frozen upstream matcher
tables (27 queries/121 line decisions) and 29 engine cases replay through
HTTP/Parquet/S3, not full LogQL/client/capacity proof. Existing limits and
native/storage/keyed bytes are unchanged; upgrade query-serving applications.

LogQL label filters accept comma/implicit conjunctions with the pinned parser's
right-attaching grammar (`a,b or c` means `a AND (b OR c)`), distinct from
explicit AND-before-OR precedence. Both keywords are case-insensitive; pipeline
boundaries, eager conjunctions, short-circuit OR and first-error ownership remain.
All 36 selected engine evaluations replay through HTTP/Parquet/S3, with separate
truth-table/admission controls. Existing evaluation paths, budgets, generic
metric-error envelopes and native/storage/keyed bytes are unchanged. Upgrade
query-serving apps; full parser errors and client/capacity acceptance remain open.

PromQL sampling supports `limitk` and `limit_ratio`, preserving names and complete
float/native-histogram samples. Ratio selection uses the pinned default upstream
full-label hash, never storage StableHash; k uses deterministic hash ordering,
not exact Mimir iteration-order subsets. Existing budgets and storage remain
unchanged; upgrade query-serving applications. See `docs/querying.md`.

PromQL duration arithmetic supports windows, explicit/default-minute subquery resolutions and
signed offsets with outer-context `step()`/`range()` and nested `min_of`/`max_of`.
Unit-bearing scalar arguments work too. Resolve once per batch source before
storage reads; preserve offset precedence and exact bare time literals. One-year
literal/result ceilings, nanosecond rounding and shared AST/work/scan/sample/
intermediate/result limits apply, not upstream millisecond truncation. No native,
storage or keyed-byte change; upgrade query-serving apps. See `docs/querying.md`.

PromQL query time literals accept hexadecimal integers and digit separators,
with exact decimal nanoseconds and unchanged one-year/int64 bounds. Omitted
subquery resolutions use one-minute epoch grids, never the outer query step.
Quoted UTF-8 grouping/matching/include labels and UTF-8 `label_replace`
destinations work; empty/duplicate/overlapping decoded names still reject.
Formatter round trips preserve identities and omitted resolutions. HTTP time
parameter grammar and native/storage/keyed formats are unchanged.

PromQL histogram quantiles warn on invalid/NaN bounds, omit same-input-identity
classic/native pairs per step, and publish real monotonic-repair info while
silently correcting tiny increases/decreases. Native quantile/fraction NaN
notices survive into query-local responses. Twelve unchanged Mimir evaluations
replay through HTTP/Parquet/S3. `histogram_quantiles(vector,"label",q0,...)`
adds per-step OpenMetrics quantile labels, overwrite/name-removal semantics and
same-step duplicate rejection; nine more unchanged Mimir evaluations replay.
Fan-out reserves shared intermediate samples before construction; bucket and
generated-label work accumulate across sources. Limits are unchanged, but
histogram work may bind earlier. Classic `histogram_fraction` now linearly
interpolates sorted/coalesced buckets without quantile monotonicity repairs;
three further unchanged Mimir evaluations and 14,400 actual pinned Go comparisons
cover it. Eight further unchanged Mimir evaluations cover corrected selected-bucket
native NaN-sum interpolation and ±Inf sums without NaN observations. This
intentionally replaces Prometheus 3.5.0's last-bucket quirk. Native counter
rate/increase also use Mimir's count-based zero-point extrapolation, scaling
the whole histogram from the original first count and reset-corrected change;
eight further unchanged evaluations and public reset/bucket/gauge controls replay.
PromQL idelta subtracts the last nonstale pair without resets/scaling/extrapolation;
the complete ten-evaluation Mimir block covers floats/native histograms, gauge/
mixed-type/schema warnings and custom bounds. Native subtraction reconciles
custom layouts to common bounds, with 4,983 actual pinned Go helper comparisons;
custom reset detection compares plain interval sums at common bounds including
+Inf, matching 9,950 actual pinned Go DetectReset comparisons. Eleven further
unchanged irate evaluations cover gauge/mixed-pair warnings and layout changes
without false resets; reset branches emit no unexecuted subtraction notices.
Four unchanged delta evaluations cover gauges/mixed windows; public controls
prove endpoint-only histogram schema/hint checks, custom reconciliation and
batch isolation. All three difference functions charge native endpoint work.
Addition now coarsens custom layouts with source-ordered compensated sums;
9,950 actual pinned FloatHistogram.Add comparisons match geometry, finite
count/sum/bucket bits and reset hints. Eleven unchanged Mimir arithmetic
evaluations cover sum/avg, sampling/set/count controls and binary +/- notices.
Binary subtraction produces gauges through nested queries; binary histogram
work shares query/batch limits. Twenty-one further unchanged range/group
aggregation evaluations cover sum/avg and their over-time forms. They report
custom reconciliation, mixed inputs, incompatible families and original
reset/no-reset conflicts, even through intervening gauge/unknown hints.
Bucket/bound work shares query/batch limits; notices stop at the first failed
family and preserve original metric names. Seven further unchanged counter
evaluations cover gauge/schema warnings, nested histogram-count and first-pair
layout resets. Rate/increase report actual subtraction/reset-addition
reconciliation under bounded constant-size notice sets; middle-schema errors
replace earlier window gauge notices without erasing other windows/sources.
Native bucket/bound work charges before resets/mapping. Twenty-six unchanged
precision evaluations cover compensated histogram sum/avg and their over-time
forms, cancellation, field overflow and singletons. Separate compensation
survives resolution, zero-threshold and custom-bound changes; averages switch
from direct sums to weighted incremental means only on field overflow. Another
55,112 actual pinned KahanAdd executions over 10,000 chains match geometry,
finite bits and nonfinite classes, separate from upstream engine replay.
Ordinary binary addition remains uncompensated. Implicit iterator reset hints
and complete annotation parity remain gaps; matching-layout/exponential reset
detection now checks every remaining previous bucket, including zero-prefixed
tails, with pinned compensated zero widening. Fifty thousand actual pinned Go
DetectReset comparisons agree; public resets/irate/rate/increase controls replay
on both adapters. Twenty-five unchanged scaling/mixed-arithmetic evaluations
replay through HTTP/Parquet/S3. Negative multiplication/division and unary
negation produce gauges, preserved by later scaling/double negation. Signed
zero/NaN factors retain hints; zero division removes ordinary buckets without
changing hints. Another 26,000 actual pinned Mul/Div comparisons agree on
geometry, finite bits, nonfinite classes and hints, separate from engine replay.
Mimir histogram trims `</` and `>/` add 124 unchanged HTTP/Parquet/S3 evaluations:
histogram-left/float-right matching, exponential/custom/biased-zero interpolation,
infinite endpoints, original totals on no-op trims and preserved names/reset hints.
Invalid matched types omit with info; bool and scalar-only trims reject before
storage. Bucket/bound work shares existing query/batch limits. Another 49,840
actual pinned Go TrimBuckets calls match geometry, hints, finite bits and
nonfinite classes with FMA disabled, matching the existing unfused math profile,
not default arm64 Go FMA bit parity. Another 37 unchanged property/dispersion and
bucket-retention evaluations cover empty histograms, sparse signed buckets,
nonfinite sums and bucket-sensitive equality/changes/distinct counts beneath
scalar wrappers. Native variance/deviation and equality now charge bucket/bound
traversal to shared query/batch work, including empty slots and false/bool
comparisons; constant-time count/sum/average remain exempt. Another 212 unchanged
evaluations cover repeated/accumulating histograms, resolution changes,
signed/zero/custom buckets, bucket-only resets, quantile/fraction interpolation
and inverses, and constant-histogram rates. Scaling/nonzero division and unary
negation now charge bucket work before traversal, including empty slots and
zero multiplication; scalar wrappers, repeated steps and shared batches cannot
evade it. Unary plus/zero division and unused custom bounds do not pay for
unexecuted scaling traversal. All seven source ramps/149 input samples match
actual pinned Go Copy/Add independently of production arithmetic. Another 25
unchanged native/mixed binary evaluations cover matching, scaling, full mixed
timestamp grids and type/layout/reconciliation notices. Binary +/- now warn on
incompatible schemas without inventing samples; successful explicit reset/no-reset
collisions retain values with warnings. Schema errors precede hint diagnostics;
unknown/gauge hints alone do not collide. Per-query/batch notice isolation remains.
The complete classic-histogram file adds 183 unchanged paired classic/native
quantile/fraction, aggregation/rate and boundary evaluations. Custom fraction
buckets crossing zero retain their bounds; first -Inf buckets contribute their
full count at finite interior bounds. Linear interpolation computes the width
fraction before population scaling to avoid intermediate overflow/underflow.
Another 24 unchanged evaluations cover paired NaN fractions, cross-schema
subtraction, anchored subqueries, full mixed-type range grids, shared-source
property/quantile queries and custom irate reconciliation versus resets.
Five more unchanged range evaluations cover custom equality/inequality,
unsupported ordering info and mixed-schema aggregation omission. Classic
quantile repair uses the upstream strict tolerance, minimum-normal absolute
floor and maximum-float-capped relative scale; large finite counts no longer
fail arithmetic and subnormals normalize correctly. Six public HTTP controls
and 10,030 actual pinned Go quantile/repair comparisons cover the extremes.
Selected Mimir engine corpus is now 1590 evaluations/85 files/5770 memory/S3 HTTP
requests. Other native arithmetic and extended repair summaries/AST positions
remain open. Upgrade query
apps; no native/storage/keyed-byte change. See `docs/querying.md`.

Early scaffolding. In place:

- Phoenix 1.8 headless app (no HTML, no assets, no Ecto)
- `Pulso.Loki` — read-only Loki HTTP client wrapping `query_range`
- `Pulso.MCP` — stateless MCP `2026-07-28` dispatcher (`server/discover`, `tools/list`, `tools/call`, native alert resource list/read/templates and `subscriptions/listen`); no `initialize` handshake, sessions, or `ping`
- `Pulso.MCP.Tools` — read-only query tools plus capability-scoped alert management/state/history tools under `Pulso.Alerting.Tools`; infrastructure remediation is never part of this registry
- `PulsoWeb.MCPController` at `POST /mcp` — Streamable HTTP transport: one message per POST (no batches), `202` for notifications, mirrored-header validation (`PulsoWeb.MCPHeaders`), `405` for GET/DELETE; `PulsoWeb.MCPRequestGate` rejects disallowed `Origin` headers (`403`) and non-JSON POST bodies (`415`) before body parsing
- `PulsoWeb.OTLPController` at `POST /v1/logs` — OTLP/HTTP JSON/protobuf logs, optionally gzip. `Log.otlp` preserves typed AnyValues, scope/schema metadata, flags, dropped counts, original times/IDs and event names; query timestamp uses event/observed time, with receive-time fallback only for unkeyed unknown-time requests. Keyed missing-time/invalid batches fail closed. Native preflight, 262,144 log request nodes, 16 MiB normalized/result budgets and pre-parser heap guard; legacy metadata is nil. Upgrade all readers/writers and drain legacy keyed retries; see `docs/logs.md`.
- `PulsoWeb.OTLPMetricsController` at `POST /v1/metrics` — JSON/protobuf/gzip gauge/cumulative-sum, cumulative explicit-histogram and summary ingestion with bounded native preflight and complete typed source envelopes. Histograms project classic cumulative bucket/count/optional-sum rows; summaries project original quantiles/count/sum. Exact uint64 counts, optional fields and histogram exemplars survive projection/storage. Delta sums/histograms are stored as reported (never accumulated; monotonic deltas get no `_total`), exponential histograms (cumulative or delta) project to classic `le` buckets on a target-scale-3/adaptively coarser grid (65,536 raw counts/side, 160 coarsened bins including at scale-10; original envelope retained). Projection keeps finite upper-tail/subnormal geometry, coalesces rounded cumulative mass at its boundary, clips partial negative zero overlaps and fails whole on conversion ceilings. Affected keyed projected bytes change without a format/native tuple change: drain retries and upgrade all ingress applications, never evade409. Classic endpoint/zero-region semantics remain approximate, and unspecified temporality is rejected; supplied/expanded sample limits and unchanged 16 MiB repeated-metadata budgets fail whole. Within-request projected-instrument/typed-owner aliases and conflicting same-time distribution envelopes reject whole; no cross-request registry. OTLP decoder capability 2 requires all serving ingress pairs upgraded; no storage-format change. Keyed rejections never publish a subset. Source time required, no delta-to-cumulative conversion. A fixed 16 MiB additive metadata ceiling is separate from the full raw-query result ceiling; ordinary scalar-only compaction retains its existing bounds. Operators must establish unique stream labels and small hard-capped batches, and upgrade all readers/writers/compactors before rich writes. Naming is not full Prometheus OTLP translation. See `docs/metrics.md`.
- `PulsoWeb.OTLPTracesController` at `POST /v1/traces` — OTLP/HTTP JSON/protobuf traces ingest, optionally gzip, with bounded native protobuf preflight; `Pulso.Record.Span` preserves IDs, timing, status, attributes, resource/scope metadata, events, links and dropped counts. New spans retain ordered original AnyValues in a typed `otlp` envelope; legacy metadata remains unknown. Full normalized bytes are proven before append, native readers/compaction validate envelopes, and assembly separates unknown legacy metadata from proven typed conflicts. Upgrade all readers/writers/compactors and drain legacy keyed retries before rich writes. `get_trace` assembles all stored spans for an exact ID natively with root/missing-parent and duplicate/conflict summaries, failing rather than truncating on budgets; it does not guarantee exporter completeness or implement Tempo APIs. `query_traces` exposes read-only exact ID/parent lookup, typed attribute search, duration/root filters and projection, with bounded native/global top-k and shared query budgets. Traces have a configurable request-node preflight (262,144 default), 16 MiB normalized segment ceiling, and pre-parser heap guard. Bounded fully anchored resource/span string regex predicates are supported. `search_traces` adds bounded resource/span original-string AND equality/regex selector syntax with optional `most_recent=true`, grouping all source-start-window spans before trace limit/order. It returns window-local aggregates, not Tempo metadata; unknown legacy types fail whole searches. All distinct window trace IDs, including nonmatches, count toward 65,536 groups (`:trace_group_limit`); native scans cap 1,000,000 rows. Experimental Tempo v1/v2 exact-ID HTTP lookup returns bounded faithful JSON/protobuf, deduplicating identical rich copies and rejecting unknown/conflicting IDs rather than guessing or truncating. Separate disabled-by-default `search_trace_metadata`/`GET /api/search` adds complete-retained-context overlap, root/duration/service metadata and sampled spansets for explicit string selectors with most_recent=true, verified against actual public Tempo v2.9.0 Parquet/TraceQL fixtures. S3 exact resource.service.name equality can omit publication-proven all-rich/core-and-column-valid sources from candidate discovery, then recover complete candidate context via conservative ID-prefix/canonical blooms without service/time restriction. Unknown/legacy proofs always read; compaction recomputes proof, never reconstructs missing types. Candidate replay uses exact admitted bytes without second GET; context validates all visible payloads and rejects new potential IDs after seal. A 2M native probe ceiling falls back to reading. Metadata/page/heap bounds can bind before the hard 8192-ref catalog maximum; actual fetched inputs keep cumulative 1024/32MiB/1M-raw-row bounds even after owners are freed. Source absence/corruption in pruned irrelevant objects is not scrubbed; read errors/budgets fail whole. Current metadata2/export3/search4/discovery12 tuple {2,1,3,4,12} requires matching app/native participants; no representative posting/capacity/client acceptance. Encoder rejects invalid core before PUT; compaction projection disagreement returns trace_projection_invalid, keeps originals, and may stall earliest flat-manifest groups pending coherent offline repair; default-off resource/span/instrumentation/event/link name and original string/int64/bool/float discovery also scans retained rows under separate 5000-entry/1MiB dictionary/result bounds and a 2,000,000 requested-owner-plus-attribute work ceiling per scan attempt charged before filtering, with distinct `trace_discovery_work_limit`; flat-scope capacity shrinks as attribute counts grow. Nested attributes never flatten into span attributes or infer instrumentation name/version, event names or link IDs as attribute keys; value filters still target resource/span strings on one parent. Discovery capability 12 is required on every serving instance before trace-duration/root/event-time/filtered-unscoped/windowed/float/stored-intrinsic discovery is used. Optional paired positive bounds (HTTP uint32 seconds, MCP int64 nanos) select whole immutable segment min-start/max-end envelopes of retention-visible parents, not point-time rows. Geometry reads actual columns, counts all retained-source header rows/provider bytes even when unselected; selected context still validates before attribute filtering and uses existing owner budgets. Expired starts cannot widen envelopes; long ends can. Two local passes share exact admitted bytes without extra GETs or a cross-window dictionary cache. Compaction/layout can broaden suggestions; unselected payload metadata is not an integrity scrub. Flat adapters reject without explicit window-aware /7 support; native/reference segments preserve boundaries. Unscoped values combine original resource/span/instrumentation only (not event/link), with summed three-owner work before filtering and no static reservations. Bounded resource/span original-string AND filters select one parent, then union its three typed owners; this is a complete extension, not Tempo's ignored-filter fast path. Matching complex targets fail, legacy/corruption fail even on nonmatches. Same-key resource/span equality, malformed/nonstring/nested/intrinsic predicates and legacy root aliases reject; regex may select strings on one owner while returned other-owner values retain their types. HTTP uses leading-dot targets. Float values return typed TraceQL-format text (signed zero, integral .0, Go exponent rules; nonfinite classes NaN.0/+Inf.0/-Inf.0) instead of 422; v1 can coalesce same-spelling float/string values. Seven stored intrinsic strings name/span:name, statusMessage/span:statusMessage, instrumentation:name/version and event:name accept bounded same-parent resource/span filters; missing protobuf strings have effective empty defaults, not inferred attrs. These data queries fail whole and never fall back to static suggestions. Native/reference field budgets charge span/scope attrs or parent+every event/attrs before filtering. Query-aware dynamic /4, unfiltered unscoped /3, filtered unscoped /4 and dedicated windowed-filtered-unscoped /6 callbacks fence old adapters. Old window /7 cannot receive this new mode; unsupported window adapters reject rather than ignore the predicate. V1 envelopes use canonical v2 target semantics, not legacy alias parity. event:timeSinceStart now yields exact signed int nanoseconds, not duration strings: event source timestamp minus its own parent's source start; nonnegative int64 source times subtract without overflow. Effective event default0 can yield negative offsets, no event invents no value. Bounded parent-string filters return complete values versus upstream's empty filtered path, not omission parity. Requested event times validate even on nonmatches and parent+all events+attrs spend work before predicates. New event-time /4 or bounded reference plus dedicated windowed-event-time /6 fence old string/window callbacks; whole parent-source envelopes, not event-time filtering, still govern windows. Actual280-parent/seven-case producer has one exact unfiltered/five complete supersets/one empty and14 public protobuf consumers, not deployed/provider/client proof. Discovery10 adds rootName/trace:rootName and rootServiceName/trace:rootService: select trace IDs from parent filters/source windows, recover all retained candidate context from exact captured binaries with no extra GETs. All retained S3 inputs must fit1024 objects/32MiB/1M actual rows regardless of window/matches, no service/ID planner; replay decodes all sources with body/header agreement, noncandidate JSON not scrubbed. 65,536 candidate groups/16MiB root bookkeeping,2M resource/span/link owners+attrs before predicates/additional context once,5000 values/independent1MiB dictionary/result bounds. Parentless without original child_of link proves root; distinct root IDs/tuples or contradictory same-ID root classification fail409, missing roots/original string service fail409 without default invention. Deliberate whole rejection makes in-flight/retention-straddling/multi-root recent/broad autocomplete often unusable; no skip-incomplete or exporter watermark, not full export/span-ID conflict proof. Dedicated root /4 or bounded sentinel reference and windowed-root /6 fence old callbacks; flat refs reject windows. Actual public44 cases have22 exact successful dictionaries and22 deliberate stricter rejections, not physical-layout/provider/capacity/deployed proof. Discovery11 adds canonical trace:duration full retained max-end/min-start, not matching-span duration. Root absence/conflicts do not block retained envelopes; in-flight spans/retention can grow/shrink them, not exporter completeness. Unfiltered int nanoseconds versus parent-filtered Go-style duration strings (µs U+00B5), both lexical/V1 corresponding strings, no floats/clamps; cache separately across filters/types. Same full-retained1024/32MiB/1M raw+replay bounds/no planner/no extra GET;65,536 groups/80B-per-summary charge within16MiB charged bookkeeping, not total native memory.2M resource/span owner+attrs before filters/additional context once, events/links not requested owners but envelopes validate.5000/independent1MiB dictionary/result bounds fail whole; high-cardinality broad queries commonly422. New dedicated duration /3 or sentinel reference plus windowed /5 fence old root/string/window callbacks. Static duration/span:duration/traceDuration empty vocabulary unchanged. Actual six public cases over280 traces/560 spans all exact,1047 Go formatter cases replayed native/reference; no physical-layout/deployed capacity claim. Omitted/empty/none HTTP name scopes or omitted/none MCP scopes combine all five namespaces in one native retained scan. V2 includes 20 static intrinsic suggestions; v1 flattens/deduplicates stored names and excludes intrinsics. Default names hint service.name whenever any visible parent exists even without an original attr or filter. Combined budgets are global: 2M owner+attribute work across all five scopes and 5000 scope/name pairs including 20 intrinsic reservations (both versions); name dictionary bytes now include scope bytes, tightening near-cap explicit queries too. Storage/legacy/corrupt/budget errors fail whole, never partial static suggestions. A distinct optional default-name callback prevents old scoped adapters dropping namespaces. Name discovery accepts the same parent-string filters but returns complete conservative hints: matching original keys plus fixed all-visible-context resource/span promotions (including Tempo's required service.name hint without an original attr). These are not matching-key proofs. Unlike upstream name iteration, earlier nonmatching keys cannot suppress matching keys; 64/75 cases are exact, 11 deliberately return independently proven extra keys. Existing string/int64/bool and explicitly scoped unfiltered-name semantics remain unchanged; adapters require separate explicit filtered-name support, never old value-only callbacks. Value discovery now accepts bounded same-span original-string equality/regex AND filters, independently exercised through actual Tempo engine/Parquet fetch; same-target equality shortcuts, malformed unfiltered fallback, hints and nonstring filters reject. Legacy/nonmatching corrupt rows and raw scan costs cannot be hidden by filters. Explicit intrinsic discovery now returns static Tempo protocol names and seven v2 keyword/empty vocabularies through authenticated/admitted MCP/HTTP, never storage; stored span-duration intrinsics, static-keyword filters and v1 static shortcuts reject. It does not prove stored presence or full TraceQL support. Native/storage formats remain unchanged. Zero/partial time bounds, exact upstream filtered-name/dynamic-string omission or dynamic dedicated-column parity, stored span-duration intrinsics or complex value parity, representative posting/index capacity acceptance/full TraceQL/direct gRPC and deployed-client parity remain unimplemented. Opt-in trace compaction merges Arrow natively with shared signal CAS publication and durable cleanup; trace retention reuses the shared event-time floor, bounded format-3 pages, staged cleanup and orphan/catch-up sweeps. Retention is based on span starts and independent of compaction; flat catalogs remain unbounded without enforcement. See `docs/traces.md`.
- Trace AnyValues now accept nonfinite double classes across all owners/nesting, storing canonical OTLP strings under doubleValue and projection strings (never BEAM nonfinite floats). Native/reference metadata, Parquet/compaction/assembly/export/discovery preserve class/type; canonical quiet NaN export does not preserve sign/payload/signaling bits. Raw query exact/regex predicates intentionally match projections incl base64/nonfinite spellings, not original type; strict original-string selectors remain typed. Service promotion requires original stringValue; prior byte-service promotion stops for new writes (old stored history unchanged, drain byte-service keyed retries because hashes change). Upgrade all readers/writers/compactors/serving pairs before new writes; current native trace tuple `{2,1,3,4,12}`. Search4 adds a dedicated bounded same-parent boolean RPN program/callback for retained-context search (original-string negation, original-int64/status/span-duration comparisons, equal-precedence left-associative AND/OR/groups); discovery/window grammar stays narrow. Expressions bypass service/ID planning, retain complete context from captured bytes, and charge20M prefilter owner/attr/instruction work units per attempt across candidate/context evaluation. Native compilation independently fences malformed programs; no stored-format change. See `docs/trace-query-expressions.md`. Scoped actual Tempo fixtures/consumers are not SDK/Collector or deployed-client acceptance. See `docs/trace-otel-compatibility.md` for remaining OTel/Tempo gaps.
- `PulsoWeb.LokiController` at `POST /loki/api/v1/push` — Loki push ingest, JSON and Snappy-compressed protobuf (decoded in Rust by `Pulso.Codec.NIF`)
- `PulsoWeb.LokiQueryController` — LogQL query/query_range (Loki sample format: numeric timestamps, Go-formatted values) plus complete stream discovery (`labels`, `label/:name/values` with `query`, `series` with `match[]`, GET/form-POST) via `Pulso.Loki.Discovery` and the native `log_discovery` accumulator (projects timestamp/service/level/resource columns as dictionaries; one verdict per distinct stream per batch; 5000-entry/1MiB fail-whole). `index/stats` is manifest-only; `index/volume{,_range}` synthesize `bytes_over_time` LogQL. `/loki/api/v1/tail` (`PulsoWeb.LokiTail`, WebSock) sends newest-`limit` history then live entries: `Pulso.Loki.Tail` registers tailers node-locally; the first starts the tenant's `Pulso.Loki.TailFollower`, which polls `Storage.follow_logs/2` (S3: fresh manifest, unseen non-compaction segments in the last hour, more than 64 unseen originals closes streams with 1013 instead of returning a partial cursor; `PULSO_LOKI_TAIL_POLL_INTERVAL_MS`) and receives this node's acknowledged appends from `Storage.append/4`, deduplicating exact detached timestamp/labels/line identities for 2 minutes under 200,000-entry/16 MiB payload bounds. This is best-effort across unclustered instances, not a complete changefeed: event-time compaction settle (default 60 s), late publication and failed polls can hide originals before observation. Native batch regex matching shares raw-query codepoint/ASCII-class semantics. Producer-side credits bound queued batches/serialized bytes (64/16 MiB; one idle-queue batch up to 32 MiB), excluding processing/frames/arena lifetime; overflow closes 1013 and small disposable local hints may drop. Lower backlog policies can still report `dropped_entries`; `PULSO_LOKI_TAIL_MAX_PER_TENANT` (default 10) caps tails per tenant per instance. `detected_labels` reuses series discovery; `detected_fields` and `detected_field/:name/values` (`Pulso.Loki.Detected`, types in `Pulso.Loki.FieldType`) sample the newest `line_limit` lines through `Evaluator.select_entries/4` (shared bounded top-k). `/api/metrics/query_range` and `/api/metrics/query` (`Pulso.Traces.Metrics`, gated with Tempo search) reduce spans natively per segment in `trace_metrics.rs` (Tempo interval alignment, typed group-by labels, log2 duration buckets/quantiles) via `Storage.trace_metrics_scan/3` (S3 `:trace_metrics` scan mode; other adapters encode their spans into segments for the same accumulator). `/loki/api/v1/patterns` (`Pulso.Loki.Patterns`) mines patterns at query time with an independent Drain implementation (`patterns.rs`, not derived from Loki's AGPL code) fed natively through the `log_metric_scan` segment path (scan resource with a miner, `log_pattern_*` NIFs, log API tuple `{1,1,3}`) or by folding evaluator entries; `|>`/`!>` pattern line filters (`Pipeline.Pattern.compile_filter/1`, `filter_match?/2`) stay in Elixir and are not pushed down. Label matchers (selectors, label filters, drop/keep) are fully anchored via `Pulso.LogQL.StreamMatchers`; line filters are not.
- LogQL evaluation streams: `Storage.fold_logs/4` (S3 decodes segments in 4096-row chunks via the native `log_segment_reader`) feeds `Evaluator.fold_log`; range aggregations reduce entries to `{ts, tie, value}` samples (unwrap failures deferred as `:error`, ties only kept for first/last) and window by binary search over canonically ordered slices. `Pulso.LogQL.NativeRange` + `log_metric.rs` run eligible pipelines (line filters, logfmt, regexp, string/regex/number label filters, plain unwrap) natively into packed `<<ts::64, tag::8, value::64>>` series kept off-heap; anything not exactly reproducible returns `:fallback`. `test/support/metric_eval_reference.ex` is the frozen materializing reference for randomized parity tests. Log queries with `limit` keep a global top-k (Loki semantics) and stop backward scans early. Budgets: `PULSO_QUERY_MAX_{SAMPLES,WORK,SCAN_ROWS,SCAN_BYTES}`; LogQL range aggregations charge kept lines to the separate `:log_samples` budget (`PULSO_QUERY_MAX_LOG_SAMPLES`, default 5M), enforced inside the native scan (`log_metric_scan_new/5` max, log API tuple `{1,1,3}`), and selected lines to work, never PromQL `:samples`; opt-in ordered segment prefetch `PULSO_QUERY_PREFETCH`.
- `PulsoWeb.OTLPGRPC` + `native/pulso_grpc` — opt-in (`PULSO_OTLP_GRPC_PORT`) tonic OTLP/gRPC listener for logs/metrics/traces. Rust owns HTTP/2, gzip/zstd, size limits, in-flight bounds; raw messages reach the BEAM as resource-backed binaries with a reply handle and run through `Pulso.OTLP.Ingest`, shared with OTLP/HTTP (`PulsoWeb.OTLPHTTP.export/3`). Dropped handles answer UNAVAILABLE; oversized messages RESOURCE_EXHAUSTED (not tonic's retryable OUT_OF_RANGE).
- Grafana compatibility: `PulsoWeb.BasicTenant` (Basic `<tenant>:<token>` names the tenant when `X-Scope-OrgID` is absent; `SharedSecret` binds Basic credentials to their username), `PulsoWeb.PathAliases` (`/otlp/v1/*`, `/api/prom/push`, `/api/v1/push` rewritten before path-specific plugs), and `PulsoWeb.CompatController` (Prometheus/Loki/Tempo buildinfo, Tempo echo, `format_query`; `metadata` serves retained remote-write metadata from a per-tenant CAS-merged object via `Pulso.MetricMetadata` + `Storage.S3.MetricMetadataStore` (published before samples append; `query_exemplars` answers empty).
- `PulsoWeb.RemoteWriteController` at `POST /api/v1/write` — Prometheus remote_write 1.0 and 2.0 ingest (Snappy-compressed protobuf, hand-decoded in Rust; the Content-Type `proto` parameter selects `decode_remote_write_limited` or `decode_remote_write_v2_limited`, which resolves labels and per-series metadata through the symbol table and trims OpenMetrics family suffixes by type as Mimir does). 2.0 responses carry written-count headers; rejected series or invalid histograms store the valid rest and answer 400, as Prometheus does. Native histograms and exemplars decode in Rust (`native_histogram.rs`, `exemplar.rs`) into optional `histogram`/`exemplars` segment columns (present only when used; histogram rows keep the stale marker as value, exemplar-only rows a reserved NaN). `Pulso.RemoteWrite.Push` merges same-label entries so exemplars attach to the nearest sample. Alloy sends metadata only over 2.0 with `honor_metadata`. Full receiver contract per the Prometheus spec; see `lib/pulso_web/controllers/remote_write_controller.ex` for the header/status-code rules.
- Shared OTLP wire handling: native allocation-free preflight followed by bounded borrowed-slice merge plans, recursive singular-message merging, repeated append and known oneof selection. Supplied fragments/losing branches/unknown occurrences consume admission; cumulative owner attribute charges survive replacement before BEAM term expansion. Exact quoted and schema-scoped unquoted decimal/exponent integer forms normalize without floats or exponent-sized bignums, matching an independent Elixir token/schema reference; invalid fractions cannot underflow to zero/receive-time fallback. Numeric tokens retain scalar/eight-byte budget costs, not cheaper strings. Zero-mantissa exponents retain double signed-zero bits. Receiver-scoped JSON rejects duplicate recognized fields, including escaped/null duplicates, even after native fallback; unknown names/opaque objects/foreign signal roots remain ignored. The independent fallback retains object pairs until validation, not first-key maps. Noncanonical leading-zero/plus strings reject; trace uint32 counts/fixed32 flags accept exact quoted integers at every owner with the updated ingress application, while enum digits must remain unquoted numeric tokens. Remaining null/presence/entity-reference forms remain open. Named enums/snake_case keys are invalid OTLP spellings, not generic ProtoJSON compatibility goals. Shared OTLP wire capability 3 is required in every serving ingress app/native pair, separate from unchanged signal storage/search tuples. See `docs/otlp-wire.md`.
- `PulsoWeb.CompressedBodyReader` — gzip-aware Plug.Parsers body reader, so JSON receivers accept compressed bodies
- `Pulso.Storage` — signal-generic behaviour (`append(signal, tenant, records, opts)` / `query(signal, tenant, opts)` with `signal :: :logs | :metrics | :traces`), with an in-memory adapter for tests and `Pulso.Storage.S3` for dev/prod. S3 objects are Apache Parquet segments (logs via `Pulso.Codec.NIF.{encode,decode}_log_segment_parquet`, metrics via `encode_metric_segment_parquet` / `decode_metric_segment_parquet`, traces via `encode_trace_segment_parquet` / `decode_trace_segment_parquet`), keyed as `tenants/<t>/v4/signal=<s>/date=<Y-m-d>/hour=<H>/<min_ts>-<max_ts>-<suffix>.parquet`, sorted by `(service, timestamp_ns)` for logs/traces and `(series_id, timestamp_ns)` for metrics, coordinated per `(tenant, signal)` through an S3-CAS manifest (`Pulso.Storage.S3.Manifest`, `Pulso.Storage.S3.ManifestOwner`, `Pulso.Storage.S3.ManifestCache`).
- `native/pulso_codec/src/stable_hash.rs` — Pulso's port of Prometheus's `labels.StableHash` (xxhash64 over `name<0xff>value<0xff>…` across labels sorted by name). Byte-exact with the Go reference; conformance-tested in-crate.
- `Pulso.PromQL` — bounded [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/) subset: float selectors/range functions, grouped aggregations, scalar/vector arithmetic, comparisons, set operators and matching, classic histogram quantiles, clamps, label replacement, ranking/sorting, rounding, absence, timestamps, deviation/regression, explicit-resolution epoch-aligned subqueries, signed offsets, unitless durations and numeric/`start()`/`end()` `@` anchors. Stale markers and IEEE non-finite values survive remote write, Parquet, and compaction. Native histograms (rate/increase/irate/delta, sum/avg, `histogram_*` functions, histogram arithmetic; `Pulso.PromQL.NativeHistogram` ports Prometheus 3.5.0 float histograms, `Pulso.PromQL.NativeRange` its range functions) and exemplars (`Pulso.Exemplars`, `/api/v1/query_exemplars`) are supported and checked against Prometheus with `mix pulso.parity`. Exposed by `query_promql` and `/api/v1/query{,_range}`.
- `discover_metrics` and GET/form-POST `/api/v1/labels`, `/api/v1/label/:name/values`, `/api/v1/series` provide complete sample-backed discovery: OR selectors, exact inclusive sample bounds, empty-valued labels projected as absent, no raw-record rewrite. Native timestamp/canonical-label projection and bounded dictionary-key reuse avoid BEAM sample expansion; hard 1024-source/128MiB/1M actual-footer-row, 5000-entry/1MiB dictionary/result and 5M-work/16MiB-probe bounds fail whole. Serving codec tuple is `{2,2,1,1,1}` (metadata/start timestamps, OTLP, postings, discovery, footer planning); discovery remains whole-object and has no external indexes. See `docs/metric-discovery.md` for POST/selector limits, TSDB differences and upgrade requirements.
- Metrics manifests carry complete, bounded metric-name and low-cardinality label-value sets for exact-match pruning, dictionary-deduplicated with independent budgets. Log manifests carry bounded nonempty promoted-service sets. Trace manifests carry versioned complete service sets, adaptive small-batch ID blooms (new up to 1 KiB, legacy up to 8 KiB), and canonical 2 KiB wide-set blooms. Managed leaf summaries combine complete prefix hashes or identical canonical geometries, capped at 2 KiB bloom / 512 encoded service bytes and 4 KiB per reference; unknown/legacy summaries scan and bloom false positives only add reads.

- Optional `PULSO_METRICS_FOOTER_PRUNING_ENABLED` / `metricsFooterPruning.enabled` (defaultfalse) probes up to64KiB plus one expanded suffix up to1MiB, skipping ordinary metric full GETs only when shared time/postings prove no surviving group. Otherwise one matching full GET; complete small suffixes reused. Strong ETags/exact ranges/If-Match and native bounded streamed EOF fail closed on protocol/version errors. Charge max(full logical source size, transferred bytes); actual footer rows before skip/expansion, independent per-source128MiB/1M rows; retries retain reserved costs. Discovery/writes/maintenance unchanged. Requires object-store `{1,1,1,1}` and metric `{2,2,1,1,1}`; footer reads add no format/keyed byte change. Not selected-column/group range reads or capacity evidence; see `docs/metric-footer-pruning.md`.

- `Pulso.Storage.S3.LogCompactor` (opt-in `PULSO_LOG_COMPACTION_ENABLED`) merges small log segments through `SignalCompactor` with the native `compact_log_segments` NIF: Arrow concat + `(service, timestamp)` lexsort with the encoder's schema/writer properties, legacy 10-column inputs gain a null `otlp` column, records never become BEAM terms, and the service summary matches `Segment.summarize_logs/2`.
- `Pulso.Storage.S3.CompactionWorker` and `MetricsCompactor` provide opt-in metrics compaction, rendezvous ownership among live eligible workers, durable manifest-based tenant discovery, and restart-safe retirement cleanup. See the metrics compaction section in `docs/architecture.md`.
- `Pulso.Storage.S3.AppendBuffer` provides opt-in bounded node-local coalescing of unkeyed appends before segment upload. Keyed requests and overflow remain direct; every acknowledgment still waits for segment PUT and manifest CAS. `PULSO_INGEST_FLUSH_INTERVAL_MS` defaults to `0` (disabled). This is not rendezvous-owned ingest forwarding or node-wide admission.

- `Pulso.Alerting` — experimental object-backed rule management, immutable revision audit history, per-rule event replay, and opt-in native Prometheus/LogQL threshold, authored mixed-source expression threshold, and Prometheus recording evaluation. Mixed conditions reuse one admitted query batch and reject label-loss notices rather than publishing partial/unlabeled alerts. Native thresholds have explicit `keep_last`/`ok` no-data policy; scheduled selectors require fresh manifest authority, with bounded-reason skip counters and no LIST reconstruction. Recordings use bounded, durable rule-head publication intents before keyed metric append, with stale-series retirement; group ordering and source closure remain unimplemented. Grafana wrappers preserve complete original definitions and remain disabled; read-only bounded expression previews require explicit tenant-local native Prometheus/LogQL source bindings and share admission/deadline/sample/work/point/scan limits across languages. Native Loki instant/range frames do not establish Grafana conversion, pipeline, lifecycle or migration parity. Native Slack outboxes and request-scoped live resource subscriptions are experimental; Grafana notification-policy parity is not implemented. See `docs/alerting.md` for current semantics and limits.

Not yet built: full Grafana alert/notification compatibility, Mimir/Tempo clients, external sidecar indexes (bloom filters, posting lists, stats; bounded native metric footer postings now exist), bounded catalogs without finite retention, full query/discovery parity, full TraceQL and Tempo search/discovery/client parity, delta-to-cumulative metric conversion, OTLP-sourced metric metadata, TraceQL `compare()`, full protocol feature breadth (see `plans/protocol-breadth.md`), remediation surface, HITL wiring, ingest forwarding and ownership. Follow-up priority: external label posting indexes and representative capacity, expanded metric compatibility, then alert evaluation.

Keyed S3 appends for every signal use conditional creation and native streaming
exact-byte verification before CAS registration. Different existing encodings,
even logically equivalent ones, fail permanently with HTTP409 `keyed_content_mismatch`;
never overwrite or infer semantic/hash equality. Object-store ABI is `{1,1,1,1}`
(retention, keyed publication, snapshot GET, async requests); every writer, including recording evaluators,
must upgrade/drain because local startup does not fence older unconditional
writers. See `docs/keyed-ingestion.md`. Codec trace `{2,1,3,5,12}` and metric
`{2,2,1,1,1}` are the current codec requirements; footer reads do not change keyed encoder bytes.
Remote-write 2.0 legacy series-created and modern per-point starts retain optional
signed-int64 `start_timestamp_ms` without synthetic zeros or start-aware rates.
Starts change content hashes only when present, never labels/time bounds; nil-start
encoder bytes stay unchanged. Retention is immediately active: pause all metric
delivery, drain old keyed retries, stop every old reader/writer/compactor, replace
the whole fleet, then resume; under-traffic rolling upgrades are unsafe. Old
ingress discarded starts and can select different derived object keys. See
`docs/metrics.md#remote-write-20-start-timestamps`.

## Design bet

Target the full feature breadth of public observability protocols, not the
subset observed in a particular application. Application inventories (including
Tuist) are compatibility examples, not Pulso's product boundary. Track generic
coverage and explicit gaps in `docs/protocol-coverage.md` and
`plans/protocol-breadth.md`.

Grafana's Loki/Mimir/Tempo (and the VictoriaMetrics stack) are already headless, API-only storage — but they're three systems coordinated externally, and existing "AI-ready" MCP servers on top are thin read-only wrappers bolted on after the fact.

Pulso's bet: build the ingest, storage-facade, alerting, and MCP-interface layer as one coherent system, on a runtime (BEAM/OTP) whose concurrency and supervision model fits this problem shape unusually well.

**v1 shape**: headless nodes with object storage as the source of truth, immutable Parquet segments, and conditional manifest writes. Query tools, protocol-compatible ingestion, and future alerting share the same storage path. `docs/architecture.md` is authoritative for this design.

## Horizontal scaling is a design invariant

Always design Pulso so operators can run as many instances as their workload needs, adding or removing them without changing correctness. Do not assume a single instance, a fixed replica count, or sticky routing.

- Any instance must be able to accept authorized ingestion and serve queries against the shared object-storage authority.
- Local buffers, caches, admission counters and process state are disposable optimizations, never the sole authority for acknowledged data, idempotency, alert state or delivery progress.
- Rendezvous ownership reduces duplicate work; correctness must survive overlapping owners, independent instances, restarts and membership changes through object-store conditional publication.
- Keep per-instance resource bounds, but do not mistake them for deployment-wide limits. Identify cross-instance coordination and aggregate provider pressure explicitly.
- Avoid central singleton bottlenecks or work that grows with every instance on the request hot path. Document membership/topology limits and address them deliberately rather than making correctness depend on a small cluster.

## Why Elixir/OTP

- **Per-process resource isolation** — each stream/tenant/query can be its own process with an independent heap and `max_heap_size` cap. Kill a runaway unit of work without taking down the node.
- **Scheduler fairness** — preemptive, reduction-based scheduling; one expensive query cannot easily starve the rest.
- **Backpressure-aware ingestion** — GenStage/Broadway for demand-driven pipelines. Process mailboxes are unbounded by default, so this must be designed for deliberately.
- **Self-healing alerting** — each alert rule as a supervised process; "let it crash and restart" maps directly onto rule-evaluation correctness.
- **Clustering without external coordinators** — membership discovery and rendezvous hashing assign expected owners; object-storage conditional writes enforce correctness during ownership changes.
- **Phoenix PubSub** — distributed pub/sub for cross-signal correlation and alert fan-out.

## Known constraints to design around

- Default distributed Erlang is a full mesh, doesn't scale cleanly past ~100–200 nodes without partitioned topologies.
- Ownership is an optimization, not a correctness guarantee. Alert fires must use conditional object creation, as described in `docs/architecture.md`; do not add leader election or consensus services.
- Binary sub-references can pin large buffers in memory — `:binary.copy/1` discipline is needed when parsing large payloads and keeping small slices.
- Distributed Erlang's cookie auth is weak by default — cluster must stay inside a VPC, or use TLS distribution.

## MCP read/write boundary (load-bearing rule)

Pulso deliberately splits its MCP surface by risk class:

| Tier | Where it lives | Examples |
|---|---|---|
| Read-only diagnosis | Pulso's MCP server (this repo) | query logs/metrics/traces, correlate signals, fetch alert context |
| Write within Pulso | Pulso's MCP server (this repo) | silence/ack alert, attach annotation |
| Infrastructure remediation | **Separate MCP server, not this repo** | restart service, roll back deploy, scale |

**Do not** add infrastructure remediation tools to `Pulso.MCP.Tools`. Their blast radius must be contained by construction, not by hoping the agent behaves. Anything destructive should be gated through the human-in-the-loop pause that the agent runtime (e.g. Google AX) provides natively.

## Repo layout

```
lib/
  pulso/
    application.ex        # OTP supervision root
    loki.ex               # Read-only Loki HTTP client
    mcp.ex                # JSON-RPC dispatcher (public MCP entry point)
    mcp/
      tools.ex            # Query and capability-scoped alert tool registry
  pulso_web/
    controllers/
      mcp_controller.ex   # POST /mcp — thin JSON-RPC transport
    endpoint.ex
    router.ex
    telemetry.ex
config/                   # Standard Phoenix config; Loki base_url lives here
charts/pulso/             # Helm chart, published with each release
docs/                     # User-facing documentation; docs/README.md is the index
```

Storage backend URLs are read from `config :pulso, Pulso.Loki, base_url: ...` and analogous config keys for future backends. Never hardcode.

## Conventions

- **HTTP client**: use `Req`. Never `HTTPoison`, `Tesla`, `:httpc`, or `Finch` directly.
- **JSON**: use `Pulso.JSON` (Rust fast path that defers to Elixir's built-in `JSON` for anything it cannot match exactly, so behavior and errors are `JSON`'s), or `JSON` itself where the native code is not loaded yet (`config/runtime.exs`); never `Jason`. Phoenix's `:json_library` is set to `Pulso.JSON` in `config/config.exs`, and a Credo rule (`Credo.Check.Warning.ForbiddenModule`) fails CI on any direct `Jason.*` reference. Jason may still appear as a transitive dep of `phoenix` or a dev dep, but no code in `lib/`, `config/`, or `test/` may call it.
- **Rust fast paths** must be semantically identical to an Elixir reference implementation: when the Rust side cannot guarantee the same result it returns `:fallback` and the Elixir code runs. Tests compare the two on randomized input and assert the Rust path actually answered. The exception is the Parquet signal-segment codecs (`Pulso.Codec.NIF.{encode,decode}_{log,metric,trace}_segment_parquet`): Parquet is not a fast path for a pre-existing Elixir behaviour, so there is no reference implementation and no Elixir fallback — a `:fallback` there is a hard error (`{:error, {:encode_failed | :decode_failed, _}}`), mirroring `Pulso.ObjectStore.NIF`, which has no Elixir S3 client to fall back to. Round-trip tests fuzz `encode → decode → encode → decode` and assert idempotency from the first decode onwards.
- **New backends** go under `Pulso.<Backend>` (e.g. `Pulso.Mimir`, `Pulso.Tempo`), with the same read-only-first shape as `Pulso.Loki`. Every read function must accept a `:base_url` override in opts.
- **MCP tools** live in `Pulso.MCP.Tools`. Each tool has an `inputSchema`, and its `call/2` clause returns `{:ok, [content_block]}` or `{:error, reason}`. Content blocks follow the MCP shape: `%{"type" => "text", "text" => "..."}`.
- **Alerting**: current native evaluation runs in bounded supervised work with rendezvous ownership and object-store conditional head publication. Full revisions are their audit records; stale candidates never become fires. Preserve the distinction between the implemented native slice and unimplemented Grafana/delivery semantics. Configuration changes currently reset native lifecycle; imported Grafana rules cannot be enabled.
- **Ingestion** (when added): pull-based via Broadway/GenStage. No unbounded process mailboxes.
- **Naming**: predicate functions end in `?`, not `is_` (see Elixir guidelines below).
- **Rust NIF distribution**: the NIF crates under `native/` (`pulso_object_store`, `pulso_codec`) ship via `rustler_precompiled`. Every `v*` tag triggers `.github/workflows/release.yml`, which builds artifacts for each crate and the target triples in its `lib/pulso/*/nif.ex` module and attaches them to the matching GitHub Release. Downstream consumers install without a Cargo toolchain. Local dev keeps compiling from source (`PULSO_NIF_FORCE_BUILD=true` is the default); unset it to opt into the precompiled path.
- **Object-store NIFs are asynchronous**: each call spawns its request on the crate's tokio runtime and returns `:ok`; the result arrives as `{ref, result}` and `Pulso.ObjectStore` waits in a plain `receive`. Never hold a BEAM scheduler (dirty or not) across provider I/O: blocking dirty I/O schedulers capped in-flight S3 requests at 10 per node and deadlocked against in-VM fake stores. ABI fence `{1,1,1,1}` (retention, keyed publication, snapshot, async).
- **Memory copies across the NIF boundary**: minimize them. GET streams the S3 body into an `OwnedBinary` sized from the response headers and released into the reply message (one copy total, no Rust-side intermediate). PUT does not copy: the Erlang binary is saved into a process-independent `OwnedEnv` and wrapped with `Bytes::from_owner`, so every clone reqwest's retry layer makes shares an owner that keeps the binary alive (a lifetime-extended slice would be unsound for exactly that reason). S3 clients are cached per config so connections are reused. Codec encoders (`pulso_codec`), including trace Parquet and native trace compaction, write output straight into an Erlang binary without a final blob copy. Arrow columns remain native. The Loki decoder decompresses once into a `NewBinary` and returns every string as a sub-binary of it, and the JSON and segment decoders return strings over 64 bytes as sub-binaries of their input, so retained decoded data pins its source buffer: `:binary.copy/1` anything kept past the request. The Parquet segment decoder allocates one Erlang binary per string column per batch (the arena) and returns every row's string field as a sub-binary of it — a batch of N rows costs one arena binary per string column, not one per row. It skips UTF-8 revalidation on Arrow's `StringBuilder` output on encode (the JSON encoder is documented to emit valid UTF-8 already), so per-row string-column encode cost is a single memcpy. Trace selection builds native top-k before term expansion and allocates a binary only per surviving row, avoiding retention of whole payload arenas; predicates inspect requested JSON fields without Erlang term allocation. Trace Parquet readers retain the input binary through `OwnedEnv` and `Bytes::from_owner`, avoiding a full-blob copy for off-heap binaries. Other signal readers still copy their input. Public S3 trace JSON queries keep top-k candidates and projection native until the final response binary; only the reference/record path expands spans into BEAM maps.

## Documentation

User-facing documentation lives in `docs/`, with [`docs/README.md`](./docs/README.md) as the index. Keep it current in the same change that alters behavior: new or renamed environment variables, endpoints, limits, chart values, storage requirements, or operational procedures must update the relevant page (and the index when a page is added).

- Write for people self-hosting Pulso: what to configure, what to expose, how to operate and upgrade it, and what is not supported yet.
- Leave out internal details that do not help an operator: module names, implementation history, review notes, and anything specific to a particular organization's deployment, rollout, or companion services. Contributor-facing design belongs in `docs/architecture.md`; rollout planning belongs in `plans/`.
- The Helm chart in `charts/pulso` ships with every release alongside the container image, at the same version. Keep `charts/pulso/values.yaml` comments, `docs/deployment.md`, and `docs/configuration.md` in sync with `config/runtime.exs`.

## Test isolation

- Tests must be `async: true` and own their state. Do not use `Application.put_env/3`, `Application.delete_env/2`, OS environment mutation, or resetting shared named processes/ETS tables to configure a test, even with an `on_exit` restore.
- Inject configuration, adapters, clocks, and process/table references through explicit options or an owned test-local dependency context. Concurrent tests must not observe or overwrite one another's configuration, caches, admission budgets, storage, metrics, or worker membership.
- Start test-owned processes with `start_supervised!/1`; propagate the test's dependencies explicitly to supervised children and tasks. Scope fixtures and external object prefixes to the test. Cleanup must affect only resources the test owns.
- If a component cannot be tested concurrently, refactor its dependency boundary rather than switching the test to `async: false`.

## Development workflow

- **No Python requirement:** Pulso's runtime, builds and automated tests must not depend on Python or probe for its availability. Use Elixir/Rust for repository tooling. Checked-in interoperability fixtures from external producers must be replayable with the native project toolchain alone.
- **Pull request titles must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)**: `type(scope): summary` or `type: summary`. Use an appropriate type (`feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `perf`, `build`, `ci`, or `style`) and a concise imperative summary. For example: `docs(alerting): plan stateless alerting and change history`. Conventional commit messages do not replace this requirement: validate the PR title itself before creating or updating a pull request.
- `mix setup` — fetch deps
- `mix compile --warnings-as-errors` — must be clean before merge
- `mix test` or `mix test test/path/to_test.exs`
- `mix precommit` — runs compile-with-warnings-as-errors, unused-deps check, formatter, tests
- The Elixir/Erlang toolchain is pinned in `mise.toml`; run `mise install` to match.

## Related resources

- Design brief covering Pulso's rationale, alerting model, and agent-integration tiers lives in the project notes (paste from the Claude Desktop session), not in-repo yet.
- Grafana Loki HTTP API — the shape `Pulso.Loki` targets.
- Model Context Protocol spec — `Pulso.MCP` targets the stateless 2026-07-28 protocol version only. Legacy initialize-based clients (2025-11-25 and earlier) receive a `400` naming the supported version.

---

The sections below are auto-managed framework rules. Preserve the markers.

<!-- usage-rules-start -->

<!-- phoenix:elixir-start -->
## Elixir guidelines

- Elixir lists **do not support index based access via the access syntax**

  **Never do this (invalid)**:

      i = 0
      mylist = ["blue", "green"]
      mylist[i]

  Instead, **always** use `Enum.at`, pattern matching, or `List` for index based list access, ie:

      i = 0
      mylist = ["blue", "green"]
      Enum.at(mylist, i)

- Elixir variables are immutable, but can be rebound, so for block expressions like `if`, `case`, `cond`, etc
  you *must* bind the result of the expression to a variable if you want to use it and you CANNOT rebind the result inside the expression, ie:

      # INVALID: we are rebinding inside the `if` and the result never gets assigned
      if connected?(socket) do
        socket = assign(socket, :val, val)
      end

      # VALID: we rebind the result of the `if` to a new variable
      socket =
        if connected?(socket) do
          assign(socket, :val, val)
        end

- **Never** nest multiple modules in the same file as it can cause cyclic dependencies and compilation errors
- **Never** use map access syntax (`changeset[:field]`) on structs as they do not implement the Access behaviour by default. For regular structs, you **must** access the fields directly, such as `my_struct.field` or use higher level APIs that are available on the struct if they exist, `Ecto.Changeset.get_field/2` for changesets
- Elixir's standard library has everything necessary for date and time manipulation. Familiarize yourself with the common `Time`, `Date`, `DateTime`, and `Calendar` interfaces by accessing their documentation as necessary. **Never** install additional dependencies unless asked or for date/time parsing (which you can use the `date_time_parser` package)
- Don't use `String.to_atom/1` on user input (memory leak risk)
- Predicate function names should not start with `is_` and should end in a question mark. Names like `is_thing` should be reserved for guards
- Elixir's builtin OTP primitives like `DynamicSupervisor` and `Registry`, require names in the child spec, such as `{DynamicSupervisor, name: MyApp.MyDynamicSup}`, then you can use `DynamicSupervisor.start_child(MyApp.MyDynamicSup, child_spec)`
- Use `Task.async_stream(collection, callback, options)` for concurrent enumeration with back-pressure. The majority of times you will want to pass `timeout: :infinity` as option

## Mix guidelines

- Read the docs and options before using tasks (by using `mix help task_name`)
- To debug test failures, run tests in a specific file with `mix test test/my_test.exs` or run all previously failed tests with `mix test --failed`
- `mix deps.clean --all` is **almost never needed**. **Avoid** using it unless you have good reason

## Test guidelines

- **Always use `start_supervised!/1`** to start processes in tests as it guarantees cleanup between tests
- **Avoid** `Process.sleep/1` and `Process.alive?/1` in tests
  - Instead of sleeping to wait for a process to finish, **always** use `Process.monitor/1` and assert on the DOWN message:

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

   - Instead of sleeping to synchronize before the next call, **always** use `_ = :sys.get_state/1` to ensure the process has handled prior messages
<!-- phoenix:elixir-end -->

<!-- phoenix:phoenix-start -->
## Phoenix guidelines

- Remember Phoenix router `scope` blocks include an optional alias which is prefixed for all routes within the scope. **Always** be mindful of this when creating routes within a scope to avoid duplicate module prefixes.

- You **never** need to create your own `alias` for route definitions! The `scope` provides the alias, ie:

      scope "/admin", AppWeb.Admin do
        pipe_through :browser

        live "/users", UserLive, :index
      end

  the UserLive route would point to the `AppWeb.Admin.UserLive` module

- `Phoenix.View` no longer is needed or included with Phoenix, don't use it
<!-- phoenix:phoenix-end -->

<!-- usage-rules-end -->
