# Bounded trace search expressions

Experimental `GET /api/search` and MCP `search_trace_metadata` support a
same-span TraceQL subset; `with (most_recent=true)` is optional because results
are always the most recent matching traces. They remain
behind `PULSO_TEMPO_SEARCH_ENABLED` / `tempoSearch.enabled`, disabled by default.
This is not full TraceQL or deployed Grafana compatibility.

```traceql
{ resource.service.name = "worker" && (status = error || span.http.response.status_code >= 500) } with (most_recent=true)
```

```traceql
{ span.http.route !~ "/(health|metrics)" && duration > 500ms } with (most_recent=true)
```

## Operators and types

| Operand | Supported comparisons | Meaning |
| --- | --- | --- |
| `resource.<key>`, `span.<key>` with a JSON string literal | `=`, `!=`, `=~`, `!~` | Original `stringValue` only, never a projected/coerced value |
| `resource.<key>`, `span.<key>` with a signed int64 literal | `=`, `!=`, `<`, `<=`, `>`, `>=` | Original `intValue`, compared exactly, including values above 2^53 |
| `status`, `span:status` | `=`, `!=` with `unset`, `ok`, `error` | Stored span status, not an attribute called `status` |
| `duration`, `span:duration` | `=`, `!=`, `<`, `<=`, `>`, `>=` with a duration literal | That span's end minus start in exact nanoseconds |

Missing and differently typed attributes **do not match even negative
predicates**. Integer-literal attribute predicates do not coerce doubles,
strings or booleans; the pinned Tempo 2.9 Parquet search also excludes a double
`503.5` from `>= 500` and `!= 503`. This is not general TraceQL numeric arithmetic.
Strings use JSON quoting, not the entire Go/TraceQL string grammar: JSON `\/`
is accepted as `/`, while raw tabs and Go `\x`/octal escapes reject. Do not assume
all string spellings accepted by Tempo are interchangeable.
Float/boolean query literals, `traceDuration` / `trace:duration` predicates,
unary negation, arithmetic, pipelines, structural/spanset operators and
unexpanded variables reject the whole query. Discovery of `trace:duration`
remains a separate API, not a supported search predicate.

Regexes use the existing bounded RE2 subset, fully anchored with dot matching
newlines. `!~` complements the regex **only for a present original string**.
Use `.*health.*` for substring matching; `/health` matches the whole value.

`&&` and `||` combine conditions on **one span**, never conditions satisfied
separately by a parent and child. As in Tempo 2.9, both have the **same precedence
and group left-to-right**. Use parentheses explicitly:

- `a || b && c` means `(a || b) && c`.
- Write `a || (b && c)` for the other grouping.

Duration literals accept nonnegative integer/fractional components using `ns`,
`us`, `µs`, `ms`, `s`, `m`, `h`, including compound forms such as `1m30s`.
Components use Go-compatible bounded fractional parsing/rounding, then truncate
to integer nanoseconds; comparisons and the summed result stay exact int64.
The sum must fit signed int64 nanoseconds. Other spellings, units,
negative durations and bare numbers reject. Span duration is **not** the full
retained trace envelope returned as search metadata.

## Context, samples and bounds

All matching spans select candidate trace IDs. Search then recovers their
complete retained context **before** trace overlap, ordering and limiting.
Root metadata, service/error counts and trace duration therefore include
nonmatching spans, other services and earlier roots. Matching is not exporter
completeness. HTTP seconds and MCP nanosecond window semantics remain as
[documented for metadata search](traces.md#experimental-tempo-search).

Samples include original typed values from individually matching predicates;
false OR branches do not invent attributes. Status samples use enum strings;
span duration is already represented by `durationNanos`, not an added attribute.
Samples are bounded examples, not all matching spans or a full trace export.

Expressions permit at most **16 predicates, 8 regexes, 16 nested groups and
16 KiB query text**, with a 576-token tokenizer ceiling covering those
predicate/group limits. Existing 256-byte keys, 4096-byte string literals and
1024-byte regex limits remain. Native execution independently validates the
program. An additional **20,000,000 work-unit ceiling per scan attempt** charges
two resource/span owners, every original resource/span attribute and every
program instruction before filtering, in both candidate and context passes.
Nonmatches and false OR branches spend work; exhaustion returns a whole-query
`422 query_resource_limit`, never truncated results.

Expression queries deliberately **do not use the exact-service/ID source
planner**, even when a branch contains service equality. They admit the entire
retained source scope under the existing **1024 objects / 32 MiB compressed
input / 1,000,000 raw rows**, 65,536 candidate IDs and independent 16 MiB
sample/result bounds. Replay uses the same captured binaries without another
provider GET. Selective predicates, smaller windows and limits do not promise
less I/O or work. Legacy uncertainty, corruption, admission and storage failures
retain complete-or-error behavior. These are per-instance safety ceilings, not
capacity or total native-memory guarantees; native work may outlive a deadline.

Original positive-string AND searches retain their existing planner. Window-local
MCP `search_traces` and tag-discovery filters retain their narrower positive
string-AND grammar; this change does **not** widen those APIs.

## Upgrade and evidence

Require matching application/native search capability **4**, current trace tuple
**`{2,1,3,4,12}`**, on every serving instance before sending new expressions.
Startup verifies only the local pair, not an old serving pool. There is no
Parquet/envelope/idempotency-format change from these operators. Existing rich
metadata upgrade and retry-draining requirements still apply independently.
Custom adapters need explicit expression-search support or the bounded complete
reference path; old string-search callbacks never receive expression programs.

Frozen synthetic inputs and expected results come from actual public **Tempo
2.9.0 Parquet blocks and TraceQL engine execution**, with ordinary offline native
and independent Elixir replay. Local authenticated HTTP ingest, S3-stub
publication, HTTP/MCP queries and the pinned Elixir clients also exercise the new
operators. See [fixture provenance](../test/fixtures/traces/search_expressions/README.md).
This is not production Alloy, real-provider, separate-node, capacity, full
TraceQL or cutover acceptance.
