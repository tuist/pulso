# Querying

Pulso has no user interface. Agents query it through the Model Context Protocol,
and dashboards or scripts use its Prometheus- and Loki-compatible HTTP APIs. Every
query is scoped to one tenant and authenticated with that tenant's bearer token.
See [tenants and tokens](configuration.md#tenants-and-tokens).

## Model Context Protocol

`POST /mcp` implements the stateless
[`2026-07-28`](https://modelcontextprotocol.io/specification/2026-07-28) revision
of the protocol over Streamable HTTP. There is no handshake or session: each
request carries its protocol version and client capabilities in
`params._meta` and mirrors them in the `MCP-Protocol-Version`, `Mcp-Method`, and,
for `tools/call`, `Mcp-Name` headers. Clients that only speak older revisions
receive `400` naming the supported version.

Send the tenant's token as `Authorization: Bearer <token>`. Browser clients must
also have their origin listed in `PULSO_MCP_ALLOWED_ORIGINS`.

The query tools below are read-only and take the tenant as an argument:

| Tool | Purpose |
| --- | --- |
| `query_logs` | Return log records for a time range, optionally filtered by service, including their typed `otlp` envelope when present. |
| `query_logql` | Evaluate a LogQL expression and return the same envelope as `/loki/api/v1/query_range`. |
| `discover_metrics` | Complete retained-sample label names, values or series with OR selectors and exact time bounds. Empty-valued labels project as absent. Same contracts as the [Prometheus metadata APIs](metric-discovery.md); safety limits fail rather than truncate. |
| `query_metrics` | Return raw metric samples for a time range, filtered by label matchers. Optional exact-equality footer postings skip native row-group decoding, not whole-object GETs or source scan budgets. See [bounds and fallback](metrics.md#bounded-native-query-acceleration). |
| `get_trace` | Assemble all stored spans for one exact trace ID, with roots, missing parents and duplicate/conflict IDs. No truncation or time window; budget exhaustion is an error. See [traces](traces.md#assemble-a-stored-trace). |
| `discover_trace_tags`, `discover_trace_tag_values` | Default-off retained resource/span/instrumentation/event/link names and original string/int64/bool/float values (nonfinite classes included). Names/values accept optional original-string equality/anchored-regex AND `query`, on one parent span; nested predicates and value same-target equality reject. Filtered names include complete matching keys plus fixed all-visible-context promotion hints, not key-presence proofs; they can exceed raw upstream names. Omit name scope/use none to combine five namespaces plus 20 static intrinsic suggestions in v2; this shares one global work/pair/byte budget, not five query budgets, and any storage/metadata error fails whole. Combined names hint service.name for any visible parent even unfiltered/absent original. Scope-none values combine resource/span/instrumentation only, not events/links. Optional bounded resource/span strings select one parent then union its three original typed owners, a complete extension rather than Tempo's ignored-query fast path. Returned other-owner values need not themselves match the literal/regex. Same-key equality across resource/span and reserved root aliases reject. Requires discovery12 matching artifacts; query-aware unscoped execution or bounded reference, and separate explicit windowed-filtered-none support, never old callbacks. Floats retain type/signed zero using TraceQL text. Seven stored intrinsic strings (span name/status message, scope name/version, event names) accept parent filters under normal stored-data bounds; complete values may exceed upstream distinct-iterator omissions. event:timeSinceStart returns exact signed int nanoseconds, not duration strings: event time minus its own parent's start. Default event time0 can yield negative values; no events invent no values. Parent-string filtering is a complete extension versus upstream's empty filtered path. Every requested event time validates even on nonmatches; parent+all events/attrs spend work first. Separate event-time /4 or bounded reference plus dedicated windowed /6 fence old string/window callbacks. Requested-owner-plus-attribute work caps at 2,000,000 per scan attempt before filtering, so dense flat scopes can fail earlier. Values require scope (including explicit none); paired positive start_ts_ns/end_ts_ns select whole retention-visible source envelopes, not point-time rows; all raw source headers/provider costs still charge, no indexed-planning improvement. Compaction can broaden dictionaries; unselected payload metadata is not integrity-scrubbed. Explicit window-aware adapters or rejection, no silent flat approximation. No zero/one-sided windows or intrinsic predicates; whole legacy/type/scan/dictionary errors never truncate. Separate `intrinsic` scope returns static protocol names and seven keyword/empty v2 shortcuts without storage, not stored presence or full TraceQL support; static filters and stored span-duration intrinsic values reject. rootName/rootServiceName and trace:rootName/trace:rootService are stored root strings: matching parents select trace IDs then recover all retained context, even outside windows/predicates. Missing/ambiguous roots or missing original string root service fail409 whole, no defaults or skip-incomplete. In-flight/retention-straddling/multi-root traces can make broad/recent queries unusable. All retained root S3 input must fit1024 objects/32MiB compressed/1M rows regardless of window, with local all-source replay and no service/ID planner. 65,536 candidates/16MiB bookkeeping and2M resource/span/link owner+attr work plus standard dictionary/result bounds. Dedicated root/window support, not old span-local callbacks; proof is not full exporter/span-ID conflict or deployed-client parity. Canonical trace:duration now measures complete retained max-end/min-start without requiring roots; in-flight ingestion/retention change that envelope, not exporter completeness. Unfiltered returns int nanoseconds, supported filters return Go duration strings (µs), both lexical; keep caches separate across query/type changes. Same whole-tenant input/replay/group/dictionary bounds, with2M resource/span owner+attr work and80B-per-summary bookkeeping charges under16MiB, not a total native-memory guarantee. Broad nanosecond dictionaries commonly fail422; no samples/truncation. Dedicated duration/window support fences old callbacks. Static empty duration/span:duration/traceDuration remain unchanged. See [discovery subset](traces.md#experimental-scoped-tag-discovery). |
| `search_trace_metadata` | Disabled-by-default experimental Tempo-shaped search over complete retained context before inclusive trace overlap, ordering, limit and sampled spansets. Requires explicit `most_recent=true`; ALL retained tenant IDs/rows consume bounds even outside the window. See [opt-in and planning costs](traces.md#experimental-tempo-search). |
| `search_traces` | Search distinct traces using bounded original-string resource/span AND selectors in an explicit span-start window. Groups before trace limits; unknown legacy types and all-window group/scan exhaustion fail. Native window-local aggregates, not a Tempo envelope. See [search limits and legacy history](traces.md#search-distinct-traces). |
| `query_traces` | Lookup spans by trace/span/parent ID or search by start time, service, name, kind, status, duration, roots and query-projection attributes, with optional output projection. See [traces](traces.md#query-spans). |
| `query_promql` | Evaluate a Prometheus Query Language expression, instant or range. |
| `query_errors` | Read Sentry error events and scrubbed payloads in a required time window. |
| `list_error_issues` | Group Sentry errors with exact window-scoped counts and first/last seen; see [error tracking](error-tracking.md). |
| `list_error_changes` | Poll the separately enabled experimental index for retained-history issue changes and explicit resets; save the cursor durably. |
| `list_indexed_error_issues` | Read compact deduplicated retained-history summaries and example references from that index; see [error monitoring](error-monitoring.md). |

`tools/list` returns each tool's full input schema. Invalid arguments, such as a
reversed time range, return an explicit error instead of an empty result.

## Tempo trace lookup

`GET /api/traces/<hex-trace-id>` returns an experimental Tempo v1 trace envelope
(JSON `batches` or `application/protobuf`). `/api/v2/traces/<hex-trace-id>` returns
the v2 `TraceByIDResponse` wrapper. Both read all visible stored spans,
not a caller-truncated window. Typed metadata is required; legacy copies and
same-ID disagreements fail rather than guess values or rewrite IDs. See
[the full lookup contract and limitations](traces.md#tempo-v1-trace-lookup).
Search/discovery APIs and verified deployed Grafana datasource parity remain separate.

## TraceQL metrics

`GET /api/metrics/query_range` and `GET /api/metrics/query` evaluate TraceQL
metrics as Tempo does, for Grafana's Traces Drilldown and Tempo metrics panels.
They are disabled unless Tempo search is enabled (`PULSO_TEMPO_SEARCH_ENABLED`).
Queries take `q` (or `query`), `start`, `end` (Unix seconds, nanoseconds or
RFC3339; by default the hour before `end`, or `since`) and, for ranges, `step`
(seconds or a duration; by default about 240 points).

Supported: a selector of `{}`, `{true}` or the
[bounded expression grammar](trace-query-expressions.md), optionally combined
with `nestedSetParent<0` for root spans, piped into `rate()`,
`count_over_time()`, `min_over_time`, `max_over_time`, `sum_over_time`,
`avg_over_time`, `quantile_over_time(<attr>, <q>...)` or
`histogram_over_time(<attr>)` over `duration`, `span.<key>`, `resource.<key>` or
`.<key>`, with `by (...)` over up to five attributes or `name`, `status` and
`kind`. As in Tempo, ranges are aligned to the step with one extra leading
interval, spans count by start time, durations aggregate in seconds, and
quantiles interpolate between power-of-two duration buckets. `with (...)` hints
are accepted and ignored, so sampled queries return exact results. `| topk(n)` and
`| bottomk(n)` keep, at each interval, the `n` series with the largest or
smallest values, as Tempo does (ties go to the series first in label order).
`compare()` is not supported.

Spans are reduced natively per stored segment without being decoded into
queryable records; the scan budgets of [query budgets](configuration.md#query-budgets)
apply, and results are capped at 10,000 series, 2,000,000 points and 64 MiB of response JSON. Windows must fit nonnegative signed 64-bit nanoseconds. These are independent work/result ceilings, not total native or process-memory bounds.

Original attribute grouping/values and selector programs fail the whole query
when in-window source rows have unknown legacy metadata; legacy attributes are
never invented as missing labels or silently excluded from averages. Unfiltered
core-only operations, such as counts grouped by span name, can still read those
rows.

## Prometheus API

`GET|POST /api/v1/query` and `/api/v1/query_range` accept the standard Prometheus
parameters (`query`, `time`, or `start`/`end`/`step`) with the tenant in the
`X-Scope-OrgID` header. Timestamps accept Unix seconds or date-time strings with
a timezone; steps accept seconds or durations such as `15s`. Client `timeout`
values are capped at ten seconds. Results use Prometheus scalar, vector, and matrix
formats, so Grafana can use Pulso as a Prometheus data source within the
supported subset below.

Pulso implements a subset of the
[Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/):

- Selectors with label matchers and positive `offset`
- `rate`, `increase`, `irate`, and `delta`
- `sum_over_time`, `avg_over_time`, `min_over_time`, `max_over_time`, and
  `count_over_time`, `stddev_over_time`, `stdvar_over_time`, `absent_over_time`,
  and `predict_linear(range, horizon_seconds)`
- Nested `sum`, `avg`, `min`, `max`, `count`, and `group` aggregations with
  `by` and `without` grouping; `topk` and `bottomk` reject NaN and ranking
  parameters at or above the signed int64 maximum; values below one yield no results
- Scalar literals, unary signs, arithmetic (`+`, `-`, `*`, `/`, `%`, `^`),
  comparisons with optional `bool`, and `and`, `or`, and `unless`
- Vector matching with `on`, `ignoring`, `group_left`, and `group_right`;
  ambiguous matches and duplicate output series return explicit errors
- Classic `histogram_quantile`, `clamp_min`, `clamp_max`, `label_replace`,
  `vector`, `scalar`, `time`, `round`, `abs`, `floor`, `sgn`, `timestamp`, `absent`,
  `sort`, and `sort_desc`
- Explicit-resolution subqueries such as `sum_over_time((max by(job)(gauge))[6h:5m])`;
  inner grids align independently to epoch multiples. Positive subquery offsets
  are supported. Omitted resolutions (`[6h:]`) and top-level range-valued results
  remain unsupported
- Stale markers: instant selectors stop at the latest stale marker until a new
  sample arrives; range functions ignore stale markers
- IEEE-754 NaN and infinities, returned as `"NaN"`, `"+Inf"`, or `"-Inf"` in
  Prometheus result values

```text
sum by (job) (rate(http_requests_total{job="api"}[5m]))
avg without (instance) (process_resident_memory_bytes)
```

```text
histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[5m])))
sum(rate(http_requests_total{status=~"5.."}[5m])) / clamp_min(sum(rate(http_requests_total[5m])), 0.001)
```

Classic histograms use ordinary float bucket series with a `le` label and a
`+Inf` bucket. Bounds accept Go floating-point spelling (including `.5`,
hexadecimal floats, and case-insensitive infinity); malformed bounds are ignored,
like Prometheus. Use numeric ordered boundaries and a positive-infinity final
bucket: NaN boundaries do not define a meaningful classic histogram. Histograms
with different metric names remain distinct; use explicit aggregation to combine
them before computing quantiles. Sums and averages use compensated summation to limit cancellation.

### Native histograms

Native histograms arrive over Remote Write 1.0 or 2.0 (Prometheus
`send_native_histograms: true`). Pulso follows Prometheus 3.5.0's semantics,
checked against it on the same data:

- Selectors return histograms, reported under `histogram` (instant vectors) or
  `histograms` (ranges) with Prometheus' bucket boundary rules
- `rate`, `increase`, `irate` and `delta` handle counter resets, schema changes
  and zero-threshold changes; `sum_over_time` and `avg_over_time` combine
  histograms; `count_over_time` counts them
- `sum` and `avg` combine histograms, `count` and `group` count them
- `histogram * scalar`, `scalar * histogram`, `histogram / scalar`, and `+`, `-`,
  `==`, `!=` between histograms
- `histogram_quantile` (native and classic series in one vector),
  `histogram_fraction`, `histogram_count`, `histogram_sum`, `histogram_avg`,
  `histogram_stddev` and `histogram_stdvar`

Functions, aggregations and comparisons that only read floats (`min`, `max`,
`topk`, `abs`, `min_over_time`, `>`, and so on) leave histograms out, and
windows or groups mixing floats and histograms have no result, as in
Prometheus. Custom-bucket histograms (schema -53) are supported; mixing them
with exponential ones has no result.

Not supported yet: omitted-resolution subqueries, negative offsets, `@`, and
functions outside the lists above. Regular expressions use a
bounded RE2-compatible subset: unsupported constructs return errors, including
lookarounds, backreferences, quoted literals (`\Q...\E`), and some character-class
and escape forms. Full Prometheus language compatibility is not claimed.
Unsupported expressions and parameters return an error. `timestamp(selector)`
returns the original sample time; `timestamp(derived_expression)` returns its
evaluation time. Millisecond-aligned timestamps match Prometheus conversion;
Pulso retains sub-millisecond precision for other inputs. Metric-name removal
merges disjoint lifetimes in range results, but simultaneous label collisions
remain errors.

For ambiguous mixed matchers on one label, `absent`/`absent_over_time` label
inference follows the local Prometheus TSDB's intersecting-before-subtracting
matcher order. Backends that do not mutate matcher order can differ. Prefer
one exact matcher per label for portable absent-instance identity; verify any
mixed-matcher rule against its former backend before migration. The inventoried
Tuist absence expressions do not use that ambiguous form.

Raw `query_metrics` results preserve storage markers as JSON strings:
`"stale"`, `"nan"`, `"infinity"`, and `"negative_infinity"`. These are distinct from
the Prometheus evaluator's string-valued result numbers.

### Datasource metadata

For Grafana's Prometheus datasource, `/api/v1/status/buildinfo` reports the
Prometheus API version Pulso follows and `/api/v1/format_query` validates a
query, returning it unchanged (there is no pretty-printer).

`/api/v1/query_exemplars` returns the exemplars of every series the query's
selectors select, with timestamps in `[start, end]` (`end` defaults to now,
`start` to an hour before it), as Prometheus does. Exemplars come from Remote
Write (`send_exemplars: true`) and OTLP metrics, where each point exemplar
becomes a Prometheus exemplar with `trace_id`, `span_id` and its filtered
attributes, on the `_bucket` series whose bound holds its value for
histograms. Exemplars are stored with their series' samples and expire with
them. As in Prometheus, label sets over 128 characters are dropped.

`/api/v1/metadata` returns the type, help and unit that remote-write senders
report for each metric family (Remote Write 1.0 metadata messages, or the
per-series metadata of Remote Write 2.0; Alloy sends it only over 2.0, see
[deployment](deployment.md)), with Prometheus's `metric`, `limit` and
`limit_per_metric` parameters. Every distinct variant a metric is reported with
is kept, most recently reported first. Variants unreported for 30 days expire;
each tenant keeps at most 8 variants per metric, 50,000 metrics and 8 MiB of
metadata, dropping the least recently reported first. Metadata is stored before
the request that carried it is acknowledged, so a failure returns HTTP 503 and
the sender retries without duplicating samples. Every instance serves the
latest stored metadata. OpenTelemetry metric descriptions and units are not
yet reported here. Tempo's `/api/echo` and `/api/status/buildinfo` answer Grafana's
Tempo health checks.

The 8 MiB metadata cap is checked on object-store reads as well as updates.
An oversized historical metadata object fails closed; Pulso does not download,
truncate or delete it automatically. Metadata queries and subsequent remote-write
metadata updates can remain unavailable until an operator arranges a coherent,
versioned backup/repair in a separate maintenance procedure. Preserve the original
object and version evidence; there is no automatic repair command.

## Loki API

`/loki/api/v1/query` and `/loki/api/v1/query_range` evaluate LogQL. They take
the tenant in the `X-Scope-OrgID` header, like Loki, or the username of HTTP
Basic credentials when the header is absent.

Metric results use Loki's sample format: `[<Unix seconds as a JSON number>,
"<value>"]`, with values formatted as Prometheus and Loki do (`"8"`, not
`"8.0"`). Log results keep nanosecond timestamp strings.

Pattern line filters (`|> "<_> level=error <_>"` and `!> "..."`) match lines
against a pattern in which `<_>` stands for any non-empty text, as in Loki: the
line must start and end where the pattern does, unless the pattern starts or
ends with `<_>`. Named placeholders and consecutive `<_><_>` are rejected.

Label matchers match the **whole** label value, as in Loki and Prometheus:
`{app=~"api"}` selects `api` but not `api-gateway`; write `{app=~"api.*"}` for a
prefix. This applies to stream selectors, pipeline label filters
(`| pod=~"..."`) and `drop`/`keep` matchers. Line filters (`|~`) and the
`regexp` parser remain unanchored searches.

### Parser labels and quantities

When a parsed key collides with an original resource or promoted name
(`service`, `service_name`, `level`, `detected_level`), `json`, `logfmt`,
`regexp`, `pattern` and `unpack` write `<name>_extracted` instead. They append
exactly one suffix, not a search for an unused name: an existing suffixed value
can be replaced. Dropping or renaming a label does not erase its original-name
provenance, and repeated parsing updates the same extracted key. Structured
per-record attributes without a resource/promoted name remain overridable.
For example, parsing `level=error` from a line whose promoted `level` is `info`
produces `level_extracted="error"`; filter that name to inspect the parsed value.

Duration/byte filters and `unwrap duration(...)`, `duration_seconds(...)` and
`bytes(...)` accept decimal fractions without binary-float scaling. Quantities
use checked signed-int64 nanoseconds or integral bytes, truncating each component
toward zero; query literals use the same exact arithmetic and checked sums.
Runtime durations accept `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`, and Pulso's `d`
extension, compound components, a single leading sign, and zero. `w`/`y` remain
query-literal units only, not producer-value units. Byte values accept unitless
numbers, `B`, decimal `kB`/`KB` through `PB`, and binary `KiB` through `PiB`, with
the existing signed-value policy. This is not full Go/humanize parser parity:
for example, exact `0.3333333333333333333h` truncates to `1199999999999ns`, whereas
Go 1.25.10 rounds to `1200000000000ns`. Overflowing literals reject the query;
invalid unwrap values fail an evaluated window, never contribute a partial sum.

Upgrade every serving app/native pair together before relying on these parser
collision and quantity semantics. They do not change stored records or keyed
encoder bytes.

### Limits and metric evaluation

`limit` keeps the newest lines (`direction=backward`, the default) or the oldest
lines (`direction=forward`) across all matching streams, as Loki does. Only
those lines are held while the query reads storage, and a backward query stops
reading as soon as no older segment can contain a newer line.

Metric queries such as `rate`, `count_over_time`, `bytes_over_time`,
`quantile_over_time` or `sum_over_time(... | unwrap ...)` reduce each selected
line to one sample of its series while reading, so their memory follows the
number of series and samples rather than the log volume. When the pipeline only
uses line filters, `logfmt`, `regexp`, string, regex or number label filters
and a plain `unwrap`, it runs natively without creating per-line Erlang terms;
other pipelines, and lines whose labels or bodies are not plain strings, use the
equivalent Elixir pipeline with the same results. Every kept line counts
against `PULSO_QUERY_MAX_LOG_SAMPLES` and every selected line as work (see
[query budgets](configuration.md#query-budgets)); log queries do not use
PromQL's `PULSO_QUERY_MAX_SAMPLES`.

### Stream discovery

`/loki/api/v1/labels`, `/loki/api/v1/label/<name>/values` (both with an
optional `query` selector) and `/loki/api/v1/series` (one or more `match[]`
selectors) accept GET or form-encoded POST. Stream labels are the labels a
stream selector can match: the stream labels of pushed logs, the resource
attributes of OTLP logs, and `service_name` for records with a service.
Empty values count as absent. Without `start`/`end`, discovery covers the last
six hours, as in Loki.

Discovery reads only the timestamp, service, level and stream-label columns
of the stored segments, natively, and evaluates each distinct stream once per
batch rather than once per log line. Results are complete or fail: more than
5,000 entries or 1 MiB returns HTTP 422 instead of a truncated list, and the
segment, byte and row budgets below apply. Narrow the time range or add a
selector when a broad request fails.

### Index statistics and volume

`/loki/api/v1/index/stats` estimates a query's size from storage metadata
alone, which Grafana shows before running a query. It counts whole stored
segments that overlap the range and can match an exact service selector, so it
is an upper bound; `streams` is always `0` because Pulso does not index
streams.

`/loki/api/v1/index/volume` and `/loki/api/v1/index/volume_range` return log
bytes per group, largest first. Both accept GET and form POST:

- `aggregateBy=series` (the default) groups by `targetLabels` (comma-separated)
  when given, otherwise by the selector's label names. `limit` (default 100,
  maximum 5,000) bounds the instant result. This uses the same storage reads
  as an equivalent `bytes_over_time` query.
- `aggregateBy=labels` counts each selected line's bytes once for every stream
  **label name**, across all values, returning metrics such as `{"pod":""}`.
  Omitted `targetLabels` includes all names on matching streams, not only names
  in the selector. Explicit targets restrict names; each target not already
  constrained by the selector also requires a nonempty stream value. Empty
  values, structured metadata, parser-created labels and Loki's internal
  `__loki_tenant__` name are not counted. This mode accepts selectors only,
  not pipelines, and scans stored lines once instead of using chunk estimates.

Label-name volumes count actual UTF-8 line bytes within the inclusive
nanosecond `[start,end]` window, not Loki's estimated chunk KB or whole-second
storage windows. In `volume_range`, `step` accepts seconds or a duration, at
least 1 ms, defaulting to the window divided by 100 (whole seconds, at least
one second). At most 11,000 epoch-aligned buckets are allowed. Internal
boundaries belong to the next bucket, and the final `end` remains inclusive;
nonfinal sample timestamps are one millisecond before the next boundary,
and the final timestamp is `end`. `limit` applies independently per bucket,
so a matrix may contain more than `limit` names. Missing buckets are omitted,
not zero-filled; range responses always use a matrix. Ties sort by name;
series sort by their first sample's bytes, then name.

Label-name queries fail whole on scan, selected-line, work, intermediate or
result-point limits. The complete dictionary is capped at 5,000 names and
1 MiB of name bytes **before** applying `limit`; reducing `limit` does not
bypass admission. These queries read Parquet lines in bounded batches and
can cost more than Loki's index-backed volume estimates. No index or storage
format change is required.

`/loki/api/v1/status/buildinfo` reports the Loki API version Pulso follows.

### Patterns

`/loki/api/v1/patterns` returns the log patterns of the lines a log `query`
selects between `start` and `end` (default: the last hour), each with its line
count per `step` (default: the window over 250, at least one second; at most
11,000 steps), most frequent first, as Logs Drilldown expects. Variable parts
of a pattern read `<_>`, and every pattern is a valid `|>` line filter that
selects the lines it counts.

Loki mines patterns continuously at ingestion; Pulso mines them when queried,
from every selected line in the window, so a pattern request reads what the
equivalent log query reads and the same query budgets apply. Mining runs
natively over stored segments when the query is a selector with optional
`|=`, `!=`, `|~` or `!~` filters. Patterns are a heuristic summary: Pulso
groups lines with its own Drain-based miner and returns at most 500 patterns,
which need not match the patterns Loki would report for the same logs.

### Detected labels and fields

Grafana's Loki query builder and Logs Drilldown use these for suggestions.

`/loki/api/v1/detected_labels` returns the stream labels an optional `query`
selector matches, with their number of distinct values, from the same complete
discovery as `series`. Like Loki, it omits labels whose values are all numbers
or UUIDs, except `cluster`, `namespace`, `instance` and `pod`.

`/loki/api/v1/detected_fields` and
`/loki/api/v1/detected_field/<name>/values` read the newest `line_limit`
lines (default 100, at most 5000) a log `query` selects between `start` and
`end` (default: `since`, one hour, before now). Fields are each line's
structured metadata (log attributes), labels the query's own parsers extract,
and the line parsed as JSON (nested objects joined with `_`, reported with their
JSON path) or otherwise as logfmt. Types are Loki's: `int`, `float`,
`boolean`, `duration`, `bytes` or `string`. `limit` (default 1000, at most
5000) caps the number of fields or values. Cardinality is the exact count of
distinct values in the sampled lines, where Loki estimates it.

### Live tail

`/loki/api/v1/tail` streams logs over a WebSocket, as Loki does and as
Grafana uses for live mode in Explore. The query must be a log query. The
socket first receives the newest `limit` entries (default 100, at most 5000)
since `start` (default: one hour ago), oldest first, then newly observed matching
entries on a best-effort live stream, in Loki's `{"streams": [...], "dropped_entries": ...}`
frames. `delay_for` (0 to 5 seconds) is accepted and ignored: entries are never
held back.

A tail sees pushes to every instance, whether or not instances are connected
to each other. Pushes to the instance serving the tail arrive as soon as they
are acknowledged; pushes to other instances arrive once the serving instance
next reads the tenant's log manifest, every `PULSO_LOKI_TAIL_POLL_INTERVAL_MS`
(default 1000) while the tenant has tails open on it. Each poll is one
conditional GET of the manifest plus one GET per new log segment. Deduplication
compares exact timestamps, labels and lines, not short hashes. Each tenant's
follower retains identities for up to 120 seconds, capped at 200,000 entries and
16 MiB of identity payload (line/label bytes and eight timestamp bytes per entry);
the oldest are evicted as entries arrive. Standalone binaries are reused; retained
slices are detached so small identities cannot pin large request/column buffers.
These are cache bounds, not total process-memory limits. An entry repeated after its
identity expires or is evicted can be delivered again.

Queue admission happens before sending record batches, with at most 64 queued
batches and 16 MiB of serialized input credit per recipient. One batch up to
32 MiB can occupy an otherwise empty queue, accommodating term overhead on a
large admitted ingest request; no second batch can join it above ordinary credit.
These credits exclude currently processing work, encoded frames and referenced
request/column arenas, so they are not a bound on total retained memory. A slow
or oversized stream closes explicitly with WebSocket code **1013**; reconnect
and query history. Follower termination or restart also closes affected sockets
with 1013 instead of silently starting a new live cursor. Reconnecting reads a
new history window; overlap can repeat entries and does not prove gap-free delivery.
If local tail supervision is unavailable before upgrade, the request returns
HTTP **503** and can be retried. A lower configured backlog threshold can still report up to
100 dropped entries per frame, unfiltered.

Live tail follows segments whose newest event is at most an hour old. A poll
with more than 64 unseen original segments closes the stream with 1013 instead
of publishing a partial cursor. Local low-latency hints can be dropped under
pressure; S3 polling is the fallback, not a durable delivery queue. Compaction
settles by **event time**, not publication time, and compacted replacements are
not replayed. A late publication, polling failure or source retired between
snapshots can therefore be missed, even without a detected backlog. This is
not a complete changefeed or an exporter-delivery guarantee; query storage for
durable history and size compaction settle/poll intervals for the workload.
Entries pushed while no tail is open are not replayed. Each instance accepts
`PULSO_LOKI_TAIL_MAX_PER_TENANT` concurrent tails per tenant (default 10);
more return HTTP 429. Admission is atomic across simultaneous connections on
that instance, including connections still preparing history. Slots are released
when the connection leaves or its process terminates. This is not a deployment-wide
limit; adding instances increases aggregate tail and object-store polling pressure.

Log queries, live tails and native log-metric scans use the same bounded
Go/RE2-subset regex policy: ASCII Perl classes/word boundaries, Unicode
codepoint `.` and strict end-of-input `$` (unless multiline mode is explicit).
Unsupported syntax rejects rather than switching dialects. Remaining pipeline
line/label predicates, drop/keep and named `regexp` capture extraction use the
same non-backtracking native engine as pushdown, rather than treating a PCRE
execution-limit failure as a nonmatch or empty capture set. Upgrade every
serving app/native pair before relying on this parity.

### Time parameters

Instant queries honor `time` (default: now); an explicitly supplied `end` is a
compatibility fallback when `time` is absent. Range queries default to an end of
now and a start one hour before that end. Supply explicit bounds for older data.
`time`, `start` and `end` accept integer Unix timestamps or RFC3339 strings,
including nanosecond fractions. As in Loki, integer strings of at most ten
characters mean Unix seconds; longer integer strings mean Unix nanoseconds.
The converted value must fit signed int64 nanoseconds. Use RFC3339 to identify
very early epoch nanoseconds unambiguously. Timestamp text caps at 128 bytes.
Malformed, overflowing or reversed supplied bounds return HTTP 400 instead of
being dropped into an unbounded scan. Decimal Unix seconds are not supported by
this adapter.

Raw MCP logs expose complete typed OTLP metadata; Loki responses remain
line-oriented projections. See [stored log content](logs.md#stored-content-and-raw-queries).

LogQL is a subset, not a complete Loki replacement. Metric queries support
suffix `by (...)` and `without (...)` grouping for unwrapped averages, extrema,
variance/deviation, quantiles and first/last observations. Quantiles interpolate
between observations after grouping across matching streams. The unwrapped
label is removed from series identity. Empty ordinary windows are missing
samples, not synthetic zeros; `absent_over_time` returns one only for an empty
window. Set operators preserve sample values and unmatched right-hand union
members. Empty-label aggregations remain vectors, and ambiguous arithmetic
matching returns an error rather than overwriting a series.

Expressions cap at 16,384 UTF-8 bytes, 256 AST nodes and 64 AST levels. A
quote-aware pre-parser guard also bounds parenthesis nesting at 64 and operator
characters at 512. Log-based metric queries share selected-sample, work and
cumulative point ceilings with the Prometheus evaluator: by default 100,000,
five million and 100,000 respectively. Repeated window visits consume work;
wide windows or many steps can fail even below the output ceiling. Object scan
budgets still apply separately to each selector, not to the deployment as a
whole. Budget failure returns no partial answer.

`rate_counter`, `@` modifiers, zero-duration windows, unsupported range grouping
and pipeline stages after `unwrap` are rejected explicitly. Invalid unwrapped
values in an evaluated window fail the whole query, including invalid middle
samples in `first_over_time`/`last_over_time`; they are never silently discarded
from an average or quantile. Invalid unwrap samples fetched only between sparse
evaluation windows do not invalidate windows they do not belong to. Nonempty `__error__` labels surviving pipeline filtering also fail
metric queries before grouping can discard them. Parser error-label generation
is not fully implemented; this guard is not complete Loki error parity.
Post-unwrap error-label filtering is not implemented. These checks
replace earlier incorrect interpretations of counters/modifiers and silent
subset calculations. Other pipeline translation and complete Loki numerical,
error-label and client-wire compatibility remain unproven. Average/deviation
folds currently use the Prometheus numerical helpers and can differ from Loki's
own incremental reducers by a few ULPs; extrema can retain a different zero sign
on equal-valued ties. Pure scalar expressions currently render as vector/matrix
results rather than Loki's scalar envelope. Matcher and pipeline-stage nodes
count toward the AST ceiling, so long valid pipelines can fail closed. Disabled
Grafana graph previews accept explicit same-tenant native Prometheus instant and
metric-producing LogQL instant/range bindings, with shared admission/deadline,
counters and conservative scan partitioning. Sparse native frames do not prove
Grafana gap filling or source conversion. See [graph previews](alerting.md#preview-an-imported-expression-graph).

## Limits

Raw `query_logs` and `query_metrics` result text each have a fixed 16 MiB ceiling, independent of row,
scan, deadline and heap budgets. Exhaustion returns `query_result_limit`, not
clipped bodies/metadata or a partial answer. Narrow the range or reduce `limit`.
The enclosing MCP transport envelope has additional overhead.

Each node runs a bounded number of queries at once, at most two per tenant, and
bounds each query's duration, memory, scanned objects, and result size. A query
that exceeds a budget fails with an explicit error rather than returning a
partial result; narrow the time range or the selector and retry. Samples that
conflict at the same timestamp resolve deterministically and add a warning to
the response. A stale marker wins a conflict with a finite sample at the same
timestamp; otherwise the maximum value wins, with ordinary NaN ordered last.
Out-of-order samples are sorted before evaluation. This is deterministic query
resolution, not ingest-time duplicate rejection.

Selected-sample and evaluation-work budgets cover all selectors and operations
in one expression. Binary/function intermediate output points also share a
bounded budget. Subqueries additionally share an independent materialized-sample
budget (100,000 by default); exhaustion returns `query_intermediate_limit`, not
an output-size error. Every outer query and inner subquery grid caps at 11,000
steps, independently of its series count. Nested subqueries share the sample,
work, materialized, deadline and heap ceilings. These can reject a supported
expression at a wide window or high cardinality; they never clip its answer.
Expressions have at most 256 nodes and 64 levels of AST depth.
Object scan budgets still apply independently to each selector; indexed scans
and aggregate object-byte accounting remain follow-up work.
