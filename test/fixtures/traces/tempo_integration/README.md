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

The separate malformed-name, exact-percent, nested `/values`, missing-key, route
suffix, authentication, search-window rejection and full-response public-engine
checks are Pulso regressions, not additional upstream integration cases.

## Replay and adaptations

```sh
mix test test/pulso_web/controllers/tempo_upstream_integration_test.exs
```

Seventeen async ExUnit tests send real authenticated OTLP/HTTP JSON/protobuf requests to an
owned Bandit listener, persist native Parquet through Pulso's S3 adapter against
an owned loopback object store, restart manifest supervision, and query JSON and
protobuf responses. Default tests need no Go, Docker, remote services or network
downloads. All data and expectations are checked in.

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

## Explicit remaining gaps

Not imported: intrinsic-name filters, partial/invalid-query fallback, unscoped
filter fallback, OR discovery, and exact dedicated-column name-hint omission
from the rest of `TestTagEndpoints`. Pulso continues to reject unsupported
filters rather than ignore them. The upstream disabled double-slash case stays
excluded. Backtick identifiers and every Go string-escape spelling are not
established by these double-quoted fixtures.

Also not ported here: gRPC streaming queries, broader trace-by-ID filters/diffs, partial
search time bounds, structural/spanset queries, metrics
exemplars and expression combinations, tenant federation, overrides, receiver
variants, fault/restart/limit matrices, metrics generation, and Tempo-specific
Kafka/WAL/livestore/block orchestration and cloud-provider tests. Existing Pulso
trace/export/search/metric tests remain separate evidence, not transplanted
upstream acceptance. Loopback S3 is not provider/capacity/deployed-client proof.

## License

The copied tables and derived ExUnit test retain Tempo's **AGPL-3.0-only**
license; `LICENSE` is copied verbatim from that source revision. They are not
relicensed under Pulso's MIT license. No Tempo runtime implementation is copied.
