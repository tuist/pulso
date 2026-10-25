# Selected Grafana Mimir service integration scenarios

Revision: **`dd039cdd4ba466adb5d28280bf1b8a374a98432e`**.

The executable fixtures are embedded in
[`mimir_service_conformance_test.exs`](../../../../pulso_web/controllers/mimir_service_conformance_test.exs).
They are ports of the public request/response portions of actual Mimir service
integration tests, not invented cases labelled upstream. This is **33 selected
scenario replays**, plus 19 separately named Pulso regression/control tests,
not the complete Mimir integration suite. The existing math corpus in the parent
directory is separate and remains unchanged.

## Sources and exact selection

All links below are pinned to the revision above.

| Upstream entry point and source | Selected scenarios | Replays |
| --- | --- | ---: |
| [`TestDistributor` → `testDistributorWithCachingUnmarshalData`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/distributor_test.go#L41-L425) | `simple counter`, `simple gauge`, `simple histogram`, `simple summary`, each with RW1 and RW2 | 8 |
| [Same distributor table, native histograms](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/distributor_test.go#L420-L605) | `simple native histogram` with RW1 and RW2; `simple nhcb histogram` with RW2 (upstream has no RW1 input) | 3 |
| [Same distributor table, exemplars](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/distributor_test.go#L803-L847) | `series with exemplars`, with RW1 and RW2 | 2 |
| [Same distributor table, label normalization](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/distributor_test.go#L607-L665) | `drop empty labels` and `wrong labels order`, each with RW1 and RW2 | 4 |
| [Same distributor table, invalid symbols](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/distributor_test.go#L1008-L1040) | `invalid rw2 symbols`, RW2-only upstream | 1 |
| [`TestIngesterQuerying`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/ingester_test.go#L23-L582) | All seven table entries: `float series`; `integer histogram series`; `float histogram series`; `series switching from float to integer histogram to float histogram`; `series including float and native histograms at same timestamp`; `float series where sample timestamps don't align with query step`; `query that returns no results` | 7 |
| [`TestQueryFrontendWithExplicitLookbackDelta` → `queryLookbackDeltaTest`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/query_frontend_test.go#L1067-L1203) | Default 5m lookback at sample+3m / sample+6m; explicit 10m / 1m at sample+6m. Each checks instant and range queries with the upstream ±100m / 100m step geometry. | 4 |
| [`TestQuerierWithBlocksStorageRunningInSingleBinaryMode`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/querier_test.go#L311-L517) → [`testMetadataQueriesWithBlocksStorage`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/querier_test.go#L573-L790) | All four helper time windows: inside head; inside ingester/outside head; partially inside ingester; entirely outside ingester. Every series lookup, metric-name values selector (including OR), and label-name expectation in that helper is retained: 29 discovery requests. | 4 |

### Preserved inputs and assertions

- Distributor cases keep the exact metric names, labels, float values,
  metadata family/type/help/unit, RW2 symbol order and references, query strings,
  one-hour evaluation range and 5m step. They assert exact query matrices and
  metadata responses. RW2 written counts remain exactly 1/1/4/3 samples,
  zero native histograms and zero exemplars for the first four cases. The
  added native cases preserve schema -1 and -53, integer bucket deltas
  `[150, -100]`, the custom bound 0.1, count 200/sum 1000 and exact bucket
  boundary rules. The exemplar case preserves `test=test`, value 123 and its
  source timestamp. Written counts are checked (also as additional controls
  where upstream did not specify a count); RW1 must not return RW2 counts.
  Label scenarios retain the exact unsorted label order, `empty=""`, duplicate
  empty string in the RW2 symbol table, label references, sample 100 and exact
  query output identities. The invalid-symbol case retains the nonempty first
  symbol, references `[0, 1]`, counter metadata/help/unit, HTTP 400 and all three
  zero written-count expectations. Upstream specifies no error-body text for
  that case; the port does not invent a matching Mimir error string.
- Ingester cases keep `foobar`, `timestamp(foobar)`, `not_foobar`, all six
  100/110/120/130/140/150 samples, 10m spacing, the -2s unaligned timestamps,
  a one-hour evaluation range and the empty-result expectations. Source
  timestamps from `timestamp()` are checked, not evaluation timestamps.
  Histogram cases use generator indices 1..6, both delta-integer and absolute
  floating count encodings, schema 1, zero threshold 0.001, symmetric sparse
  positive/negative spans `[{0, 2}, {1, 2}]` and all nine exact output buckets.
  Mixed-type lifetime transitions and the scalar-wins same-time collision
  expectation are retained. This revision has only one unaligned table case
  (scalar samples), already included; no invented histogram cases are counted.
- Discovery keeps `series_1`/`series_2`/`series_3`, the same-name extra labels,
  relative timestamps (0/2/4 block periods), every selector and every expected
  identity/dictionary. Bounds stay inclusive, including a stored sample exactly
  at the last window's end. These are sample-backed results, not instant-vector
  discovery limited by a five-minute lookback.
- Expected responses come from the upstream tables and assertion rules, never
  from querying Pulso to regenerate an oracle. Sorting the histogram's two
  bucket series ignores response ordering only; every identity/value is exact.

### Histogram oracle provenance

[`pkg/util/test/histogram.go`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/pkg/util/test/histogram.go#L20-L104)
delegates `GenerateTestHistogram` and `GenerateTestFloatHistogram` to vendored
[`tsdbutil/histogram.go`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/vendor/github.com/prometheus/prometheus/tsdb/tsdbutil/histogram.go#L39-L128).
`GenerateTestSampleHistogram` supplies explicit decoded bounds and boundary
rules, so no Go fixture generation or Pulso-derived oracle was needed. The
Elixir test transcribes those input formulas and that expected geometry,
including the shortest-roundtrip spellings of `18.4 * float64(i+1)` sums.

## Explicit adaptations and omissions

1. Upstream wall-clock origins are replaced with Unix second `1700000000`.
   Upstream `generateFloatSeries` aliases the vendored
   [`e2e.GenerateSeries`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/vendor/github.com/grafana/e2e/util.go#L132-L179),
   which chooses `rand.Float64()`. Its random value is frozen to 0.5 wherever
   used. Its unasserted `trace_id=1234` exemplar is omitted. The fixed clock is
   a reproducibility adaptation, not a Pulso-derived expected value.
2. Mimir's `/prometheus/api/v1/*` and remote-write push paths become Pulso's
   `/api/v1/*` and `/api/v1/write`. Successful pushes assert Pulso's documented
   **204**, not Mimir's **200/202**. Read status and envelopes remain exact.
3. Discovery freezes the block period to 60s (the helper accepts this as a
   parameter). `series_2` uses the upstream generated native histogram again,
   not a scalar substitute. Its generator's random integer is frozen to 1,
   retaining exact source-generated geometry. The unasserted `trace_id=1234`
   exemplar from
   [`generateHistogramSeriesWrapper`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/integration/e2ehistograms/e2ehistograms.go#L55-L108)
   is omitted. This proves these sample-backed discovery results with native
   histogram input, not TSDB-tier boundary parity.
4. Docker, Consul, Kafka, MinIO provisioning, quorum/ring readiness, TSDB head
   compaction/shipping, store-gateway synchronization, cache counters,
   ingester gRPC status assertions, query-engine and caching-unmarshal flag
   permutations are not translated into pretend equivalents. Pulso has none
   of those service boundaries. Each test owns a loopback S3 protocol stub,
   real native object-store HTTP client, manifest processes, native Parquet
   writes and metric reads. Assertions check actual `PAR1` objects. Public
   requests enter the real Phoenix endpoint with `Phoenix.ConnTest`, not a
   network Grafana/Prometheus client.
5. Discovery requests also run as form POSTs with the same expected response;
   these 29 additional transport controls are not counted as upstream scenarios.
   Tenant-isolated metadata reads and durable metadata-object assertions are
   likewise additional controls.
6. The extra Pulso lookback control covers decimal/exponent seconds, nested
   `timestamp()`, subqueries, offset, a left-open boundary, unchanged explicit range
   windows, form POST, request isolation and malformed/overflow rejection.
   A separate evaluator batch regression checks per-source isolation and invalid
   internal options. These are **not** upstream cases. Pulso deliberately
   requires a positive override; zero/negative values reject rather than using
   upstream Prometheus's nonpositive-default fallback.
7. Two additional Pulso collision controls (RW1/RW2) put histograms before
   scalar samples in separate same-label entries. They check scalar precedence,
   exemplar preservation, written counts and exact keyed retries through S3.
   Deliberately corrupting the test-owned object at the same deterministic key
   must return 409 without replacing any object or manifest. These are not
   upstream cases. A different normalized caller body derives a different key,
   as documented, rather than reserving an exclusive idempotency token.
8. Sixteen additional Pulso label controls are separate from upstream scenarios:
   normalized same-label merging/keyed retries in RW1/RW2, and seven partial-write
   invalid-label variants in each version. Variants cover differing and identical
   duplicate values, duplicates with one/both values empty, duplicate metric-name
   labels, and an empty label name with nonempty/empty value. Each invalid series
   carries two points before a valid sibling is ingested; both points must count
   as rejected. RW1 retains its documented 204/rejected header and RW2 its
   400/rejected/written-count response. Public query/discovery and actual S3 rows
   must contain only valid surviving identities. These controls do not assert
   full Mimir name-validation/tenant-policy parity.

## Gaps reproduced and fixed

Before the runtime change, **17/19** selected scenarios passed. The two explicit
lookback scenarios failed at their first instant query with **HTTP 400**:
`{"status":"error","errorType":"bad_data","error":"Invalid or unsupported query parameters."}`.
No failure was skipped or converted into a success expectation.

The query endpoints now admit bounded positive `lookback_delta` and pass it to
the evaluator. Plain selectors, `timestamp()` and nested selectors use the
request's value; explicit range vectors retain their own windows. Batch sources
reset their own default and keep the same aggregate admission/budgets. That
lookback change has no storage format, native ABI, keyed-byte or global
configuration changes.

The follow-up added four ingester cases and restored discovery's native input.
Before the collision fix, **23/24 tests passed** (22/23 upstream scenarios plus
the existing Pulso control). Integer geometry, floating geometry, mixed-type
transitions and restored discovery passed unchanged. The same-time collision
case failed: Pulso returned histogram index 3 at step 2, losing scalar 120,
where upstream expects the scalar and no histogram at that timestamp.
No assertion was weakened or skipped. Request-local remote-write normalization
now omits native histograms shadowed by same-label/same-time floats, after
native validation/admission and before immutable append. It does not change
global query conflict resolution, add cross-request state, or bypass keyed
verification. Only colliding requests change normalized caller hashes/segment
bytes; drain affected retries and replace all receivers as described in
[`docs/querying.md`](../../../../../docs/querying.md#native-histograms).
The five histogram/exemplar distributor replays were coverage-only gaps and
needed no runtime changes. That phase passed 31 tests: 28 upstream replays and
three Pulso controls.

The label/symbol follow-up added five upstream replays and sixteen Pulso
controls. Before its runtime changes, **45/52 tests passed** (30/33 upstream
replays and 15/19 Pulso controls). Both wrong-label-order cases already passed.
Both empty-label cases returned `empty: ""` in query results, rather than
omitting it; normalized same-label entries failed to merge and retained an
otherwise shadowed histogram. The invalid RW2 symbol table was acknowledged
204 instead of rejected 400. RW1 empty-name labels before their point fields
reported one rejected record instead of both supplied points.

Native remote-write normalization now validates UTF-8, empty names and duplicate
names before dropping empty-valued labels, and does so before hashing or BEAM
materialization. Invalid RW1 names are checked after all points have been read
so rejection counts are complete. A nonempty reserved first RW2 symbol rejects
before samples or metadata can escape; the controller reports zero RW2 written
counts on this `invalid_protobuf` path. Invalid-series partial-write status
distinctions, raw admission bounds and exact keyed publication remain unchanged.
There is no stored-format/native tuple change. Empty-valued labels change caller
hashes, series hashes and encoded bytes, so drain affected retries and replace
all ingress application/native pairs before resuming them; historical rows are
not rewritten. See the Prometheus section of
[`docs/querying.md`](../../../../../docs/querying.md#prometheus-api).

The complete selection now passes **52 tests: 33 upstream replays and 19 Pulso
controls**, with no skips. Two additional native unit tests check canonical
label identities and an unused but invalid first symbol; existing native invalid
label tests also cover complete rejection counts and validation before removal.

Remaining service-suite gaps include exemplar age/disabled policies and histogram
downscaling under tenant overrides, broader label/name-validation and partial-write
permutations, HA/relabel/tenant configuration, UTF-8 query grammar, OTLP translation,
remote-read, tenant federation, cardinality analysis, query formatting,
streaming transport and Mimir-specific sharding/caching/limits/failure topology.
Passing this selection is not full PromQL, full Mimir service or deployed-client
compatibility evidence.

## Offline replay

```sh
ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mix test \
  test/pulso_web/controllers/mimir_service_conformance_test.exs \
  --warnings-as-errors
```

No Go, Docker, live Mimir or externally configured object store is required.
Tests are `async: true`; all runtime configuration and processes are owned by the
test, with no application/OS environment mutation.

## Attribution and licenses

The adapted service test retains **AGPL-3.0-only** attribution to Grafana Mimir.
Its `querier_test.go` and `query_frontend_test.go` sources also preserve
**Apache-2.0** Cortex provenance, copyright **The Cortex Authors**. The vendored
histogram generator is **Apache-2.0**, copyright **The Prometheus Authors**.
The test header retains these attributions. The parent directory's [`LICENSE`](../LICENSE)
is Mimir's AGPL license; [`LICENSE-Prometheus`](../LICENSE-Prometheus) provides the
Apache-2.0 license text. These adapted tests are not relicensed under Pulso's MIT
license. Runtime changes are independently written Elixir/Rust, with **no upstream
AGPL implementation code** copied into runtime.
