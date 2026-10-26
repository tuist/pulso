# Public-protocol feature breadth

## Goal and acceptance rule

Build a generic observability backend against public protocol contracts. Real
application inventories, including Tuist, are compatibility examples and useful
regressions, not the scope boundary. No feature is accepted merely because its
currently observed queries parse or its sampled client exports successfully.

Keep a versioned contract matrix with supported forms, explicit rejections,
semantic references, bounds and operational qualifications. The operator-facing
current boundary is [protocol coverage](../docs/protocol-coverage.md).

## Upstream service integration ports

The normal Elixir suite now includes selected public request/response scenarios
from all three upstream service suites. These remain selected ports, **not a copy of
their complete integration suites or proof of full compatibility**. Each source
selection is pinned and separately licensed; expected data comes from upstream,
not from regenerating answers with Pulso.

| Backend | Imported selection and provenance | Elixir replay |
| --- | --- | --- |
| Mimir | [48 scenarios](../test/fixtures/promql/mimir/services/README.md): RW1/RW2 scalar/classic/native/custom histograms, summaries, exemplars and metadata; all seven ingester-query table rows including mixed/colliding types; four lookback cases and four discovery windows; empty-label/order normalization, invalid RW2 symbols, all three UTF8Validation name rows over both write versions, the exact upstream fingerprint fixture and eight invalid-range requests with explicitly adapted diagnostics | `test/pulso_web/controllers/mimir_service_conformance_test.exs` (69 tests including 21 controls) |
| Loki | [28 checks](../test/fixtures/logs/loki-upstream/README.md): ingest/query/labels/series/errors, disabled label policy, all four categorized-label cases, count/unwrap/filter results, detected-field windows, grouped stream ordering, per-request query length, standalone request-policy isolation, four-line WebSocket tail, `approx_topk` and grouped/ungrouped `approx_count_distinct` public results using exact bounded evaluation; one range-resolution adaptation is explicit | `test/pulso_web/controllers/loki_upstream_integration_test.exs` (49 tests including 21 controls) |
| Tempo | [Eight integration source entry points plus fifteen public engine step rows](../test/fixtures/traces/tempo_integration/README.md): seven two-span batches, v1 names/values, six complete v2 discovery filters, all 19 enabled special-character pairs, invalid-scope rejection, basic unscoped search, all 31 V2 trace-by-ID depth combinations, 20 selected metric/HTTP-validation subtests and two span-ID metric-cardinality selections; 16 separately labelled public Parquet/engine controls | `test/pulso_web/controllers/tempo_upstream_integration_test.exs` (59 wrappers including two new local step controls); independent native/reference/MCP replay in `search_expression_test.exs` |

All replays use native Parquet and the real S3 HTTP adapter against test-owned
loopback object stores. Loki and Tempo also use live authenticated HTTP
listeners; Mimir uses the production Phoenix endpoint through ConnTest. They
require no upstream server, Go, Docker or network downloads during replay. They
do not model upstream WAL/head/block/live-store phases: Pulso publishes durable
segments before acknowledging ingestion. Separate Pulso controls cover malformed
inputs, boundaries, isolation and transport variants, and are not counted as
additional upstream cases.

The ports exposed and fixed bounded HTTP compatibility gaps: Prometheus
`lookback_delta`; Loki selector-only discovery/stats and window-derived default
range resolution; Tempo bare v1 keys, quoted attribute targets and unescaped
slashes. Follow-ups fixed remote-write float/native-histogram timestamp
collisions, negotiated Loki label categories and Tempo unscoped/default-window
search. The next selected Loki detected-field/ordering cases passed unchanged;
Tempo V2 exact-ID filtering now validates the full stored trace before independent
ancestor/descendant expansion. Explicitly selected subsets carry PARTIAL; missing
reads, corrupt/legacy/conflicting nonmatches and exhausted budgets still fail
whole. This filter adds bounded BEAM conversion work, not a native/storage change.
Its oversized-URI control retains HTTP 414 rather than upstream's 400.
Mimir's next five scenarios normalize empty-valued labels before merging and
reject a nonempty reserved RW2 symbol before any sample/metadata publication.
Duplicate/empty names and invalid UTF-8 validate before removing empty values;
all raw labels/points still consume admission and rejected-point accounting.
Label-order scenarios already passed, so no ordering fix was invented.
Existing query budgets, feature gates and shared-object-store authority
remain unchanged. Tempo now requires search capability5, tuple `{2,1,3,5,12}`;
unscoped predicates use span-key precedence, not a resource/span OR. Omitted or
paired-zero HTTP bounds search all retained context, not Tempo's ingester-only
tier. Colliding or empty-valued-label remote-write requests change normalized
fingerprints and bytes: drain affected retries and replace all receiving
application/native pairs before resuming them. The native tuple is unchanged
and startup does not fence older receivers. Never evade keyed 409s;
[operator guidance](../docs/querying.md#native-histograms) applies.

Previous increment's whole-repository validation: `mix precommit --seed 0
--max-cases 4 --warnings-as-errors` passed **1760 tests, 1 existing skipped,
37 optional excluded**. A final three-service replay passed all 95 tests at
seed 41. Codec `cargo test --release` passed 133 tests; strict Credo, formatter,
Rustfmt and whitespace checks passed. Strict codec Clippy passed for debug and
release with Rust/Clippy **1.97.1**; an earlier Rust 1.94 run reported two unchanged
`nonminimal_bool` warnings in trace-segment pruning, not fixed by these ports.
The green suite still logged compaction timeouts/CAS warnings and the intentional
gRPC handler crash; it does not close those diagnostics or capacity acceptance.

The next increment ports six Mimir UTF8Validation rows, one Loki per-request
length case and fifteen Tempo metric-only rows. Baselines reproduced Mimir's
quoted-metric query returning 400 after successful ingestion and Loki's ignored
one-minute policy returning 200. Quoted UTF-8 selector names now reuse the
existing matcher AST; no receiver validation-mode knob or grammar expansion
outside selectors is introduced. Loki's `maxQueryLength` checks effective finite
bounds before storage across finite endpoints. Other policy fields, duplicate or
malformed policies and live-tail policies explicitly reject rather than silently
run unrestricted; this stricter policy is not exact malformed-header parity.
Existing server ceilings cannot be increased by the request. Upgrade all query
applications before relying on either feature; no native/storage/keyed-byte
change or retry drain is required.

Tempo's selected metric tables passed on unchanged runtime. The timed/random
producer is frozen to sixty single spans; no exemplar request/assertion, gRPC
transport or metrics expression arithmetic is claimed. All 3/4/122 interval
counts, six ranking cardinalities, five range/instant reductions and the average
comparison are preserved. Independent asymmetric controls verify actual ranking
identities and span-weighted durations, not just agreement between two Pulso
outputs. Fixture READMEs preserve pinned source hashes, licenses, baseline
evidence and exact harness adaptations.

Validation before the span-ID continuation:

- `ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mise exec -- mix precommit
  --seed 69 --max-cases 4 --warnings-as-errors`: **1800 passed, 1 existing
  optional promtool skip, 37 optional exclusions**, including compile/dependency/
  formatter checks. Runtime: 225.3 seconds.
- All three service replay files together at seed 70: **130 passed**.
  The targeted Tempo metric/HTTP set also passed all 46 tests at seed 68.
- Strict Credo checked 437 source files with no findings; `git diff --check`
  passed. No Rust sources changed; native Parquet/S3 execution was exercised by
  the Elixir suites, not a separate new Cargo unit-test run.
- The full green run still logged compaction timeouts/conflicts, manifest CAS
  warnings and an acceptor teardown error, alongside deliberate oversized-URI
  and gRPC-crash controls. Passing tests do not resolve those diagnostics or
  establish provider/capacity/deployed-client acceptance. No deployment or
  shared infrastructure changes were performed.

The subsequent continuation selects two Tempo span-ID metric-cardinality cases
and Loki's four-line tail plus standalone per-request-limits test. Both Tempo
source selections failed with HTTP 400 before runtime edits (34/37 wrappers
passed, seed 71). Native `by (span:id)` now groups validated core IDs as
fixed-width lowercase strings, distinct from ordinary `span.id` attributes.
Original string/int64 types, reused IDs across traces, leading zeros, high bits,
legacy core-only rows and instant/range responses have separate controls.
Exactly 10,000 ID series succeed; 10,001 fail whole while an ungrouped count
still sees all source spans. The upstream unlimited-series override, exemplar
request and streaming transport are explicitly not selected or implemented.
The metrics capability advances independently to **2**, fenced at startup;
the trace tuple, storage format and keyed bytes remain unchanged. Upgrade all
query-serving app/native pairs before relying on the syntax, without a data
migration or retry drain for this addition.

Both new Loki ports passed unchanged. A separate authenticated native-S3/socket
control covers explicit history bounds, matching live formatted lines, durable
finite-query visibility and monitored socket shutdown. These tests do not
establish cross-instance continuity, full tail completeness or Loki's topology.
All additions preserve pinned source/license provenance and distinguish harness
adaptations and local controls from source expectations.

Final validation for this continuation:

- `mix precommit --seed 75 --max-cases 4 --warnings-as-errors`: **1807 passed,
  1 existing optional promtool skip, 37 optional exclusions**, 196.7 seconds.
  Compile/dependency/formatter checks passed too.
- All three service files plus the trace metrics unit and HTTP files:
  **149 passed**, seed 76. The service files contain 60 Mimir, 39 Loki and
  37 Tempo wrappers, with source ports and local controls counted separately.
- Codec `cargo test --release`: **133 passed**. Release Clippy with all targets
  and warnings denied passed on Rust 1.97.1. Direct native validation admitted
  ID grouping and rejected numeric ID operations and six-key grouping.
- Strict Credo checked 437 source files without findings. Elixir formatter,
  Rustfmt and whitespace checks passed. The green full suite still logged
  compaction timeouts/conflicts and manifest CAS warnings, alongside intentional
  oversized-URI/gRPC-crash controls; none is claimed resolved by these ports.
- All changes remain local and uncommitted. No upstream multi-service suite,
  deployed provider/client, infrastructure change or data migration was run.

The next continuation selects five more Tempo metric/HTTP-validation rows and
Mimir's `TestHashCollisionHandling` public result. Both empty-result Tempo
assertions failed before the native fix (seed 78); zero-step cases already
rejected, but the six-minute `35ms` request incorrectly succeeded (seed 80).
Ungrouped counts/rates now return zero series values after a successful scan,
without fabricating grouped/numeric observations or masking scan failures.
Range admission checks the original interval quotient against 10,000 before
alignment adds boundary intervals. Separate public-query controls distinguish
10,000 requested intervals, a sub-step remainder and 10,001 intervals, and
exercise empty tenants, filtered/absent spans, instant results and all aligned
zero timestamps. No safety ceiling is increased. The small-step diagnostic uses
Pulso's own HTTP-400 wording; exemplar production/request and gRPC remain outside
this selection. Serving app/native pairs must all upgrade for consistent results;
storage, keyed bytes and the native ABI are unchanged in this continuation.

Mimir's exact tenant, two label sets, same-time values and full ordered vector
passed before runtime changes. Native diagnostics confirmed distinct Pulso IDs:
the upstream labels collide under its fingerprints, not Pulso's StableHash.
This port is not evidence for Pulso hash-collision handling. No new Loki behavior
is claimed in this continuation; its existing public ports remain in combined
regression validation. Source hashes, licenses and adaptations are retained in
each fixture README.

Validation for the empty-selection/interval continuation:

- `mix precommit --seed 82 --max-cases 4 --warnings-as-errors`: **1815 passed,
  1 existing optional promtool skip, 37 optional exclusions**, 238.4 seconds.
- All three service files plus trace metrics unit/HTTP tests: **157 passed**,
  seed 83. Service totals are 61 Mimir, 39 Loki and 42 Tempo wrappers.
- Codec release tests: **133 passed**. Release Clippy with all targets and
  warnings denied passed using Rust 1.97.1. Strict Credo checked 437 files with
  no findings; formatter, Rustfmt and whitespace checks passed.
- The full green run still logged compaction timeouts/conflicts and manifest
  CAS warnings, plus intentional oversized-URI and gRPC-crash controls. These
  ports do not resolve those diagnostics or establish provider/client capacity.
  Changes remain local and uncommitted; no deployment or data migration ran.

The next continuation ports eight Mimir invalid-range requests and Loki's
`TestProbabilisticQuery`. All eight Mimir HTTP/status/errorType assertions
already passed; diagnostics deliberately retain Pulso's generic wording.
The omitted-resolution subquery rejects as unsupported syntax, not proof of
range-vector result typing. An initial Mimir baseline had two existing writes
return 503 without captured bodies; its same-seed expanded replay passed all
69 tests. No cause or runtime fix for those write failures is claimed.

The Loki instant query failed with HTTP 400 before the syntax/evaluator change
(seed 84). `approx_topk` now uses exact ranking with unchanged scan, log-sample,
work, intermediate-point, deadline and heap budgets. It requires a positive
signed-int64 integer literal, rejects all outer grouping and rejects range mode
across the whole expression before provider work. Three local controls cover
full values, windows, inner aggregation, arithmetic, GET/form POST, invalid
ranks/grouping, nested mode rejection and fail-whole budgets with `k=1`.
This does not implement probabilistic sketches, distributed merging or
high-cardinality acceleration. Only serving application upgrades are needed
for this operator; no native/storage/keyed-byte change or retry drain.

Validation for the bounded ranking/invalid-request continuation:

- Loki HTTP/WebSocket, module, LogQL and mixed-query-batch tests: **386 passed**,
  seed 87. All three service files plus trace metrics unit/HTTP tests:
  **169 passed**, seed 89; service totals are 69 Mimir, 43 Loki and 42 Tempo.
- `mix precommit --seed 88 --max-cases 4 --warnings-as-errors`: **1824/1827
  passed, 3 failed, 1 optional promtool skip, 37 optional exclusions**, 400s.
  Failures were the 60s filtered-intrinsic discovery test timeout, gRPC admission
  reply timing, and the 150k-span trace query deadline. All three passed unchanged
  under `mix test --failed --max-cases 1 --seed 88 --warnings-as-errors` in 8.9s.
  No timeout/budget was raised or assertion skipped. Runner load averages were
  approximately 193/201/138 during the failed run, but causation is unproven;
  the full suite is not claimed green or the underlying timing issue fixed.
- Strict Credo: **437 files, no issues**. Precommit compile, unused-dependency
  and formatter stages passed. No native code changed in this increment.
  Source pins, attribution, adaptations and baseline failures are recorded in
  each selected fixture README. Changes remain local and uncommitted.
- Repeated combined runs at seed 90 each passed **168/169**: the four-case
  concurrent run hit HTTP 500 in an existing Loki category-control push; the
  serial run hit HTTP 503 ingesting an existing Tempo legacy-filter control.
  Both passed alone unchanged in 1.9s and 4.9s respectively. Original response
  bodies were not captured; the Loki push helper now includes the body in its
  failure diagnostic. These failures remain unexplained, even with serial
  tests; neither lower concurrency nor this increment is claimed to fix them.
- Final selection of all twelve additions, including exact result lengths:
  **12 passed, 100 excluded**, seed 91, serial execution, 7.9s. This covers
  nine selected upstream cases and three Pulso controls on the final runtime.

### Bounded distinct-count continuation

The two public queries in Loki's `TestApproxCountDistinctQuery` failed with
HTTP 400 before the change (seed 101). Their four original logfmt writes,
overlapping `shared` values across devices, relative timestamps, full labels,
exact queries and one-series value `3` expectations now replay in Elixir.
The upstream `Shards > 1` assertions are excluded: Pulso counts exactly under
existing bounds, not with HyperLogLog or distributed sketch merging.

Four local HTTP/S3 controls cover raw string identity, missing/empty fields,
default/explicit/empty grouping, labels and parser values, arithmetic, instant/
range windows, nanosecond boundaries, offsets, GET/form POST, pre-storage invalid
queries and fail-whole budgets. Detached observations, including duplicates,
spend both selected-log and existing materialized-sample limits before window
deduplication; repeated window visits spend work. No new memory-byte ceiling,
high-cardinality acceleration, native/storage/keyed-byte change or retry drain.
Upgrade every query-serving application before relying on the syntax.

Combined Loki HTTP/WebSocket/module/LogQL/batch and Mimir/Tempo service regression:
**503 passed**, seed 104. The Loki service file contains 28 upstream-derived
checks and 21 local controls. Source pin, hashes, adaptations and baseline
failures are in its fixture README; no upstream cluster or deployed provider
was executed. This continuation remains local and uncommitted.

### Tempo default metric-step continuation

The fifteen original `TestDefaultQueryRangeStep` engine rows now run through
authenticated HTTP and persisted native Parquet with fresh manifest state.
Their window/expected-step table stays exact; an explicitly adapted one-span
fixture makes the chosen step visible in every response timestamp. These are
public engine-table ports, not additional upstream service-integration cases.

The unchanged-runtime baseline failed seven source rows plus the local minimum-
step control (seed 105). Omitted steps now use Tempo's 50 ms floor below one
minute and coarser window-dependent rounding otherwise. Explicit steps, instant
queries, interval admission and native/storage/keyed-byte contracts are unchanged.
Two local controls cover shifted-epoch and nanosecond/minute boundaries, explicit
700 ms spacing, instant ignored-step behavior and fail-whole interval admission.
Upgrade all query-serving applications for consistent defaults; no migration or
native ABI change. The targeted Tempo unit/HTTP/service replay passed **74**,
seed 106, including all **59 service-file wrappers**.

Broader Loki/Mimir/Tempo regression passed **510**, seed 107; the separate
batch suite passed **9**, seed 108. The seventeen additions passed eleven
consecutive fixed-seed runs (**187 executions**, seed 109). Strict compile,
Credo across 437 source files, scoped formatting and whitespace checks passed.
No native source changed or native unit tests reran. These are offline loopback
regressions, not upstream-server, provider or deployed-client acceptance.
Changes remain local and uncommitted.

### Remaining upstream coverage and product gaps

- **Mimir:** additional histogram/exemplar policies and tenant downscaling,
  remaining invalid/partial-write permutations, OTLP translation, remote-read, federation, cardinality,
  formatting, UTF-8 grammar outside selectors and streaming. Sample-backed discovery now uses native histogram
  input but still does not establish TSDB-tier behavior.
- **Loki:** enabled label policies, deletion,
  federation, other per-request query policies, tail/client permutations, OTLP
  mapping, probabilistic sketches/distributed merging/high-cardinality acceleration.
  The upstream seven-day no-step metric case cannot reproduce its expected
  last-four-second samples on the regular default grid; its imported count
  assertion explicitly supplies 60s resolution. No extra terminal sample or
  budget exemption is invented to conceal the discrepancy.
- **Tempo:** partial time bounds, intrinsic/OR/partial-query discovery,
  structural/spanset TraceQL, metrics exemplars and
  expression combinations, query streaming, broader trace filters/diffs and legacy-v1
  collisions. Existing typed/export/search fixtures do not fill these gaps by
  implication. Unsupported discovery filters still reject rather than vanish.
- **Architecture-specific exclusions:** upstream Kafka/WAL/ring/quorum,
  sharding/cache and multi-storage-engine permutations are not transplanted as
  fake equivalents. Pulso needs its own multi-instance CAS, maintenance,
  admission, restart and provider tests, plus separately authorized deployed
  clients and representative capacity validation.

Prioritize additional portable upstream tables by those missing public
behaviors. Keep exact ports, explicit adaptations, deliberate stricter failures
and unsupported surfaces separate; never skip a mismatch or widen a safety
ceiling solely to turn a selected case green.

## Historical pause point: landed protocol increment

The implementation below landed in `f7a3ebe` (PR #34). Work has resumed with
explicitly requested, bounded follow-ups. The original landing constraint and
its validation caveats are preserved here; landing did not satisfy operational
or Tuist cutover gates.

Pause new protocol/storage/alerting feature increments and prepare the existing
implementation for review and landing. The backlog below is follow-up work, not
an instruction to expand this PR until every protocol is complete.

Before landing, review the actual diff (including untracked source/tests/fixtures),
keep operator contracts and upgrade caveats accurate, and verify the final PR
revision. Existing correctness/safety regressions remain review blockers; deferred
feature breadth is distinct from those blockers. Passing bounded tests does not
close the recorded timeout/lifetime diagnostics or establish production capacity.
No merge, deployment or migration is authorized merely by this planning note.

See [the landing review checkpoint](landing-review.md) for concrete fixes,
failed/successful validation, review coverage limits and explicit open findings.

Resume from [Remaining protocol work](#remaining-protocol-work-independent-of-application-usage),
including the storage/scale and operational follow-ups, after this PR lands.

## Completed increment: Loki label-name volumes

- Remove `aggregateBy=labels` from the unsupported list. Both volume endpoints
  accept GET/form POST, count bytes by stream label name across values, restrict
  explicit targets, and rank by descending bytes with lexical tie-breaking.
  Range limits apply per bucket, not globally before aggregation.
- Semantic references are Grafana Loki v3.5.0 at
  `4b16bc4fa823f7ec6e865ebf51fad89d98a2371c`:
  [TSDB volume tests](https://github.com/grafana/loki/blob/4b16bc4fa823f7ec6e865ebf51fad89d98a2371c/pkg/storage/stores/shipper/indexshipper/tsdb/single_file_index_test.go#L593),
  [response conversion tests](https://github.com/grafana/loki/blob/4b16bc4fa823f7ec6e865ebf51fad89d98a2371c/pkg/querier/queryrange/volume_test.go#L174),
  and [target-label preparation](https://github.com/grafana/loki/blob/4b16bc4fa823f7ec6e865ebf51fad89d98a2371c/pkg/util/series_volume.go).
  The upstream label-volume, response and middleware tests pass unmodified.
  [Extracted upstream fixtures](../test/fixtures/logs/README.md#exact-upstream-loki-volume-cases)
  preserve all ten label-mode cases; eight portable cases replay through both
  HTTP endpoints/methods with exact byte expectations. Chunk-filter callbacks
  and partial-chunk estimates remain explicit exclusions. Additional independent
  Pulso cases exercise row-byte and safety contracts.
- Deliberate boundaries: selector-only; actual inclusive nanosecond rows rather
  than estimated chunk overlap/whole-second windows; matrix range responses
  even for a single bucket. Stream names reuse discovery's projection, excluding
  structured metadata and the internal tenant name. No new storage authority,
  native ABI or keyed encoder bytes. Existing series mode remains unchanged.
- Scan/line/work/intermediate/result budgets and a complete 5,000-name/1 MiB
  name dictionary fail whole before top-k. The implementation scans Parquet
  in batches; it does not claim Loki's index-backed cost or provider capacity.
  See [operator contract](../docs/querying.md#index-statistics-and-volume).

## Live-tail reliability follow-up

- Replace count-then-register admission with unique process-owned slots, retaining
  the per-tenant, per-instance cap during concurrent connection/history startup.
  Leave, caller death and failed follower startup release reservations.
- Close existing sockets with WebSocket1013 on follower loss, including a tail
  supervision restart. Never silently establish a replacement cursor after losing
  dedupe/baseline state or tailer memberships. Before upgrade, unavailable local
  supervision returns retryable HTTP503 instead of a request-process crash.
- Reconnection queries history and resumes best-effort delivery; it does not
  establish continuity, deduplicate across sockets or turn polling into a journal.
  Independent instances still multiply provider pressure. No storage/ABI/keyed-byte
  changes, new durable authority or rollout are involved.
- The old configuration-ETS traceback points to a test-owned shared table expiring;
  the current follower fixture already captures immutable configuration. A new
  full run reproduced a different teardown error: a periodic follower accessed
  its manifest cache after storage supervision stopped. Starting tail supervision
  after its storage dependencies now ensures followers stop first. No exception
  suppression was added to storage polling. This does not independently resolve
  the old shared-configuration failure or broader lifecycle/capacity acceptance.

Validation for this follow-up (logs in `/tmp/pulso-tail-lifecycle/`):

- Before production changes, regressions failed on non-idempotent rejoin and a
  missing close frame after follower loss. The concurrency control passed on that
  run; the count-then-register race is a source finding, not a reproduced overflow.
- `ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mix precommit --max-cases 4 --seed 432996`:
  **1,618 passed, 1 skipped, 37 excluded**; strict compile, dependency and format
  checks included. Strict Credo is clean across 428 files. No native sources or
  storage formats changed; native crates were built, not separately unit-tested.
- Four focused tail files: **30 passed** on each of 11 consecutive runs using
  `--repeat-until-failure 10`; no tail ETS/manifest-cache teardown error in these
  runs or the final full run. Loopback WebSockets exercise follower death,
  orderly registry-supervisor restart, explicit 1013/503 and history/live reconnect.
  This does not establish abrupt registry-kill storms or provider acceptance.
- The first full run failed on a compaction peer's 30s application-start timeout,
  alongside acceptor teardown diagnostics and the manifest-cache error above.
  The exact failing peer test and full suite pass on same-seed replay, but the
  peer/acceptor root causes remain open. Existing compaction timeout warnings
  also remain; a later green run is not diagnostic closure. The intentional
  gRPC handler-crash test still emits its expected error log.
- No deployment, provider exercise, private application change, commit or cutover.

## Completed increment: exponential projection correctness

The working tree already contains exponential/delta receivers and other protocol
extensions beyond the historical checkpoints below. This increment hardens that
existing implementation rather than claiming a new receiver or accepting every
current extension by inspection.

- Keep finite bounds below exponent1024 and rounded subnormals above-1075.
  Merge equal rounded boundaries at their own complete cumulative count, including
  signed-zero collisions; retain negative infinity explicitly.
- Reject ordinary populated bins wholly inside the closed zero region, clip
  partial negative overlap, and preserve stale-value/optional/source-bit behavior.
- Enforce existing 65,536-count per-side conversion and 160-populated-coarsened-bin
  ceilings, even at scale-10. Exhaustion fails whole, including valid neighbors,
  keyed/unkeyed and JSON/protobuf. No cap increases or new mutable state.
- Freeze 1,160 public OTLP1.11.0/Google1.36.12 source points with Go1.26.5
  decomposed mathematical geometry and actual Prometheus0.315.0 bucket iterators.
  Preserve the observed direct-Exp2 subnormal discrepancy separately, not as a
  silently changed golden. Original typed owners/time/arrays survive native
  Parquet and gzip exact-key S3 replay across owned runtime instances.
- Classic projection is deliberately approximate, not native-histogram/negative
  endpoint parity. Mathematical overflow differs from Prometheus's max-finite
  sentinel. Source envelopes remain faithful, with unchanged formats/API tuples.
- Affected projected keyed bytes can change; upgrade every ingress application
  and drain retries. Exact-byte409 remains permanent; never evade it by key
  mutation, overwrite, unkeyed/subset resend or metadata reconstruction.

## Completed increment: quoted trace counts and flags

- Accept exact quoted uint32 dropped counts and fixed32 flags at every trace
  owner, matching public OTLP v1.9.0/Google protobuf v1.36.11 decisions without
  float rounding or exponent-sized integers. Preserve whole-request metadata
  rejection, per-span partial success and keyed fail-whole publication.
- Keep enums as numeric JSON tokens. The dedicated public corpus proves quoted
  enum digits reject; generic ProtoJSON named enums remain invalid OTLP forms.
- Freeze 451 actual quoted-field decisions plus complete public JSON/protobuf
  requests. Native/reference JSON, Parquet and cross-instance gzip keyed S3 replay
  establish component behavior, not deployed-client/provider/capacity acceptance.
- Existing Rust decoding/preflight/storage stays in place. Only application-side
  trace metadata normalization changes; no native ABI, format or keyed-byte
  change. Upgrade all ingress applications before relying on accepted forms.

### Additional trace uint32 validation

The branch's independent corpus retains 306 public OTLP1.9.0/Google1.36.11
quoted-integer decisions across the nine count/flag fields, plus original and
canonical JSON and actual public protobuf. It adds native/reference/direct-map
parity, 50 randomized all-owner requests, native Parquet and gzip keyed S3 replay
across owned independent runtime instances. Both corpora remain replayable without
Go or network access.

Before merging the independently landed implementation:

- Five focused wire/JSON/controller files: **46 passed**.
- `ERL_FLAGS='+S 4:4' PULSO_NIF_FORCE_BUILD=1 mix precommit --max-cases 4`:
  **1,627 passed, 1 skipped, 37 excluded**. Existing compaction timeout warnings
  remain diagnostic follow-ups, not resolved by this passing run.
- Strict Credo: **429 files, no issues**. No Rust source/ABI changed; the native
  crates were built, not separately unit-tested for this app-only increment.
- A local, sequential 20-sample/1,000-span decoder check alternated the original
  baseline and branch implementation after warmup: numeric sparse medians
  **10,328/10,302 µs** and numeric dense **14,534/14,569 µs**. These historical
  component checks are not concurrent capacity/provider proof or measurements
  of the combined implementation.
- Claude design iteration rejected early native string rewriting because it
  would reclassify admission terms. Adversarial source review found no blocking
  defect; count-field body-vs-attribute byte bounds and the prevalidated
  construction invariant were clarified. Raw public captures retain their
  provenance; regeneration whitespace differences stay documented.

The merge retains the landed parse-once span/event/link decoder and both
independent fixture and regression-test sets, without changing admission,
numeric-only enums, storage formats or keyed fail-whole behavior. Combined
validation: six focused OTLP files **55 passed**; full precommit **1,637 passed,
1 skipped, 37 excluded**. Existing compaction timeout diagnostics remain open.

## Completed checkpoint: receiver-scoped duplicate JSON fields

- Reject recognized duplicate JSON fields before any append, across scalar,
  message, repeated-container and oneof fields. Equal values, nulls and escaped
  key spellings do not evade rejection; separate object siblings reuse masks.
- Bind JSON parsing to the static receiver signal, not payload inference. Unknown
  names, opaque unknown objects and foreign signal roots remain ignored. General
  JSON outside OTLP retains its previous first-key behavior.
- Ignore syntactically valid overflowing numeric tokens in opaque unknown fields
  without losing their scalar node/eight-byte admission. Do not hide malformed
  JSON, drop supplied containers or infer a schema from nested field names.
- Keep object pairs in the independent fallback until duplicate validation. A
  native fallback must not silently collapse duplicates into an ordinary map.
  Existing exact integers, signed zero, scalar costs and all safety caps remain.
- Require shared wire capability 3; unchanged Parquet/envelope/keyed scalar formats
  and signal metadata/search tuples. Upgrade every serving ingress app/native pair.
- Freeze 704 actual Google protobuf v1.36.11/OTLP v1.9.0 field decisions over 35
  contexts, including all fields enumerated by public message descriptors. Prove
  known-duplicate whole-request rejection and unknown-field exact keyed publication
  in owned gzip HTTP/S3 cross-instance contexts. No SDK/Go/network in normal replay.
- Correct generic ProtoJSON assumptions: OTLP requires integer enums/lowerCamelCase.
  Named enums/snake_case aliases are not missing mandatory OTLP features.
- [GreptimeDB and OpenObserve reference notes](backend-protocol-references.md)
  identify concrete naming/default/oneof lessons without copying code or adopting
  another authority/topology. Public protocol semantics remain the acceptance oracle.

## Completed checkpoint: exact raw JSON integer numbers

- Decode numeric tokens in supported OTLP message schemas without first rounding
  through float64. Cover source times, int64 values, uint64 counts/bucket arrays
  and numeric 32-bit/enum fields, including escaped keys.
- Reject true fractions/overflow as invalid protocol numbers, including tiny
  nonzero fractions that previously underflowed to zero. Invalid source time must
  never trigger unknown-time/receive-time fallback. Preserve scalar node/eight-byte
  accounting, not string reclassification or synthetic JSON containers.
- Keep ordinary floating/unknown fields and quoted values separate. An OTLP-only
  zero-mantissa adapter preserves double `-0e0` sign; general JSON behavior stays
  unchanged. Native answers match an independent Elixir token/schema reference,
  randomized exact integers and explicit fallback/malformed-input cases.
- Require shared wire capability 2 across every serving ingress app/native pair;
  no signal metadata/search tuple, Parquet, scalar keyed-byte or budget change.
- Freeze 156 actual public Google protobuf integer-token decisions, 11 float-bit
  cases and three full raw-number/canonical-JSON/protobuf request sets. Prove
  native Parquet fidelity and gzip keyed publication across independent runtimes
  sharing an owned local S3 stub. Ordinary replay needs no Go or network.
- Remaining null/entity-ref forms, other metric types and actual
  deployment/capacity acceptance remain independent gaps. Quoted trace counts/flags
  are now covered by the dedicated increment above; enums require numeric tokens. The earlier generic
  named-enum/alias assumptions are corrected by the OTLP-specific contract above.

## Completed checkpoint: shared OTLP wire semantics

This checkpoint introduced wire capability 1 and quoted forms; its raw-number
limitations are superseded by the capability-2 increment above.

- Merge singular protobuf messages recursively, append repeated values and follow
  known oneof selection for AnyValue, Metric data, scalar points and exemplars.
  Switching away and back must not resurrect a previous message member.
- Run allocation-free raw preflight before a bounded native borrowed-slice plan;
  account source fragments, losing data/oneofs and unknown field occurrences.
  Apply cumulative logical-owner attribute counts/bytes/nodes before BEAM term
  materialization, without refunding replaced metadata.
- Accept exact quoted decimal/exponent integer forms without float rounding or
  exponent-sized bignums. Reject noncanonical leading-zero/plus spellings that
  were previously extensions. Broader unquoted forms and trace count/flag strings
  were addressed by the later dedicated increments above, not inferred from this
  component; enum strings remain invalid.
- Fence matching native artifacts with shared OTLP wire capability 1, separate
  from unchanged log/metric/trace storage/search capabilities. Upgrade every
  serving ingress pair; there is no Parquet, envelope or keyed scalar format change.
- Freeze actual public Google protobuf v1.36.11/OTLP v1.9.0 decoding, canonical
  wire and JSON across all three signals, including signed zero and 60 numeric
  string cases. Replay offline, validate native Parquet and owned local HTTP/S3
  CAS keyed replay across two runtime instances. These are not capacity or
  deployed-client acceptance. See [operator contract](../docs/otlp-wire.md).

## Completed checkpoint: explicit distributions

- Accept cumulative explicit OTLP histograms and summaries in HTTP JSON/protobuf,
  including packed/unpacked primitive arrays, optional sum/min/max, exact uint64
  counts, original quantiles, typed owners and histogram exemplars.
- Project classic Prometheus float samples without replacing the original point.
  Independently cap supplied source points, expanded samples and additive encoded
  metadata. Rejections count source points; safety failures append nothing.
- Honor no-recorded-value markers without validating ignored count/sum/value
  relationships; retain their originals, validate label-defining structure, and
  prove the latest marker suppresses an earlier ordinary value after compaction.
- Reject ambiguous projected families and conflicting same-time source envelopes
  within a request. Do not introduce a mutable cross-request metric registry.
- Fence the enriched wire decoder with capability 2, leaving metadata/storage
  formats and the other metric capabilities unchanged: `{1,2,1,1,1}`.
- Validate with public OTLP proto v1.9.0, actual protoc/Google ProtoJSON fixtures,
  actual Go fixed-decimal label spellings, native storage, HTTP/MCP/PromQL,
  cross-instance publication, restart and compaction. These are independent
  protocol components, not application bootstrap or provider-capacity evidence.

Full source envelopes repeat per projected row. Keeping the existing 16 MiB cap
is deliberate: do not increase resource bounds to make oversized distributions
pass. Shared source-point representation would require a separately designed,
self-contained immutable storage format and coherent reader/compactor upgrade,
not a second mutable authority or dangling point-reference store.

## Completed checkpoint: Remote Write 2.0 source starts

- Investigated the experimental schema transition: public Prometheus 3.5.0
  (rc.3, per-series created timestamp) versus 3.8.0 (rc.4, per-sample/histogram
  start timestamp). Retain both in an optional signed-int64 millisecond column,
  including exact extremes, zero-as-unset and same-label entries with differing
  starts. Hybrid precedence is explicitly a Pulso policy, not upstream parity.
- Match default public receiver operation with optional zero-injection disabled;
  preserve ordinary PromQL samples and exact written counts, not synthetic rows.
  No per-series local/cluster authority, ingest forwarding or sticky routing.
- Keep source-record/body/label bounds and native preflight; malformed start
  fields and safety failures append nothing. Preserve sample-time retention,
  series identity, immutable PUT/CAS, old nil-start encoder bytes and keyed
  fingerprint stability. Starts join content hashes only when present.
- Metric metadata/start capability 2 changes the current tuple to `{2,2,1,1,1}`.
  Retention activates immediately. Require a fleet-wide maintenance pause:
  block metric delivery, drain old keyed retries, stop all old readers/writers/
  compactors, replace every participant, then resume traffic. Old ingress can
  change derived object keys; old compactors erase starts. No under-traffic
  rolling rollout, default-off switch, automatic historical repair or safe old
  compactor rollback is provided.
- Freeze actual public Go-schema production/decoding plus actual release
  receiver written headers and stored-query results: 56 cases / 112 source
  samples across floats and integer/float/custom histograms. Replay with native
  project tooling; validate raw MCP native/fallback JSON, Parquet, owned local
  S3/CAS, independent runtime instances, restart and mixed null/start compaction.
  See [fixture provenance](../test/fixtures/metrics/remote_write_start/README.md).
  This does not establish separate-node, real-provider, SDK/Alloy, pressure or
  full compliance-suite acceptance.

## Remaining protocol work, independent of application usage

1. **OTLP metric semantics:** native exponential-histogram representation beyond
   existing bounded classic projection; broader source-scale/grid and mixed-series
   aggregation acceptance. Existing stored delta interval reports do not establish
   cumulative conversion, reset, interval-boundary, overlap, out-of-order, retry
   or multi-writer semantic acceptance. Do not accumulate acknowledged
   deltas only in node memory or depend on sticky routing. Evaluate retaining
   interval observations and query-time conversion before adding mutable state.
2. **Prometheus interoperability:** explicit versioned OTLP naming/unit/resource
   policies; broader native-histogram/exemplar query/export, remote-write2
   metadata/client/compliance acceptance and newer stateless-mode validation.
   Optional created/start zero-injection and start-aware query/reset semantics
   remain open: do not infer them from source-start retention or add a
   node-local durable reset cache. Projection must
   not silently mix incompatible original instruments or invent counter resets.
3. **OTLP wire breadth:** entity references, remaining
   null/presence/request-shape and unsupported-message validation, broader OTLP
   conformance and broader direct-gRPC client/default-policy acceptance. Keep shared auth, tenant boundaries, budgets and partial-success
   semantics across transports. Validate public independent producers, not only
   hand-built encoders.
4. **Query languages:** expand the remaining PromQL functions/modifiers and
   LogQL against public semantic engines, then full TraceQL types,
   structural/spanset operations and discovery. Preserve
   independent time contracts, same-parent ownership, complete retained context,
   original-type proofs, native accounting and whole-query failure.
5. **Alert compatibility:** native and imported rule/lifecycle semantics, missing
   series/no-data/error behavior, keep-firing, recordings, templates and delivery
   routing/grouping/silences/mutes/resolution. Parser and empty-data passes do not
   establish execution or lifecycle compatibility.
6. **Client and operational acceptance:** versioned public client matrices,
   batching, queues, post-publication ambiguity, TLS/auth gateways, cancellation,
   node-wide pressure, multiple instances and real providers. Keep capacity,
   restore, rollout and cutover acceptance separate from feature semantics.
7. **Storage and scale:** external label/posting indexes; selective column/row-group
   reads; bounded large-history catalogs without finite retention; node-wide
   admission, tenant fairness and aggregate object-store pressure; representative
   cardinality, cold-history and sustained-ingestion measurements. Current native
   footer postings and per-request caps are not completion of these items. See
   [architecture](../docs/architecture.md#storage-model) and
   [storage costs](../docs/storage-costs.md).
8. **Reliability follow-ups:** root-cause the retained compaction timeouts,
   tail-follower configuration-ETS lifetime error and earlier high-parallelism
   failures. Preserve diagnostic evidence and distinguish deliberate fault-test
   output from actual lifecycle defects; do not treat a subsequent passing run
   as closure. Separate-node/provider crash recovery, backup/restore and rolling
   upgrade/rollback/cutover rehearsals require their own acceptance and authorization.

## Required proof for each increment

- Public specification/version pin and clearly identified deviations.
- Independent wire producers and semantic consumers where applicable, retaining
  source inputs, observed bytes and derived expectations as distinct artifacts.
- JSON/protobuf/non-finite/signed-zero/boundary and malformed-input parity tests.
- Native source/expanded-node/work/byte limits before expensive materialization;
  no dropped filters, silent truncation or keyed subset publication.
- S3/manifest/CAS, tenant denial, cross-instance reads, restart and compaction
  regressions; no new non-S3 authority.
- Matching local artifact fences plus explicit all-serving-pool upgrade rules.
- Frozen ordinary replay without external tools/network; optional live exercises
  fail explicitly when opted into without prepared tools.
- Documentation synchronized with implemented behavior and remaining gaps.

## Completed increment: PromQL basic math

- Add `ceil`, `sqrt`, `exp`, `ln`, `log2`, and `log10` using Prometheus 3.5.0's
  one-instant-vector contract. Histograms are omitted, metric names removed and
  other labels retained. Negative domains return NaN; logarithm zero returns
  negative infinity; signed zeros survive. `exp` and `log2` reproduce Go 1.24.5
  arm64 reduction/scaling, including its early exponential underflow, rather
  than promising universal libm or correctly rounded mathematical results.
- Extend the existing offline conformance corpus with 166 independently
  promtool-verified cases, including adjacent overflow inputs, subnormal values,
  staleness and signed-zero reciprocals. The complete 946-case snapshot remains
  linked to its exact input SHA-256; it contains verified Pulso answers, not
  captured upstream API responses. Finite math replay allows one ULP for
  platform differences; 780 exact boolean checks independently establish
  NaN/zero/infinity classes and zero signs, without fuzzy comparison.
- Import 30 verbatim Mimir query-engine integration evaluations, with pinned
  source ranges and AGPL/Apache notices, covering nested calls, histogram
  omission, metric-name collisions and disjoint range-series lifetimes.
  This is a selected suite, not Mimir's Docker/service integration suite.
- Preserve the existing query sample/work/materialization bounds and shared
  HTTP/MCP evaluator. No new state, native ABI, stored format or keyed encoder
  bytes. Every query-serving application must upgrade before callers rely on
  these functions. This closes the basic-math slice, not full PromQL, provider,
  deployed-client or capacity acceptance.

Validation:

- The new parser and IEEE regression tests failed on the unchanged application
  with `invalid_function_arguments`, then passed with the implementation.
- Adversarial review found near-one `log2` cancellation, Go arm64 `exp`
  underflow differences, and fuzzy verification accepting classification
  errors. Source-compatible arithmetic and exact classification checks fix
  these sampled mismatches. Regression failed before the fix; an isolated
  mutation passed fuzzy comparison but failed the new exact classifier.
- Promtool 3.5.0 verified all 946 cases, 780 exact math classifications and
  four exact epoch checks. A separate 4,303-value sweep concentrated on
  adjacent-to-one logs and exponential boundaries: finite answers within
  one ULP, zero/infinity classes exact. This is sampled arm64 validation,
  not an all-input or cross-architecture proof. The
  previous 780 stored oracle cases are unchanged. Focused PromQL/HTTP/MCP tests:
  174 passed, one optional live conformance test skipped. All 30 imported Mimir
  evaluations pass on memory and native S3/Parquet storage: 114 HTTP endpoint
  checks including instant cases replayed as one-step ranges.
- An owned loopback S3 stub exercised all 156 stored math cases through an
  independent runtime, exact-key retries, compaction/cleanup and reader restart.
  MCP, tenant isolation, authorization denial and work-budget rejection passed.
  This earlier temporary validation was removed; the imported upstream replay
  now permanently covers the native S3/Parquet HTTP path.
- `ERL_FLAGS='+S 4:4' mix precommit --max-cases 4`: 1,654 passed, one skipped,
  37 excluded; strict compile, dependency and format checks included. Existing
  compaction timeout/conflict diagnostics remain open; the passing suite does
  not establish provider capacity or close those reliability findings. Strict
  Credo is clean across 433 source files.

## Quoted trace integer validation checkpoint

- `ERL_FLAGS='+S 4:4' mix precommit --max-cases 4 --seed 432996`:
  **1,627 passed, 1 skipped, 37 excluded**, 81.1s; strict application compile,
  unused-dependency and formatting checks included. Strict Credo is clean across
  429 source files. Native crates were built; no native sources/ABI changed and
  native unit tests were not separately rerun for this application-only increment.
- Dedicated corpus: 451 actual public decisions and 102 randomized/explicit-boundary
  normalization cases, native/reference JSON parity, faithful native Parquet and
  gzip HTTP exact-byte keyed replay across independent owned S3 runtime instances.
  The pinned optional Go producer passed offline generation, vet and build.
- Claude reviewed the design and implementation adversarially. No blocking
  implementation findings; corrected the randomized zero generator and added
  explicit zero/uint32-max cases. Public evidence also corrected the initial
  assumption that enum numeric strings should be accepted: they must reject.
- Logs in `.sc/trace-integers/` retain compaction timeout/conflict and manifest-CAS
  diagnostics. The passing suite does not close earlier reliability findings or
  establish capacity. No provider/deployed-client/rollout or commit is implied.

## Current exponential-boundary validation checkpoint

- `ERL_FLAGS='+S 4:4' mix precommit --max-cases 4`: **1,479 passed,
  1 skipped, 37 excluded**, 278.6s. Logs retain compaction timeout diagnostics
  and a test tail-follower ETS-lifetime error; no operational cause/capacity
  closure follows from passing assertions.
- Focused projection/distribution/HTTP tests: **40 passed**, including 64
  randomized mass-collision cases and cross-instance exact-key replay.
- Locked/offline native codec tests: **123 passed**; strict compile, Credo,
  all-target Clippy, formatting and whitespace/local-link checks pass. No Rust
  source/native ABI change was required for this scalar-normalization correction.
- 1,160 public points, 796 actual Prometheus iterator observations, and 20
  separately retained direct-Exp2/decomposed differences. Six-file index SHA256:
  `bfcab3e720eaa1fa0349a42dbab2a40b0a70553b2b1f907d7f0fd3e76514aff7`.
- Prepared public dependency downloads were preparation; final generation/vet/
  build succeeded offline/read-only. Actual linked identities are captured, not
  tool/cache attestation. Ordinary replay requires neither Go nor network.
- No provider/separate-node/deployed-client/capacity or independent-reviewer
  acceptance, commit, rollout or historical projection repair is implied.

## Historical field-validation checkpoint

- `ERL_FLAGS='+S 4:4' mix precommit --max-cases 4`: **1,293 passed,
  1 skipped, 37 excluded**, 310.8s. Logs retain a self-metrics worker timeout and
  ThousandIsland acceptor `noproc` diagnostics; this pass does not establish their
  root cause or capacity acceptance.
- Locked/offline codec tests: **96 passed**; strict all-target Clippy, Credo,
  compile, formatting and whitespace/local-link checks pass.
- 704 public field decisions replay without Go/network in ordinary tests. Offline
  pinned producer generation/vet/build succeeds; actual linked dependency
  versions/checksums are captured, not cryptographic attestation.
- Four-file field-corpus index SHA-256:
  `1d0dedc0dedd1be09c22da1568d80bfb0741e0411b44b950a49bf55223f52768`.
- Source-only GreptimeDB/OpenObserve comparisons do not establish backend,
  deployed-client, provider or default-policy parity. Earlier high-parallelism
  failures remain unresolved; no independent reviewer closure is claimed.

## Historical explicit-distribution validation checkpoint

- `ERL_FLAGS='+S 4:4' mix precommit --max-cases 4`: **1,259 passed,
  1 skipped, 37 excluded** on the final corpus. Earlier full validation passed
  the same counts; no resource ceilings were raised.
- Locked/offline codec tests: **89 passed**; strict all-target Clippy and Rust
  formatting clean. Compile with warnings-as-errors, strict Credo, formatter
  and whitespace/local-link checks clean.
- Optional producer `go vet -mod=readonly ./...` passed offline on the isolated
  pinned module. Public protoc/Google capture and 2,055 Go label cases replay
  without those tools in ordinary tests.
- Final eight-file corpus index SHA-256:
  `ce2cc283a84965c80815b86e6c8d4c74e977cf8117eea28b23ebf20b0034b2d6`.

These are owned loopback/S3-stub contexts, not separate-node/provider/load,
full SDK/application bootstrap, deployed-client or capacity acceptance. The
previous high-parallelism failures remain unresolved.

No commit, deployment or production migration is implied by this plan.
