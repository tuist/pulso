# Public-protocol feature breadth

## Goal and acceptance rule

Build a generic observability backend against public protocol contracts. Real
application inventories, including Tuist, are compatibility examples and useful
regressions, not the scope boundary. No feature is accepted merely because its
currently observed queries parse or its sampled client exports successfully.

Keep a versioned contract matrix with supported forms, explicit rejections,
semantic references, bounds and operational qualifications. The operator-facing
current boundary is [protocol coverage](../docs/protocol-coverage.md).

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
- Quoted trace32-bit integer/null/entity-ref forms, other metric types and actual
  deployment/capacity acceptance remain independent gaps. The earlier generic
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
  were previously extensions. Broader unquoted forms and trace 32-bit string
  handling remain open, not inferred from this component.
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

## Remaining protocol work, independent of application usage

1. **OTLP metric semantics:** native exponential-histogram representation beyond
   existing bounded classic projection; broader source-scale/grid and mixed-series
   aggregation acceptance. Existing stored delta interval reports do not establish
   cumulative conversion, reset, interval-boundary, overlap, out-of-order, retry
   or multi-writer semantic acceptance. Do not accumulate acknowledged
   deltas only in node memory or depend on sticky routing. Evaluate retaining
   interval observations and query-time conversion before adding mutable state.
2. **Prometheus interoperability:** explicit versioned OTLP naming/unit/resource
   policies; broader native-histogram/exemplar query/export and remote-write2/
   metadata acceptance beyond their current working-tree implementations. Projection must
   not silently mix incompatible original instruments or invent counter resets.
3. **OTLP wire breadth:** entity references, remaining quoted trace 32-bit integers,
   null/presence/request-shape and unsupported-message validation, broader OTLP
   conformance and broader direct-gRPC client/default-policy acceptance. Keep shared auth, tenant boundaries, budgets and partial-success
   semantics across transports. Validate public independent producers, not only
   hand-built encoders.
4. **Query languages:** expand PromQL and LogQL against public semantic engines,
   then full TraceQL types, structural/spanset operations and discovery. Preserve
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
