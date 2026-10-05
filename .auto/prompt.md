# Autoresearch: stateless node memory and request capacity

## Objective
Improve Pulso's Rust-backed request hot paths without changing results, protocol semantics, durability, admission limits, or storage format. Work in this active worktree, do not create a branch/worktree. User requested memory optimization and more requests handled, explicitly no benchmark cheating or overfitting.

## Metrics
- Primary: capacity_index (higher): geometric mean of paired candidate/reference throughput ratios multiplied by reference/candidate live worker memory ratios across six fixed request-shaped workloads. Frozen reference is git revision 8876777, built as a second NIF module using `.auto/build-reference.sh` (ignored `.auto/reference/`). Same process, payloads and work; alternate candidate/reference ordering for 7 rounds. Baseline ~1, improvements >1. This controls host contention, not an extra algorithm or benchmark special case. NOT HTTP/S3 production requests/sec.
- Secondary: throughput_rps, live_mb, peak_rss_mb; per-case rps, live_mb and retained result words.
- Live worker memory measured after GC with result held alive: process heap + unique referenced off-heap binaries, including captured inputs. RSS includes BEAM and native heaps across the whole benchmark (high-water mark, noisy).
- Workloads: repeated-series metrics read (8000 samples), high-cardinality metrics read (2000), selective metric read, log Parquet read (2000 varied bodies), JSON decode/encode roundtrip, metric Parquet write (8000). Four concurrent workers, 480 operations, median of seven paired rounds. Fixed scheduler counts. Host load >32 caused >2x swings even after longer runs, so now frozen-reference pairs control those swings. Benchmark baseline reset includes the already-kept metric map sharing. Candidate outputs checked equal to reference (decoded equality for encoded blobs).

## How to Run
`./.auto/measure.sh` compiles release NIFs and runs `.auto/workload.exs`; emits METRIC lines. `.auto/checks.sh` runs full non-integration ExUnit suite excluding bench and Rust codec tests after each successful benchmark.

## Files in Scope
native/pulso_codec/src/* (Rust codec hot paths), native/pulso_object_store/src/* if justified; lib/pulso/{json,storage,codec,remote_write,otlp,logql,promql} and tests for parity/resource safety; architecture docs only if implementation boundary changes. Prefer small Rust allocation/copy reductions first.

## Off Limits
No benchmarking special cases, no changing fixed fixture sizes or budgets to favor a candidate, no dependencies, no skipped tests or weakened validation. No external S3 setup needed; integration tests remain excluded. Do not claim production server capacity from this local codec benchmark. No git manual commits/reverts: log_experiment owns them. `.auto` files preserved across reverts.

## Constraints
Exact semantic parity with existing references, preserve error behavior (including malformed/nonfinite input), bounded caches only, native data lifetime safety, dirty scheduling for bulk work. Run compile warnings-as-errors and full available correctness checks. Add tests for each structural optimization. Preserve timestamp/series tie-order semantics.

## What's Been Tried
- Initial baseline: capacity 262.11, geomean live memory 2.058 MiB.
- Kept consecutive canonical-label sharing: repeated-series live memory 6.36 -> 2.43 MiB, retained words 512000 -> 148002; filtered memory 0.36 -> 0.14 MiB. Full ExUnit/Rust tests pass. Added actual sharing and same-ID/different-label regression tests. Cache is single-entry per Arrow batch and never keyed by hash alone.
- Throughput numbers in first three runs are unreliable: unmodified cases also moved 50–100%. Unchanged verification confirmed exact memory sizes but capacity moved 817 -> 337 due to host contention. Longer sampling did not solve contention; moving to paired frozen-reference sampling.
- Dependencies fetched and MIX_ENV=test compiled successfully. Existing bench tests only cover metrics; this session adds logs and JSON and uses memory as part of objective.
- Discard run 5: borrowing Arrow label bytes/removing arena passed tests; short labels (<64 bytes) have no live memory change, long-label test proved no arena pinning, but absolute throughput comparison confounded. Revisit with paired controls.
- Discard run 6: writer borrowed binary slices instead of per-label HashMap allocations passed tests, improved writer rps slightly while other unchanged cases slowed. Revisit with paired controls.
- Discard run 7: log resource sharing passed tests and deterministically reduced live memory 1.79 -> 1.22 MiB and result words 164288 -> 122540, but severe contention lowered absolute mixed score. Revisit now paired-control assumption changes.
- Inspection: metrics Parquet read rebuilds a labels map and matcher results for every sample even for identical canonical labels; BinaryArena duplicates Arrow offsets and whole repeated label column into Erlang heap. Metric writer copies label keys/values into a HashMap per sample before canonicalizing.
