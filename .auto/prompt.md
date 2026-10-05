# Autoresearch: stateless node memory and request capacity

## Objective
Improve Pulso's Rust-backed request hot paths without changing results, protocol semantics, durability, admission limits, or storage format. Work in this active worktree, do not create a branch/worktree. User requested memory optimization and more requests handled, explicitly no benchmark cheating or overfitting.

## Metrics
- Primary: capacity_index (higher): geometric mean of concurrent operation throughput divided by live worker MiB across six fixed request-shaped codec workloads. This is a relative local capacity proxy, NOT HTTP/S3 production requests/sec.
- Secondary: throughput_rps, live_mb, peak_rss_mb; per-case rps, live_mb and retained result words.
- Live worker memory measured after GC with result held alive: process heap + unique referenced off-heap binaries, including captured inputs. RSS includes BEAM and native heaps across the whole benchmark (high-water mark, noisy).
- Workloads: repeated-series metrics read (8000 samples), high-cardinality metrics read (2000), selective metric read, log Parquet read (2000 varied bodies), JSON decode/encode roundtrip, metric Parquet write (8000). Four concurrent workers, 480 operations, median of five rounds. Fixed scheduler counts. Round length increased after the first three runs because host load >32 caused >2x swings; benchmark baseline is reset with the already-kept map-sharing optimization.

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
- Throughput numbers in first three runs are unreliable: unmodified cases also moved 50–100%. Unchanged verification confirmed exact memory sizes but capacity moved 817 -> 337 due to host contention. Longer sampling baseline pending.
- Dependencies fetched and MIX_ENV=test compiled successfully. Existing bench tests only cover metrics; this session adds logs and JSON and uses memory as part of objective.
- Inspection: metrics Parquet read rebuilds a labels map and matcher results for every sample even for identical canonical labels; BinaryArena duplicates Arrow offsets and whole repeated label column into Erlang heap. Metric writer copies label keys/values into a HashMap per sample before canonicalizing.
