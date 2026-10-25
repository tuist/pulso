# Grafana Mimir math integration cases

These are **30 selected query-engine integration cases**, copied without
changing their input samples, expressions or expected values from
[grafana/mimir at dd039cdd4ba466adb5d28280bf1b8a374a98432e](https://github.com/grafana/mimir/tree/dd039cdd4ba466adb5d28280bf1b8a374a98432e).
This is not Mimir's complete test suite or its Docker/service integration suite.

The upstream entry points are `TestUpstreamTestCases` and `TestOurTestCases` in
[`pkg/streamingpromql/comparisons/comparisons_test.go`](https://github.com/grafana/mimir/blob/dd039cdd4ba466adb5d28280bf1b8a374a98432e/pkg/streamingpromql/comparisons/comparisons_test.go).
They execute the DSL against real query engines; Mimir's own cases also run
against Prometheus. The imported files cover the six basic math functions,
histogram omission, arithmetic/nested calls, NaN/infinities, signed zero,
metric-name collisions, and merging disjoint series lifetimes in range queries.

## Exact source selection

Paths below are relative to Mimir's `pkg/streamingpromql/testdata/` at that
revision. Each local file concatenates the listed inclusive source line ranges,
in order. Only unrelated cases are omitted; expectations are not regenerated
from Pulso.

| Local file | Source | Inclusive ranges | Evaluations |
| --- | --- | --- | --- |
| `upstream-math.test` | `upstream/functions.test` | 1–5, 2017–2030, 2042–2052, 2270–2358 | 21 |
| `mimir-math.test` | `ours/functions.test` | 1–2, 74–77, 90–92, 96–97, 406–412, 437–439, 452–454, 476–484, 503–504 | 7 |
| `collision.test` | `ours/collision.test` | 1–17 | 2 |

For example, reproduce the first extraction from a checkout of that revision:

```sh
sed -n '1,5p;2017,2030p;2042,2052p;2270,2358p' \
  pkg/streamingpromql/testdata/upstream/functions.test
```

## Offline Pulso replay

```sh
mix test test/pulso_web/controllers/prometheus_math_conformance_test.exs
```

The test-only adapter interprets the selected `load`, `clear`, `eval instant`,
`eval range` and `eval_fail` forms. Unsupported syntax fails rather than being
silently skipped. Every evaluation runs through Pulso's HTTP instant/range
query endpoints, using both memory storage and the real native S3/Parquet
adapter against an owned loopback S3 stub. Instant cases also run as one-step
range queries: 57 HTTP requests per adapter, 114 total. Default tests require
no Go executable, running Mimir, Docker, external store or downloaded fixtures.

Series identities, timestamp grids, sample counts and omission are exact.
Finite values use upstream's relative epsilon of `1e-6`; signed zeros compare
equal and NaN signs normalize, as in upstream's comparator. The separate
Prometheus oracle corpus checks zero signs and nonfinite classifications with
exact boolean predicates, and finite replay within one ULP. Pulso translates
upstream's duplicate-label error into its existing HTTP execution diagnostic;
the adapter asserts that exact diagnostic, not an arbitrary failure status.
Error wording is not claimed identical to Mimir.

## License and attribution

The `.test` files retain Mimir's **AGPL-3.0-only** SPDX headers. `LICENSE` is
copied from Mimir's root license. `upstream-math.test` also retains its embedded
**Apache-2.0** Prometheus provenance and copyright notice; `LICENSE-Prometheus`
and `NOTICE-Prometheus` are copied verbatim from Mimir's vendored Prometheus.
These copied test fixtures are not relicensed under Pulso's MIT license. No
Mimir implementation or test-runner code is copied into Pulso's runtime.
