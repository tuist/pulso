# Independent Prometheus numerical corpus

`conformance.json` defines controlled 30-second input series and expression/time cases, including resets, missing samples, stale markers, NaN/infinities, compensated sums, large values, label matching, classic histograms and Tuist's region enrichment pattern. It covers the new used functions and epoch-aligned nested/offset subqueries. It is synthetic, not production telemetry.

`verified-results.json` contains Pulso's results independently checked by **Prometheus promtool 3.5.0**, linked to the exact source fixture SHA-256. The default test suite checks both the Memory reference path and native Parquet path against these verified results. It does not invoke/download promtool during tests.

Recheck and regenerate explicitly:

```sh
MIX_ENV=test PULSO_CONFORMANCE_RESULTS=test/fixtures/promql/verified-results.json \
  mix run scripts/promql_conformance.exs /path/to/promtool
```

The command is local/offline. It uses a privately owned runtime with memory storage and no production credentials or telemetry. It creates a temporary promtool rule-test file, deletes it on completion and writes verified results **only after** promtool succeeds. Set `PULSO_CONFORMANCE_ARTIFACT=/tmp/failure.yaml` to retain a failed comparison for diagnosis. Use the pinned 3.5.0 oracle when regenerating this snapshot.

Promtool's `fuzzy_compare` tolerates accumulated floating-point ULP rounding, not label differences. YAML NaN bit patterns differ from computed NaNs, so NaN-containing vectors are tested both by an independent `!= bool` mask and by equality-filtered non-NaN values. Top-k inputs avoid ties because tied ranking is not a deterministic Prometheus label-selection contract. Exact large-epoch timestamp comparisons have separate regression coverage.

`alert_inventory_contract_test.exs` additionally checks all 104 distinct Prometheus source expressions in the complete 115-rule inventory. Empty-input execution is a grammar/dispatch check, **not** numerical conformance of all 104 expressions or alerting parity. A paused rule's `$__rate_interval` is represented as `5m` for syntax only; importing/enabling it still needs an explicit approved window.
