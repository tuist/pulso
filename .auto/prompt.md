# Autoresearch: Pulso Tigris cost amplification

## Objective
Reduce lossless observability storage cost amplification, not telemetry volume by dropping records. User motivated by https://github.com/tuist/tuist/pull/13847 (Grafana rejected samples, high cardinality, routine repeated log context, probe sampling).
Work only in current managed worktree. Do not create branches/worktrees or delegate via sc. Never access production telemetry or buckets.
Read docs/architecture.md before design changes. Object storage remains source of truth; ack only after segment PUT and manifest CAS. No WAL/database/consensus.

## Pricing research
https://www.tigrisdata.com/pricing/ and https://www.tigrisdata.com/pricing.md retrieved during setup.
Matching Global/Standard rates: $0.02/binary GiB-month, $0.005/1000 Class A (PUT/LIST), $0.0005/1000 Class B (GET/HEAD), deletes free; no egress/retrieval; 304 and 412 free. Storage is average daily peak, not wire bytes. Soft deletes/version history can increase retained bytes. IA $0.01/GiB-month + $0.01/GiB retrieval, 30-day minimum; Archive Instant $0.004 + $0.03 retrieval, 90-day minimum; Archive needs restore. No automatic tier changes. HTML and Markdown disagree about multi-region prices: don't claim those verified.

## Metrics
Primary: cost_usd_per_million, lower better. Equal-weight mean across logs/metrics, repeated/churn content, 64/1024/10000-record append batches. Modeled marginal cost at Global Standard (no free tier), retaining 1M records for 30 days and running 1000 storage queries per million records; measured actual native S3 HTTP requests and live bytes against a disposable local HTTP S3 double, including manifest. Each workload runs six append batches and six queries (broad, time-filtered, absent label, repeated after dropping cache). Measures whole-object costs; no fake bandwidth charge. This is a model, not a production bill. No trace ingest exists yet. Not a total compute-cost optimization: monitor runtime and add profiling when useful.
Secondary: retained_bytes_per_million, class_a_per_million, class_b_per_1000_queries, read_bytes_per_1000_queries, encode_us_per_record, query_us_per_record.
Deterministic byte/request metrics need no repeated wall-clock median; timing is diagnostic only. Fixtures are lossless, deterministic digest-based entropy; preserve all shapes and samples across experiments. Inspect per-case costs, not only aggregate.

## How to run
`./.auto/measure.sh` emits METRIC lines; `./.auto/checks.sh` runs all non-integration Elixir tests automatically. Toolchain installed with mise; deps fetched, both release NIFs built. NIF_FORCE_BUILD=1. No live Tigris credentials needed.

## Files in scope
lib/pulso/storage/**, lib/pulso/object_store.ex, native/pulso_codec/src/{segment_parquet,metric_segment_parquet}.rs; storage supervision under lib/pulso/application.ex if needed; related test/pulso/storage/** and test/pulso/codec/**. Runtime/config/chart/docs only to document necessary operator-visible changes. No new deps. Benchmark files live in .auto and aren't production code.

## Off limits / constraints
Do not drop, sample, aggregate, resample, or deduplicate supplied records. Preserve labels/identity, query semantics, idempotent retries, mixed old/new readable codecs and fail-closed manifest behavior. Avoid fixture-specific special cases. No changing workload/rates/weights to win; target changes require reinitialization and new baseline. Correctness checks must pass. No production bucket writes.

## What's been tried
Setup: append writes one object per request; manifest registration coalesces only CAS (10 ms default); segment cache is architecture intent, not implemented. Logs dictionary/zstd level 3; metrics zstd default, 8192 row groups, timestamp dictionary enabled by default may mask explicit delta encoding. Prior compaction preserves all samples.
