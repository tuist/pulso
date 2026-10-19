# Error monitoring (experimental)

Sentry ingestion and bounded, ad hoc diagnosis work without a monitoring worker.
An optional S3-derived index adds retained-history issue summaries and a replayable
change feed for agents. It is a bounded experimental implementation, not Sentry
parity or a throughput/latency guarantee. Enable it separately from ingestion.

The original Parquet logs remain the only event authority. Successful ingestion
still waits only for the segment PUT and log-manifest conditional publication;
index failure never turns accepted events into unaccepted events. Nodes need no
SQL database, durable local disk, queue, or leader election.

## Enable the worker

```sh
PULSO_ERROR_PROJECTION_ENABLED=true
PULSO_ERROR_HISTORY_DAYS=1
PULSO_ERROR_PROJECTION_INTERVAL_MS=5000
```

Helm equivalents are `sentry.projectionEnabled`, `sentry.historyDays` and
`sentry.projectionIntervalMs`. Defaults are `false`, `1` and `5000`. History is
1–365 whole days; cadence is 1000–60000 milliseconds. Invalid integers fail
startup. Only S3-backed nodes start the worker. It discovers projects from
`PULSO_SENTRY_PROJECTS_JSON`; keep project-to-tenant identity fixed.

One pass handles one configured project and up to 32 previously unprocessed
committed segments. Projects rotate, so with N projects each is visited roughly
every N × cadence, plus work duration. Eligible nodes use rendezvous ownership;
conditional S3 writes fence overlapping work during membership changes. Work
shares the reserved bounded background query admission with alert evaluation.

Set log retention comfortably longer than the monitoring history plus expected
downtime/catchup. The index cannot recover events that raw retention already
removed. It never delays raw retention to protect a stalled worker.

## Poll changes, then fetch payloads separately

Tenant bearer credentials authorize these read-only MCP tools; SDK DSN keys do
not. Both tools require `tenant` and numeric `project`, accept `limit` (1–100,
default 50), and return `next_cursor`:

- `list_error_changes`: ordered `baseline_ready`, `new_in_retained_history`,
  `issue_observation` and `reset` changes. Even an empty response returns a cursor
  to reuse for the next poll.
- `list_indexed_error_issues`: small issue summaries with exact deduplicated
  `retained_count`, occurrence `first_seen_ns`/`last_seen_ns`, title, service and
  `latest_example` containing an event ID and timestamp. An incomplete
  reconciliation returns `projection_catching_up`, never partial counts.

For a payload, call `query_errors` with the same tenant/project, the example's
`event_id`, and `start_ts_ns`/`end_ts_ns` equal to its occurrence timestamp. Raw
retention may remove that payload before it is fetched; handle reset/not-found
and empty results rather than interpreting them as recovery.

Persist change cursors in the agent runtime, durably and independently of Pulso
nodes. Process changes idempotently using `change_id`; after acting, save the
returned cursor. A crash between acting and saving can replay the same change.
Subscriptions are not implemented; polling is the durable contract.

## What newness and counts mean

The scope is one tenant, one project, and grouping version `sentry-group-v1`.
Newness means **new in this retained history**, never first ever, new release,
resolved, or reopened. The explicit floor is the greater of the raw log-retention
floor and index time minus `historyDays`. Evidence below it is forgotten. An
issue can therefore become new-in-retained-history again after its last evidence
expires. Changing the configured horizon forces a new baseline.

Bootstrap and rebaseline collect history without announcing historical issues
as new. Only complete reconciliation publishes issue changes or exact counts.
`indexed_at_ns` is discovery/publication time; occurrence timestamps are kept
separate. A late upload inside the horizon is discovered through its committed
segment identity even if its occurrence time precedes the last agent poll.

The index separately remembers processed committed segment keys and deduplicates
occurrences by `(tenant, project, event_id)`. Retries in different random segments
count once. Conflicting copies deterministically select the greatest occurrence
timestamp, then the SHA-256 digest of canonical JSON `[payload, service, issue_id]`;
receipt time does not choose the winner. Counts are not summed segment totals,
and deduplication does not extend beyond the explicit retained-history horizon.

## Freshness, gaps, and resets

Responses expose the last inspected source manifest's ETag, `indexed_at_ns`,
`history_floor_ns`, `horizon_ns`, `backlog_segments_at_index`,
`complete_at_index`, `baseline_pending`, `sequence` and `replay_floor`.
`lag_status` is `unknown` and `lag_ns` is null: zero backlog in that inspected
snapshot does not prove that no newer ingestion exists. Alert on stale index
time in your agent runtime; a failed/overloaded pass leaves the old index intact.

Known unprocessed inputs disappearing force a reset. The worker also resets if
the raw retention floor moves beyond its previously inspected history floor:
an entirely unseen segment could have appeared and expired during downtime.
Current manifests and processed IDs alone cannot disprove that gap. A reset
change records the reason, `last_reset_reason` remains visible, and a fresh
baseline suppresses novelty claims about the missing interval. This guarantee
requires Pulso-managed retention; do not independently expire live segment or
manifest objects using bucket lifecycle policies or out-of-band writes.

Changes live in immutable, digest-checked pages referenced by the same conditional
root that publishes deduplication, summaries and source coverage. Failed or
unconfirmed publications cannot independently emit a change. Retain the last
64 pages, each holding up to 64 changes: this is a count bound, **not a promised
time window**. The root advances `replay_floor` before obsolete pages are deleted.
A cursor below that floor, a missing retained page, a recreated index, or a
changed cursor-signing secret returns `reset_required`. Re-read the baseline and
resume at the advertised floor; never silently skip changes. Issue-list cursors
also reset if their result snapshot changes. Cursors are authenticated, bound to
tenant/project/operation, and require the same `SECRET_KEY_BASE` across nodes.

## Capacity and operations

Per project the index holds at most 4096 candidate segments, 20,000 stored
occurrence copies before deduplication, and an 8 MiB conditional root. A pass
reads at most 32 new segments, less than 64 MiB of segment bytes and 100,000
declared rows. Independently, a **32 MiB decoded-column budget** is cumulative
across all newly decoded segments in that pass, including rows later filtered
out. Highly compressed payloads cannot bypass it; exhaustion returns
`query_decoded_limit` and publishes no progress or changes from that pass.
Indexed titles are at most 2000 UTF-8 bytes, without changing event grouping.
Fresh source enumeration also obeys query metadata/scan budgets;
unknown error/project summaries must scan. Each replay page is at most 256 KiB.
Work runs under the query deadline, heap and admission limits. Exceeding a bound
fails the pass rather than truncating counts or manufacturing continuity.
Small segments, high retry volume, long history and high-cardinality issues can
reach these bounds well before ordinary log queries do. Reduce history, improve
ingest batching, or leave monitoring disabled until a partitioned index is
appropriate for your measured volume. Capacity failure never blocks ingestion.

Derived objects live under `tenants/<tenant>/errors/v1/project=<project>/`.
Obsolete replay-page deletion is best-effort. Interrupted publications and losing
writers can leave unreferenced pages; there is no automatic orphan sweep yet.
To reclaim them, stop all projection workers first, load each project's current
head, and remove only page objects not referenced by that head. Do not run an
uncoordinated lifecycle rule over this prefix. Disabling the worker does not
delete its last index or revoke tenant read access; include this derived prefix
in explicit tenant erasure procedures. Bucket versioning retains old object
versions according to the bucket's separate policy.

No native error alert evaluator, issue lifecycle, symbolication, notifications,
self-service project registry, or lifetime counters are included. See
[error tracking](error-tracking.md) for SDK delivery limitations and the useful
diagnosis-only contract.
