# Pulso deployment plan for Tuist

Status: implementation started. Repository inventory completed on October 2, 2026; milestone 1 operational evidence and exit gate remain open.

The goal is to collect and query Tuist's logs, metrics, and traces through Pulso, expose diagnosis tools through Atlas, and progressively replace Grafana Cloud's storage and alerting services. Pulso remains headless. Keeping Grafana for visualization or replacing that experience in Atlas is a separate product decision.

Start with a staging deployment that receives a copy of selected logs and metrics. Production replacement requires traces, query compatibility, alerting, and predictable storage cost and recovery behavior. Each milestone below has an explicit exit gate; shipping endpoints alone does not satisfy those gates.

[Architecture](../docs/architecture.md) remains the source of truth. This plan proposes work and does not change the implemented storage contract. Update the architecture alongside each implementation that changes formats, coordination, signal support, or tool capabilities.

## Implementation progress

- [x] Milestone 1 repository inventory: capture committed collection paths, dashboard requests, compatibility gaps, and downstream consumers in [the versioned workload inventory](tuist-workload-inventory.md) and its reproducible query snapshot.
- [ ] Milestone 1 operational evidence: reconcile deployed paths, capture sanitized exporter payloads, export cloud-managed rules and workflows, measure seven representative days, and approve pilot tenant boundaries and acceptance targets. The milestone exit gate is not complete.
- [x] Pulso query-tool argument validation and read-only annotations (commit `cfafc6e`).
- [ ] Next reviewable implementation: align Pulso's protocol lifecycle and transport for Atlas. This can proceed alongside evidence gathering; it does not authorize deployment before the pilot entry checks pass.

## Constraints

- Object storage holds every acknowledged record until its configured retention expires. Acknowledge ingestion only after segments, required indexes, and manifest publication are durable.
- Keep local buffers and caches disposable. Do not introduce a shared database, local durability log, leader election, or consensus service.
- Reuse `Pulso.Rendezvous` for eligible ingest, maintenance, and rule-evaluation owners. Ownership reduces duplicate work; conditional object writes enforce correctness.
- Keep infrastructure remediation in a separate server. This plan covers diagnosis and writes contained within Pulso, such as alert acknowledgements and silences.
- Prefer compatibility needed by Tuist's actual collectors, dashboards, and alerts over full query-language coverage.
- Bound memory, work, concurrency, and queues for every signal. Return explicit errors rather than incomplete results disguised as success.

## Baseline and integration points

| Area | Implemented baseline | Gap |
| --- | --- | --- |
| Logs | Loki push in both supported wire formats; OpenTelemetry text-format logs; log query endpoints and tools | Binary OpenTelemetry receiver, compaction, retention, production query limits |
| Metrics | Prometheus remote-write version 1; float samples; basic selectors, rates, window functions, and grouped aggregations | Tuist dashboard expressions, stale markers, label indexes, OpenTelemetry receiver |
| Traces | Design documented | Receiver, record model, codec, storage, lookup, search, tools |
| Storage | One Parquet object per append; batched manifest registrations; optional metrics compaction | Buffered segments, bounded manifest metadata, retention, orphan cleanup, segment cache |
| Atlas | Upstream tool proxy with bearer tokens, custom headers, and permission groups | Stateless upstream support, Pulso permission mapping, tool annotations, protocol alignment |
| Operations | Container release and production configuration | Tuist chart, probes, self-monitoring, recovery procedures, capacity measurements |
| Alerting | Design documented | Evaluation, durable transitions, notification delivery, silences, acknowledgements |

Primary code locations in this repository are [the router](../lib/pulso_web/router.ex), [storage adapter](../lib/pulso/storage/s3.ex), [manifest owner](../lib/pulso/storage/s3/manifest_owner.ex), [metrics evaluator](../lib/pulso/promql/evaluator.ex), [log evaluator](../lib/pulso/logql/evaluator.ex), and [tool registry](../lib/pulso/mcp/tools.ex).

Companion changes belong in these repositories:

- `tuist/tuist`: `infra/helm/k8s-monitoring/`, `cache/platform/alloy.nix`, and `infra/grafana-dashboards/` define the collector destinations and query inventory. Include every environment and collectors outside Kubernetes.
- `tuist/atlas`: `lib/atlas/mcp/proxy.ex`, `lib/atlas/mcp/proxy/server.ex`, and `config/runtime.exs` define upstream transport, credentials, permissions, and configuration.
- `tuist/hive`: `lib/hive/forage/grafana.ex` and `grafana_alert.ex` consume Grafana firing and resolved webhook payloads. Inventory any deployed consumers and preserve their contract or migrate them before changing notification sources.

These paths describe the inspected checkouts. Recheck their configuration before implementing companion changes.

## Milestone 1 Establish the workload and replacement contract

Produce a versioned workload fixture and compatibility inventory before promising capacity or savings.

- Inventory all sources, collectors, destinations, tenants, authentication modes, scrape intervals, batching settings, and retry queues. Record trace sampling and current retention for each signal.
- Extract dashboard queries, variable discovery requests, alert expressions, recording rules, contact points, notification receivers, and incident workflows. Include Atlas's alert tools and Hive's alert-triggered agents. Classify each as supported, requiring implementation, or deliberately retained elsewhere.
- Capture sanitized payload fixtures from the actual Alloy and application exporters. Include classic histogram buckets, stale markers, structured logs, and spans with events and links.
- Measure at least seven representative days: accepted records and bytes, batch counts and sizes, active and newly created series, query ranges, concurrency, and trace sampling. Existing destinations remain the reference during this period.
- Record current Grafana Cloud spend and what it buys beyond storage, including alerting and incident management. Separate compute and maintenance costs when comparing Pulso.
- Define the production tenant model and measure dashboard panel fan-out, agent concurrency, and scheduled evaluation concurrency. Start by evaluating environment-scoped internal tenants; choose boundaries for authorization and operational isolation rather than splitting tenants solely to bypass capacity limits.

**Exit gate:** the inventory covers every production telemetry path and critical investigation or alert. Publish measured peak and sustained loads, required query latency, ingest freshness, retention, and monthly cost ceiling. Until measured, all numeric examples are assumptions.

## Milestone 2 Enable Atlas access and a staging deployment

Expose Pulso's existing read tools behind Atlas and deploy it privately with a dedicated staging bucket and tenant.

**Entry condition:** milestone 1 has selected the pilot's tenant boundaries and named tenants. Configure their tokens and collector headers explicitly before deploying the chart.

### Pulso changes

- Align the Model Context Protocol lifecycle and transport with a supported version: negotiate versions, handle initialized notifications, return the specified notification status, validate transport headers and request origin, and handle the optional streaming route explicitly.
- Add liveness and readiness endpoints. Liveness should reflect process health; readiness should fail when the node cannot serve its configured role. Avoid a storage request on every probe by using a bounded periodic check.
- Export self-monitoring for accepted and rejected records, queue depth, ingest latency, query failures, object operations, transferred bytes, and compaction. Keep it independent of Pulso during the initial rollout.
- Add configuration documentation and the Tuist deployment chart: resources, secrets, networking, shutdown grace, storage permissions, and compaction enablement. Start with one node to establish the baseline; add multiple nodes for failure validation later.
- Bound compressed and decompressed request sizes, record counts, attribute sizes, and decompression work before admitting pilot traffic. The current gzip reader inflates the complete body, so a compressed-size limit alone is insufficient. Authenticate as early as the transport permits and bound pre-authentication decode work.

### Atlas and Tuist changes

- Allow stateless upstreams in Atlas. Its current proxy requires a session header that Pulso does not emit. Also respect the negotiated protocol version on subsequent requests.
- Map `pulso` to Atlas's existing `observability` permission group (“Production systems”), rather than the default group.
- Register Pulso with a tenant-scoped bearer secret and an explicit query-tool allowlist. Preserve existing upstreams when configuring `MCP_PROXY_SERVERS`, which replaces the configured server list.
- Keep shared access limited to internal Tuist telemetry. Before exposing customer-specific telemetry, implement subject-aware authorization and audit attribution through the trusted proxy boundary.
- Add a second collector destination for selected logs and metrics. Change authentication from Grafana Cloud credentials to Pulso's bearer token and tenant header. Keep independent retries so Pulso failures do not block the existing destination.
- Require explicit tenant headers in the production deployment, avoiding accidental routing to `default`. Exercise tenant-token rotation with overlapping credentials or a documented coordinated rollout, and keep old credentials valid through the collector rollout window.
- Before real metric traffic, replay stale-marker and non-finite samples through ingestion, storage, compaction, raw queries, and evaluated queries. Establish where unsupported values fail and whether one unsupported sample poisons a whole batch or scan. Enable only proven-safe pilot sources until this passes; full stale-series semantics remain in milestone 5.
- Verify the actual collectors' behavior on status 400, 413, 429, and server errors, connection resets, and lost acknowledgements. Status 429 retries are sender-dependent. Document retry horizons, persistent queue support, queue-full drops, and partial-success accounting. Measure the extra collector memory, disk, and network required for dual delivery.
- Keep the initial pilot low-volume and time-bounded. After a representative 24-hour run, report segment count per hour and signal, manifest size and bytes rewritten, publication latency, and query success. Stop ingestion or reduce scope before hitting measured bounds. At one uncombined segment per second, a signal produces 3,600 objects per hour, exceeding the metrics evaluator's default 1,024-candidate ceiling before other budgets. Buffered ingest and bounded manifest growth in milestone 4 are prerequisites for an uncapped multi-day soak.
- Publish supported pilot query windows per signal in the operator runbook and Atlas tool guidance, based on candidate counts and all scan budgets. Test those windows through Atlas and return an explicit capacity error outside them. At one candidate per second, the 1,024-object ceiling allows less than eighteen minutes even before sample, byte, or work limits; a 24-hour ingestion run does not imply a 24-hour query is supported.

**Exit gate:** an authorized Atlas session can discover and call all four Pulso tools; an unauthorized session cannot. Wrong-tenant access fails. Restarting or disabling Pulso leaves the existing collection path healthy. Validate discovery, initialization, permission filtering, query errors, and timeout behavior across both repositories. The bounded pilot passes the stale-sample and collector-failure checks and its 24-hour storage-growth report; no longer soak begins until its projected metadata and scan budgets are safe.

## Milestone 3 Bound query work and add label indexes

Keep this ahead of OpenTelemetry metrics ingestion, matching the architecture's current follow-up priority.

- Apply supervised deadlines, heap limits, scan-byte and row budgets, result limits, and per-tenant admission to log queries, label discovery, raw sample tools, and future trace queries. Retain the existing metrics evaluator's protections.
- Replace hardcoded query-slot assumptions with configurable global, tenant, and work-class admission. Today the metrics evaluator admits four tasks per node and two per tenant. Reserve capacity for alert evaluation, bound queues, and prevent dashboard or agent bursts from starving scheduled work. Include native calls that outlive a request deadline in occupied-capacity accounting.
- Add metrics label-to-series postings with a versioned index format and manifest references. Canonical labels remain the identity; the stable hash is only an accelerator.
- Define publication rules for required indexes. For older or unindexed segments, retain a correct bounded scan path. Corrupt required indexes must produce an explicit error or a proven complete fallback.
- Measure exact-name and selective-label queries against high-cardinality fixtures. Account for the added index writes and bytes in the cost model.

**Exit gate:** indexed and unindexed queries return equivalent results; hash collisions and missing-label semantics cannot lose series. Selective queries demonstrably reduce downloaded bytes. Over-budget queries fail explicitly, and one tenant cannot exhaust all query capacity. Replay the measured dashboard fan-out alongside agent queries and scheduled evaluations; each class meets its latency target without starvation.

## Milestone 4 Make storage cost and lifecycle predictable

Split this milestone into separately reviewable changes. Query guards from milestone 3 are a prerequisite for broader load testing.

### Buffer and publish segments

- Add pull-based, bounded ingestion with supervised buffers per tenant and signal. Flush on age, bytes, or record count, and bound total buffer memory and waiting requests per node.
- Begin experiments with the architecture's approximately one-second cadence and ten-mebibyte size threshold. Select actual defaults from measured cost and freshness; these are starting points, not capacity guarantees.
- Forward ingestion to eligible owners with bounded timeouts and no forwarding loops. During membership changes, preserve correctness through conditional publication.
- Preserve request identity across combined batches. Define a retry horizon and durable deduplication representation before changing current idempotency behavior. Stable request identity must not depend on which node or flush accepted it.
- Keep requests unacknowledged until all objects they require are published. On node loss, collectors retry unacknowledged data. Document possible duplicates when senders provide no stable identity.
- Choose and test duplicate semantics for each signal when collectors omit `Idempotency-Key`, as standard exporters may do. Evaluate canonical batch identity and exact-record deduplication against legitimate repeated events and differently rebatched retries. Require retry-safe log counts and metric aggregations; a warning about duplicates alone is not sufficient for parity. Define trace span identity and update/conflict handling as part of milestone 6.

### Cache and compact

- Add a bounded local cache of immutable segments, keyed by store identity and object key, with atomic writes, bounded concurrent fills, and eviction. Cache loss must affect only latency and cost.
- Add log compaction using the existing conditional replacement and durable retirement model. Extend it to traces after their storage schema ships.
- Preserve timestamp precision, labels, attributes, and duplicate semantics during compaction. Measure temporary storage amplification, merge reads, and cleanup operations.
- Evaluate range reads and columnar execution with DataFusion after measuring the cache and index gains. Avoid assuming that decoder predicate pushdown reduces network bytes when the whole object was downloaded first.

### Retain and recover

- Define configurable retention per tenant and signal, including the treatment of late records and segments spanning the cutoff. Decide whether boundary segments are rewritten or retained until wholly expired.
- Expire manifest references conditionally before deleting objects after reader grace. Reuse restart-safe retirement cleanup; do not use bucket expiration to delete live objects independently.
- Design bounded manifest partitions and tombstone expiry with the retry horizon. Specify migration, concurrent append and maintenance behavior, and how readers find a complete snapshot before implementation. Avoid replacing one large manifest with an unbounded rewritten root.
- Reclaim abandoned uploads only after a grace period and proof that no published or in-flight manifest can reference them.
- Protect indispensable manifests with recoverable version history or backups. Bound retained backup versions and test restoration; frequent full-manifest versions can themselves become expensive.
- Specify freshness behavior on manifest refresh failures. The current owner serves a cached manifest after refresh errors without a maximum stale age. Bound tolerated staleness for interactive queries and fail closed for alert evaluation when freshness cannot be established. Expose freshness and degraded-state information without returning an apparently current incomplete result.

**Exit gate:** load at the measured production peak plus agreed headroom fits the cost and resource ceilings. Buffered ingestion survives retries and ownership changes without losing acknowledged data. Cache loss preserves results. Retention, compaction, and concurrent writes survive restarts; expired objects disappear without breaking readers; metadata remains bounded across multiple retention windows.

## Milestone 5 Complete the required metrics behavior

Implement the query inventory in dependency order, rather than claiming full Prometheus compatibility.

- Add scalar and vector arithmetic, comparisons, set operators, and vector matching required by Tuist queries.
- Add classic `histogram_quantile`, then the required functions such as `clamp_min`, `label_replace`, `vector`, `time`, `topk`, and `sort_desc`. Classify `quantile_over_time` by its query language, because Tuist also computes quantiles from logs, and verify that path independently. Native histograms and exemplars are separate capabilities, enabled only if the inventory requires them.
- Support stale markers through decode, storage, compaction, and evaluation. Define non-finite sample behavior and out-of-order and duplicate semantics explicitly.
- Add series, metric-name, and label discovery routes needed by Grafana variables and integrations, including Loki series discovery and selector-filtered label discovery where required. Existing Loki label routes ignore query selectors; implement filtering before relying on dashboard variables. Record these requests alongside expressions in the compatibility inventory.
- Add `/v1/metrics` for OpenTelemetry Protocol in text and Protocol Buffers formats, with compression and partial-success behavior. Map resource and scope attributes consistently; define cumulative and delta temporality and restart behavior before accepting delta sums. Reject unsupported types explicitly rather than silently changing their meaning.
- Complete binary-format OpenTelemetry logs using the same decoding and rejection conventions.

**Exit gate:** replay the required metric fixtures through Pulso and Prometheus and compare selector, rate, histogram, arithmetic, and stale-series results with documented numeric tolerances. Every required dashboard query and variable request works, or has an explicitly accepted replacement. Existing Loki and remote-write clients still ingest without application changes.

## Milestone 6 Collect and query traces

Use OpenTelemetry over the [Hypertext Transfer Protocol](https://developer.mozilla.org/en-US/docs/Web/HTTP) first. Alloy can translate existing application exporters, so Pulso does not need an additional native streaming receiver for the initial Tuist rollout.

- Add a span record model and versioned Parquet schema, preserving trace and span identifiers, parent relationships, timestamps, status, resource and scope attributes, span attributes, events, and links.
- Add `/v1/traces` accepting Protocol Buffers and text payloads, with compression, tenant authorization, partial success, payload limits, and the same durable acknowledgement contract.
- In Tuist's Alloy configuration, add a second trace destination using the chart's supported Hypertext Transfer Protocol exporter configuration, Pulso's `/v1/traces` route, bearer authentication, and tenant header. Keep the existing streaming Tempo destination until the gate passes. Application-to-Alloy transports can remain unchanged; verify rendered collector configuration and actual exporter delivery rather than assuming a destination address change is sufficient.
- Extend both storage adapters, manifests, buffering, retention, and compaction to `:traces`. Decide sort order and schema migration in the architecture before shipping the codec.
- Add trace-identifier lookup across all candidate segments and partitions, including spans arriving late or out of order. Add per-segment trace membership filters and bounded attribute search.
- Expose `get_trace` and `query_traces` through Pulso and Atlas. Add a documented path from log trace identifiers to spans and back to related logs. Choose the initial trace-search language from the investigation inventory; full TraceQL support is not a prerequisite.
- Preserve existing collector sampling initially. Add trace-derived service metrics or Tempo-compatible routes only where the inventory requires them.

**Exit gate:** sampled reference traces retain their span counts, relationships, events, and attributes across ingestion, restart, and compaction. Late spans remain discoverable; partial traces are identified honestly. Trace lookup and search meet measured investigation latency and scan budgets. Atlas can correlate a known failure across logs, metrics, and traces.

## Milestone 7 Replace alerting safely

Start once the required expression semantics and storage lifecycle are stable. Inventory-driven alert evaluation can precede trace search completion.

- Store versioned rules in object storage with conditional updates and supervised, rendezvous-assigned evaluators.
- Implement evaluation cadence, pending duration, firing and resolution transitions, missing-data behavior, and execution-error behavior. Query failure must not silently resolve a firing alert.
- Specify pending `for` duration, optional keep-firing behavior, label and annotation templates, notification grouping, repeat intervals, resolved notifications, and stable fingerprints. Define a durable state model for pending, firing, resolved, and repeated notifications; immutable fire records alone do not describe the full lifecycle.
- Define deterministic evaluation and transition identities before implementing immutable fire records. Test overlapping owners, clock skew, restart, and ambiguous write responses.
- Persist delivery progress and retry notifications after crashes. A unique fire record does not guarantee exactly-once delivery to an external webhook; use provider-supported idempotency or deduplication and document delivery guarantees.
- Add routing, acknowledgements, silences with expiry, and audit records. Keep these writes separate from Atlas's diagnosis-only allowlist.
- Implement required recording rules, or identify their retained external evaluator. Document which incident-management responsibilities stay outside Pulso.
- Preserve or migrate downstream payload contracts, especially Hive's fingerprints, firing/resolved status, labels, annotations, and investigation links. Replace Atlas's required alert and silence tools as well as its query tools. Validate with consumer fixtures before changing contact points.

**Exit gate:** run critical rules alongside the existing evaluator for at least seven representative days. Compare pending, firing, and resolved timelines and notifications. Demonstrate notification recovery, overlapping-owner deduplication, silence expiry, and alert behavior during storage and query failures before moving paging responsibility. A refresh or query failure cannot produce a false resolution. Hive threads firing and resolved deliveries correctly and does not start duplicate work from replayed notifications.

## Milestone 8 Validate and cut over production

Run a production pilot for one bounded workload before moving signals broadly. Keep a fallback destination and independent monitoring throughout migration.

1. Load-test representative payloads and query mixes, including high series churn and sparse collectors. Record resource usage, rejected load, freshness, latency, and actual object-store charges.
2. Exercise node termination before and after acknowledgement, collector retries, object-store throttling, lost publication responses, ownership changes, compactor overlap, cache eviction, and manifest restoration. Verify retry-safe log counts and metric aggregations and the documented span conflict policy, including retries that are rebatched.
3. Run logs, metrics, and traces to both destinations for at least fourteen representative days. Compare completeness, required queries, and investigation outcomes at equivalent sampling and retention settings.
4. Move collection per environment and signal. Move paging only after milestone 7. Verify that agents and humans can perform the replacement workflows before removing their former tools.
5. Retain access to historical data until its old retention window expires. Document that switching destinations does not migrate historical data; any backfill requires a separate bounded migration plan.
6. Roll back routing if completeness, freshness, critical alerts, latency, or the agreed cost ceiling fail. Re-enabling the former destination captures new data; it does not automatically restore the interval written only to Pulso. Keep dual delivery through the agreed rollback window.

**Exit gate:** production owners accept the evidence, recovery procedure, ongoing operating cost, and retained responsibilities. Remove each Grafana Cloud dependency only after its replacement gate passes. Decide separately whether Grafana visualization and incident management remain.

## Cost measurement and decision model

Track operations and bytes by purpose: ingest, manifests, indexes, queries, compaction, retention, retries, and backups. Reconcile the application counters against provider billing. Report cost per million accepted records and per representative investigation as well as the monthly total.

Include manifest bytes rewritten per month, conditional-write conflicts and retries, listing operations, full-object query read amplification, and dual-delivery collector overhead in the measured report. These costs do not scale solely with retained telemetry volume.

For a thirty-day month, an initial estimate is:

```text
monthly write operations = 2,592,000 × (segment writes/second + manifest writes/second)
                          + index, maintenance, retry, and backup operations
steady retained bytes   ≈ compressed bytes/day × retention days
monthly cost            = retained storage + writes + reads + listings
                          + data transfer + compute + backup overhead
```

Measure manifest write rate separately from segment rate. Today each append uploads a segment, while manifest registrations are combined over a default ten-millisecond window. Future buffered ingestion changes both rates. Collector count therefore matters as much as total volume. Compaction cannot refund original upload charges.

As reference prices checked on October 2, 2026, Amazon Simple Storage Service (S3) Standard in Northern Virginia charges $0.023 per gigabyte-month in its first storage tier, $5 per million write operations, and $0.40 per million read operations. Cloudflare R2 Standard lists $0.015, $4.50, and $0.36 respectively, with no internet egress charge. See [Amazon's regional price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json), [Amazon's pricing explanation](https://aws.amazon.com/s3/pricing/), and [Cloudflare's pricing](https://developers.cloudflare.com/r2/pricing/). Region, allowances, billing rounding, storage class, and future price changes affect the bill.

Illustrative assumptions: 100 gigabytes of input daily, five-to-one compression, thirty-day retention, and three aggregate incoming batches per second with one manifest write per batch. Steady storage is approximately 600 gigabytes, costing $13.80 monthly on Amazon, and 15.552 million writes cost $77.76. The $91.56 subtotal excludes reads, transfer, indexes, compaction, backups, and compute. It is not an estimate of Tuist's actual usage.

Benchmark Amazon and Cloudflare from Tuist's actual hosting locations before choosing a provider. Check conditional-create and conditional-replace correctness, tail latency, throughput, retry behavior, and recovery support. Repeated full-object downloads from Amazon to Hetzner or Scaleway can dominate storage charges. Avoid colder storage classes until retrieval charges, minimum retention, and small-object billing are modeled.

## Implementation order and decisions

Milestone 1 defines all acceptance targets. Milestone 2 enables a bounded staging pilot with protocol and input-safety entry checks. Milestone 3 protects queries and adds the documented next indexing capability. Milestone 4 makes sustained storage practical and must pass before an uncapped multi-day soak. Milestone 5 completes the required metrics and OpenTelemetry collection paths. Milestone 6 adds traces. Milestone 7 can start after milestones 4 and 5, with trace-dependent alerts waiting for milestone 6. Milestone 8 requires all replacement gates that apply to the workload being moved.

Suggested first reviewable changes are the workload inventory, Pulso transport and tool annotations, Atlas stateless transport and permission mapping, and the staging chart with selected dual delivery. Follow with query admission, label postings, buffered ingest, cache, and storage lifecycle changes in separate reviews. Keep companion repository dependencies explicit in each change.

Resolve these decisions with evidence before the relevant milestone ships:

| Decision | Evidence needed | Due |
| --- | --- | --- |
| Keep Grafana visualization or build Atlas investigations | Required human workflows and cost of replacing them | Before production cutover |
| Storage provider and region | Conditional-write conformance, hosting latency, total measured cost | Before production pilot |
| Retention, late-arrival policy, and retry horizon | Operational and privacy needs, collector retry configuration | Before lifecycle and deduplication changes |
| Manifest partition format | Growth benchmark and concurrent publication/recovery design | Before production scale |
| Metrics types and query scope | Collector, dashboard, and alert inventory | Before compatibility work |
| Trace sampling and search scope | Existing sampling, incident examples, measured volume | Before trace rollout |
| Customer telemetry authorization | Tenant model, trusted actor identity, audit requirements | Before customer data exposure |
| Alert and incident ownership | Required routing, on-call workflows, delivery guarantees | Before moving paging |

## Validation references

An adversarial review by Claude of the original plan at commit `8666478` prompted the early pilot limits, work-class admission, stale-sample checks, explicit duplicate policy, collector trace migration, and downstream alert contracts above. Supporting code was inspected, but application code was not run. Capacity depends on the measured workload; stale-marker failure behavior remains a test requirement rather than a confirmed runtime defect. The one-segment-per-second illustration is per signal, not a claim that the cost example's three aggregate batches all belong to each signal.

Claude's follow-up review found no remaining material planning blocker and recommended explicitly gating deployment on tenant configuration and documenting supported pilot query windows. Both requirements are included in milestone 2. This approves the plan's coverage for starting the inventory and bounded pilot, not production readiness or runtime correctness.

Use the repository's required compilation, formatting, and test checks for implementation changes, plus object-store integration tests against local storage and the selected production provider. Keep protocol and query conformance fixtures independent of the implementation. Documentation-only planning changes do not require starting the application.

Atlas interface changes must also follow its repository verification rules, including local browser verification and before/after screenshots in pull requests where applicable. Pulso's headless endpoints need protocol and load validation.

- [Model Context Protocol lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle) and [transport](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports).
- [OpenTelemetry Protocol](https://opentelemetry.io/docs/specs/otlp/) for receivers, formats, and partial success.
- [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/) and [remote-write specification](https://prometheus.io/docs/specs/prw/remote_write_spec/).
