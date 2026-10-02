# Tuist workload inventory

This is the repository inventory for milestone 1 of [the deployment plan](tuist-deployment-plan.md), inspected on October 2, 2026. It establishes the known collection paths and compatibility backlog before a staging pilot. The repository inventory is complete for the sources listed below. The production workload inventory and milestone exit gate remain open: deployed configuration, cloud-managed rules, sanitized payloads, seven-day measurements, spend, and tenant decisions have not been collected.

## Versioned evidence

The snapshot uses committed files from these revisions. It does not read secret values or assert that these revisions are deployed.

| Repository | Revision | Evidence |
| --- | --- | --- |
| Tuist | `650d726f3831d80785b47eaaed311a648aa4408f` | [Monitoring chart](https://github.com/tuist/tuist/tree/650d726f3831d80785b47eaaed311a648aa4408f/infra/helm/k8s-monitoring), [cache Alloy](https://github.com/tuist/tuist/blob/650d726f3831d80785b47eaaed311a648aa4408f/cache/platform/alloy.nix), [dashboards](https://github.com/tuist/tuist/tree/650d726f3831d80785b47eaaed311a648aa4408f/infra/grafana-dashboards) |
| Atlas | `62c9f7cc957fda4854ec5609cf5cca215df48494` | [Proxy](https://github.com/tuist/atlas/blob/62c9f7cc957fda4854ec5609cf5cca215df48494/lib/atlas/mcp/proxy.ex), [runtime configuration](https://github.com/tuist/atlas/blob/62c9f7cc957fda4854ec5609cf5cca215df48494/config/runtime.exs) |
| Hive | `0b4afe3696db82a7b5e823720f0578e1dad9a51d` | [Grafana receiver](https://github.com/tuist/hive/blob/0b4afe3696db82a7b5e823720f0578e1dad9a51d/lib/hive/forage/grafana.ex), [alert model](https://github.com/tuist/hive/blob/0b4afe3696db82a7b5e823720f0578e1dad9a51d/lib/hive/forage/grafana_alert.ex) |

[The versioned dashboard snapshot](fixtures/tuist-dashboard-queries.json) contains 407 request occurrences across all nine committed dashboards: 385 panel queries (including five Grafana expression queries), twenty variable queries, and two built-in annotation queries. Hidden queries and nested panels are included. Full variable definitions and annotation models are preserved separately as context, without counting them a second time. Occurrences retain their file path and [JavaScript Object Notation Pointer](https://www.rfc-editor.org/rfc/rfc6901) within the source document. Repeated requests are preserved because they contribute to panel fan-out. This is a query fixture, not a captured telemetry payload or evidence of query compatibility.

Reproduce it with Python 3 and Git, using any checkout containing the pinned commit:

```sh
python3 scripts/inventory_tuist_queries.py /path/to/tuist \
  --revision 650d726f3831d80785b47eaaed311a648aa4408f \
  --output plans/fixtures/tuist-dashboard-queries.json
```

The extractor reads committed dashboard objects rather than local modifications. Both dashboard schema versions are handled. Template variables remain unexpanded. Datasource variables take precedence over conflicting query groups. Literal datasource names and identifiers are inferred from unambiguous current datasource selections across the pinned dashboards; this resolves the Oban metrics datasource without guessing from its name. That cross-dashboard inference is repository evidence, not verification of the deployed datasource registry. Conflicting or unknown selections remain `unknown`. The original query group is retained for auditing mismatches. Each dashboard also carries a SHA-256 digest ([Secure Hash Algorithm 256](https://csrc.nist.gov/projects/hash-functions)) of the exact source bytes from Git, preserving line endings. Unsupported query containers and legacy panel entries or version 2 panel nodes without recognized requests fail extraction, except known query-free row, text, and dashboard-list panels; static, interval, constant, and datasource variables remain in the definitions because they do not themselves issue upstream requests. Updating a source revision requires regenerating this fixture and revisiting the compatibility table.

## Collection paths

The Kubernetes wrapper pins Grafana's monitoring chart to 4.0.3. Its common values and three overlays define `tuist-staging`, `tuist-canary`, and `tuist-production`. These cluster labels and the `env` label identify environments; they are not a Pulso authorization boundary or tenant configuration.

| Sources | Collector and destination | Timing and transformations | Authentication and unknowns |
| --- | --- | --- | --- |
| Kubernetes state, kubelet, container resources, Linux hosts, annotated application and infrastructure metrics | Clustered `alloy-metrics` to Grafana Cloud Prometheus remote write | Global scrape interval 30 seconds; allowlists and write relabeling remove unused metrics and transient labels. Application annotations choose ports and paths. | Basic authentication from a Kubernetes Secret populated by 1Password. Rendered chart defaults, retry queues, active targets, and actual batch sizes need deployment evidence. |
| macOS host and tart-kubelet metrics; PostgreSQL instances, pooler, and operator | Five custom scrape jobs in `alloy-metrics`, directly forwarded to `grafana_cloud_metrics` | Explicit 30-second scrapes with clustering enabled. macOS reachability uses operator-managed tailnet egress. | Same metrics destination. Direct forwards must be updated explicitly for dual delivery; changing feature destinations alone is insufficient. |
| Pod logs, selected node journal units, and Kubernetes events | `alloy-logs` for pod/journal logs; singleton collector for events; Grafana Cloud Loki | Journal `maxAge` is four hours, a collection lookback, not storage retention. Event collection must remain singleton-scoped. | Basic authentication from the same Secret. Log batching and retry horizons are not explicitly established by these values. |
| Managed application spans | `alloy-receiver`, accepting both application streaming transport on 4317 and [Hypertext Transfer Protocol](https://developer.mozilla.org/en-US/docs/Web/HTTP) on 4318, to Grafana Cloud Tempo | Server/processor and cache configuration use the streaming exporter. Managed Kura points to `/v1/traces` on 4318. Resource attributes identify service and environment. | Tempo destination uses basic authentication. Sampling and effective processor/exporter defaults need rendered configuration and runtime evidence. |
| Cache hosts outside Kubernetes: application metrics, Unix/process exporters, Alloy self metrics | Host Alloy to Grafana Cloud Prometheus remote write | Cache application: 30 seconds in production, 120 seconds on hosts ending `-staging` or `-canary`. Other scrapes: 30 seconds in production, 60 seconds otherwise. Hostname is the instance label. | Endpoint and basic credentials read from secret files; actual destinations are intentionally not copied. Host membership and retry configuration need deployment evidence. |
| Cache host Docker logs, nginx access/error files, incoming Loki push on port 3100 | Host Alloy to Grafana Cloud Loki | nginx processing creates response counters and classic duration histogram buckets before sampling successful responses at 10%. Other response classes are not sampled by that stage. Incoming timestamps are preserved. | Secret-file basic credentials. Dual delivery must preserve processing order so metrics still count every response. |
| Cache host application spans | Host Alloy receiver on 4317, batch processor, streaming exporter to Grafana Cloud Tempo | Application resource is `tuist-cache`; batch parameters and trace sampling are not explicitly set here. | Secret-file basic credentials. Collector defaults and effective sampling are unverified. |

Additional repository paths must be resolved against deployed releases before declaring production coverage:

- [The embedded observability chart](https://github.com/tuist/tuist/blob/650d726f3831d80785b47eaaed311a648aa4408f/infra/helm/tuist/templates/observability.yaml) conditionally deploys local Loki, Prometheus, Tempo, and an OpenTelemetry collector, with 15-second Prometheus scrapes. Treat this as a separate self-hosted path unless deployment evidence says it is part of the managed workload.
- [Managed values](https://github.com/tuist/tuist/blob/650d726f3831d80785b47eaaed311a648aa4408f/infra/helm/tuist/values-managed-common.yaml) configure receiver addresses; application deployment templates and server/cache/registry runtime configurations complete the exporter wiring. Pod annotations cover server, cache, processor, registry, runners controller, Swift registry sync, and result processing. Verify conditional features and every active annotated target in the live target inventory.
- Kura's local development observability stack and Tuist's Grafana datasource plugin are separate visualization/integration paths. Preserve them until their owners choose otherwise. They do not establish production collection coverage.

No production Pulso tenant or token rotation contract is selected by this inventory. Proposed environment-scoped pilot tenants still require an operator decision, explicit tenant headers, and independent credentials.

## Compatibility and downstream consumers

Classification below is based on Pulso's checked-in parsers, evaluators, routes, and tools, not a successful replay. Requests in the snapshot remain unverified until datasource resolution, template expansion, and reference-result comparison are complete.

| Required behavior | Classification | Evidence and next check |
| --- | --- | --- |
| Loki push and float Prometheus remote write | Implemented, replay required | Existing receivers. Capture actual exporter batches, including histograms and stale markers, before enabling sources. |
| Metric selectors, `rate`, `irate`, `increase`, and grouped aggregations | Implemented subset, replay required | Existing metrics parser/evaluator. Oban uses `irate` on classic `_bucket` series and aggregation by `le`; the complete quantile expression still requires `histogram_quantile`. Unsupported syntax must fail explicitly. |
| Metric arithmetic, comparisons, set operators, and vector matching | Requires implementation | Dashboard expressions include ratios, fallbacks, `on`, and `group_left`; current metrics parser has no binary expressions. |
| Metric `histogram_quantile`, `clamp_min`, `label_replace`, `vector`, `time`, `topk`, `sort_desc`, and `round` | Requires implementation | Function names occur in dashboard requests, including `round` in the Oban requests attributed from committed datasource selections. The existing metrics parser does not accept these functions. |
| Metric label discovery | Requires implementation | Nineteen Prometheus variable requests occur in the snapshot, including `label_values` with selector filters. The twentieth variable request selects Loki and is tracked separately below. No metric discovery routes currently exist. These are discovery requests, not expressions to pass unchanged to the metrics parser. |
| Log filters, extraction, unwrapping, and log `quantile_over_time` | Implemented subset, replay required | Page-load dashboard requests use `logfmt`, `regexp`, `unwrap`, rates, and quantiles; registry and Kura use stream filters and `json`. The parser and pipeline implement these stages; log quantile evaluation lives in `lib/pulso/logql/metric_eval.ex`. Syntax verification and full result replay are separate checks. Log quantiles must be verified separately from metric functions. |
| Loki label discovery | Partially implemented; selector filtering requires implementation | One Kura variable selects the Loki datasource with stream filters. Existing label/label-value routes only read time bounds and ignore the query selector, so they can return pods from unrelated streams. Add selector filtering and replay the actual Grafana request; the capped scan also needs production query guards. |
| Traces and trace-backed service investigations | Requires implementation | Tempo panel and datasource variables in committed dashboards; no Pulso trace receiver or query tools yet. |
| Grafana expression queries, annotation history, datasource selection, transformations, and visualization | Retained externally for this phase | Preserve the original models in the snapshot. Pulso remains headless; storage compatibility alone does not replace Grafana panel processing. |
| Atlas diagnosis tools | Requires integration changes | Four read tools exist in Pulso. Atlas requires a session identifier, hardcodes protocol version `2025-03-26` in initialization and subsequent transport headers, and does not read the server-negotiated version. Add stateless support and negotiated-version propagation. Map `pulso` to `observability`, the internal identifier for “Production systems”; it is not `production-systems`. The unset `MCP_PROXY_SERVERS` default contains Tuist, Grafana, and Sentry; setting it replaces those defaults. Preserve configured upstreams. Read-only filtering relies on tool annotations, making Pulso’s annotations from `cfafc6e` part of integration verification. |
| Hive firing/resolved webhook deliveries | Retained externally until alert migration | Preserve stable fingerprints, labels, annotations, status, timestamps, and investigation links. Hive threads by `(project_id, fingerprint)` and derives a fingerprint when absent. Replay repeated and resolved deliveries against its receiver before switching notifications. |
| Public status page incidents | Retained externally until a separate decision | [The status service](https://github.com/tuist/tuist/blob/650d726f3831d80785b47eaaed311a648aa4408f/status/src/grafana-irm.ts) reads Grafana incident records with bearer authentication. Storage and alert evaluation do not replace this incident-management dependency. |

Atlas's committed default Grafana upstream requests read and write authorization scopes; deployed overrides remain unverified. Its exact deployed tool list, alert/silence workflows, account access, and permission filtering need an authorized live export. Cloud-managed dashboards, recording rules, alert expressions, contact points, notification policies, and retention are also missing; absence from these repositories does not imply absence in production.

## Remaining evidence and exit gate

Complete these tasks before marking milestone 1 done or selecting pilot sources:

- Export deployed collector versions, rendered configurations, active sources/targets, destination identities, effective tenant routing, batching, retry horizons, persistent queues, queue limits, trace sampling, and retention for every environment and host fleet. Compare them with the paths above and reconcile differences.
- Export all cloud dashboards, discovery requests, recording and alert rules, contact points, notification policies, Atlas alert/silence tools, and incident workflows. Assign each critical workflow an owner and an accepted replacement or retained dependency.
- Capture sanitized payloads from actual exporters: classic histogram buckets, stale/non-finite samples, structured logs, and spans with events and links. Record provenance and redaction rules; retain no credentials or customer identifiers. The query snapshot is not a substitute.
- Measure at least seven representative days of accepted records/bytes, batch counts/sizes, sustained and peak load, series churn, query ranges, panel fan-out, agent concurrency, and scheduled evaluation concurrency. Record query latency and ingestion freshness targets with their owners.
- Obtain current invoices and break out storage, alerting, visualization, and incident-management costs. Record the agreed monthly ceiling, retention, and operational ownership. No capacity or savings estimate is established here.
- Choose named pilot tenants and authorization boundaries; document token rotation and approved low-volume sources. Establish supported query windows and the 24-hour storage-growth report required by milestone 2.

The next implementation change is Pulso transport alignment in milestone 2, which can proceed while operational evidence is collected. Deployment remains gated on the milestone 1 pilot decisions and milestone 2 safety checks.

## Validation and adversarial review

The extractor's regression tests cover conflicting query-group metadata, literal datasource selections, hidden nested panels, unresolved names, and omitted query shapes:

```sh
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/inventory_tuist_queries.py /path/to/tuist \
  --revision 650d726f3831d80785b47eaaed311a648aa4408f \
  --output plans/fixtures/tuist-dashboard-queries.json --check
mise exec -- elixir scripts/check_tuist_log_queries.exs
git diff --check
```

Seven regression tests passed, including legacy attribution provenance, untitled panel detection, and preservation of source line endings. Regeneration is byte-identical; check mode also rejected a deliberately changed snapshot without rewriting it. All 407 request models were compared with their committed source nodes, and every pointer resolved. The standalone syntax check parsed all 22 Loki panel expressions after substituting `$__interval=1m`; the separate Loki variable discovery request is not a log expression. The syntax check installs only the pinned parser dependency in Elixir's disposable package cache and loads the existing parser; it does not start Pulso or access object storage. This establishes syntax coverage, not runtime result equivalence, ingestion compatibility, or production performance.

Claude's adversarial review identified incorrect language attribution in three Kura requests, ambiguous request totals, unresolved Oban datasource identity, and imprecise compatibility/Atlas descriptions. Its follow-up identified missing Loki discovery selector filtering and smaller provenance/coverage issues. The extractor now prioritizes resolved datasource variables over query groups, uses unambiguous repository datasource selections for literal names, records source digests, and fails on unsupported query containers or omitted query panels. The inventory distinguishes request types and metric versus log discovery, classifies Loki discovery selector filtering as missing, describes bucket-rate prerequisites, and qualifies Atlas defaults and deployed configuration. All request schemas carry attribution provenance; source digests hash raw Git bytes, and panel guards include untitled entries while allowing known query-free panels. Production payload replay and operational evidence remain open.
