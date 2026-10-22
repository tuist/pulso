# Dashboard query contracts

`dashboard-queries.json` is a read-only snapshot of panel query expressions from
Tuist's nine provisioned dashboards. It contains 301 PromQL contracts, one Loki
selector and one TraceQL selector. Only expressions, datasource references,
query type and source basename are exported; no credentials, webhook settings,
Grafana tokens or complete dashboards are included.

Regenerate from an explicitly supplied dashboard checkout:

```sh
mix run --no-start scripts/grafana_inventory.exs /path/to/grafana-dashboards test/fixtures/grafana/dashboard-queries.json
```

Unlike the normalized PromQL fixture, this snapshot preserves dashboard variables
and TraceQL hints. Parser acceptance with representative variable substitutions
is a grammar regression test, not independent numerical conformance or proof of
live Grafana datasource compatibility. Live-only queries, dashboard variables,
recording rules and alert rules must be inventoried separately.
