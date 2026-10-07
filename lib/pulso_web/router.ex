defmodule PulsoWeb.Router do
  use PulsoWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", PulsoWeb do
    get "/metrics", MetricsController, :index
    get "/healthz", HealthController, :live
    get "/readyz", HealthController, :ready
  end

  # MCP clients advertise both application/json and text/event-stream, and
  # the transport defines its own status codes, so `/mcp` skips `:accepts`.
  # Origin and media-type checks run in `PulsoWeb.MCPRequestGate`.
  scope "/", PulsoWeb do
    post "/mcp", MCPController, :rpc
    match :*, "/mcp", MCPController, :method_not_allowed
  end

  scope "/", PulsoWeb do
    pipe_through :api

    post "/api/v1/alerting/import/preview", AlertingController, :import_preview
    get "/api/v1/alerting/rules", AlertingController, :index
    get "/api/v1/alerting/rules/:id", AlertingController, :show
    post "/api/v1/alerting/rules/:id", AlertingController, :create
    put "/api/v1/alerting/rules/:id", AlertingController, :update
    delete "/api/v1/alerting/rules/:id", AlertingController, :delete
    get "/api/v1/alerting/rules/:id/changes", AlertingController, :changes
    post "/api/v1/alerting/rules/:id/changes", AlertingController, :changes
    get "/api/v1/alerting/rules/:id/revisions/:revision", AlertingController, :revision
    post "/api/v1/alerting/rules/:id/revisions/:revision", AlertingController, :revision
    post "/api/v1/alerting/rules/:id/restore", AlertingController, :restore
    get "/api/v1/alerting/rules/:id/state", AlertingController, :state
    post "/api/v1/alerting/rules/:id/events", AlertingController, :events
    post "/api/v1/alerting/rules/:id/preview", AlertingController, :preview
    post "/api/v1/alerting/rules/:id/evaluate", AlertingController, :evaluate

    post "/v1/logs", OTLPController, :logs
    post "/loki/api/v1/push", LokiController, :push
    post "/api/v1/write", RemoteWriteController, :write
    get "/api/v1/query", PrometheusQueryController, :query
    post "/api/v1/query", PrometheusQueryController, :query
    get "/api/v1/query_range", PrometheusQueryController, :query_range
    post "/api/v1/query_range", PrometheusQueryController, :query_range

    get "/loki/api/v1/query_range", LokiQueryController, :query_range
    post "/loki/api/v1/query_range", LokiQueryController, :query_range
    get "/loki/api/v1/query", LokiQueryController, :query
    post "/loki/api/v1/query", LokiQueryController, :query
    get "/loki/api/v1/labels", LokiQueryController, :labels
    get "/loki/api/v1/label/:name/values", LokiQueryController, :label_values
  end
end
