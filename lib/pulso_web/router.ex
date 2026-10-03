defmodule PulsoWeb.Router do
  use PulsoWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  # MCP clients advertise both application/json and text/event-stream, and
  # the transport defines its own status codes, so `/mcp` skips `:accepts`.
  scope "/", PulsoWeb do
    post "/mcp", MCPController, :rpc
    match :*, "/mcp", MCPController, :method_not_allowed
  end

  scope "/", PulsoWeb do
    pipe_through :api

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
