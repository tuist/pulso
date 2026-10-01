defmodule PulsoWeb.Router do
  use PulsoWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", PulsoWeb do
    pipe_through :api

    post "/mcp", MCPController, :rpc
    post "/v1/logs", OTLPController, :logs
    post "/loki/api/v1/push", LokiController, :push

    get "/loki/api/v1/query_range", LokiQueryController, :query_range
    post "/loki/api/v1/query_range", LokiQueryController, :query_range
    get "/loki/api/v1/query", LokiQueryController, :query
    post "/loki/api/v1/query", LokiQueryController, :query
    get "/loki/api/v1/labels", LokiQueryController, :labels
    get "/loki/api/v1/label/:name/values", LokiQueryController, :label_values
  end
end
