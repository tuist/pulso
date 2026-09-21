defmodule PulsoWeb.Router do
  use PulsoWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", PulsoWeb do
    pipe_through :api

    post "/mcp", MCPController, :rpc
  end
end
