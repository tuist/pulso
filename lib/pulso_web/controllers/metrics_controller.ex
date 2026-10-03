defmodule PulsoWeb.MetricsController do
  use PulsoWeb, :controller

  def index(conn, _params) do
    conn
    |> put_resp_header("content-type", "text/plain; version=0.0.4; charset=utf-8")
    |> put_resp_header("cache-control", "no-store")
    |> send_resp(:ok, Pulso.SelfMetrics.render())
  end
end
