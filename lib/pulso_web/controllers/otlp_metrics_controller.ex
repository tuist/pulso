defmodule PulsoWeb.OTLPMetricsController do
  use PulsoWeb, :controller

  alias Pulso.OTLP.Metrics

  @doc "OTLP/HTTP JSON or protobuf metrics, optionally gzip, at POST /v1/metrics."
  def create(conn, _params), do: PulsoWeb.OTLPHTTP.export(conn, :metrics, Metrics)
end
