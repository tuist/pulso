defmodule PulsoWeb.OTLPController do
  use PulsoWeb, :controller

  alias Pulso.OTLP.Logs

  @doc "OTLP/HTTP JSON or protobuf logs, optionally gzip, at POST /v1/logs."
  def logs(conn, _params), do: PulsoWeb.OTLPHTTP.export(conn, :logs, Logs)
end
