defmodule PulsoWeb.OTLPController do
  use PulsoWeb, :controller

  @doc "OTLP/HTTP JSON or protobuf logs, optionally gzip, at POST /v1/logs."
  def logs(conn, _params), do: PulsoWeb.OTLPHTTP.export(conn, :logs)
end
