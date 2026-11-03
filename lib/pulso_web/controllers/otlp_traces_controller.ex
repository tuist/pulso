defmodule PulsoWeb.OTLPTracesController do
  use PulsoWeb, :controller

  @doc "OTLP/HTTP traces: JSON or protobuf, optionally gzip-compressed."
  def create(conn, _params), do: PulsoWeb.OTLPHTTP.export(conn, :traces)
end
