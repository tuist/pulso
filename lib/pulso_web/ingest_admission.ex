defmodule PulsoWeb.IngestAdmission do
  @moduledoc """
  Admits ingest requests through `Pulso.IngestAdmission` before their bodies
  are read, answering 429 with `Retry-After` when the request's signal has no
  reserved or shared slot left.
  """
  @behaviour Plug

  import Plug.Conn

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "POST"} = conn, _opts) do
    case ingest(Enum.map(conn.path_info, &URI.decode/1)) do
      nil -> conn
      {format, signal} -> admit(conn, format, signal)
    end
  end

  def call(conn, _opts), do: conn

  defp admit(conn, format, signal) do
    case Pulso.IngestAdmission.acquire(signal) do
      :ok ->
        register_before_send(conn, fn conn ->
          Pulso.IngestAdmission.release()
          conn
        end)

      {:error, :ingest_overloaded} ->
        conn = put_resp_header(conn, "retry-after", "1")

        conn =
          if format == :otlp,
            do: PulsoWeb.OTLPHTTP.error(conn, 429, "ingest capacity exhausted"),
            else: conn |> put_resp_content_type("text/plain") |> send_resp(429, "ingest capacity exhausted")

        halt(conn)
    end
  end

  # Each request is admitted under the signal class it stores; see
  # `Pulso.IngestAdmission` for the per-signal reserves.
  defp ingest(["v1", "traces"]), do: {:otlp, :traces}
  defp ingest(["v1", "metrics"]), do: {:otlp, :metrics}
  defp ingest(["v1", "logs"]), do: {:otlp, :logs}
  defp ingest(["loki", "api", "v1", "push"]), do: {:text, :logs}
  defp ingest(["api", "v1", "write"]), do: {:text, :metrics}
  defp ingest(["api", "store"]), do: {:text, :logs}
  defp ingest(["api", _project, endpoint]) when endpoint in ["envelope", "store"], do: {:text, :logs}
  defp ingest(_path), do: nil
end
