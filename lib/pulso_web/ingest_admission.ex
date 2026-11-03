defmodule PulsoWeb.IngestAdmission do
  @moduledoc """
  Admits ingest requests through `Pulso.IngestAdmission` before their bodies
  are read, answering 429 with `Retry-After` when the node is at its bound.
  """
  @behaviour Plug

  import Plug.Conn

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "POST"} = conn, _opts) do
    case ingest(Enum.map(conn.path_info, &URI.decode/1)) do
      nil -> conn
      signal -> admit(conn, signal)
    end
  end

  def call(conn, _opts), do: conn

  defp admit(conn, signal) do
    case Pulso.IngestAdmission.acquire() do
      :ok ->
        register_before_send(conn, fn conn ->
          Pulso.IngestAdmission.release()
          conn
        end)

      {:error, :ingest_overloaded} ->
        conn = put_resp_header(conn, "retry-after", "1")

        conn =
          if signal == :otlp,
            do: PulsoWeb.OTLPHTTP.error(conn, 429, "ingest capacity exhausted"),
            else: conn |> put_resp_content_type("text/plain") |> send_resp(429, "ingest capacity exhausted")

        halt(conn)
    end
  end

  defp ingest(["v1", signal]) when signal in ["traces", "metrics", "logs"], do: :otlp
  defp ingest(["loki", "api", "v1", "push"]), do: :loki
  defp ingest(["api", "v1", "write"]), do: :remote_write
  defp ingest(["api", "store"]), do: :sentry
  defp ingest(["api", _project, endpoint]) when endpoint in ["envelope", "store"], do: :sentry
  defp ingest(_path), do: nil
end
