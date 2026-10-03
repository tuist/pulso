defmodule PulsoWeb.SelfMetrics do
  @moduledoc "Measures HTTP operations, including failures before body parsing completes."

  alias Pulso.SelfMetrics

  @request_key {__MODULE__, :request}
  @ingest_paths %{"/v1/logs" => :otlp, "/loki/api/v1/push" => :loki, "/api/v1/write" => :remote_write}
  @query_paths [
    "/api/v1/query",
    "/api/v1/query_range",
    "/loki/api/v1/query",
    "/loki/api/v1/query_range",
    "/loki/api/v1/labels"
  ]

  def handle_event([:phoenix, :endpoint, :start], _measurements, %{conn: conn}, _config) do
    Process.delete(@request_key)

    if operation = operation(conn) do
      Process.put(@request_key, {operation, System.monotonic_time()})
    end
  end

  def handle_event([:phoenix, :endpoint, :stop], %{duration: duration}, %{conn: conn}, _config) do
    case Process.delete(@request_key) do
      {{kind, dimension}, _start} ->
        outcome = if conn.status < 400, do: :success, else: :error
        SelfMetrics.operation(kind, dimension, outcome, duration)

      nil ->
        :ok
    end
  end

  # Plug.Telemetry's stop event is not emitted for every exception. Phoenix's
  # error renderer uses the original conn on parser failures, losing the pipeline's
  # before-send callbacks. A request-local start covers that path without counting
  # errors twice when the stop event did run.
  def handle_event([:phoenix, :error_rendered], _measurements, _metadata, _config) do
    case Process.delete(@request_key) do
      {{kind, dimension}, start} ->
        SelfMetrics.operation(kind, dimension, :error, System.monotonic_time() - start)

      nil ->
        :ok
    end
  end

  defp operation(conn) do
    path = decode_path(conn.request_path)

    case {conn.method, Map.get(@ingest_paths, path)} do
      {"POST", dimension} when not is_nil(dimension) -> {:ingest, dimension}
      _ -> query_operation(path)
    end
  end

  defp query_operation(path) when path in @query_paths, do: {:query, :http}

  defp query_operation(path) do
    if String.starts_with?(path, "/loki/api/v1/label/"), do: {:query, :http}
  end

  defp decode_path(path) do
    URI.decode(path)
  rescue
    ArgumentError -> path
  end
end
