defmodule PulsoWeb.PrometheusQueryController do
  @moduledoc "Prometheus-compatible instant and range queries over Pulso's stored metrics."
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.Parser
  alias Pulso.PromQL.Time

  def query(conn, params), do: run(conn, params, :instant)
  def query_range(conn, params), do: run(conn, params, :range)

  defp run(conn, params, mode) do
    tenant =
      case get_req_header(conn, "x-scope-orgid") do
        [value | _] when value != "" -> value
        _ -> "default"
      end

    with :ok <- authorize(conn, tenant),
         :ok <- supported_parameters(params, mode),
         query when is_binary(query) <- params["query"],
         {:ok, opts} <- options(params, mode),
         {:ok, opts} <- timeout_options(params, opts),
         {:ok, result} <- Evaluator.query(query, tenant, opts) do
      json(conn, result)
    else
      {:error, :unauthorized} ->
        error(conn, :unauthorized, "unauthorized", "unauthorized")

      {:error, {:unauthorized, _}} ->
        error(conn, :unauthorized, "unauthorized", "unauthorized")

      {:error, :query_execution_failed} ->
        error(conn, :internal_server_error, "execution", "Metric query execution failed.")

      {:error, :query_overloaded} ->
        error(conn, :too_many_requests, "execution", "Query capacity is busy; retry later.")

      {:error, :invalid_stored_sample} ->
        error(conn, :internal_server_error, "execution", "Stored metric data contains an unsupported sample value.")

      {:error, {:storage_error, _}} ->
        error(conn, :service_unavailable, "execution", "Metric storage is unavailable.")

      {:error, reason}
      when reason in [
             :query_sample_limit,
             :query_scan_limit,
             :query_work_limit,
             :query_result_limit,
             :query_resource_limit,
             :query_timeout
           ] ->
        error(
          conn,
          :unprocessable_entity,
          "execution",
          "Query exceeded its resource budget; narrow the selector or time range."
        )

      {:error, _} ->
        error(conn, :bad_request, "bad_data", "Invalid or unsupported query parameters.")

      _ ->
        error(conn, :bad_request, "bad_data", "query is required")
    end
  end

  defp supported_parameters(params, mode) do
    allowed = if mode == :instant, do: ["query", "time", "timeout"], else: ["query", "start", "end", "step", "timeout"]
    if Enum.all?(Map.keys(params), &(&1 in allowed)), do: :ok, else: {:error, :unsupported_parameters}
  end

  defp timeout_options(params, opts) do
    case Map.fetch(params, "timeout") do
      :error ->
        {:ok, opts}

      {:ok, value} ->
        case step(value) do
          {:ok, ns} when ns > 0 -> {:ok, Map.put(opts, :timeout_ms, min(10_000, div(ns + 999_999, 1_000_000)))}
          _ -> {:error, :invalid_timeout}
        end
    end
  end

  defp authorize(conn, tenant) do
    case Auth.verify(conn, tenant) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unauthorized, reason}}
    end
  end

  defp options(params, :instant) do
    with {:ok, time} <- Time.parse_timestamp(Map.get(params, "time", System.system_time(:second))) do
      {:ok, %{end_ts_ns: time}}
    end
  end

  defp options(params, :range) do
    with {:ok, start} <- Time.parse_timestamp(params["start"]),
         {:ok, finish} <- Time.parse_timestamp(params["end"]),
         {:ok, step} <- step(params["step"]) do
      {:ok, %{start_ts_ns: start, end_ts_ns: finish, step_ns: step}}
    end
  end

  defp step(value) when is_binary(value) do
    case Time.parse_seconds(value) do
      {:ok, ns} when ns > 0 -> {:ok, ns}
      _ -> Parser.parse_duration(value)
    end
  end

  defp step(_), do: {:error, :invalid_step}

  defp error(conn, status, type, message) do
    conn |> put_status(status) |> json(%{"status" => "error", "errorType" => type, "error" => message})
  end
end
