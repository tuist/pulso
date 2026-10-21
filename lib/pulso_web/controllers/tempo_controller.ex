defmodule PulsoWeb.TempoController do
  @moduledoc "Tempo v1/v2 exact-ID trace lookup. Unsupported shard/partial modes fail closed."
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.OTLP.Traces
  alias Pulso.PromQL.Parser
  alias Pulso.Traces.Metrics
  alias Pulso.Traces.SearchExpression
  alias Pulso.Traces.Tempo
  alias Pulso.Traces.TempoDiscovery
  alias Pulso.Traces.TempoIntrinsics
  alias Pulso.Traces.TempoSearch

  @conflicts [
    :trace_metadata_unknown,
    :trace_span_id_conflict,
    :trace_root_conflict,
    :trace_root_unknown,
    :trace_timestamp_unknown
  ]

  def tags(conn, params), do: discover(conn, params, 1, :names)
  def tags_v2(conn, params), do: discover(conn, params, 2, :names)
  def tag_values(conn, params), do: discover(conn, params, 1, :values)
  def tag_values_v2(conn, params), do: discover(conn, params, 2, :values)

  defp discover(conn, params, version, mode) do
    with {:ok, tenant} <- tenant(conn),
         :ok <- authorize(conn, tenant),
         {:ok, scope, key} <- discovery_params(params, mode),
         {:ok, window} <- discovery_window(params),
         {:ok, format} <- format(conn),
         {:ok, body} <- TempoDiscovery.query(tenant, scope, key, version, format, params["q"], window) do
      type = if format == "protobuf", do: "application/protobuf", else: "application/json"

      conn
      |> put_resp_content_type(type)
      |> put_resp_header("x-pulso-trace-coverage", discovery_coverage(scope, mode, params["q"], version, key))
      |> put_resp_header("x-pulso-trace-discovery-window", window_coverage(scope, key, window))
      |> send_resp(200, body)
    else
      {:error, reason} -> failure(conn, reason)
    end
  end

  defp discovery_coverage("intrinsic", _mode, _query, _version, "trace:duration"),
    do: "complete-retained-trace-duration; exporter-completeness-unknown"

  defp discovery_coverage("intrinsic", _mode, _query, _version, key)
       when key in ~w(rootName trace:rootName rootServiceName trace:rootService),
       do: "complete-retained-trace-roots; exporter-completeness-unknown"

  defp discovery_coverage(scope, mode, query, version, key) do
    if scope == "intrinsic" and TempoIntrinsics.dynamic_key?(key),
      do: "stored-intrinsic-values; exporter-completeness-unknown",
      else: discovery_coverage(scope, mode, query, version)
  end

  defp discovery_coverage("intrinsic", _mode, _query, _version), do: "static-protocol-vocabulary; not-stored-values"

  defp discovery_coverage("none", :names, _query, 1), do: "stored-context-name-hints; not-key-presence"

  defp discovery_coverage("none", :names, _query, _version),
    do: "stored-context-name-hints-and-static-vocabulary; not-key-presence"

  defp discovery_coverage(_scope, :names, query, _version) when query not in [nil, "", "{}"],
    do: "stored-context-name-hints; not-matching-key-presence"

  defp discovery_coverage(_scope, _mode, _query, _version),
    do: "all-visible-stored-attributes; exporter-completeness-unknown"

  defp discovery_params(params, mode) do
    allowed = if mode == :names, do: ~w(scope q start end), else: ~w(tag_name q start end)

    if Enum.all?(Map.keys(params), &(&1 in allowed)) do
      discovery_target(params, mode)
    else
      {:error, :unsupported_trace_discovery}
    end
  end

  defp discovery_window(params) do
    case {params["start"], params["end"]} do
      {nil, nil} ->
        {:ok, nil}

      {start, finish} ->
        with {:ok, start} when is_integer(start) and start > 0 <- seconds(start),
             {:ok, finish} when is_integer(finish) and finish >= start <- seconds(finish),
             true <- Regex.match?(~r/^[0-9]+$/, params["start"]) and Regex.match?(~r/^[0-9]+$/, params["end"]) do
          {:ok, {start * 1_000_000_000, finish * 1_000_000_000 + 999_999_999}}
        else
          _ -> {:error, :unsupported_parameters}
        end
    end
  end

  defp window_coverage(scope, key, nil) do
    if scope == "intrinsic" and not TempoIntrinsics.dynamic_key?(key),
      do: "static-protocol-vocabulary; window-independent",
      else: "all-retained"
  end

  defp window_coverage("intrinsic", key, _window)
       when key in ~w(rootName trace:rootName rootServiceName trace:rootService trace:duration),
       do: "retained-source-candidates-with-complete-trace-context; not-trace-time-filter"

  defp window_coverage("intrinsic", key, _window),
    do:
      if(TempoIntrinsics.dynamic_key?(key),
        do: "retention-visible-segment-envelope; not-span-time-filter",
        else: "static-protocol-vocabulary; window-independent"
      )

  defp window_coverage(_scope, _key, _window), do: "retention-visible-segment-envelope; not-span-time-filter"

  defp discovery_target(params, :names) do
    scope = if params["scope"] in [nil, ""], do: "none", else: params["scope"]
    with :ok <- TempoDiscovery.target(scope, ""), do: {:ok, scope, ""}
  end

  defp discovery_target(%{"tag_name" => name}, :values) when is_binary(name) do
    if TempoIntrinsics.value_key?(name) do
      {:ok, "intrinsic", name}
    else
      stored_discovery_target(name)
    end
  end

  defp discovery_target(_params, _mode), do: {:error, :unsupported_trace_discovery}

  defp stored_discovery_target(name) do
    case String.split(name, ".", parts: 2) do
      ["", key] when key != "" ->
        with :ok <- TempoDiscovery.validate("none", key), do: {:ok, "none", key}

      [scope, key] when key != "" and scope in ["resource", "span", "instrumentation", "event", "link"] ->
        with :ok <- TempoDiscovery.validate(scope, key), do: {:ok, scope, key}

      _ ->
        {:error, :unsupported_trace_discovery}
    end
  end

  def search(conn, params) do
    with {:ok, tenant} <- tenant(conn),
         :ok <- authorize(conn, tenant),
         true <- Enum.all?(Map.keys(params), &(&1 in ~w(q start end limit spss))),
         {:ok, start} <- seconds(params["start"]),
         {:ok, finish} <- seconds(params["end"]),
         true <- is_integer(start) and is_integer(finish) and start > 0 and finish >= start,
         {:ok, limit} <- positive(params["limit"], 100, 1000),
         {:ok, spss} <- positive(params["spss"], 3, 100),
         {:ok, format} <- format(conn),
         {:ok, body} <-
           TempoSearch.query(
             tenant,
             params["q"],
             [trace_start: start * 1_000_000_000, trace_end: finish * 1_000_000_000, limit: limit, spss: spss],
             format
           ) do
      type = if format == "protobuf", do: "application/protobuf", else: "application/json"

      conn
      |> put_resp_content_type(type)
      |> put_resp_header("x-pulso-trace-coverage", "all-visible-stored-context; exporter-completeness-unknown")
      |> send_resp(200, body)
    else
      false -> failure(conn, :unsupported_parameters)
      {:error, reason} -> failure(conn, reason)
    end
  end

  @doc "TraceQL metrics over a range (`Pulso.Traces.Metrics`). Requires Tempo search to be enabled."
  def metrics_query_range(conn, params), do: metrics(conn, params, false)

  @doc "Instant TraceQL metrics: one value per series over `[start, end]`."
  def metrics_query(conn, params), do: metrics(conn, params, true)

  defp metrics(conn, params, instant) do
    with {:ok, tenant} <- tenant(conn),
         :ok <- authorize(conn, tenant),
         :ok <- TempoSearch.available(),
         {:ok, query} <- metrics_text(params),
         {:ok, window} <- metrics_window(params, instant),
         {:ok, body} <- Metrics.query(tenant, query, window, instant) do
      conn
      |> put_resp_content_type("application/json")
      |> put_resp_header("x-pulso-trace-coverage", "all-visible-stored-spans; exporter-completeness-unknown")
      |> send_resp(200, body)
    else
      {:error, reason} -> failure(conn, reason)
    end
  end

  # Tempo reads `q`, falling back to the Prometheus-style `query`.
  defp metrics_text(%{"q" => q}) when is_binary(q) and q != "", do: {:ok, q}
  defp metrics_text(%{"query" => q}) when is_binary(q) and q != "", do: {:ok, q}
  defp metrics_text(_params), do: {:error, :unsupported_parameters}

  # As Tempo: `end` defaults to now, `start` to `since` (default 1h) before
  # `end`; times are Unix seconds (at most ten digits), nanoseconds, decimal
  # seconds or RFC3339; `step` is decimal seconds or a Go duration.
  defp metrics_window(params, instant) do
    now = System.system_time(:nanosecond)

    with {:ok, finish} <- tempo_time(params["end"], now),
         {:ok, since} <- tempo_duration(params["since"], 3_600_000_000_000),
         {:ok, start} <- tempo_time(params["start"], min(finish, now) - since),
         {:ok, step} <- if(instant, do: {:ok, nil}, else: tempo_duration(params["step"], nil)) do
      {:ok, {start, finish, step}}
    end
  end

  defp tempo_time(nil, default), do: {:ok, default}

  defp tempo_time(value, _default) when is_binary(value) and byte_size(value) <= 64 do
    cond do
      Regex.match?(~r/\A[0-9]{1,10}\z/, value) ->
        {:ok, String.to_integer(value) * 1_000_000_000}

      Regex.match?(~r/\A[0-9]{11,19}\z/, value) ->
        {:ok, String.to_integer(value)}

      Regex.match?(~r/\A[0-9]+\.[0-9]+\z/, value) ->
        [whole, fraction] = String.split(value, ".")
        millis = fraction |> String.pad_trailing(3, "0") |> String.slice(0, 3) |> String.to_integer()
        {:ok, String.to_integer(whole) * 1_000_000_000 + millis * 1_000_000}

      true ->
        case DateTime.from_iso8601(value) do
          {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime, :nanosecond)}
          _ -> {:error, :unsupported_parameters}
        end
    end
  end

  defp tempo_time(_value, _default), do: {:error, :unsupported_parameters}

  defp tempo_duration(nil, default), do: {:ok, default}

  defp tempo_duration(value, _default) when is_binary(value) do
    case Pulso.PromQL.Time.parse_seconds(value) do
      {:ok, ns} when ns > 0 ->
        {:ok, ns}

      _ ->
        with {:error, _} <- SearchExpression.go_duration(value),
             {:error, _} <- Parser.parse_duration(value) do
          {:error, :unsupported_parameters}
        else
          {:ok, ns} when ns > 0 -> {:ok, ns}
          _ -> {:error, :unsupported_parameters}
        end
    end
  end

  defp tempo_duration(_value, _default), do: {:error, :unsupported_parameters}

  defp positive(nil, default, _max), do: {:ok, default}

  defp positive(value, _default, max) do
    case seconds(value) do
      {:ok, n} when n >= 1 and n <= max -> {:ok, n}
      _ -> {:error, :unsupported_parameters}
    end
  end

  def trace(conn, params), do: lookup(conn, params, false)
  def trace_v2(conn, params), do: lookup(conn, params, true)

  defp lookup(conn, params, v2) do
    with {:ok, tenant} <- tenant(conn),
         :ok <- authorize(conn, tenant),
         :ok <- parameters(params),
         {:ok, trace_id} <- trace_id(params["trace_id"]),
         {:ok, format} <- format(conn),
         export_format = if(v2, do: format <> "_v2", else: format),
         {:ok, body} <- Tempo.lookup(tenant, trace_id, export_format) do
      type = if format == "protobuf", do: "application/protobuf", else: "application/json"

      conn
      |> put_resp_content_type(type)
      |> put_resp_header("x-pulso-trace-coverage", "all-visible-stored-spans; exporter-completeness-unknown")
      |> send_resp(200, body)
    else
      {:error, reason} -> failure(conn, reason)
    end
  end

  defp tenant(conn) do
    case get_req_header(conn, "x-scope-orgid") do
      [] -> {:ok, "default"}
      [value] when value != "" -> {:ok, value}
      _ -> {:error, :invalid_tenant_header}
    end
  end

  defp authorize(conn, tenant) do
    if Auth.verify(conn, tenant) == :ok, do: :ok, else: {:error, :unauthorized}
  end

  defp trace_id(value) when is_binary(value) and byte_size(value) in 1..32 do
    Traces.id(String.pad_leading(value, 32, "0"), 32)
    |> case do
      {:ok, id} -> {:ok, id}
      _ -> {:error, :invalid_trace_id}
    end
  end

  defp trace_id(_value), do: {:error, :invalid_trace_id}

  defp parameters(params) do
    allowed = ~w(trace_id start end mode allowPartialTrace)

    with true <- Enum.all?(Map.keys(params), &(&1 in allowed)),
         true <- Map.get(params, "mode", "all") == "all",
         true <- Map.get(params, "allowPartialTrace", "false") == "false",
         :ok <- time_hints(params) do
      :ok
    else
      _ -> {:error, :unsupported_parameters}
    end
  end

  defp time_hints(params) do
    with {:ok, start} <- seconds(params["start"]),
         {:ok, finish} <- seconds(params["end"]),
         true <- is_nil(start) or is_nil(finish) or start <= finish do
      :ok
    else
      _ -> {:error, :unsupported_parameters}
    end
  end

  defp seconds(nil), do: {:ok, nil}

  defp seconds(value) when is_binary(value) and byte_size(value) <= 10 do
    case Integer.parse(value) do
      {number, ""} when number in 0..4_294_967_295 -> {:ok, number}
      _ -> {:error, :unsupported_parameters}
    end
  end

  defp seconds(_value), do: {:error, :unsupported_parameters}

  defp format(conn) do
    case get_req_header(conn, "accept") do
      ["application/protobuf"] -> {:ok, "protobuf"}
      values -> json_format(values)
    end
  end

  defp json_format(values) do
    types =
      Enum.flat_map(values, &String.split(&1, ",")) |> Enum.map(&(&1 |> String.split(";") |> hd() |> String.trim()))

    if types == [] or Enum.any?(types, &(&1 in ["application/json", "*/*"])),
      do: {:ok, "json"},
      else: {:error, :unsupported_accept}
  end

  defp failure(conn, :unauthorized), do: error(conn, 401, "unauthorized")

  defp failure(conn, :invalid_tenant_header),
    do: error(conn, 400, "exactly one nonempty tenant header is required when supplied")

  defp failure(conn, :tempo_search_disabled),
    do:
      error(
        conn,
        501,
        "experimental Tempo search disabled; requires explicit operator opt-in and retained-context capacity validation"
      )

  defp failure(conn, :not_found), do: send_resp(conn, 404, "")

  defp failure(conn, reason)
       when reason in [
              :invalid_trace_id,
              :unsupported_parameters,
              :unsupported_trace_selector,
              :unsupported_trace_discovery,
              :unexpanded_trace_variable,
              :query_expression_limit
            ], do: error(conn, 400, "invalid or unsupported trace lookup parameters")

  defp failure(conn, {:invalid_tenant, _}), do: error(conn, 400, "invalid tenant")
  defp failure(conn, {:decode_failed, _}), do: error(conn, 500, "stored trace data is invalid")
  defp failure(conn, :query_execution_failed), do: error(conn, 500, "trace query execution failed")

  defp failure(conn, :unsupported_accept),
    do: error(conn, 406, "supported trace response formats: application/json, application/protobuf")

  defp failure(conn, :query_overloaded),
    do: conn |> put_resp_header("retry-after", "1") |> error(429, "query capacity busy")

  defp failure(conn, :query_timeout), do: error(conn, 504, "trace query timed out")

  defp failure(conn, reason) when reason in @conflicts, do: error(conn, 409, Atom.to_string(reason))

  defp failure(conn, reason)
       when reason in [
              :query_scan_limit,
              :query_sample_limit,
              :query_resource_limit,
              :trace_group_limit,
              :trace_duration_limit,
              :trace_discovery_work_limit
            ], do: error(conn, 422, "complete trace exceeds query safety budgets")

  defp failure(conn, :unsupported_trace_tag_type),
    do:
      error(
        conn,
        422,
        "trace tag values require original string, int64 or boolean types; floats/complex values are unsupported, not coerced or omitted"
      )

  defp failure(conn, :invalid_trace_export), do: error(conn, 500, "stored trace cannot be exported faithfully")
  defp failure(conn, _reason), do: error(conn, 503, "trace storage unavailable")
  defp error(conn, status, message), do: conn |> put_resp_content_type("text/plain") |> send_resp(status, message)
end
