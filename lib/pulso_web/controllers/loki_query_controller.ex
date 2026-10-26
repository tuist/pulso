defmodule PulsoWeb.LokiQueryController do
  @moduledoc """
  Loki-compatible query endpoints backed by `Pulso.LogQL.Evaluator`.

  Routes served here are the read-side complement of
  `PulsoWeb.LokiController`'s push endpoint. They accept the same
  `X-Scope-OrgID` tenant header and pass every request through
  `Pulso.Auth.verify/2` so a shared-secret deployment applies uniformly
  across ingest and query.

  Finite reads accept `X-Loki-Query-Limits: {"maxQueryLength":"1m"}` as
  an additional request-local time-window ceiling. No other policy fields
  are supported; invalid/ambiguous policies reject rather than run unlimited.
  Existing server admission and resource budgets always remain authoritative.

  Endpoints:

    * `GET|POST /loki/api/v1/query_range` — range query over `[start,
      end]` at `step`. Returns `streams` for log queries and `matrix`
      for metric queries.
    * `GET|POST /loki/api/v1/query` — instant query at `time`. Returns
      `streams` for log queries and `vector` for metric queries.
    * `GET /loki/api/v1/labels` — list of every label name seen across
      the tenant's segments within `[start, end]`.
    * `GET /loki/api/v1/label/:name/values` — list of every value seen
      for one label within `[start, end]`.
  """

  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Entry
  alias Pulso.LogQL.Envelope
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.Parser
  alias Pulso.LogQL.Quantity
  alias Pulso.LogQL.QueryValidation
  alias Pulso.LogQL.StreamMatchers
  alias Pulso.Loki.Detected
  alias Pulso.Loki.Discovery
  alias Pulso.Loki.LabelVolume
  alias Pulso.Loki.Patterns
  alias Pulso.Loki.Tail
  alias Pulso.QueryRunner
  alias Pulso.Storage

  @default_tenant "default"
  @default_direction :backward
  @default_limit 100
  # ---------------------------------------------------------------------------
  # query_range
  # ---------------------------------------------------------------------------

  def query_range(conn, params), do: run_query(conn, params, :range)

  # ---------------------------------------------------------------------------
  # query (instant)
  # ---------------------------------------------------------------------------

  def query(conn, params), do: run_query(conn, params, :instant)

  defp run_query(conn, params, mode) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, query_str} <- require_query(params),
         {:ok, ast} <- parse_query(query_str),
         {:ok, opts} <- build_opts(params, mode),
         :ok <- query_length_limit(conn, query_window_start(ast, opts), opts.end_ts_ns) do
      dispatch_eval(conn, ast, tenant, opts)
    else
      err -> render_error(conn, err)
    end
  end

  defp dispatch_eval(conn, %AST.LogQuery{} = ast, tenant, opts) do
    flags = response_encoding_flags(conn)
    opts = Map.put(opts, :categorize_labels, "categorize-labels" in flags)

    case Evaluator.evaluate_log(ast, tenant, opts) do
      {:ok, streams} -> json(conn, Envelope.streams(streams, flags))
      {:error, reason} -> render_query_error(conn, reason)
    end
  end

  defp dispatch_eval(conn, ast, tenant, opts) do
    flags = response_encoding_flags(conn)

    case Evaluator.evaluate_metric(ast, tenant, opts) do
      {:ok, {:matrix, series}} -> json(conn, Envelope.matrix(series, flags))
      {:ok, {:vector, series}} -> json(conn, Envelope.vector(series, flags))
      {:error, reason} -> render_query_error(conn, reason)
    end
  end

  # Flags are opaque, comma-separated tokens. Like Loki, echo unknown tokens
  # without assigning them behavior. Only the exact known token opts into
  # category tracking; no header leaves the existing representation untouched.
  defp response_encoding_flags(conn) do
    conn
    |> get_req_header("x-loki-response-encoding-flags")
    |> Enum.flat_map(fn
      "" -> []
      value -> String.split(value, ",")
    end)
    |> Enum.uniq()
  end

  # ---------------------------------------------------------------------------
  # labels / label values / series — complete stream-label dictionaries
  # from `Pulso.Loki.Discovery`. Like Loki, an omitted range is the last
  # six hours, and `query` (labels/values) or `match[]` (series) selects
  # streams with log-query matcher semantics.
  # ---------------------------------------------------------------------------

  def labels(conn, params), do: discover(conn, params, :names, "", "query")

  def label_values(conn, %{"name" => name} = params), do: discover(conn, params, :values, name, "query")

  def series(conn, params), do: discover(conn, params, :series, "", "match")

  defp discover(conn, params, mode, key, selector_param) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, selectors} <- discovery_selectors(params, selector_param),
         {:ok, times} <- discovery_times(params),
         :ok <- query_length_limit(conn, times.start_ns, times.end_ns),
         {:ok, json} <- Discovery.query(tenant, mode, key, selectors, times) do
      conn
      |> put_resp_content_type("application/json")
      |> send_resp(200, [~s({"status":"success","data":), json, "}"])
    else
      err -> render_error(conn, err)
    end
  end

  # `match[]` repeats; `query` and Loki's legacy singular `match` do not.
  defp discovery_selectors(params, name) do
    values =
      case params[name] do
        nil -> []
        "" -> []
        value when is_binary(value) -> [value]
        values when is_list(values) -> values
        _ -> :invalid
      end

    if values == :invalid or not Enum.all?(values, &(is_binary(&1) and byte_size(&1) <= 8192)),
      do: {:error, {:invalid_query_parameter, name}},
      else: parse_selectors(values, name)
  end

  defp parse_selectors(values, name) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case Parser.parse(value) do
        {:ok, %AST.LogQuery{selector: selector, stages: []}} -> {:cont, {:ok, [selector | acc]}}
        {:ok, %AST.LogQuery{}} -> {:halt, {:error, :label_matchers_only}}
        _ -> {:halt, {:error, {:invalid_query_parameter, name}}}
      end
    end)
    |> case do
      {:ok, selectors} -> {:ok, Enum.reverse(selectors)}
      error -> error
    end
  end

  defp discovery_times(params) do
    now = System.system_time(:nanosecond)

    with {:ok, start} <- query_timestamp(params["start"], "start"),
         {:ok, finish} <- query_timestamp(params["end"], "end") do
      finish = finish || now
      start = start || max(-9_223_372_036_854_775_808, finish - 6 * 3_600_000_000_000)

      if start > finish,
        do: {:error, {:invalid_query_parameter, "start/end"}},
        else: {:ok, %{start_ns: start, end_ns: finish}}
    end
  end

  # ---------------------------------------------------------------------------
  # index/stats, index/volume, index/volume_range, status/buildinfo
  # ---------------------------------------------------------------------------

  @doc """
  Approximate query size, which Grafana shows before running a query.
  Whole matching segments are counted from manifest metadata, so this is
  an upper bound that never reads logs. Pulso does not index streams, so
  `streams` is not estimated.
  """
  def index_stats(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, selectors} <- discovery_selectors(params, "query"),
         {:ok, times} <- discovery_times(params),
         :ok <- query_length_limit(conn, times.start_ns, times.end_ns),
         {:ok, stats} <- index_stats(tenant, selectors, times) do
      json(conn, %{"streams" => 0, "chunks" => stats.segments, "entries" => stats.entries, "bytes" => stats.bytes})
    else
      err -> render_error(conn, err)
    end
  end

  defp index_stats(tenant, selectors, times) do
    service =
      case selectors do
        [selector] -> selector.matchers |> StreamMatchers.split() |> elem(0)
        _ -> nil
      end

    QueryRunner.run(tenant, :discovery, fn deadline ->
      opts = [start_ts: times.start_ns, end_ts: times.end_ns] ++ QueryRunner.storage_opts(deadline)
      opts = if service, do: Keyword.put(opts, :service, service), else: opts
      Storage.log_index_stats(tenant, opts)
    end)
  end

  @doc """
  Log volume in bytes per stream group or label name, as Loki's volume API.
  Series groups use `targetLabels` or the selector's label names; label
  aggregation counts all stream names unless `targetLabels` is supplied.
  """
  def volume(conn, params), do: volume(conn, params, :instant)

  def volume_range(conn, params), do: volume(conn, params, :range)

  defp volume(conn, params, mode) do
    case Auth.verify(conn, tenant_from(conn)) do
      :ok ->
        if params["aggregateBy"] == "labels",
          do: label_volume(conn, params, mode),
          else: series_volume(conn, params, mode)

      err ->
        render_error(conn, err)
    end
  end

  defp label_volume(conn, params, mode) do
    with {:ok, query, selector} <- volume_selector(params),
         {:ok, ast} <- Parser.parse(query),
         {:ok, times} <- discovery_times(params),
         :ok <- query_length_limit(conn, times.start_ns, times.end_ns),
         {:ok, limit} <- volume_limit(params["limit"]),
         {:ok, targets} <- label_volume_targets(params["targetLabels"], selector),
         {:ok, step} <- label_volume_step(mode, params["step"], times),
         {:ok, result} <- LabelVolume.query(tenant_from(conn), ast, times, targets, limit, step) do
      case result do
        {:vector, series} -> json(conn, Envelope.vector(series))
        {:matrix, series} -> json(conn, Envelope.matrix(series))
      end
    else
      {:error, {:invalid_regex, _, _, _} = reason} -> render_query_error(conn, reason)
      err -> render_error(conn, err)
    end
  end

  defp label_volume_targets(value, _selector) when value in [nil, ""], do: {:ok, nil}
  defp label_volume_targets(value, selector), do: volume_labels(value, selector)

  defp label_volume_step(:instant, _value, _times), do: {:ok, nil}

  defp label_volume_step(:range, value, times) do
    default = "#{max(1, div(times.end_ns - times.start_ns, 250_000_000_000))}s"

    with {:ok, step} <- seconds_or_duration(value || default),
         true <- step >= 1_000_000,
         last = max(times.start_ns, times.end_ns - 1),
         true <-
           Integer.floor_div(last, step) - Integer.floor_div(times.start_ns, step) <
             Pulso.QueryLimits.max_evaluation_steps() do
      {:ok, step}
    else
      _ -> {:error, {:invalid_query_parameter, "step"}}
    end
  end

  defp series_volume(conn, params, mode) do
    with {:ok, query, selector} <- volume_selector(params),
         {:ok, times} <- discovery_times(params),
         :ok <- query_length_limit(conn, times.start_ns, times.end_ns),
         {:ok, limit} <- volume_limit(params["limit"]),
         :ok <- volume_aggregation(params["aggregateBy"]),
         {:ok, labels} <- volume_labels(params["targetLabels"], selector) do
      grouping = Enum.join(labels, ",")
      range_ms = max(1, div(times.end_ns - times.start_ns, 1_000_000))

      {expression, overrides} =
        case mode do
          :instant ->
            {"topk(#{limit}, sum by (#{grouping}) (bytes_over_time(#{query}[#{range_ms}ms])))",
             %{"time" => Integer.to_string(times.end_ns)}}

          :range ->
            step = params["step"] || "#{max(1, div(range_ms, 1000 * 100))}s"

            {"sum by (#{grouping}) (bytes_over_time(#{query}[#{step}]))",
             %{"start" => Integer.to_string(times.start_ns), "end" => Integer.to_string(times.end_ns), "step" => step}}
        end

      run_query(conn, Map.merge(%{"query" => expression}, overrides), mode)
    else
      err -> render_error(conn, err)
    end
  end

  defp volume_selector(%{"query" => query}) when is_binary(query) and byte_size(query) in 1..8192 do
    case Parser.parse(query) do
      {:ok, %AST.LogQuery{selector: selector}} -> {:ok, query, selector}
      _ -> {:error, {:invalid_query_parameter, "query"}}
    end
  end

  defp volume_selector(_params), do: {:error, {:missing_param, "query"}}

  defp volume_limit(nil), do: {:ok, 100}

  defp volume_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit in 1..5000 -> {:ok, limit}
      _ -> {:error, {:invalid_query_parameter, "limit"}}
    end
  end

  defp volume_limit(_value), do: {:error, {:invalid_query_parameter, "limit"}}

  defp volume_aggregation(value) when value in [nil, "", "series"], do: :ok
  defp volume_aggregation(_value), do: {:error, {:invalid_query_parameter, "aggregateBy"}}

  defp volume_labels(value, selector) when value in [nil, ""],
    do: selector.matchers |> Enum.map(& &1.name) |> Enum.uniq() |> valid_labels()

  defp volume_labels(value, _selector) when is_binary(value),
    do: value |> String.split(",", trim: true) |> Enum.map(&String.trim/1) |> Enum.uniq() |> valid_labels()

  defp volume_labels(_value, _selector), do: {:error, {:invalid_query_parameter, "targetLabels"}}

  defp valid_labels(labels) do
    if labels != [] and length(labels) <= 32 and Enum.all?(labels, &Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, &1)),
      do: {:ok, labels},
      else: {:error, {:invalid_query_parameter, "targetLabels"}}
  end

  # ---------------------------------------------------------------------------
  # detected_labels / detected_fields / detected_field values (`Pulso.Loki.Detected`)
  # ---------------------------------------------------------------------------

  @doc "Stream labels and their cardinality, for Grafana's label suggestions."
  def detected_labels(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, selectors} <- discovery_selectors(params, "query"),
         {:ok, window} <- detected_window(params),
         :ok <- query_length_limit(conn, window.start_ns, window.end_ns),
         {:ok, labels} <- Detected.labels(tenant, selectors, window) do
      json(conn, %{"detectedLabels" => Enum.map(labels, &%{"label" => &1.label, "cardinality" => &1.cardinality})})
    else
      err -> render_error(conn, err)
    end
  end

  @doc "Fields detected in the newest `line_limit` lines a log query selects."
  def detected_fields(conn, params) do
    with {:ok, tenant, ast, window, line_limit, limit} <- detected_request(conn, params),
         {:ok, fields} <- Detected.fields(tenant, ast, window, line_limit, limit) do
      json(
        conn,
        if(fields == [], do: %{}, else: %{"fields" => Enum.map(fields, &detected_field_json/1), "limit" => limit})
      )
    else
      err -> render_error(conn, err)
    end
  end

  @doc "Values of one detected field in the newest `line_limit` lines a log query selects."
  def detected_field_values(conn, %{"name" => name} = params) do
    with {:ok, tenant, ast, window, line_limit, limit} <- detected_request(conn, params),
         {:ok, values} <- Detected.values(tenant, ast, name, window, line_limit, limit) do
      json(conn, if(values == [], do: %{}, else: %{"values" => values, "limit" => limit}))
    else
      err -> render_error(conn, err)
    end
  end

  defp detected_field_json(field) do
    base = %{
      "label" => field.label,
      "type" => field.type,
      "cardinality" => field.cardinality,
      "parsers" => field.parsers
    }

    if field.json_path, do: Map.put(base, "jsonPath", field.json_path), else: base
  end

  defp detected_request(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, query_str} <- require_query(params),
         {:ok, ast} <- tail_query(query_str),
         :ok <- tail_validate(ast),
         {:ok, window} <- detected_window(params),
         :ok <- query_length_limit(conn, window.start_ns, window.end_ns),
         {:ok, line_limit} <- detected_limit(params["line_limit"], 100, "line_limit"),
         {:ok, limit} <- detected_limit(params["limit"] || params["field_limit"], 1000, "limit") do
      {:ok, tenant, ast, window, line_limit, limit}
    end
  end

  defp detected_limit(nil, default, _name), do: {:ok, default}

  defp detected_limit(value, _default, name) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 and n <= 5000 -> {:ok, n}
      _ -> {:error, {:invalid_query_parameter, name}}
    end
  end

  defp detected_limit(_value, _default, name), do: {:error, {:invalid_query_parameter, name}}

  # Like Loki: `end` defaults to now and `start` to `since` (default 1h) before it.
  defp detected_window(params) do
    now = System.system_time(:nanosecond)

    with {:ok, start} <- query_timestamp(params["start"], "start"),
         {:ok, finish} <- query_timestamp(params["end"], "end"),
         {:ok, since} <- detected_since(params["since"]) do
      finish = finish || now
      start = start || finish - since

      if start > finish,
        do: {:error, {:invalid_query_parameter, "start/end"}},
        else: {:ok, %{start_ns: start, end_ns: finish}}
    end
  end

  defp detected_since(nil), do: {:ok, 3_600_000_000_000}

  defp detected_since(value) do
    case seconds_or_duration(value) do
      {:ok, n} -> {:ok, n}
      _ -> {:error, {:invalid_query_parameter, "since"}}
    end
  end

  # ---------------------------------------------------------------------------
  # patterns (`Pulso.Loki.Patterns`)
  # ---------------------------------------------------------------------------

  @doc """
  Log patterns with their line counts per `step`, mined from the lines a log
  query selects. `step` defaults to Loki's range step (the window over 250,
  at least one second); at most 11,000 steps per pattern.
  """
  def patterns(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         {:ok, query_str} <- require_query(params),
         {:ok, ast} <- tail_query(query_str),
         {:ok, window} <- detected_window(params),
         :ok <- query_length_limit(conn, window.start_ns, window.end_ns),
         {:ok, step} <- patterns_step(params["step"], window),
         {:ok, patterns} <- Patterns.query(tenant, ast, Map.put(window, :step_ns, step)) do
      data =
        Enum.map(patterns, fn {pattern, samples} ->
          %{"pattern" => pattern, "samples" => Enum.map(samples, fn {ts, n} -> [div(ts, 1_000_000_000), n] end)}
        end)

      json(conn, %{"status" => "success", "data" => data})
    else
      err -> render_error(conn, err)
    end
  end

  defp patterns_step(nil, window),
    do: patterns_steps(max(div(window.end_ns - window.start_ns, 250 * 1_000_000_000), 1) * 1_000_000_000, window)

  defp patterns_step(value, window) do
    case seconds_or_duration(value) do
      {:ok, n} -> patterns_steps(n, window)
      _ -> {:error, {:invalid_query_parameter, "step"}}
    end
  end

  # Float seconds or a Prometheus duration (`5m`, `1h30m`), as Loki parses `step` and `since`.
  defp seconds_or_duration(value) when is_binary(value) do
    case Pulso.PromQL.Time.parse_seconds(value) do
      {:ok, ns} when ns > 0 -> {:ok, ns}
      _ -> Pulso.PromQL.Parser.parse_duration(value)
    end
  end

  defp seconds_or_duration(_value), do: {:error, :invalid_duration}

  defp patterns_steps(step, window) do
    if div(window.end_ns - window.start_ns, step) > 11_000,
      do: {:error, {:invalid_query_parameter, "step"}},
      else: {:ok, step}
  end

  # ---------------------------------------------------------------------------
  # tail — live logs over WebSocket (`PulsoWeb.LokiTail`)
  # ---------------------------------------------------------------------------

  @doc """
  Loki's live tail. Like Loki, the socket first receives the newest `limit`
  entries since `start` (default: one hour ago), oldest first, then every
  newly acknowledged matching entry. `delay_for` (at most 5 seconds) is
  accepted for compatibility; entries are sent as soon as they are appended.
  """
  def tail(conn, params) do
    tenant = tenant_from(conn)
    config = Pulso.Runtime.get_env(:pulso, Tail, [])

    with :ok <- Auth.verify(conn, tenant),
         :ok <- tail_query_limits(conn),
         :ok <- websocket_upgrade(conn),
         {:ok, query_str} <- require_query(params),
         {:ok, %AST.LogQuery{} = ast} <- tail_query(query_str),
         :ok <- tail_validate(ast),
         {:ok, limit} <- tail_limit(params["limit"], Keyword.get(config, :max_limit, 5000)),
         :ok <- tail_delay(params["delay_for"]),
         {:ok, start} <- query_timestamp(params["start"], "start"),
         {:ok, follower} <- Tail.join(tenant, Keyword.get(config, :max_per_tenant, 10)),
         {:ok, history} <- tail_history(ast, tenant, start, limit) do
      state = %{
        tenant: tenant,
        follower: follower,
        compiled: Tail.compile(ast),
        limit: limit,
        max_backlog: Keyword.get(config, :max_backlog, 64),
        history: history
      }

      conn
      |> WebSockAdapter.upgrade(PulsoWeb.LokiTail, state, timeout: :infinity)
      |> halt()
    else
      {:error, :too_many_tails} ->
        error_json(conn, :too_many_requests, "too_many_tails", "The tenant has reached its live tail limit.")

      {:error, :tail_unavailable} ->
        error_json(
          conn,
          :service_unavailable,
          "tail_unavailable",
          "Live tail is temporarily unavailable. Reconnect and query history."
        )

      {:error, {:invalid_query_limits, _}} = err ->
        render_error(conn, err)

      err ->
        :ok = Tail.leave(tenant)
        render_error(conn, err)
    end
  end

  defp websocket_upgrade(conn) do
    upgrade = conn |> get_req_header("upgrade") |> Enum.map(&String.downcase/1)
    if "websocket" in upgrade, do: :ok, else: {:error, {:invalid_query_parameter, "upgrade"}}
  end

  # A live tail has no finite end; do not claim to enforce a duration policy.
  defp tail_query_limits(conn) do
    if get_req_header(conn, "x-loki-query-limits") == [],
      do: :ok,
      else: {:error, {:invalid_query_limits, "not supported for live tail"}}
  end

  defp tail_query(query_str) do
    case parse_query(query_str) do
      {:ok, %AST.LogQuery{}} = ok -> ok
      {:ok, _metric} -> {:error, {:invalid_query_parameter, "query"}}
      error -> error
    end
  end

  defp tail_validate(ast) do
    case QueryValidation.validate(ast) do
      :ok -> :ok
      {:error, reason} -> {:error, {:parse_error, reason}}
    end
  end

  defp tail_limit(nil, _max), do: {:ok, @default_limit}

  defp tail_limit(value, max) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n > 0 and n <= max -> {:ok, n}
      _ -> {:error, {:invalid_query_parameter, "limit"}}
    end
  end

  defp tail_limit(_value, _max), do: {:error, {:invalid_query_parameter, "limit"}}

  defp tail_delay(nil), do: :ok

  defp tail_delay(value) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} when n >= 0 and n <= 5 -> :ok
      _ -> {:error, {:invalid_query_parameter, "delay_for"}}
    end
  end

  defp tail_delay(_value), do: {:error, {:invalid_query_parameter, "delay_for"}}

  # The connection already receives live entries, so the history window
  # ends now and the two overlap rather than leave a gap.
  defp tail_history(ast, tenant, start, limit) do
    now = System.system_time(:nanosecond)
    opts = %{start_ts_ns: start || now - 3_600_000_000_000, end_ts_ns: now, limit: limit, direction: :backward}

    case Evaluator.evaluate_log(ast, tenant, opts) do
      {:ok, streams} ->
        entries =
          for {labels, values} <- streams, {ts, line} <- values do
            %Entry{labels: labels, timestamp_ns: ts, line: line}
          end

        {:ok, Enum.sort_by(entries, & &1.timestamp_ns)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Loki build information; Grafana uses it to detect API features."
  def buildinfo(conn, _params) do
    json(conn, %{
      "version" => "3.5.0",
      "revision" => "pulso-" <> pulso_version(),
      "branch" => "",
      "buildUser" => "",
      "buildDate" => "",
      "goVersion" => ""
    })
  end

  defp pulso_version, do: :pulso |> Application.spec(:vsn) |> to_string()

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp require_query(%{"query" => q}) when is_binary(q) and byte_size(q) > 0, do: {:ok, q}
  defp require_query(_), do: {:error, {:missing_param, "query"}}

  defp parse_query(str) do
    case Parser.parse(str) do
      {:ok, ast} -> {:ok, ast}
      {:error, reason} -> {:error, {:parse_error, reason}}
    end
  end

  # Integer times follow Loki's length rule: at most ten characters means
  # seconds, otherwise nanoseconds. RFC3339 retains nanosecond fractions.
  # Invalid supplied bounds fail before storage; omitted instant time is now.
  # `step` is a duration string (`15s`, `1m`) or an integer seconds value.
  defp build_opts(params, mode) do
    with {:ok, time_opts} <- query_time_opts(params, mode) do
      opts =
        time_opts
        |> maybe_put(:step_ns, step_for(mode, params, time_opts))
        |> maybe_put(:limit, parse_limit(params["limit"]))
        |> maybe_put(:direction, parse_direction(params["direction"]))

      {:ok, opts}
    end
  end

  defp step_for(:range, params, times) do
    default = max(1, div(times.end_ts_ns - times.start_ts_ns, 250_000_000_000)) * 1_000_000_000
    parse_step(params["step"]) || default
  end

  defp step_for(:instant, _params, _times), do: nil

  defp parse_limit(nil), do: @default_limit
  defp parse_limit(v) when is_integer(v) and v > 0, do: v

  defp parse_limit(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} when n > 0 -> n
      _ -> @default_limit
    end
  end

  defp parse_limit(_), do: @default_limit

  defp parse_direction("forward"), do: :forward
  defp parse_direction("backward"), do: :backward
  defp parse_direction(_), do: @default_direction

  defp query_time_opts(params, mode) do
    now = System.system_time(:nanosecond)

    with {:ok, start} <- query_timestamp(params["start"], "start"),
         {:ok, finish} <- query_timestamp(params["end"], "end"),
         {:ok, time} <- query_timestamp(params["time"], "time") do
      finish = if mode == :instant, do: time || finish || now, else: finish || now
      start = start || if(mode == :range, do: max(-9_223_372_036_854_775_808, finish - 3_600_000_000_000))

      if start != nil and start > finish,
        do: {:error, {:invalid_query_parameter, "start/end"}},
        else: {:ok, %{end_ts_ns: finish} |> maybe_put(:start_ts_ns, start)}
    end
  end

  defp query_timestamp(nil, _), do: {:ok, nil}

  defp query_timestamp(value, name) when is_integer(value) or (is_binary(value) and byte_size(value) <= 128) do
    case parse_ts(value) do
      n when is_integer(n) and n >= -9_223_372_036_854_775_808 and n <= 9_223_372_036_854_775_807 -> {:ok, n}
      _ -> {:error, {:invalid_query_parameter, name}}
    end
  end

  defp query_timestamp(_, name), do: {:error, {:invalid_query_parameter, name}}

  defp parse_ts(nil), do: nil
  defp parse_ts(v) when is_integer(v), do: parse_ts(Integer.to_string(v))

  # RFC3339 goes through DateTime for calendar/offset validation. Its stored
  # fraction is microseconds, so restore the final three nanosecond digits.
  defp parse_ts(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} ->
        if byte_size(v) <= 10, do: n * 1_000_000_000, else: n

      _ ->
        parse_rfc3339(v)
    end
  end

  defp parse_rfc3339(text) do
    with true <- Regex.match?(~r/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:\d{2})\z/, text),
         {:ok, dt, _offset} <- DateTime.from_iso8601(text) do
      DateTime.to_unix(dt, :nanosecond) + fractional_ns(text)
    else
      _ -> nil
    end
  end

  defp fractional_ns(text) do
    case Regex.run(~r/[Tt]\d{2}:\d{2}:\d{2}[.,](\d+)(?:[Zz]|[+-]\d{2}:\d{2})$/, text) do
      [_, fraction] -> fraction |> String.slice(0, 9) |> String.pad_trailing(9, "0") |> String.to_integer() |> rem(1000)
      _ -> 0
    end
  end

  defp parse_step(nil), do: nil
  defp parse_step(v) when is_integer(v), do: v * 1_000_000_000

  defp parse_step(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} -> n * 1_000_000_000
      {n, "s"} -> n * 1_000_000_000
      {n, "ms"} -> n * 1_000_000
      {n, "m"} -> n * 60 * 1_000_000_000
      {n, "h"} -> n * 3600 * 1_000_000_000
      _ -> nil
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  # Instant metrics have a zero evaluation window. Their range selectors are
  # not maxQueryLength (Loki calls that separate policy maxQueryInterval).
  # An instant log query without `start` has no finite lower bound here.
  defp query_window_start(%AST.LogQuery{}, opts), do: Map.get(opts, :start_ts_ns)
  defp query_window_start(_ast, opts), do: Map.get(opts, :start_ts_ns, opts.end_ts_ns)

  defp query_length_limit(conn, start, finish) do
    with {:ok, limit} <- request_query_length(conn) do
      cond do
        limit == 0 -> :ok
        start == nil -> {:error, {:invalid_query_limits, "a positive maxQueryLength requires a finite start"}}
        finish - start > limit -> {:error, {:query_length_exceeded, finish - start, limit}}
        true -> :ok
      end
    end
  end

  defp request_query_length(conn) do
    case get_req_header(conn, "x-loki-query-limits") do
      [] -> {:ok, 0}
      [header] when byte_size(header) <= 1024 -> parse_query_limits(header)
      _ -> {:error, {:invalid_query_limits, "expected one policy of at most 1024 bytes"}}
    end
  end

  # Keep object pairs: duplicate (including escaped) keys must not silently
  # replace a stricter policy. This intentionally rejects unsupported fields,
  # unlike Loki's permissive Go struct decoder and malformed-header fallback.
  defp parse_query_limits(header) do
    options = [
      object_start: fn _ -> [] end,
      object_push: fn key, value, acc -> [{key, value} | acc] end,
      object_finish: fn acc, old -> {{:object, acc}, old} end
    ]

    case JSON.decode(header, nil, options) do
      {{:object, []}, nil, ""} -> {:ok, 0}
      {{:object, [{"maxQueryLength", duration}]}, nil, ""} -> query_limit_duration(duration)
      _ -> {:error, {:invalid_query_limits, "expected only maxQueryLength, without duplicate keys"}}
    end
  rescue
    JSON.DecodeError -> {:error, {:invalid_query_limits, "malformed JSON policy"}}
  end

  # Prometheus model.Duration grammar used by the pinned Loki header: ordered,
  # unrepeated integer y/w/d/h/m/s/ms components, or a bare zero. Quantity owns
  # bounded int64 arithmetic; do not use step's float-seconds parser here.
  defp query_limit_duration("0"), do: {:ok, 0}

  defp query_limit_duration(text) when is_binary(text) and byte_size(text) in 1..128 do
    with true <-
           Regex.match?(
             ~r/\A(?:[0-9]+y)?(?:[0-9]+w)?(?:[0-9]+d)?(?:[0-9]+h)?(?:[0-9]+m)?(?:[0-9]+s)?(?:[0-9]+ms)?\z/,
             text
           ),
         {:ok, duration} <- Quantity.duration_literal(text) do
      {:ok, duration}
    else
      _ -> {:error, {:invalid_query_limits, "invalid maxQueryLength duration"}}
    end
  end

  defp query_limit_duration(_), do: {:error, {:invalid_query_limits, "invalid maxQueryLength duration"}}

  defp tenant_from(conn) do
    case Plug.Conn.get_req_header(conn, "x-scope-orgid") do
      [t | _] when is_binary(t) and t != "" -> t
      _ -> @default_tenant
    end
  end

  # -- Error rendering -------------------------------------------------------

  defp render_error(conn, {:error, {:missing_param, name}}) do
    error_json(conn, :bad_request, "missing_param", name)
  end

  defp render_error(conn, {:error, {:invalid_query_parameter, name}}),
    do: error_json(conn, :bad_request, "invalid_parameter", name)

  defp render_error(conn, {:error, {:invalid_query_limits, reason}}),
    do: error_json(conn, :bad_request, "invalid_parameter", "X-Loki-Query-Limits: " <> reason)

  defp render_error(conn, {:error, {:query_length_exceeded, length, limit}}),
    do:
      error_json(
        conn,
        :bad_request,
        "query_limit",
        "the query time range exceeds the limit (query length: #{length}ns, limit: #{limit}ns)"
      )

  defp render_error(conn, {:error, :label_matchers_only}),
    do: error_json(conn, :bad_request, "invalid_parameter", "only label matchers are supported")

  defp render_error(conn, {:error, {:parse_error, reason}}) do
    error_json(conn, :bad_request, "parse_error", inspect(reason))
  end

  defp render_error(conn, {:error, reason}) when reason in [:missing_token, :invalid_token, :unknown_tenant] do
    conn |> put_status(:unauthorized) |> json(%{"status" => "error", "error" => to_string(reason)})
  end

  defp render_error(conn, {:error, reason})
       when reason in [
              :query_overloaded,
              :query_timeout,
              :query_sample_limit,
              :query_scan_limit,
              :query_resource_limit,
              :query_result_limit,
              :query_work_limit,
              :query_intermediate_limit,
              :query_execution_failed
            ], do: render_query_error(conn, reason)

  defp render_error(conn, {:error, reason}),
    do: error_json(conn, :service_unavailable, "storage_unavailable", inspect(reason))

  defp render_query_error(conn, :query_overloaded),
    do: error_json(conn, :too_many_requests, "query_overloaded", "Query capacity is busy.")

  defp render_query_error(conn, :query_timeout),
    do: error_json(conn, :gateway_timeout, "query_timeout", "Query timed out.")

  defp render_query_error(conn, reason) when reason in [:query_execution_failed, :query_resource_limit],
    do: error_json(conn, :internal_server_error, "query_failed", "Query execution failed.")

  defp render_query_error(conn, reason)
       when reason in [
              :query_scan_limit,
              :query_sample_limit,
              :query_result_limit,
              :query_work_limit,
              :query_intermediate_limit
            ], do: error_json(conn, :unprocessable_entity, "query_limit", "Query exceeded its resource budget.")

  defp render_query_error(conn, {:storage_error, _}),
    do: error_json(conn, :service_unavailable, "storage_unavailable", "Storage is unavailable.")

  defp render_query_error(conn, reason) when reason in [:object_store_unavailable, :storage_unavailable],
    do: error_json(conn, :service_unavailable, "storage_unavailable", "Storage is unavailable.")

  defp render_query_error(conn, reason), do: error_json(conn, :bad_request, "evaluation_failed", inspect(reason))

  defp error_json(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{"status" => "error", "error" => code, "message" => message})
  end
end
