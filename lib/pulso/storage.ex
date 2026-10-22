defmodule Pulso.Storage do
  @moduledoc """
  Behaviour for signal storage backends and the runtime dispatcher.

  Every call carries an explicit `signal` — `:logs`, `:metrics`, or `:traces` — so the
  same behaviour, dispatcher, and manifest machinery serve all signal
  types. Each signal owns its own record struct (`Pulso.Record.Log`,
  `Pulso.Record.MetricSample`, `Pulso.Record.Span`) and its own query-opts shape; the adapter
  dispatches on `signal` to pick the right encoder, decoder, and sort
  order.

  `:max_records` is a hard rejection bound, not a truncation request. Without
  an explicit `:limit`, adapters return the complete selected records or an error,
  never an unmarked subset. The root-discovery reference also uses a sentinel row.

  The active adapter is read from application configuration at call time
  so it can be swapped in tests without recompiling. In dev and prod the
  default adapter is `Pulso.Storage.S3`; tests fall back to
  `Pulso.Storage.Memory` because no adapter is configured.
  """

  alias Pulso.Codec.NIF
  alias Pulso.LogQL.Entry
  alias Pulso.Loki.Tail
  alias Pulso.PromQL.Discovery
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Record.Span
  alias Pulso.Runtime
  alias Pulso.Storage.Memory
  alias Pulso.Storage.S3
  alias Pulso.Traces.Assembly
  alias Pulso.Traces.Search
  alias Pulso.Traces.TempoDiscovery
  alias Pulso.Traces.TempoSearch

  @type tenant :: String.t()
  @type signal :: :logs | :metrics | :traces
  @type signal_record :: Log.t() | MetricSample.t() | Span.t()

  @type append_opts :: [
          # Opt-in idempotency: two `append` calls with the same tenant and
          # the same idempotency_key resolve to the same underlying object
          # so a retry does not duplicate.
          {:idempotency_key, String.t()}
        ]

  @type matcher_op :: :eq | :neq | :re | :nre
  @type matcher :: {name :: String.t(), op :: matcher_op(), value :: String.t()}

  @type line_filter_op :: :contains | :not_contains | :match_re | :not_match_re
  @type line_filter :: {op :: line_filter_op(), value :: String.t()}

  @type query_opts :: [
          {:start_ts, integer()}
          | {:end_ts, integer()}
          | {:limit, pos_integer()}
          | {:max_records, non_neg_integer()}
          # S3 logs: cumulative expanded column-buffer bytes, before row filters.
          | {:max_decoded_bytes, non_neg_integer()}
          | {:max_scan_segments, pos_integer()}
          | {:max_scan_bytes, pos_integer()}
          | {:max_scan_rows, pos_integer()}
          | {:deadline_ms, integer()}
          # Require a fresh committed manifest; never serve stale cached data.
          | {:fresh, boolean()}
          # Logs/traces convenience filter.
          | {:service, String.t()}
          | {:trace_id, String.t()}
          | {:span_id, String.t()}
          | {:name, String.t()}
          | {:kind, non_neg_integer()}
          | {:status_code, non_neg_integer()}
          | {:attributes, map()}
          | {:resource, map()}
          # Label matchers. On logs these filter on the stream labels
          # the Loki push path stores in `Log.resource`; on metrics they
          # filter on the sample's label set. Pushed into the Rust
          # Parquet decoder so rejected rows never materialise as
          # Erlang terms.
          | {:matchers, [matcher()]}
          # Logs-only: substring or regex predicates on `Log.body`,
          # pushed into the Rust decoder alongside the label matchers.
          | {:line_filters, [line_filter()]}
          # Metrics-only: only samples that carry exemplars.
          | {:exemplars, boolean()}
        ]

  @callback append(signal, tenant, [signal_record], append_opts) :: :ok | {:error, term()}
  @callback query(signal, tenant, query_opts) :: {:ok, [signal_record]} | {:error, term()}
  @callback metric_discovery_json(tenant, String.t(), String.t(), [[matcher]], query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback query_traces_json(tenant, query_opts, [String.t()] | nil) :: {:ok, binary()} | {:error, term()}
  @callback get_trace_json(tenant, String.t(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback search_traces_json(tenant, list(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_search_json(tenant, list(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_expression_search_json(tenant, list(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_discovery_json(tenant, String.t(), String.t(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_windowed_discovery_json(
              tenant,
              String.t(),
              String.t(),
              String.t() | nil,
              non_neg_integer(),
              non_neg_integer(),
              query_opts
            ) :: {:ok, binary()} | {:error, term()}
  @callback tempo_unscoped_tag_values_json(tenant, String.t(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_unscoped_tag_values_json(tenant, String.t(), String.t(), query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_windowed_unscoped_tag_values_json(
              tenant,
              String.t(),
              String.t(),
              pos_integer(),
              pos_integer(),
              query_opts
            ) :: {:ok, binary()} | {:error, term()}
  @callback tempo_trace_duration_tag_values_json(tenant, String.t() | nil, query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_windowed_trace_duration_tag_values_json(
              tenant,
              String.t() | nil,
              pos_integer(),
              pos_integer(),
              query_opts
            ) :: {:ok, binary()} | {:error, term()}
  @callback tempo_root_tag_values_json(tenant, String.t(), String.t() | nil, query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_windowed_root_tag_values_json(
              tenant,
              String.t(),
              String.t() | nil,
              pos_integer(),
              pos_integer(),
              query_opts
            ) :: {:ok, binary()} | {:error, term()}
  @callback tempo_event_time_tag_values_json(tenant, String.t(), String.t() | nil, query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_windowed_event_time_tag_values_json(
              tenant,
              String.t(),
              String.t() | nil,
              pos_integer(),
              pos_integer(),
              query_opts
            ) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_intrinsic_tag_values_json(tenant, String.t(), String.t() | nil, query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_default_tag_names_json(tenant, String.t() | nil, query_opts) :: {:ok, binary()} | {:error, term()}
  @callback tempo_filtered_tag_names_json(tenant, String.t(), String.t(), query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback tempo_filtered_discovery_json(tenant, String.t(), String.t(), String.t(), query_opts) ::
              {:ok, binary()} | {:error, term()}
  @callback log_discovery_json(tenant, String.t(), String.t(), query_opts) :: {:ok, binary()} | {:error, term()}
  @callback log_index_stats(tenant, query_opts) ::
              {:ok, %{segments: non_neg_integer(), entries: non_neg_integer(), bytes: non_neg_integer()}}
              | {:error, term()}
  @callback fold_logs(tenant, query_opts, acc, (list(), acc -> acc)) :: {:ok, acc} | {:error, term()} when acc: term()
  @callback log_metric_scan(tenant, query_opts, reference()) ::
              {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, term()}
  @callback trace_metrics_scan(tenant, query_opts, reference()) :: {:ok, non_neg_integer()} | {:error, term()}
  @callback put_metric_metadata(tenant, [Pulso.MetricMetadata.entry()], integer()) :: :ok | {:error, term()}
  @callback metric_metadata(tenant) :: {:ok, Pulso.MetricMetadata.set()} | {:error, term()}
  @callback follow_logs(tenant, cursor :: term()) :: {:ok, [Log.t()], term()} | {:error, term()}
  @optional_callbacks follow_logs: 2,
                      metric_discovery_json: 5,
                      log_metric_scan: 3,
                      trace_metrics_scan: 3,
                      put_metric_metadata: 3,
                      metric_metadata: 1,
                      fold_logs: 4,
                      log_index_stats: 2,
                      log_discovery_json: 4,
                      query_traces_json: 3,
                      get_trace_json: 3,
                      search_traces_json: 3,
                      tempo_search_json: 3,
                      tempo_expression_search_json: 3,
                      tempo_discovery_json: 4,
                      tempo_windowed_discovery_json: 7,
                      tempo_unscoped_tag_values_json: 3,
                      tempo_unscoped_tag_values_json: 4,
                      tempo_windowed_unscoped_tag_values_json: 6,
                      tempo_intrinsic_tag_values_json: 4,
                      tempo_trace_duration_tag_values_json: 3,
                      tempo_windowed_trace_duration_tag_values_json: 5,
                      tempo_root_tag_values_json: 4,
                      tempo_windowed_root_tag_values_json: 6,
                      tempo_event_time_tag_values_json: 4,
                      tempo_windowed_event_time_tag_values_json: 6,
                      tempo_default_tag_names_json: 3,
                      tempo_filtered_tag_names_json: 4,
                      tempo_filtered_discovery_json: 5

  @spec append(signal, tenant, [signal_record], append_opts) :: :ok | {:error, term()}
  def append(signal, tenant, records, opts \\ []) when is_atom(signal) and is_binary(tenant) and is_list(records) do
    result = adapter().append(signal, tenant, records, opts)
    outcome = if result == :ok, do: :accepted, else: :failed
    if signal == :logs and result == :ok, do: Tail.publish(tenant, records)
    Pulso.SelfMetrics.records(signal, outcome, length(records))
    result
  catch
    kind, reason ->
      Pulso.SelfMetrics.records(signal, :failed, length(records))
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  @spec query(signal, tenant, query_opts) :: {:ok, [signal_record]} | {:error, term()}
  def query(signal, tenant, opts \\ []) when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    dimension =
      case signal do
        :logs -> :storage_logs
        :metrics -> :storage_metrics
        :traces -> :storage_traces
      end

    Pulso.SelfMetrics.track(:query, dimension, fn -> adapter().query(signal, tenant, opts) end)
  end

  @doc "Complete metric label/series dictionaries; old adapters use a bounded sample-backed reference."
  def metric_discovery_json(tenant, mode, key, selectors, opts) do
    Pulso.SelfMetrics.track(:query, :storage_metrics, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :metric_discovery_json, 5),
        do: backend.metric_discovery_json(tenant, mode, key, selectors, opts),
        else: Discovery.reference(backend, tenant, mode, key, selectors, opts)
    end)
  end

  @doc """
  Approximate log volume for `[start_ts, end_ts]`: S3 counts whole matching
  segments from manifest metadata alone; other adapters count records.
  """
  def log_index_stats(tenant, opts) do
    Pulso.SelfMetrics.track(:query, :storage_logs, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :log_index_stats, 2),
        do: backend.log_index_stats(tenant, opts),
        else: record_index_stats(backend, tenant, opts)
    end)
  end

  defp record_index_stats(backend, tenant, opts) do
    with {:ok, records} <- backend.query(:logs, tenant, opts) do
      bytes = Enum.sum_by(records, &byte_size(Entry.from_record(&1).line))
      {:ok, %{segments: min(length(records), 1), entries: length(records), bytes: bytes}}
    end
  end

  @doc """
  Fold matching log records into `acc` batch by batch (`fun.(records, acc)`),
  without materializing the whole result. Adapters without native folding
  query once and fold that single batch. Records arrive in no particular
  order; callers needing an order must impose it.
  """
  def fold_logs(tenant, opts, acc, fun) do
    Pulso.SelfMetrics.track(:query, :storage_logs, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :fold_logs, 4),
        do: backend.fold_logs(tenant, opts, acc, fun),
        else: fold_queried(backend, tenant, opts, acc, fun)
    end)
  end

  defp fold_queried(backend, tenant, opts, acc, fun) do
    with {:ok, records} <- backend.query(:logs, tenant, opts), do: {:ok, fun.(records, acc)}
  end

  @doc "Whether the adapter runs native LogQL range scans (`Pulso.LogQL.NativeRange`)."
  def native_log_metric? do
    backend = adapter()
    Code.ensure_loaded!(backend)
    function_exported?(backend, :log_metric_scan, 3)
  end

  @doc "Feed the matching log segments to a native range scan."
  def log_metric_scan(tenant, opts, scan) do
    Pulso.SelfMetrics.track(:query, :storage_logs, fn -> adapter().log_metric_scan(tenant, opts, scan) end)
  end

  @doc "Durably merge metric metadata entries reported on `today` (see `Pulso.MetricMetadata`)."
  def put_metric_metadata(tenant, entries, today) do
    backend = adapter()
    Code.ensure_loaded!(backend)

    if function_exported?(backend, :put_metric_metadata, 3),
      do: backend.put_metric_metadata(tenant, entries, today),
      else: :ok
  end

  @doc "The tenant's retained metric metadata set; empty for adapters that keep none."
  def metric_metadata(tenant) do
    backend = adapter()
    Code.ensure_loaded!(backend)
    if function_exported?(backend, :metric_metadata, 1), do: backend.metric_metadata(tenant), else: {:ok, %{}}
  end

  @doc """
  Log records any instance published since `cursor`, for live tail; `nil`
  starts a cursor at what already exists. Adapters whose appends all happen
  on this node (the in-memory one) have nothing to follow: `append/4` already
  hands their records to local tailers.
  """
  def follow_logs(tenant, cursor) do
    backend = adapter()
    Code.ensure_loaded!(backend)
    if function_exported?(backend, :follow_logs, 2), do: backend.follow_logs(tenant, cursor), else: {:ok, [], cursor}
  end

  @doc """
  Feed the spans starting in `[start_ts, end_ts]` to a native TraceQL metrics
  accumulator. Adapters without native scans encode their spans into segments
  for the same accumulator, so results never depend on the adapter.
  """
  def trace_metrics_scan(tenant, opts, metrics) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :trace_metrics_scan, 3),
        do: backend.trace_metrics_scan(tenant, opts, metrics),
        else: encoded_trace_metrics(backend, tenant, opts, metrics)
    end)
  end

  defp encoded_trace_metrics(backend, tenant, opts, metrics) do
    with {:ok, spans} <- backend.query(:traces, tenant, Keyword.take(opts, [:start_ts, :end_ts, :max_records])) do
      spans
      |> Enum.chunk_every(4096)
      |> Enum.reduce_while({:ok, 0}, &add_encoded_spans(&1, &2, metrics, opts[:start_ts] || 0))
    end
  end

  defp add_encoded_spans(chunk, {:ok, rows}, metrics, floor) do
    with {:ok, blob, _, _} <- S3.encode_segment(:traces, chunk),
         {:ok, n} <- NIF.trace_metrics_add(metrics, blob, floor) do
      {:cont, {:ok, rows + n}}
    else
      :fallback -> {:halt, {:error, {:decode_failed, :invalid_trace_metadata}}}
      {:error, _} = error -> {:halt, error}
    end
  end

  @doc "Complete Loki stream-label dictionaries; old adapters use a bounded record-backed reference."
  def log_discovery_json(tenant, mode, key, opts) do
    Pulso.SelfMetrics.track(:query, :storage_logs, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :log_discovery_json, 4),
        do: backend.log_discovery_json(tenant, mode, key, opts),
        else: Pulso.Loki.Discovery.reference(backend, tenant, mode, key, opts)
    end)
  end

  @doc "Trace JSON queries keep storage-native candidates off the BEAM heap; other adapters use the reference renderer."
  def query_traces_json(tenant, opts, fields) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      if function_exported?(backend, :query_traces_json, 3),
        do: backend.query_traces_json(tenant, opts, fields),
        else: reference_trace_json(backend, tenant, opts, fields)
    end)
  end

  @doc "Assemble all visible stored spans for one exact trace ID, reporting any retention floor, or fail without truncation."
  def get_trace_json(tenant, trace_id, opts \\ []) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      opts =
        opts
        |> Keyword.take([:max_scan_segments, :max_scan_bytes, :max_scan_rows, :deadline_ms])
        |> Keyword.put(:trace_id, trace_id)
        |> Keyword.put(:limit, 5001)

      assembled_trace_json(backend, tenant, trace_id, opts)
    end)
  end

  @doc "Search and limit distinct traces after grouping all stored spans in an explicit source-start window."
  def search_traces_json(tenant, predicates, opts) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      searched_trace_json(backend, tenant, predicates, opts)
    end)
  end

  @doc "Complete retained trace context before overlap, most-recent selection and sampled spansets."
  def tempo_search_json(tenant, predicates, opts) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      backend = adapter()
      Code.ensure_loaded!(backend)

      searched_tempo_json(backend, tenant, predicates, opts)
    end)
  end

  def tempo_discovery_json(tenant, scope, key, opts) do
    Pulso.SelfMetrics.track(:query, :storage_traces, fn ->
      discovered_tempo_json(adapter(), tenant, scope, key, opts)
    end)
  end

  defp discovered_tempo_json(backend, tenant, scope, key, opts) do
    with :ok <- TempoDiscovery.validate(scope, key),
         :ok <- TempoDiscovery.validate_window(opts[:discovery_window]),
         {:ok, predicates} <- TempoDiscovery.filter(scope, key, opts[:discovery_query]) do
      Code.ensure_loaded!(backend)

      discovery_backend(backend, tenant, scope, key, opts, predicates, opts[:discovery_window])
    end
  end

  defp discovery_backend(backend, tenant, scope, key, opts, predicates, nil),
    do: discovery_backend(backend, tenant, scope, key, opts, predicates)

  defp discovery_backend(backend, tenant, "intrinsic", "event:timeSinceStart" = key, opts, _predicates, {start, finish}) do
    if function_exported?(backend, :tempo_windowed_event_time_tag_values_json, 6),
      do: backend.tempo_windowed_event_time_tag_values_json(tenant, key, opts[:discovery_query], start, finish, opts),
      else: {:error, :unsupported_trace_discovery}
  end

  defp discovery_backend(backend, tenant, "intrinsic", "trace:duration", opts, _predicates, {start, finish}) do
    if function_exported?(backend, :tempo_windowed_trace_duration_tag_values_json, 5),
      do: backend.tempo_windowed_trace_duration_tag_values_json(tenant, opts[:discovery_query], start, finish, opts),
      else: {:error, :unsupported_trace_discovery}
  end

  defp discovery_backend(backend, tenant, "intrinsic", key, opts, _predicates, {start, finish})
       when key in ~w(rootName trace:rootName rootServiceName trace:rootService) do
    if function_exported?(backend, :tempo_windowed_root_tag_values_json, 6),
      do: backend.tempo_windowed_root_tag_values_json(tenant, key, opts[:discovery_query], start, finish, opts),
      else: {:error, :unsupported_trace_discovery}
  end

  defp discovery_backend(backend, tenant, "none", key, opts, [_ | _], {start, finish}) when key != "" do
    if function_exported?(backend, :tempo_windowed_unscoped_tag_values_json, 6),
      do: backend.tempo_windowed_unscoped_tag_values_json(tenant, key, opts[:discovery_query], start, finish, opts),
      else: {:error, :unsupported_trace_discovery}
  end

  defp discovery_backend(backend, tenant, scope, key, opts, _predicates, {start, finish}) do
    if function_exported?(backend, :tempo_windowed_discovery_json, 7),
      do: backend.tempo_windowed_discovery_json(tenant, scope, key, opts[:discovery_query], start, finish, opts),
      else: {:error, :unsupported_trace_discovery}
  end

  defp discovery_backend(backend, tenant, "intrinsic", "event:timeSinceStart" = key, opts, _predicates) do
    if function_exported?(backend, :tempo_event_time_tag_values_json, 4),
      do: backend.tempo_event_time_tag_values_json(tenant, key, opts[:discovery_query], opts),
      else: discovery_reference(backend, tenant, "intrinsic", key, opts)
  end

  defp discovery_backend(backend, tenant, "intrinsic", "trace:duration", opts, _predicates) do
    if function_exported?(backend, :tempo_trace_duration_tag_values_json, 3),
      do: backend.tempo_trace_duration_tag_values_json(tenant, opts[:discovery_query], opts),
      else: root_discovery_reference(backend, tenant, "trace:duration", opts)
  end

  defp discovery_backend(backend, tenant, "intrinsic", key, opts, _predicates)
       when key in ~w(rootName trace:rootName rootServiceName trace:rootService) do
    if function_exported?(backend, :tempo_root_tag_values_json, 4),
      do: backend.tempo_root_tag_values_json(tenant, key, opts[:discovery_query], opts),
      else: root_discovery_reference(backend, tenant, key, opts)
  end

  defp discovery_backend(backend, tenant, "none", "", opts, _predicates) do
    if function_exported?(backend, :tempo_default_tag_names_json, 3),
      do: backend.tempo_default_tag_names_json(tenant, opts[:discovery_query], opts),
      else: discovery_reference(backend, tenant, "none", "", opts)
  end

  defp discovery_backend(backend, tenant, "none", key, opts, [_ | _]) when key != "" do
    if function_exported?(backend, :tempo_unscoped_tag_values_json, 4),
      do: backend.tempo_unscoped_tag_values_json(tenant, key, opts[:discovery_query], opts),
      else: discovery_reference(backend, tenant, "none", key, opts)
  end

  defp discovery_backend(backend, tenant, scope, key, opts, _predicates) when scope in ["none", "intrinsic"] do
    callback = if scope == "none", do: :tempo_unscoped_tag_values_json, else: :tempo_intrinsic_tag_values_json

    args = if scope == "none", do: [tenant, key, opts], else: [tenant, key, opts[:discovery_query], opts]

    if function_exported?(backend, callback, length(args)),
      do: apply(backend, callback, args),
      else: discovery_reference(backend, tenant, scope, key, opts)
  end

  defp discovery_backend(backend, tenant, scope, "", opts, [_ | _]) do
    if function_exported?(backend, :tempo_filtered_tag_names_json, 4),
      do: backend.tempo_filtered_tag_names_json(tenant, scope, opts[:discovery_query], opts),
      else: discovery_reference(backend, tenant, scope, "", opts)
  end

  defp discovery_backend(backend, tenant, scope, key, opts, predicates) do
    # A pre-filter adapter callback must never silently ignore the new query.
    # Require explicit support, otherwise use the bounded reference scan.
    cond do
      predicates == [] and function_exported?(backend, :tempo_discovery_json, 4) ->
        backend.tempo_discovery_json(tenant, scope, key, opts)

      predicates != [] and function_exported?(backend, :tempo_filtered_discovery_json, 5) ->
        backend.tempo_filtered_discovery_json(tenant, scope, key, opts[:discovery_query], opts)

      true ->
        discovery_reference(backend, tenant, scope, key, opts)
    end
  end

  defp root_discovery_reference(backend, tenant, key, opts) do
    maximum = min(opts[:max_scan_rows] || 1_000_000, 1_000_000)
    scan = opts |> Keyword.drop([:start_ts, :end_ts, :limit]) |> Keyword.put(:max_records, maximum + 1)

    with {:ok, spans} <- backend.query(:traces, tenant, scan), true <- length(spans) <= maximum do
      TempoDiscovery.json(spans, "intrinsic", key, opts)
    else
      false -> {:error, :query_scan_limit}
      error -> error
    end
  end

  defp discovery_reference(backend, tenant, scope, key, opts) do
    scan = opts |> Keyword.drop([:start_ts, :end_ts, :limit])
    scan = Keyword.put(scan, :max_records, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))
    with {:ok, spans} <- backend.query(:traces, tenant, scan), do: TempoDiscovery.json(spans, scope, key, opts)
  end

  # Never pass a new expression to an old adapter that might drop OR/types.
  defp searched_tempo_json(backend, tenant, {:expression, program} = expression, opts) do
    if function_exported?(backend, :tempo_expression_search_json, 3) do
      backend.tempo_expression_search_json(tenant, program, opts)
    else
      scan = opts |> Keyword.drop([:start_ts, :end_ts, :limit])
      maximum = min(opts[:max_scan_rows] || 1_000_000, 1_000_000)
      scan = Keyword.put(scan, :max_records, maximum + 1)

      with {:ok, spans} <- backend.query(:traces, tenant, scan), true <- length(spans) <= maximum do
        TempoSearch.json(spans, expression, opts)
      else
        false -> {:error, :query_scan_limit}
        error -> error
      end
    end
  end

  defp searched_tempo_json(backend, tenant, predicates, opts) do
    if function_exported?(backend, :tempo_search_json, 3) do
      backend.tempo_search_json(tenant, predicates, opts)
    else
      scan = opts |> Keyword.drop([:start_ts, :end_ts, :limit])
      scan = Keyword.put(scan, :max_records, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))
      with {:ok, spans} <- backend.query(:traces, tenant, scan), do: TempoSearch.json(spans, predicates, opts)
    end
  end

  defp searched_trace_json(backend, tenant, predicates, opts) do
    if function_exported?(backend, :search_traces_json, 3) do
      backend.search_traces_json(tenant, predicates, opts)
    else
      reference_search_json(backend, tenant, predicates, opts)
    end
  end

  defp reference_search_json(backend, tenant, predicates, opts) do
    scan_opts =
      opts |> Keyword.delete(:limit) |> Keyword.put(:max_records, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))

    with {:ok, spans} <- backend.query(:traces, tenant, scan_opts),
         do: Search.json(spans, predicates, opts)
  end

  defp assembled_trace_json(backend, tenant, trace_id, opts) do
    if function_exported?(backend, :get_trace_json, 3) do
      backend.get_trace_json(tenant, trace_id, opts)
    else
      with {:ok, spans} <- backend.query(:traces, tenant, opts), do: Assembly.json(spans, trace_id)
    end
  end

  defp reference_trace_json(backend, tenant, opts, fields) do
    with {:ok, spans} <- backend.query(:traces, tenant, opts) do
      if opts[:limit] in [nil, 5001] and length(spans) > 5000 do
        {:error, :query_sample_limit}
      else
        {:ok, Pulso.JSON.encode!(Enum.map(spans, &trace_projection(&1, fields)))}
      end
    end
  end

  defp trace_projection(span, fields) do
    map = Map.put(Span.to_map(span), "duration_ns", span.end_timestamp_ns - span.timestamp_ns)
    if is_nil(fields), do: map, else: Map.take(map, fields)
  end

  @spec adapter() :: module()
  def adapter, do: Runtime.get_env(:pulso, __MODULE__)[:adapter] || Memory
end
