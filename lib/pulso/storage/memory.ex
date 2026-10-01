defmodule Pulso.Storage.Memory do
  @moduledoc """
  In-memory signal storage backed by a public ETS table.

  Test-only adapter. Meant to prove the ingest → storage → query spine
  end to end without dragging in Rust, Parquet, or S3. Dev and prod use
  `Pulso.Storage.S3`; this module stays wired as the default in
  `mix test` because `config/test.exs` sets no adapter override.

  Records for each `(tenant, signal)` are kept in a private list, appended
  to as batches arrive and scanned linearly on query. That is deliberately
  naive: we want to burn nothing on this adapter that we would not throw
  away when Parquet lands.
  """

  @behaviour Pulso.Storage

  use GenServer

  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage.SortOrder

  @table __MODULE__

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Pulso.Storage
  def append(signal, tenant, records, _opts \\ [])
      when is_atom(signal) and is_binary(tenant) and is_list(records) do
    key = {tenant, signal}

    existing =
      case :ets.lookup(@table, key) do
        [{^key, list}] -> list
        [] -> []
      end

    :ets.insert(@table, {key, existing ++ records})
    :ok
  end

  @impl Pulso.Storage
  def query(signal, tenant, opts)
      when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    key = {tenant, signal}

    records =
      case :ets.lookup(@table, key) do
        [{^key, list}] -> list
        [] -> []
      end

    filtered =
      records
      |> filter_by_time(Keyword.get(opts, :start_ts), Keyword.get(opts, :end_ts))
      |> filter_by_service(Keyword.get(opts, :service))
      |> filter_by_matchers(Keyword.get(opts, :matchers, []))
      |> filter_by_line_filters(Keyword.get(opts, :line_filters, []))
      |> SortOrder.sort(signal)
      |> take_limit(Keyword.get(opts, :limit))

    {:ok, filtered}
  end

  @doc false
  @spec reset() :: :ok
  def reset do
    if :ets.info(@table) != :undefined do
      :ets.delete_all_objects(@table)
    end

    :ok
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true, write_concurrency: true])
    {:ok, %{}}
  end

  defp filter_by_time(records, nil, nil), do: records

  defp filter_by_time(records, start_ts, end_ts) do
    Enum.filter(records, fn record ->
      # A nil timestamp does not fit inside a time-bounded range. Elixir's
      # term ordering puts atoms greater than numbers, so `nil >= 5` is
      # true without an explicit guard — leaving nil-ts records leaking
      # through every time filter.
      ts = record_ts(record)

      is_integer(ts) and
        (start_ts == nil or ts >= start_ts) and
        (end_ts == nil or ts <= end_ts)
    end)
  end

  defp record_ts(%Log{timestamp_ns: ts}), do: ts
  defp record_ts(%MetricSample{timestamp_ns: ts}), do: ts

  # Logs-only convenience filter — metric samples have no `service`
  # field so a service filter is undefined for them; pass them through
  # unchanged rather than silently drop them.
  defp filter_by_service(records, nil), do: records

  defp filter_by_service(records, service) do
    Enum.filter(records, fn
      %Log{service: s} -> s == service
      %MetricSample{} -> true
    end)
  end

  # Selector-matcher parity with the S3 Rust pushdown. Every matcher must
  # pass. Missing labels are treated as the empty string, matching Loki
  # semantics: `foo=""` matches records with no `foo` label.
  defp filter_by_matchers(records, []), do: records

  defp filter_by_matchers(records, matchers) do
    Enum.filter(records, fn record ->
      Enum.all?(matchers, &matcher_matches?(&1, record))
    end)
  end

  defp matcher_matches?({name, op, value}, record) do
    raw = label_from_record(record, name)
    label_value = if is_binary(raw), do: raw, else: ""
    apply_matcher_op(op, label_value, value)
  end

  # Logs: promoted typed fields first (service, service_name, level,
  # detected_level), then resource JSON. Attributes are NOT consulted —
  # they are per-record structured metadata, not stream labels, and
  # mixing them here would diverge from the S3 Rust decoder which only
  # reads resource + promoted columns. Callers who want to filter on
  # attributes use the post-parse label filter (`| foo = "bar"`) which
  # runs over the full merged bag in the pipeline.
  #
  # Metrics: labels live in the `labels` map directly — there is no
  # separation of "stream" vs "structured" labels in the Prometheus
  # data model.
  defp label_from_record(%Log{} = record, name) do
    promoted_field(record, name) || Map.get(record.resource || %{}, name)
  end

  defp label_from_record(%MetricSample{labels: labels}, name), do: Map.get(labels, name)

  defp promoted_field(%Log{} = record, name) when name in ["service", "service_name"],
    do: record.service

  defp promoted_field(%Log{} = record, name) when name in ["level", "detected_level"],
    do: record.severity_text

  defp promoted_field(_record, _name), do: nil

  defp apply_matcher_op(:eq, actual, wanted), do: actual == wanted
  defp apply_matcher_op(:neq, actual, wanted), do: actual != wanted
  defp apply_matcher_op(:re, actual, pattern), do: regex_match?(pattern, actual)
  defp apply_matcher_op(:nre, actual, pattern), do: not regex_match?(pattern, actual)

  # Invalid regexes are caught by `Pulso.LogQL.QueryValidation` before the
  # storage layer runs. If we somehow get here with one, raise rather
  # than silently invert to `true` for the `:nre` / `:not_match_re` op.
  defp regex_match?(pattern, subject) do
    case Regex.compile(pattern) do
      {:ok, re} -> Regex.match?(re, subject)
      {:error, reason} -> raise ArgumentError, "invalid regex #{inspect(pattern)}: #{inspect(reason)}"
    end
  end

  # Line-filter parity with the S3 Rust decoder. Body normalization
  # mirrors `Pulso.LogQL.Entry.from_record/1` and Rust's
  # `line_bytes_for_match`: a string body is matched directly; a
  # non-string body is JSON-encoded first so both paths agree on what
  # "the log line" means. Metric samples have no `body`, so line
  # filters pass them through unchanged.
  defp filter_by_line_filters(records, []), do: records

  defp filter_by_line_filters(records, filters) do
    Enum.filter(records, fn
      %MetricSample{} ->
        true

      record ->
        body = line_from_record(record)
        Enum.all?(filters, &line_filter_matches?(&1, body))
    end)
  end

  defp line_from_record(%{body: nil}), do: ""
  defp line_from_record(%{body: body}) when is_binary(body), do: body
  defp line_from_record(%{body: body}), do: Pulso.JSON.encode!(body)

  defp line_filter_matches?({:contains, needle}, body), do: String.contains?(body, needle)
  defp line_filter_matches?({:not_contains, needle}, body), do: not String.contains?(body, needle)
  defp line_filter_matches?({:match_re, pattern}, body), do: regex_match?(pattern, body)
  defp line_filter_matches?({:not_match_re, pattern}, body), do: not regex_match?(pattern, body)

  defp take_limit(records, nil), do: records
  defp take_limit(records, limit) when is_integer(limit) and limit > 0, do: Enum.take(records, limit)
end
