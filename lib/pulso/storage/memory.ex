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
  def append(signal, tenant, records, _opts \\ []) when is_atom(signal) and is_binary(tenant) and is_list(records) do
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
  def query(signal, tenant, opts) when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    key = {tenant, signal}

    records =
      case :ets.lookup(@table, key) do
        [{^key, list}] -> list
        [] -> []
      end

    filtered =
      records
      |> filter_by_time(Keyword.get(opts, :start_ts), Keyword.get(opts, :end_ts))
      |> filter_signal(signal, opts)
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

  defp filter_signal(records, :logs, opts) do
    case Keyword.get(opts, :service) do
      nil -> records
      service -> Enum.filter(records, &(&1.service == service))
    end
  end

  defp filter_signal(records, :metrics, opts) do
    case Keyword.get(opts, :matchers, []) do
      [] -> records
      matchers -> Enum.filter(records, &matches_all?(&1, matchers))
    end
  end

  defp matches_all?(%MetricSample{labels: labels}, matchers) do
    Enum.all?(matchers, fn {name, op, value} ->
      label_value = Map.get(labels, name)
      apply_matcher(op, label_value, value)
    end)
  end

  defp apply_matcher(:eq, label_value, value), do: label_value == value
  defp apply_matcher(:neq, label_value, value), do: label_value != value

  defp apply_matcher(:re, nil, _value), do: false

  defp apply_matcher(:re, label_value, pattern) when is_binary(label_value) do
    case Regex.compile(pattern) do
      {:ok, re} -> Regex.match?(re, label_value)
      _ -> false
    end
  end

  defp apply_matcher(:nre, nil, _value), do: true

  defp apply_matcher(:nre, label_value, pattern) when is_binary(label_value) do
    not apply_matcher(:re, label_value, pattern)
  end

  defp take_limit(records, nil), do: records
  defp take_limit(records, limit) when is_integer(limit) and limit > 0, do: Enum.take(records, limit)
end
