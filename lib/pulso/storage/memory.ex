defmodule Pulso.Storage.Memory do
  @moduledoc """
  In-memory log storage backed by a public ETS table.

  Test-only adapter as of step 2. Meant to prove the ingest → storage → query
  spine end to end without dragging in Rust, Parquet, or S3. Dev and prod use
  `Pulso.Storage.S3`; this module stays wired as the default in `mix test`
  because `config/test.exs` sets no adapter override.

  Records for each tenant are kept in a private list-per-tenant, appended to
  as batches arrive and scanned linearly on query. That is deliberately naive:
  we want to burn nothing on this adapter that we would not throw away when
  Parquet lands.
  """

  @behaviour Pulso.Storage

  use GenServer

  alias Pulso.Record.Log
  alias Pulso.Storage.SortOrder

  @table __MODULE__

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Pulso.Storage
  def append(tenant, records) when is_binary(tenant) and is_list(records) do
    now = System.system_time(:nanosecond)

    normalized =
      for %Log{} = record <- records do
        %{record | observed_timestamp_ns: record.observed_timestamp_ns || now}
      end

    existing =
      case :ets.lookup(@table, tenant) do
        [{^tenant, list}] -> list
        [] -> []
      end

    :ets.insert(@table, {tenant, existing ++ normalized})
    :ok
  end

  @impl Pulso.Storage
  def query(tenant, opts) when is_binary(tenant) and is_list(opts) do
    records =
      case :ets.lookup(@table, tenant) do
        [{^tenant, list}] -> list
        [] -> []
      end

    filtered =
      records
      |> filter_by_time(Keyword.get(opts, :start_ts), Keyword.get(opts, :end_ts))
      |> filter_by_service(Keyword.get(opts, :service))
      |> SortOrder.sort()
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
    Enum.filter(records, fn %Log{timestamp_ns: ts} ->
      (start_ts == nil or ts >= start_ts) and (end_ts == nil or ts <= end_ts)
    end)
  end

  defp filter_by_service(records, nil), do: records
  defp filter_by_service(records, service), do: Enum.filter(records, &(&1.service == service))

  defp take_limit(records, nil), do: records
  defp take_limit(records, limit) when is_integer(limit) and limit > 0, do: Enum.take(records, limit)
end
