defmodule Pulso.SelfMetrics do
  @moduledoc """
  Disposable, node-local operational counters. Writes use ETS directly, never a
  process mailbox. Dimensions are fixed enums, not tenants, keys, or expressions.
  Scraping never queries signal storage or waits for manifest owners.
  """
  use GenServer

  alias Pulso.Storage.S3.ManifestRegistry

  @table __MODULE__
  @operations %{
    ingest: [:otlp, :loki, :remote_write],
    query: [:storage_logs, :storage_metrics, :promql, :logql_log, :logql_metric, :mcp, :http],
    object: [:put, :put_if_match, :put_if_none_match, :get, :get_if_none_match, :delete, :list, :list_prefixes],
    compaction: [:compact, :cleanup, :worker_merge, :worker_cleanup, :worker_discovery]
  }
  @definitions [
    {"pulso_operations_total", "counter", "Completed operations by layer and outcome."},
    {"pulso_operation_duration_seconds", "summary", "Operation latency; count and sum only, no quantiles."},
    {"pulso_ingest_records_total", "counter", "Decoded records accepted, rejected, or failed at publication."},
    {"pulso_object_payload_bytes_total", "counter",
     "Successful object payload bytes, excluding wire overhead and internal retries."},
    {"pulso_compaction_segments_total", "counter", "Source segments merged by successful compaction."},
    {"pulso_compaction_deleted_objects_total", "counter", "Retired objects deleted by successful cleanup."}
  ]

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true, read_concurrency: true])

    for {kind, dimensions} <- @operations, dimension <- dimensions, outcome <- [:success, :error, :exception] do
      labels = operation_labels(kind, dimension, outcome)

      for name <- [
            "pulso_operations_total",
            "pulso_operation_duration_seconds_count",
            "pulso_operation_duration_seconds_sum"
          ] do
        increment(name, labels, 0)
      end
    end

    for signal <- [:logs, :metrics], outcome <- [:accepted, :rejected, :failed], do: records(signal, outcome, 0)
    for direction <- [:read, :write], do: object_bytes(direction, 0)
    compaction(:compact, {:ok, %{merged: 0}})
    compaction(:cleanup, {:ok, 0})

    :telemetry.detach(__MODULE__)

    :ok =
      :telemetry.attach_many(
        __MODULE__,
        [[:phoenix, :endpoint, :start], [:phoenix, :endpoint, :stop], [:phoenix, :error_rendered]],
        &PulsoWeb.SelfMetrics.handle_event/4,
        nil
      )

    {:ok, nil}
  end

  @impl true
  def terminate(_reason, _state), do: :telemetry.detach(__MODULE__)

  @doc "Measure a completed operation without changing its return value or exception."
  def track(kind, dimension, fun) when is_function(fun, 0) do
    start = System.monotonic_time()

    try do
      result = fun.()
      outcome = if match?({:error, _}, result), do: :error, else: :success
      operation(kind, dimension, outcome, System.monotonic_time() - start)
      result
    catch
      class, reason ->
        operation(kind, dimension, :exception, System.monotonic_time() - start)
        :erlang.raise(class, reason, __STACKTRACE__)
    end
  end

  def operation(kind, dimension, outcome, duration)
      when outcome in [:success, :error, :exception] and is_integer(duration) and duration >= 0 do
    if dimension in Map.get(@operations, kind, []) do
      labels = operation_labels(kind, dimension, outcome)
      increment("pulso_operations_total", labels, 1)
      increment("pulso_operation_duration_seconds_count", labels, 1)

      increment(
        "pulso_operation_duration_seconds_sum",
        labels,
        System.convert_time_unit(duration, :native, :microsecond)
      )
    end

    :ok
  end

  defp operation_labels(kind, dimension, outcome) do
    ~s(layer="#{kind}",operation="#{dimension}",outcome="#{outcome}")
  end

  def records(signal, outcome, count)
      when signal in [:logs, :metrics] and outcome in [:accepted, :rejected, :failed] and is_integer(count) and
             count >= 0 do
    increment("pulso_ingest_records_total", ~s(signal="#{signal}",outcome="#{outcome}"), count)
  end

  def object_bytes(direction, count) when direction in [:read, :write] and is_integer(count) and count >= 0 do
    increment("pulso_object_payload_bytes_total", ~s(direction="#{direction}"), count)
  end

  def compaction(:compact, {:ok, %{merged: count}}) do
    increment("pulso_compaction_segments_total", "", count)
  end

  def compaction(:cleanup, {:ok, count}) when is_integer(count) do
    increment("pulso_compaction_deleted_objects_total", "", count)
  end

  def compaction(_operation, _result), do: :ok

  defp increment(name, labels, value) do
    :ets.update_counter(@table, {name, labels}, {2, value}, {{name, labels}, 0})
    :ok
  rescue
    # Supervision can briefly remove the table. Monitoring must not fail work.
    ArgumentError -> :ok
  end

  @doc "Render Prometheus text format 0.0.4 using only node-local state."
  def render do
    samples = snapshot()

    families = Enum.map(@definitions, &render_family(&1, samples))

    [
      families,
      "# HELP pulso_manifest_queue_depth Pending publication callers plus owner mailbox messages.\n",
      "# TYPE pulso_manifest_queue_depth gauge\n",
      "pulso_manifest_queue_depth #{manifest_queue_depth()}\n"
    ]
    |> IO.iodata_to_binary()
  end

  defp render_family({name, type, help}, samples) do
    names = if type == "summary", do: [name <> "_count", name <> "_sum"], else: [name]

    values =
      samples
      |> Enum.filter(fn {{sample_name, _labels}, _value} -> sample_name in names end)
      |> Enum.sort()
      |> Enum.map(&render_sample/1)

    ["# HELP #{name} #{help}\n# TYPE #{name} #{type}\n", values]
  end

  defp render_sample({{name, labels}, value}) do
    value = if String.ends_with?(name, "_seconds_sum"), do: value / 1_000_000, else: value
    labels = if labels == "", do: "", else: "{#{labels}}"
    "#{name}#{labels} #{value}\n"
  end

  defp snapshot do
    :ets.tab2list(@table)
  rescue
    ArgumentError -> []
  end

  defp manifest_queue_depth do
    if Process.whereis(ManifestRegistry) do
      Registry.select(ManifestRegistry, [{{:"$1", :"$2", :"$3"}, [], [{{:"$2", :"$3"}}]}])
      |> Enum.reduce(0, fn owner, total -> total + owner_queue_depth(owner) end)
    else
      0
    end
  rescue
    ArgumentError -> 0
  end

  # Read both legacy integer snapshots and the detailed owner snapshot. Count
  # publication callers here, not segments, to preserve the canonical gauge.
  defp pending_waiters(%{waiters: count}) when is_integer(count), do: count
  defp pending_waiters(count) when is_integer(count), do: count
  defp pending_waiters(_), do: 0

  defp owner_queue_depth({pid, pending}) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, count} -> count + pending_waiters(pending)
      nil -> 0
    end
  end
end
