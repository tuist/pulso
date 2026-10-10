defmodule Pulso.Metrics do
  @moduledoc """
  Node-local self-monitoring, exported as Prometheus text without using storage.

  Telemetry handlers update ETS synchronously, with a finite label vocabulary and
  no reporting mailbox. Counters reset when this supervised process restarts.
  """
  use Pulso.Runtime.GenServer

  alias Pulso.PromQL.QuerySlots
  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.Registry
  alias Pulso.Storage.S3.AppendBuffer
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestRegistry

  @table __MODULE__
  @handler {__MODULE__, :metrics}
  @event [:pulso, :operation, :stop]
  @timeout_event [:pulso, :compaction, :timeout]
  @events [
    @event,
    @timeout_event,
    [:phoenix, :endpoint, :start],
    [:phoenix, :endpoint, :stop],
    [:phoenix, :error_rendered]
  ]
  @request_start {__MODULE__, :request_start}
  @buckets [5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
  @bucket_labels Enum.map(@buckets, &{&1, Float.to_string(&1 / 1_000_000)})
  @operations %{
    ingest: ["otlp", "loki", "remote_write"],
    query: [
      "http_promql",
      "http_logql",
      "http_labels",
      "query_logs",
      "query_metrics",
      "query_logql",
      "query_promql",
      "unknown_tool"
    ],
    object: ["put", "put_if_match", "put_if_none_match", "get", "get_if_none_match", "delete", "list", "list_prefixes"],
    compaction: ["compact", "cleanup"],
    retention: ["advance", "cleanup", "cleanup_retired", "sweep"]
  }
  @http_routes %{
    {"POST", ["v1", "logs"]} => {:ingest, "otlp"},
    {"POST", ["loki", "api", "v1", "push"]} => {:ingest, "loki"},
    {"POST", ["api", "v1", "write"]} => {:ingest, "remote_write"},
    {"GET", ["api", "v1", "query"]} => {:query, "http_promql"},
    {"POST", ["api", "v1", "query"]} => {:query, "http_promql"},
    {"GET", ["api", "v1", "query_range"]} => {:query, "http_promql"},
    {"POST", ["api", "v1", "query_range"]} => {:query, "http_promql"},
    {"GET", ["loki", "api", "v1", "query"]} => {:query, "http_logql"},
    {"POST", ["loki", "api", "v1", "query"]} => {:query, "http_logql"},
    {"GET", ["loki", "api", "v1", "query_range"]} => {:query, "http_logql"},
    {"POST", ["loki", "api", "v1", "query_range"]} => {:query, "http_logql"},
    {"GET", ["loki", "api", "v1", "labels"]} => {:query, "http_labels"}
  }
  @families [
    {"pulso_operations_total", "counter", "Completed node-local operations by bounded operation and outcome."},
    {"pulso_operation_duration_seconds", "histogram", "Duration of completed node-local operations in seconds."},
    {"pulso_ingest_records_total", "counter",
     "Known receiver record deliveries accepted or rejected, not unique stored rows."},
    {"pulso_object_bytes_total", "counter",
     "Successful logical object body bytes, excluding native retries and headers."},
    {"pulso_compaction_segments_total", "counter",
     "Source segments merged or retirement deletions confirmed by successful maintenance calls."},
    {"pulso_compaction_timeouts_total", "counter",
     "Background worker deadlines exceeded, including still-running native work."}
  ]
  @purposes ["none", "segment", "manifest", "metadata_page", "other"]
  @outcomes ["ok", "error", "exception", "conflict", "not_modified", "not_found", "rejected"]
  @gauge_help %{
    "pulso_manifest_mailbox_messages" => "Messages in node-local manifest-owner mailboxes.",
    "pulso_manifest_pending_segments" => "Segments in node-local publication batches, including in-flight batches.",
    "pulso_manifest_waiting_requests" => "Requests waiting in node-local manifest publication batches.",
    "pulso_ingest_buffers" => "Active node-local unkeyed ingest buffers.",
    "pulso_ingest_buffer_reserved_calls" => "Queued and executing requests reserved in unkeyed ingest buffers.",
    "pulso_ingest_buffer_input_bytes" =>
      "Estimated external-term bytes reserved in unkeyed ingest buffers, not heap size.",
    "pulso_ingest_buffer_rows" => "Queued and executing rows reserved in unkeyed ingest buffers.",
    "pulso_query_occupied_slots" => "Registered node-local PromQL tenant query slots.",
    "pulso_retention_root_capacity_ratio" =>
      "Worst metadata capacity ratio among locally cached managed scopes, without tenant labels.",
    "pulso_retention_pending_buckets" => "Expired buckets awaiting reclamation in locally cached managed scopes.",
    "pulso_retention_managed_scopes" => "Retention-managed tenant/signal scopes cached on this node.",
    "pulso_vm_memory_bytes" => "Total BEAM-reported memory in bytes.",
    "pulso_vm_run_queue" => "BEAM scheduler run-queue length."
  }

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  # Every table operation goes through the current runtime's instance name.
  defp table, do: Pulso.Runtime.table(@table)

  @impl true
  def init(_opts) do
    :ets.new(table(), [:named_table, :public, :set, write_concurrency: true])
    # Handlers are node-global and route each event to the emitting process's
    # runtime instance, so attach once and never re-attach under concurrent events.
    case :telemetry.attach_many(@handler, @events, &__MODULE__.handle_event/4, nil) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end

    {:ok, nil}
  end

  # Owned (scoped) instances must not detach the handlers other instances share.
  @impl true
  def terminate(_reason, _state) do
    if Pulso.Runtime.name(__MODULE__) == __MODULE__, do: :telemetry.detach(@handler), else: :ok
  end

  @doc "Measure an operation without changing its result or exception semantics."
  def measure(kind, operation, fun, measurements \\ fn _ -> %{} end, purpose \\ "none") do
    started = System.monotonic_time()

    try do
      result = fun.()
      report(kind, operation, purpose, outcome(result), started, measurements.(result))
      result
    catch
      kind_of_error, reason ->
        report(kind, operation, purpose, "exception", started, %{})
        :erlang.raise(kind_of_error, reason, __STACKTRACE__)
    end
  end

  @doc "Append a decoded receiver batch; count accepted records only after durable success."
  def append(signal, tenant, records, rejected, opts) do
    # Storage owns accepted/failed canonical counts; decoder rejections are
    # known here, before append. The detailed delivery view has its own family.
    :ok = Pulso.SelfMetrics.records(signal, :rejected, rejected)
    result = Pulso.Storage.append(signal, tenant, records, opts)

    record_counts(
      signal,
      if(result == :ok, do: length(records), else: 0),
      rejected + if(result == :ok, do: 0, else: length(records))
    )

    result
  catch
    kind, reason ->
      record_counts(signal, 0, rejected + length(records))
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp record_counts(signal, accepted, rejected) when signal in [:logs, :metrics] do
    increment({"pulso_ingest_records_total", [{"signal", to_string(signal)}, {"outcome", "accepted"}]}, accepted)
    increment({"pulso_ingest_records_total", [{"signal", to_string(signal)}, {"outcome", "rejected"}]}, rejected)
  end

  defp report(kind, operation, purpose, outcome, started, measurements) do
    :telemetry.execute(@event, Map.put(measurements, :duration, System.monotonic_time() - started), %{
      kind: kind,
      operation: operation,
      purpose: purpose,
      outcome: outcome
    })
  end

  defp outcome({:error, reason}) when reason in [:already_exists, :precondition_failed], do: "conflict"
  defp outcome({:error, :not_found}), do: "not_found"
  defp outcome({:error, _}), do: "error"
  defp outcome(:not_modified), do: "not_modified"
  defp outcome(_), do: "ok"

  @doc false
  def handle_event(@event, measurements, metadata, _config) do
    kind = metadata[:kind]
    operation = metadata[:operation]
    purpose = metadata[:purpose]
    outcome = metadata[:outcome]

    if valid_event?(measurements, kind, operation, purpose, outcome) do
      labels = [{"kind", to_string(kind)}, {"operation", operation}, {"purpose", purpose}, {"outcome", outcome}]
      increment({"pulso_operations_total", labels}, 1)
      record_duration(labels, System.convert_time_unit(measurements.duration, :native, :microsecond))
      record_work(kind, operation, purpose, measurements)
    end
  end

  def handle_event(@timeout_event, _measurements, metadata, _config) do
    operation = metadata[:operation]

    if operation in ["merge", "cleanup", "discovery"],
      do: increment({"pulso_compaction_timeouts_total", [{"operation", operation}]}, 1)
  end

  def handle_event([:phoenix, :endpoint, :start], _measurements, metadata, _config) do
    Process.delete(@request_start)
    if http_operation(metadata.conn), do: Process.put(@request_start, System.monotonic_time())
    :ok
  end

  def handle_event(event, _measurements, metadata, config) do
    # Plug's stop hook may run while Phoenix renders an exception, or Phoenix
    # may render from the original conn without that hook. Whichever arrives
    # first completes the request; the other must not double-count it.
    with started when is_integer(started) <- Process.delete(@request_start),
         {kind, operation} <- http_operation(metadata.conn) do
      phase = if event == [:phoenix, :error_rendered], do: :exception, else: :stop
      measurements = %{duration: System.monotonic_time() - started}

      handle_event(
        @event,
        measurements,
        %{kind: kind, operation: operation, purpose: "none", outcome: http_outcome(phase, metadata)},
        config
      )
    else
      _ -> :ok
    end
  end

  defp valid_event?(measurements, kind, operation, purpose, outcome) do
    operation in Map.get(@operations, kind, []) and purpose in @purposes and outcome in @outcomes and
      is_integer(measurements[:duration]) and measurements.duration >= 0
  end

  defp record_duration(labels, duration) do
    increment({"pulso_operation_duration_seconds_count", labels}, 1)
    increment({"pulso_operation_duration_seconds_sum", labels}, duration)

    for {bound, label} <- @bucket_labels do
      increment(
        {"pulso_operation_duration_seconds_bucket", labels ++ [{"le", label}]},
        if(duration <= bound, do: 1, else: 0)
      )
    end

    increment({"pulso_operation_duration_seconds_bucket", labels ++ [{"le", "+Inf"}]}, 1)
  end

  defp record_work(:object, operation, purpose, measurements) do
    for direction <- [:read, :write], bytes = Map.get(measurements, direction), is_integer(bytes) and bytes >= 0 do
      increment(
        {"pulso_object_bytes_total",
         [{"operation", operation}, {"purpose", purpose}, {"direction", to_string(direction)}]},
        bytes
      )
    end
  end

  defp record_work(:compaction, operation, _purpose, measurements) do
    count = Map.get(measurements, :segments, 0)

    if is_integer(count) and count >= 0,
      do: increment({"pulso_compaction_segments_total", [{"operation", operation}]}, count)
  end

  defp record_work(_kind, _operation, _purpose, _measurements), do: :ok

  defp http_operation(conn) do
    # Match the router's decoded segments, not a decoded whole request_path:
    # empty segments are discarded upstream, but an encoded slash stays inside
    # its segment and must not become a separator here.
    path = Enum.map(conn.path_info, &URI.decode/1)
    Map.get(@http_routes, {conn.method, path}) || label_operation(conn.method, path)
  end

  defp label_operation("GET", ["loki", "api", "v1", "label", _name, "values"]), do: {:query, "http_labels"}
  defp label_operation(_method, _path), do: nil

  defp http_outcome(:stop, %{conn: %{status: status}}) when status < 400, do: "ok"
  defp http_outcome(:stop, %{conn: %{status: status}}) when status < 500, do: "rejected"
  defp http_outcome(:stop, _), do: "error"

  defp http_outcome(:exception, metadata) do
    reason = metadata[:reason]
    if is_exception(reason) and Plug.Exception.status(reason) < 500, do: "rejected", else: "exception"
  end

  # A metrics process restart must never fail the operation being observed.
  defp increment(key, amount) do
    :ets.update_counter(table(), key, {2, amount}, {key, 0})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Render counters and instantaneous queue gauges, without storage I/O."
  def render do
    families = snapshot() |> Enum.group_by(&counter_family/1)

    [
      Enum.map(@families, fn {name, type, help} ->
        samples = families |> Map.get(name, []) |> Enum.sort_by(&counter_order/1)
        [metric_header(name, type, help), Enum.map(samples, &render_counter/1)]
      end),
      render_gauges()
    ]
    |> IO.iodata_to_binary()
  end

  defp snapshot do
    :ets.tab2list(table())
  rescue
    ArgumentError -> []
  end

  defp counter_family({{name, _labels}, _value}) do
    case name do
      "pulso_operation_duration_seconds_" <> _ -> "pulso_operation_duration_seconds"
      _ -> name
    end
  end

  # Prometheus requires histogram buckets in numerical order, with +Inf last.
  defp counter_order({{"pulso_operation_duration_seconds_bucket", labels}, _}) do
    {"le", bound} = List.last(labels)
    rank = if bound == "+Inf", do: 11, else: Enum.find_index(@bucket_labels, fn {_, label} -> label == bound end)
    {"pulso_operation_duration_seconds", Enum.drop(labels, -1), rank}
  end

  defp counter_order({{"pulso_operation_duration_seconds_count", labels}, _}),
    do: {"pulso_operation_duration_seconds", labels, 12}

  defp counter_order({{"pulso_operation_duration_seconds_sum", labels}, _}),
    do: {"pulso_operation_duration_seconds", labels, 13}

  defp counter_order({{name, labels}, _}), do: {name, labels, 0}

  defp exported_name("pulso_operations_total"), do: "pulso_detailed_operations_total"

  defp exported_name("pulso_operation_duration_seconds" <> suffix),
    do: "pulso_detailed_operation_duration_seconds" <> suffix

  defp exported_name("pulso_ingest_records_total"), do: "pulso_ingest_delivery_records_total"
  defp exported_name("pulso_compaction_segments_total"), do: "pulso_detailed_compaction_segments_total"
  defp exported_name(name), do: name

  defp render_counter({{name, labels}, value}) do
    value = if name == "pulso_operation_duration_seconds_sum", do: seconds(value), else: Integer.to_string(value)

    [
      exported_name(name),
      "{",
      Enum.map_join(labels, ",", fn {key, value} -> "#{key}=\"#{value}\"" end),
      "} ",
      value,
      "\n"
    ]
  end

  defp render_gauges do
    {mailbox, pending, waiters} = manifest_queues()
    {buffers, reserved_calls, input_bytes, input_rows} = AppendBuffer.stats()
    retention = ManifestCache.retention_statistics()
    capacity = Enum.map(retention, & &1.capacity_ratio) |> Enum.max(fn -> 0.0 end)
    expired = Enum.reduce(retention, 0, &(&1.pending_buckets + &2))

    [
      gauge("pulso_manifest_mailbox_messages", mailbox),
      gauge("pulso_manifest_pending_segments", pending),
      gauge("pulso_manifest_waiting_requests", waiters),
      gauge("pulso_ingest_buffers", buffers),
      gauge("pulso_ingest_buffer_reserved_calls", reserved_calls),
      gauge("pulso_ingest_buffer_input_bytes", input_bytes),
      gauge("pulso_ingest_buffer_rows", input_rows),
      gauge("pulso_query_occupied_slots", occupied_queries()),
      gauge("pulso_retention_root_capacity_ratio", capacity),
      gauge("pulso_retention_pending_buckets", expired),
      gauge("pulso_retention_managed_scopes", length(retention)),
      gauge("pulso_vm_memory_bytes", :erlang.memory(:total)),
      gauge("pulso_vm_run_queue", :erlang.statistics(:run_queue))
    ]
  end

  defp manifest_queues do
    registry = ManifestRegistry

    if Pulso.Runtime.whereis(registry) do
      registry
      |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$2", :"$3"}}]}])
      |> Enum.reduce({0, 0, 0}, &add_manifest_queue/2)
    else
      {0, 0, 0}
    end
  rescue
    ArgumentError -> {0, 0, 0}
  end

  defp add_manifest_queue({pid, info}, {mailbox, pending, waiters}) do
    len =
      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, len} -> len
        _ -> 0
      end

    info = if is_map(info), do: info, else: %{}
    {mailbox + len, pending + Map.get(info, :pending, 0), waiters + Map.get(info, :waiters, 0)}
  end

  defp occupied_queries do
    if Pulso.Runtime.whereis(QuerySlots) do
      QuerySlots
      |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
      |> Enum.uniq()
      |> length()
    else
      0
    end
  rescue
    ArgumentError -> 0
  end

  defp gauge(name, value),
    do: [
      metric_header(name, "gauge", Map.fetch!(@gauge_help, name)),
      name,
      " ",
      if(is_float(value), do: Float.to_string(value), else: Integer.to_string(value)),
      "\n"
    ]

  defp metric_header(name, type, help) do
    name = exported_name(name)
    ["# HELP ", name, " ", help, "\n# TYPE ", name, " ", type, "\n"]
  end

  defp seconds(value), do: Float.to_string(value / 1_000_000)
end
