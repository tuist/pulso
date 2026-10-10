defmodule Pulso.QueryRunner do
  @moduledoc """
  Runs public queries in supervised, bounded tasks. Registry ownership stays with
  the worker, including when a native call outlives the request deadline.
  """

  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Runtime
  alias Pulso.Runtime.Registry
  alias Pulso.Runtime.Task

  def run(tenant, class, fun, opts \\ %{}) when is_binary(tenant) and is_function(fun, 1) do
    with {:ok, timeout} <- timeout(opts) do
      deadline = System.monotonic_time(:millisecond) + timeout

      task =
        try do
          Task.Supervisor.async_nolink(TaskSupervisor, fn ->
            Process.flag(:max_heap_size, %{
              size: heap_words(class),
              kill: true,
              error_logger: false
            })

            with :ok <- acquire({tenant}, config(:max_per_tenant, 2)),
                 :ok <- acquire_interactive(class),
                 :ok <- acquire({:class, class}, class_limit(class)) do
              fun.(deadline)
            end
          end)
        rescue
          RuntimeError -> nil
        end

      await(task, max(1, deadline - System.monotonic_time(:millisecond)))
    end
  end

  def storage_opts(deadline, max_records \\ 100_000) do
    [
      max_records: max_records,
      max_scan_segments: config(:max_scan_segments, 1024),
      max_scan_bytes: config(:max_scan_bytes, 134_217_728),
      max_scan_rows: config(:max_scan_rows, 1_000_000),
      deadline_ms: deadline
    ]
  end

  defp await(nil, _timeout), do: {:error, :query_overloaded}

  defp await(task, timeout) do
    case Task.yield(task, timeout) do
      {:ok, result} ->
        result

      {:exit, :killed} ->
        {:error, :query_resource_limit}

      {:exit, _} ->
        {:error, :query_execution_failed}

      nil ->
        case Task.shutdown(task, :brutal_kill) do
          {:ok, result} -> result
          _ -> {:error, :query_timeout}
        end
    end
  end

  defp timeout(opts) do
    case Map.get(opts, :timeout_ms, config(:timeout_ms, 10_000)) do
      ms when is_integer(ms) and ms > 0 -> {:ok, min(ms, config(:timeout_ms, 10_000))}
      _ -> {:error, :invalid_timeout}
    end
  end

  defp acquire(_key, max) when not is_integer(max) or max < 1, do: {:error, :query_overloaded}

  defp acquire(key, max), do: acquire_slot(key, max, 0)
  defp acquire_slot(_key, max, max), do: {:error, :query_overloaded}

  defp acquire_slot(key, max, slot) do
    registry_key =
      case key do
        {tenant} -> {tenant, slot}
        {:class, class} -> {:class, class, slot}
      end

    case Registry.register(QuerySlots, registry_key, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> acquire_slot(key, max, slot + 1)
    end
  end

  defp config(key, default), do: Keyword.get(Runtime.get_env(:pulso, __MODULE__, []), key, default)

  defp acquire_interactive(:alerting), do: :ok
  defp acquire_interactive(_class), do: acquire({:class, :interactive}, config(:max_interactive, 3))

  defp class_limit(class) do
    default = if class in [:raw, :discovery, :alerting], do: 1, else: 3
    Map.get(config(:class_limits, %{}), class, default)
  end

  defp heap_words(class) when class in [:promql, :alerting] do
    Keyword.get(Runtime.get_env(:pulso, Evaluator, []), :max_heap_words, config(:max_heap_words, 16_000_000))
  end

  defp heap_words(_class), do: config(:max_heap_words, 16_000_000)
end
