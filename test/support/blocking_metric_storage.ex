defmodule Pulso.Test.BlockingMetricStorage do
  @moduledoc false
  @behaviour Pulso.Storage

  alias Pulso.Storage.Memory

  def append(signal, tenant, records, opts), do: Memory.append(signal, tenant, records, opts)

  def query(_signal, tenant, _opts) do
    case Pulso.Runtime.fetch_env!(:pulso, __MODULE__) do
      {:error, _} = error ->
        error

      owner when is_pid(owner) ->
        ref = Process.monitor(owner)
        send(owner, {:blocked_metric_query, self(), tenant})

        receive do
          {:release, result} ->
            Process.demonitor(ref, [:flush])
            result

          {:DOWN, ^ref, :process, ^owner, _} ->
            {:error, :query_timeout}
        end
    end
  end
end
