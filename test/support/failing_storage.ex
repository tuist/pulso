defmodule Pulso.Test.FailingStorage do
  @moduledoc false
  @behaviour Pulso.Storage

  @impl true
  def append(_signal, _tenant, _records, _opts) do
    case Pulso.Runtime.get_env(:pulso, __MODULE__, :error) do
      :error ->
        {:error, :storage_unavailable}

      :raise ->
        raise "storage crashed"

      {:block, owner} ->
        send(owner, {:append_started, self()})

        receive do
          :continue -> :ok
        end

      {:error, _} = error ->
        error
    end
  end

  @impl true
  def query(_signal, _tenant, _opts), do: {:error, :storage_unavailable}
end
