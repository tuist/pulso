defmodule Pulso.Test.FailingStorage do
  @moduledoc false
  @behaviour Pulso.Storage

  @impl true
  def append(_signal, _tenant, _records, _opts) do
    case Pulso.Runtime.get_env(:pulso, __MODULE__, :error) do
      :error -> {:error, :storage_unavailable}
      :raise -> raise "storage crashed"
      {:error, _} = error -> error
    end
  end

  @impl true
  def query(_signal, _tenant, _opts), do: {:error, :storage_unavailable}
end
