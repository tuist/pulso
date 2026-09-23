defmodule Pulso.Storage do
  @moduledoc """
  Behaviour for log storage backends and the runtime dispatcher.

  The active adapter is read from application configuration at call time so it
  can be swapped in tests without recompiling. In dev and prod the default
  adapter is `Pulso.Storage.S3` (step 2). Tests fall back to
  `Pulso.Storage.Memory` because no adapter is configured. Step 3 will replace
  the flat NDJSON layout of the S3 adapter with columnar segments and a
  manifest that supports conditional writes.
  """

  alias Pulso.Record.Log
  alias Pulso.Storage.Memory

  @type tenant :: String.t()
  @type query_opts :: [
          {:start_ts, non_neg_integer()}
          | {:end_ts, non_neg_integer()}
          | {:limit, pos_integer()}
          | {:service, String.t()}
        ]

  @callback append(tenant, [Log.t()]) :: :ok | {:error, term()}
  @callback query(tenant, query_opts) :: {:ok, [Log.t()]} | {:error, term()}

  @spec append(tenant, [Log.t()]) :: :ok | {:error, term()}
  def append(tenant, records), do: adapter().append(tenant, records)

  @spec query(tenant, query_opts) :: {:ok, [Log.t()]} | {:error, term()}
  def query(tenant, opts \\ []), do: adapter().query(tenant, opts)

  @spec adapter() :: module()
  def adapter, do: Application.get_env(:pulso, __MODULE__)[:adapter] || Memory
end
