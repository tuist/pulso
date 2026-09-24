defmodule Pulso.Storage do
  @moduledoc """
  Behaviour for log storage backends and the runtime dispatcher.

  The active adapter is read from application configuration at call time so it
  can be swapped in tests without recompiling. In dev and prod the default
  adapter is `Pulso.Storage.S3` — see that module for the schema layout and
  its `[min_ts, max_ts]` per-segment key bounds that let `query/2` skip
  objects outside a time range without a separate manifest. Tests fall back
  to `Pulso.Storage.Memory` because no adapter is configured.
  """

  alias Pulso.Record.Log
  alias Pulso.Storage.Memory

  @type tenant :: String.t()
  @type append_opts :: [
          # Opt-in idempotency: two `append` calls with the same tenant and
          # the same idempotency_key resolve to the same underlying object
          # so a retry does not duplicate. Callers who want distinct writes
          # for identical payloads (e.g. two producers with genuinely
          # different events that happen to serialize the same) simply
          # omit the key.
          {:idempotency_key, String.t()}
        ]
  @type query_opts :: [
          {:start_ts, non_neg_integer()}
          | {:end_ts, non_neg_integer()}
          | {:limit, pos_integer()}
          | {:service, String.t()}
        ]

  @callback append(tenant, [Log.t()], append_opts) :: :ok | {:error, term()}
  @callback query(tenant, query_opts) :: {:ok, [Log.t()]} | {:error, term()}

  @spec append(tenant, [Log.t()], append_opts) :: :ok | {:error, term()}
  def append(tenant, records, opts \\ []), do: adapter().append(tenant, records, opts)

  @spec query(tenant, query_opts) :: {:ok, [Log.t()]} | {:error, term()}
  def query(tenant, opts \\ []), do: adapter().query(tenant, opts)

  @spec adapter() :: module()
  def adapter, do: Application.get_env(:pulso, __MODULE__)[:adapter] || Memory
end
