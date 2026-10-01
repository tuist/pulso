defmodule Pulso.Storage do
  @moduledoc """
  Behaviour for signal storage backends and the runtime dispatcher.

  Every call carries an explicit `signal` — `:logs` or `:metrics` — so the
  same behaviour, dispatcher, and manifest machinery serve all signal
  types. Each signal owns its own record struct (`Pulso.Record.Log`,
  `Pulso.Record.MetricSample`) and its own query-opts shape; the adapter
  dispatches on `signal` to pick the right encoder, decoder, and sort
  order.

  The active adapter is read from application configuration at call time
  so it can be swapped in tests without recompiling. In dev and prod the
  default adapter is `Pulso.Storage.S3`; tests fall back to
  `Pulso.Storage.Memory` because no adapter is configured.
  """

  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage.Memory

  @type tenant :: String.t()
  @type signal :: :logs | :metrics
  @type signal_record :: Log.t() | MetricSample.t()

  @type append_opts :: [
          # Opt-in idempotency: two `append` calls with the same tenant and
          # the same idempotency_key resolve to the same underlying object
          # so a retry does not duplicate.
          {:idempotency_key, String.t()}
        ]

  @type query_opts :: [
          {:start_ts, non_neg_integer()}
          | {:end_ts, non_neg_integer()}
          | {:limit, pos_integer()}
          # Logs-only convenience filter.
          | {:service, String.t()}
          # Metrics-only: `{name, op, value}` label matchers where `op` is
          # `:eq | :neq | :re | :nre`. Logs adapters ignore it.
          | {:matchers, [{String.t(), :eq | :neq | :re | :nre, String.t()}]}
        ]

  @callback append(signal, tenant, [signal_record], append_opts) :: :ok | {:error, term()}
  @callback query(signal, tenant, query_opts) :: {:ok, [signal_record]} | {:error, term()}

  @spec append(signal, tenant, [signal_record], append_opts) :: :ok | {:error, term()}
  def append(signal, tenant, records, opts \\ []) when is_atom(signal) and is_binary(tenant) and is_list(records) do
    adapter().append(signal, tenant, records, opts)
  end

  @spec query(signal, tenant, query_opts) :: {:ok, [signal_record]} | {:error, term()}
  def query(signal, tenant, opts \\ []) when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    adapter().query(signal, tenant, opts)
  end

  @spec adapter() :: module()
  def adapter, do: Application.get_env(:pulso, __MODULE__)[:adapter] || Memory
end
