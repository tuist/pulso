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

  @type matcher_op :: :eq | :neq | :re | :nre
  @type matcher :: {name :: String.t(), op :: matcher_op(), value :: String.t()}

  @type line_filter_op :: :contains | :not_contains | :match_re | :not_match_re
  @type line_filter :: {op :: line_filter_op(), value :: String.t()}

  @type query_opts :: [
          {:start_ts, integer()}
          | {:end_ts, integer()}
          | {:limit, pos_integer()}
          | {:max_records, non_neg_integer()}
          | {:max_scan_segments, pos_integer()}
          | {:max_scan_bytes, pos_integer()}
          | {:max_scan_rows, pos_integer()}
          | {:deadline_ms, integer()}
          # Logs-only convenience filter.
          | {:service, String.t()}
          # Label matchers. On logs these filter on the stream labels
          # the Loki push path stores in `Log.resource`; on metrics they
          # filter on the sample's label set. Pushed into the Rust
          # Parquet decoder so rejected rows never materialise as
          # Erlang terms.
          | {:matchers, [matcher()]}
          # Logs-only: substring or regex predicates on `Log.body`,
          # pushed into the Rust decoder alongside the label matchers.
          | {:line_filters, [line_filter()]}
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
