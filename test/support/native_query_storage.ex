defmodule Pulso.Test.NativeQueryStorage do
  @moduledoc false
  @behaviour Pulso.Storage

  alias Pulso.Codec.NIF
  alias Pulso.Storage.Memory

  def append(signal, tenant, records, opts), do: Memory.append(signal, tenant, records, opts)

  def query(:logs, tenant, opts) do
    with {:ok, records} <- Memory.query(:logs, tenant, []),
         {:ok, blob, _, _, _} <- NIF.encode_log_segment_parquet(records) do
      NIF.decode_log_segment_parquet(
        blob,
        opts[:start_ts],
        opts[:end_ts],
        opts[:service],
        Keyword.get(opts, :matchers, []),
        Keyword.get(opts, :line_filters, [])
      )
    end
  end

  def query(:metrics, tenant, opts) do
    with {:ok, samples} <- Memory.query(:metrics, tenant, []),
         {:ok, blob, _, _, _} <- NIF.encode_metric_segment_parquet(samples) do
      NIF.decode_metric_segment_parquet(blob, opts[:start_ts], opts[:end_ts], Keyword.get(opts, :matchers, []))
    end
  end
end
