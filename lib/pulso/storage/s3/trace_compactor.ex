defmodule Pulso.Storage.S3.TraceCompactor do
  @moduledoc """
  Bounded native Arrow trace compaction through shared signal maintenance.
  Retention, paged retirement cleanup and orphan reclamation use the same
  authoritative event-time floor as logs and metrics, independently of merges.
  """
  alias Pulso.Storage.S3.SignalCompactor

  def maintain(tenant, config, opts \\ []), do: compact(tenant, config, opts)

  def compact(tenant, config, opts \\ []),
    do: SignalCompactor.compact(tenant, Map.put(config, :compaction_signal, :traces), opts)

  def cleanup(tenant, config, opts \\ []),
    do: SignalCompactor.cleanup(tenant, Map.put(config, :compaction_signal, :traces), opts)
end
