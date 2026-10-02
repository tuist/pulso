defmodule Pulso.Record.MetricSample do
  @moduledoc """
  One Prometheus-style metric sample: a label set, a timestamp, and a value.

  By convention the metric name lives inside `labels` under `"__name__"`,
  which matches the Prometheus data model and keeps the wire boundary
  thin: the `remote_write` decoder promotes every label pair into this
  map as-is, no field splitting.

  `series_id` is the stable 64-bit fingerprint of the label set, computed
  by the Rust codec (Pulso's port of Prometheus's `labels.StableHash`). It
  is left `nil` at wire-decode time and filled in before Parquet encode.
  Readers should treat `series_id` as a pruning accelerator only — identity
  is the canonical labels, not the hash.

  `timestamp_ns` is nanoseconds since the Unix epoch. The wire protocols
  (Prometheus remote_write v1 and OTLP metrics) express timestamps in
  milliseconds and nanoseconds respectively; conversion happens at the
  wire boundary so the storage path only ever sees nanoseconds.
  """

  defstruct [:series_id, :timestamp_ns, :value, labels: %{}]

  @type t :: %__MODULE__{
          series_id: non_neg_integer() | nil,
          timestamp_ns: integer() | nil,
          value: float() | nil,
          labels: %{optional(String.t()) => String.t()}
        }
end
