defmodule Pulso.Record.Log do
  @moduledoc """
  Internal representation of a single log record after decoding from OTLP.

  Timestamps are Unix nanoseconds. `service` and `severity_text` are surfaced
  as top-level fields because they are the most common filter dimensions;
  everything else lives in `attributes`.
  """

  # `timestamp_ns` may be nil at decode time — the OTLP spec allows both
  # `time_unix_nano` and `observed_time_unix_nano` to be absent, and it
  # explicitly treats a value of 0 as "unknown". The storage adapter fills
  # in a stored timestamp from the observed value or the wall clock; the
  # nil is what lets a retry keep an idempotent fingerprint (the caller
  # sent the same batch with no timestamps, so both retries hash the same).
  defstruct [
    :timestamp_ns,
    :observed_timestamp_ns,
    :severity_number,
    :severity_text,
    :service,
    :body,
    :trace_id,
    :span_id,
    attributes: %{},
    resource: %{}
  ]

  @type t :: %__MODULE__{
          timestamp_ns: non_neg_integer() | nil,
          observed_timestamp_ns: non_neg_integer() | nil,
          severity_number: non_neg_integer() | nil,
          severity_text: String.t() | nil,
          service: String.t() | nil,
          body: String.t() | nil,
          trace_id: String.t() | nil,
          span_id: String.t() | nil,
          attributes: %{optional(String.t()) => term()},
          resource: %{optional(String.t()) => term()}
        }
end
