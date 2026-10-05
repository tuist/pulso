defmodule Pulso.Storage.S3.Manifest.Segment do
  @moduledoc """
  One row inside a manifest — the identity and summary metadata of a
  segment that has been PUT to S3.

  `key` is the FULL S3 object key (`tenants/<tenant>/v3/logs/<tail>`),
  not the tail alone. The wire form uses only the tail, and
  `from_wire/1` requires the containing tenant + signal to reconstitute
  the full key. Keeping the full key in memory means the query path can
  hand it straight to `ObjectStore.get/2` without a per-segment format
  operation.
  """

  alias Pulso.Storage.S3.MetricLabelSummary

  defstruct [:key, :min_ts, :max_ts, :row_count, :byte_size, :metric_names, :log_services, :metric_labels]

  @type t :: %__MODULE__{
          key: String.t(),
          min_ts: non_neg_integer() | nil,
          max_ts: non_neg_integer() | nil,
          row_count: non_neg_integer() | nil,
          byte_size: non_neg_integer() | nil,
          metric_names: [String.t()] | nil,
          log_services: [String.t()] | nil,
          metric_labels: %{String.t() => [String.t()]} | nil
        }

  @doc """
  Build a segment record for a segment the ingester just wrote.

  `tail` is the object key with the leading `tenants/<tenant>/v3/<signal>/`
  stripped. The struct holds the full key so the query path never has to
  re-glue the prefix.
  """
  @spec build(String.t(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: t()
  def build(key, min_ts, max_ts, row_count)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) and is_integer(row_count) do
    %__MODULE__{
      key: key,
      min_ts: min_ts,
      max_ts: max_ts,
      row_count: row_count,
      byte_size: nil
    }
  end

  @spec build(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer() | nil
        ) :: t()
  def build(key, min_ts, max_ts, row_count, byte_size)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) and is_integer(row_count) do
    %__MODULE__{
      key: key,
      min_ts: min_ts,
      max_ts: max_ts,
      row_count: row_count,
      byte_size: byte_size
    }
  end

  @doc """
  Reduce a segment to its compact wire form: short keys, the segment's
  key relative to its containing tenant/signal prefix.
  """
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = segment) do
    base = %{
      "k" => segment.key,
      "mn" => segment.min_ts,
      "mx" => segment.max_ts,
      "r" => segment.row_count
    }

    base = if segment.byte_size, do: Map.put(base, "b", segment.byte_size), else: base
    base = if segment.metric_names, do: Map.put(base, "n", segment.metric_names), else: base
    base = if segment.log_services, do: Map.put(base, "ls", segment.log_services), else: base
    if segment.metric_labels, do: Map.put(base, "l", segment.metric_labels), else: base
  end

  @doc """
  Parse one wire-form segment. Returns `{:error, :invalid_segment}` on
  a shape that the writer would never produce, so a garbled manifest
  fails loudly rather than silently mispruning a query.
  """
  @spec from_wire(map()) :: {:ok, t()} | {:error, :invalid_segment}
  def from_wire(%{"k" => key, "mn" => min_ts, "mx" => max_ts} = wire)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) do
    row_count = valid_non_neg(wire["r"])
    byte_size = valid_non_neg(wire["b"])

    {:ok,
     %__MODULE__{
       key: key,
       min_ts: min_ts,
       max_ts: max_ts,
       row_count: row_count,
       byte_size: byte_size,
       metric_names: valid_names(wire["n"]),
       log_services: valid_services(wire["ls"]),
       metric_labels: MetricLabelSummary.parse(wire["l"])
     }}
  end

  def from_wire(_), do: {:error, :invalid_segment}

  @doc """
  Does this segment's `[min_ts, max_ts]` intersect `[start_ts, end_ts]`?
  A nil bound on either side is treated as unbounded on that side.
  Segments whose own bounds are nil are kept — the query cannot safely
  skip them without knowing their real range.
  """
  @spec intersects?(t(), non_neg_integer() | nil, non_neg_integer() | nil) :: boolean()
  def intersects?(%__MODULE__{min_ts: nil}, _start_ts, _end_ts), do: true
  def intersects?(%__MODULE__{max_ts: nil}, _start_ts, _end_ts), do: true

  def intersects?(%__MODULE__{min_ts: min_ts, max_ts: max_ts}, start_ts, end_ts) do
    (start_ts == nil or max_ts >= start_ts) and (end_ts == nil or min_ts <= end_ts)
  end

  # Unknown or malformed summaries must never exclude a segment.
  defp valid_names(names) when is_list(names) do
    if length(names) <= 128 and Enum.all?(names, &(is_binary(&1) and byte_size(&1) <= 256)),
      do: names
  end

  defp valid_names(_), do: nil

  @doc "Attach a complete, bounded metric-name set; nil means unknown, never truncated."
  def summarize_metrics(segment, records) do
    names = Enum.reduce_while(records, MapSet.new(), &collect_metric_name/2)

    names = if names, do: names |> Enum.map(&:binary.copy/1) |> Enum.sort()
    %{segment | metric_names: names, metric_labels: MetricLabelSummary.build(records)}
  end

  defp collect_metric_name(record, names) do
    name = Map.get(record.labels, "__name__", "")

    if is_binary(name) and byte_size(name) <= 256 do
      names = MapSet.put(names, name)
      if MapSet.size(names) <= 128, do: {:cont, names}, else: {:halt, nil}
    else
      {:halt, nil}
    end
  end

  @doc "Keep unknown summaries and segments that could satisfy all exact name matchers."
  def matches_metric_name?(%__MODULE__{metric_names: nil}, _matchers), do: true

  def matches_metric_name?(%__MODULE__{metric_names: names}, matchers) do
    Enum.all?(matchers, fn
      {"__name__", :eq, value} -> value in names
      _ -> true
    end)
  end

  # Only nonempty promoted fields are summarized: null/empty services fall
  # back to resource labels in the native matcher, so they remain unknown.
  @doc "Prune metrics only when a complete name or label value set proves an exact mismatch."
  def matches_metrics?(segment, matchers) do
    matches_metric_name?(segment, matchers) and MetricLabelSummary.matches?(segment.metric_labels, matchers)
  end

  defp valid_services([_ | _] = services) do
    if length(services) <= 128 and
         Enum.all?(services, &(is_binary(&1) and byte_size(&1) in 1..256)),
       do: services
  end

  defp valid_services(_), do: nil

  @doc "Attach a complete bounded set of nonempty promoted log services, or leave it unknown."
  def summarize_logs(segment, records) do
    services =
      Enum.reduce_while(records, MapSet.new(), fn record, services ->
        service = record.service

        if is_binary(service) and byte_size(service) in 1..256 do
          services = MapSet.put(services, service)
          if MapSet.size(services) <= 128, do: {:cont, services}, else: {:halt, nil}
        else
          {:halt, nil}
        end
      end)

    services = if services, do: services |> Enum.map(&:binary.copy/1) |> Enum.sort()
    %{segment | log_services: services}
  end

  @doc "Prune known log services for exact promoted-field selectors; unknown summaries always scan."
  def matches_log_service?(%__MODULE__{log_services: nil}, _opts), do: true

  def matches_log_service?(%__MODULE__{log_services: services}, opts) do
    direct = Keyword.get(opts, :service)
    matchers = Keyword.get(opts, :matchers, [])

    Enum.any?(services, fn service ->
      (is_nil(direct) or service == direct) and
        Enum.all?(matchers, fn
          {name, :eq, value} when name in ["service", "service_name"] -> service == value
          _ -> true
        end)
    end)
  end

  defp valid_non_neg(v) when is_integer(v) and v >= 0, do: v
  defp valid_non_neg(_), do: nil
end
