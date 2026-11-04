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

  alias Pulso.Codec.NIF
  alias Pulso.Storage.S3.MetricLabelSummary
  alias Pulso.Storage.S3.TraceSummary

  defstruct [
    :key,
    :min_ts,
    :max_ts,
    :row_count,
    :byte_size,
    :metric_names,
    :log_services,
    :metric_labels,
    :trace_summary,
    :log_labels
  ]

  @type t :: %__MODULE__{
          key: String.t(),
          min_ts: non_neg_integer() | nil,
          max_ts: non_neg_integer() | nil,
          row_count: non_neg_integer() | nil,
          byte_size: non_neg_integer() | nil,
          metric_names: [String.t()] | nil,
          log_services: [String.t()] | nil,
          metric_labels: %{String.t() => [String.t()]} | nil,
          trace_summary: TraceSummary.t() | nil,
          log_labels: %{String.t() => [String.t()]} | nil
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
    base = if segment.metric_labels, do: Map.put(base, "l", segment.metric_labels), else: base
    base = if segment.log_labels, do: Map.put(base, "el", segment.log_labels), else: base
    if segment.trace_summary, do: Map.put(base, "t", TraceSummary.to_wire(segment.trace_summary)), else: base
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
       metric_labels: MetricLabelSummary.parse(wire["l"]),
       trace_summary: TraceSummary.parse(wire["t"]),
       log_labels: MetricLabelSummary.parse(wire["el"])
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

  @doc "Keep unknown summaries and segments whose name set could satisfy every name matcher."
  def matches_metric_name?(segment, matchers) do
    matchers = Enum.filter(matchers, &match?({"__name__", _, _}, &1))
    matches_metrics?(segment, metrics_filter(matchers))
  end

  @doc "Compile storage matchers once per query for `matches_metrics?/2`."
  def metrics_filter({:metric_plan, _} = plan), do: plan
  def metrics_filter(matchers), do: {:metric_plan, MetricLabelSummary.plan(matchers)}

  @doc """
  Prune metrics only when a complete name or label value set proves that some
  matcher (equality, inequality, or a recognized regex shape) cannot match.
  """
  def matches_metrics?(segment, {:metric_plan, plan}) do
    MetricLabelSummary.satisfiable?(plan, fn
      "__name__" when is_list(segment.metric_names) -> {:ok, segment.metric_names}
      "__name__" -> :error
      _name when segment.metric_labels == nil -> :error
      name -> Map.fetch(segment.metric_labels, name)
    end)
  end

  def matches_metrics?(segment, matchers), do: matches_metrics?(segment, metrics_filter(matchers))

  defp valid_services([_ | _] = services) do
    if length(services) <= 128 and
         Enum.all?(services, &(is_binary(&1) and byte_size(&1) in 1..256)),
       do: services
  end

  defp valid_services(_), do: nil

  # Stream labels summarized per log segment, in priority order: the label
  # summary's byte budget drops later names first. These are the labels Loki
  # pushes from Alloy/Promtail commonly carry, plus Pulso's error labels.
  @log_label_names ~w(error_source error_project cluster namespace container service_name env job pod)

  @doc "Attach a complete bounded set of nonempty promoted log services, or leave it unknown."
  def summarize_logs(segment, records) do
    services = Enum.reduce_while(records, MapSet.new(), &collect_log_service/2)

    services = if services, do: services |> Enum.map(&:binary.copy/1) |> Enum.sort()

    # Atom or invalid UTF-8 keys can normalize during JSON encoding; only
    # summarize maps whose key identity is unchanged at the native boundary.
    valid_resources? =
      Enum.all?(records, fn record ->
        Enum.all?(Map.keys(record.resource || %{}), &(is_binary(&1) and String.valid?(&1)))
      end)

    labels =
      if valid_resources? do
        records
        |> Enum.map(&%{labels: Map.new(@log_label_names, fn name -> {name, log_label_value(&1, name)} end)})
        |> MetricLabelSummary.build(@log_label_names)
      end

    %{segment | log_services: services, log_labels: labels}
  end

  @doc "Stream-label summary of a segment holding exactly the rows of segments with `summaries`."
  def merge_log_labels(summaries), do: MetricLabelSummary.union(summaries, @log_label_names)

  # The value the native matcher sees for `name` (`matchers_pass` in
  # segment_parquet.rs): `service_name` prefers a nonempty promoted service,
  # every other summarized name reads the top-level resource string. A missing
  # label is "", as in LogQL. Non-string values stay unknown independently:
  # atoms in particular become strings at the JSON/native boundary.
  # A non-string promoted service is returned as is, so the label stays unknown.
  defp log_label_value(%{service: service}, "service_name") when service not in [nil, ""], do: service
  defp log_label_value(record, name), do: Map.get(record.resource || %{}, name, "")

  defp collect_log_service(%{service: service}, services) when is_binary(service) and byte_size(service) in 1..256 do
    services = MapSet.put(services, service)
    if MapSet.size(services) <= 128, do: {:cont, services}, else: {:halt, nil}
  end

  defp collect_log_service(_record, _services), do: {:halt, nil}

  @doc """
  Prune service and stream-label selectors only with complete summaries.

  Equality and inequality matchers compare against the summarized values.
  Regex matchers need `opts[:log_label_regexes]` from `compile_log_matchers/1`
  (the native matcher's own regex engine); without it they never prune.
  """
  def matches_log_service?(%__MODULE__{} = segment, opts) do
    service_matches?(segment.log_services, opts) and
      log_labels_match?(segment.log_labels, Keyword.get(opts, :matchers, []), opts[:log_label_regexes] || %{})
  end

  @doc """
  Compile the regex stream matchers of a log query once per query, with the
  regex engine the native segment matcher uses, for `matches_log_service?/2`.
  Patterns that do not compile are left out and so never prune.
  """
  def compile_log_matchers(opts) do
    regexes =
      for {name, op, pattern} <- Keyword.get(opts, :matchers, []),
          op in [:re, :nre],
          name in @log_label_names,
          into: %{} do
        case NIF.compile_log_regex(pattern) do
          {:ok, regex} -> {pattern, regex}
          _ -> {pattern, nil}
        end
      end

    if map_size(regexes) == 0, do: opts, else: Keyword.put(opts, :log_label_regexes, regexes)
  end

  defp log_labels_match?(nil, _matchers, _regexes), do: true

  defp log_labels_match?(summary, matchers, regexes) do
    Enum.all?(matchers, fn {name, op, value} ->
      case Map.fetch(summary, name) do
        {:ok, values} -> Enum.any?(values, &label_value_matches?(op, value, &1, regexes))
        :error -> true
      end
    end)
  end

  defp label_value_matches?(:eq, expected, value, _regexes), do: value == expected
  defp label_value_matches?(:neq, expected, value, _regexes), do: value != expected

  defp label_value_matches?(op, pattern, value, regexes) do
    case Map.get(regexes, pattern) do
      nil -> true
      regex -> regex_matches?(pattern, regex, value) == (op == :re)
    end
  end

  # Summaries repeat the same few values across thousands of segments; one
  # native call per distinct pattern/value pair, bounded per process. A
  # pattern always compiles to the same program, so entries never go stale.
  @regex_memo {__MODULE__, :regex_memo}
  defp regex_matches?(pattern, regex, value) do
    memo = Process.get(@regex_memo, %{})

    case memo do
      %{{^pattern, ^value} => matched} ->
        matched

      _ ->
        matched = NIF.match_log_regex(regex, value)
        if map_size(memo) < 4096, do: Process.put(@regex_memo, Map.put(memo, {pattern, value}, matched))
        matched
    end
  end

  defp service_matches?(nil, _opts), do: true

  defp service_matches?(services, opts) do
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
