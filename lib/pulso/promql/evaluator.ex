defmodule Pulso.PromQL.Evaluator do
  @moduledoc """
  Evaluates float metric selectors, range functions, and vector aggregations.

  Each selector loads its whole evaluation interval once through Pulso.Storage,
  including the lookback or range window. Series identity is the label map,
  never the series hash. Rates correct counter resets and extrapolate using
  Prometheus's float-sample algorithm, including the counter's zero boundary.
  Results use Prometheus's envelope with numeric timestamps in seconds.

  This first evaluator folds selected samples in Elixir. Columnar aggregation
  in Rust is a future optimization; unsupported expression types fail in parsing.
  """
  alias Pulso.PromQL.Parser
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Storage

  @lookback_ns 300_000_000_000
  @max_steps Pulso.QueryLimits.max_evaluation_steps()

  def query(query, tenant, opts \\ %{}) do
    Pulso.SelfMetrics.track(:query, :promql, fn -> do_query(query, tenant, opts) end)
  end

  defp do_query(query, tenant, opts) do
    with :ok <- check_slots(tenant),
         {:ok, timeout} <- query_timeout(opts),
         {:ok, task} <- start_query_task(query, tenant, opts, timeout) do
      case Task.yield(task, timeout) do
        {:ok, result} ->
          result

        {:exit, :killed} ->
          {:error, :query_resource_limit}

        {:exit, _} ->
          {:error, :query_execution_failed}

        nil ->
          Task.shutdown(task, :brutal_kill)
          {:error, :query_timeout}
      end
    end
  end

  defp check_slots(tenant) do
    full = Enum.all?(0..1, &(Registry.lookup(QuerySlots, {tenant, &1}) != []))
    if full, do: {:error, :query_overloaded}, else: :ok
  end

  defp query_timeout(opts) do
    case Map.get(opts, :timeout_ms, 10_000) do
      ms when is_integer(ms) and ms > 0 -> {:ok, min(ms, 10_000)}
      _ -> {:error, :invalid_timeout}
    end
  end

  defp start_query_task(query, tenant, opts, timeout) do
    task =
      Task.Supervisor.async_nolink(TaskSupervisor, fn ->
        Process.put(:promql_deadline_ms, System.monotonic_time(:millisecond) + timeout)

        Process.flag(:max_heap_size, %{
          size: Keyword.get(Application.get_env(:pulso, __MODULE__, []), :max_heap_words, 16_000_000),
          kill: true,
          error_logger: false
        })

        with :ok <- acquire_slot(tenant),
             {:ok, expr} <- Parser.parse(query),
             {:ok, kind, steps} <- evaluation_steps(opts) do
          run_query(expr, tenant, steps, kind)
        end
      end)

    {:ok, task}
  rescue
    RuntimeError -> {:error, :query_overloaded}
  end

  defp acquire_slot(tenant, slot \\ 0)
  defp acquire_slot(_tenant, 2), do: {:error, :query_overloaded}

  defp acquire_slot(tenant, slot) do
    case Registry.register(QuerySlots, {tenant, slot}, nil) do
      {:ok, _} -> :ok
      {:error, {:already_registered, _}} -> acquire_slot(tenant, slot + 1)
    end
  end

  defp run_query(expr, tenant, steps, kind) do
    with {:ok, series, warnings} <- evaluate(expr, tenant, steps) do
      result = envelope(kind, series)
      {:ok, if(warnings == [], do: result, else: Map.put(result, "warnings", Enum.uniq(warnings)))}
    end
  rescue
    ArithmeticError -> {:error, :nonfinite_result}
  end

  defp limits do
    config = Application.get_env(:pulso, __MODULE__, [])

    %{
      samples: Keyword.get(config, :max_samples, 100_000),
      work: Keyword.get(config, :max_work, 5_000_000),
      points: Keyword.get(config, :max_result_points, 100_000),
      segments: Keyword.get(config, :max_scan_segments, 1024),
      bytes: Keyword.get(config, :max_scan_bytes, 134_217_728),
      rows: Keyword.get(config, :max_scan_rows, 1_000_000)
    }
  end

  defp evaluation_steps(opts) do
    finish = Map.get(opts, :end_ts_ns, System.system_time(:nanosecond))

    finish = if timestamp?(finish), do: finish, else: :invalid

    case Map.fetch(opts, :step_ns) do
      :error when is_integer(finish) and not is_map_key(opts, :start_ts_ns) ->
        {:ok, :vector, [finish]}

      {:ok, step} when is_integer(step) and step > 0 and is_integer(finish) ->
        range_steps(Map.get(opts, :start_ts_ns), Map.get(opts, :end_ts_ns), step)

      _ ->
        {:error, :invalid_evaluation_time_or_step}
    end
  end

  defp timestamp?(value) when is_integer(value),
    do: value >= -9_223_372_036_854_775_808 and value <= 9_223_372_036_854_775_807

  defp timestamp?(_value), do: false

  defp range_steps(start, finish, step)
       when is_integer(start) and is_integer(finish) and start <= finish and start >= -9_223_372_036_854_775_808 and
              start <= 9_223_372_036_854_775_807 do
    count = div(finish - start, step)

    if count < @max_steps do
      {:ok, :matrix, Enum.map(0..count, &(start + &1 * step))}
    else
      {:error, :invalid_range_or_too_many_steps}
    end
  end

  defp range_steps(_, _, _), do: {:error, :invalid_range_or_too_many_steps}

  defp evaluate({:aggregate, op, grouping, inner}, tenant, steps) do
    with {:ok, series, warnings} <- evaluate(inner, tenant, steps) do
      grouped = for {labels, samples} <- series, {ts, value} <- samples, do: {{group_key(labels, grouping), ts}, value}

      result =
        grouped
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Enum.map(fn {{labels, ts}, values} -> {labels, {ts, aggregate(op, values)}} end)
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Enum.map(fn {labels, samples} -> {labels, Enum.sort(samples)} end)

      {:ok, result, warnings}
    end
  end

  defp evaluate({:selector, matchers, offset}, tenant, steps) do
    select(tenant, matchers, offset, @lookback_ns, steps, :latest)
  end

  defp evaluate({:function, op, {:range, matchers, window, offset}}, tenant, steps) do
    select(tenant, matchers, offset, window, steps, op)
  end

  defp select(tenant, matchers, offset, window, steps, op) do
    limits = limits()

    opts = [
      start_ts: max(-9_223_372_036_854_775_808, hd(steps) - offset - window + 1),
      end_ts: max(-9_223_372_036_854_775_808, List.last(steps) - offset),
      matchers: Enum.map(matchers, &anchored_matcher/1),
      max_records: limits.samples,
      max_scan_segments: limits.segments,
      max_scan_bytes: limits.bytes,
      max_scan_rows: limits.rows,
      deadline_ms: Process.get(:promql_deadline_ms)
    ]

    with {:ok, samples} <- query_samples(tenant, opts),
         {:ok, groups, warnings} <- prepare_series(samples),
         {:ok, result} <- evaluate_groups(groups, steps, offset, window, op, limits) do
      if length(Enum.uniq_by(result, &elem(&1, 0))) == length(result),
        do: {:ok, result, warnings},
        else: {:error, :duplicate_result_label_sets}
    end
  end

  defp query_samples(tenant, opts) do
    case Storage.query(:metrics, tenant, opts) do
      {:ok, samples} -> {:ok, samples}
      {:error, reason} when reason in [:query_sample_limit, :query_scan_limit, :query_timeout] -> {:error, reason}
      {:error, reason} -> {:error, {:storage_error, reason}}
    end
  end

  defp evaluate_groups(groups, steps, offset, window, op, limits) do
    Enum.reduce_while(groups, {:ok, [], 0, 0}, fn group, acc ->
      evaluate_group(group, acc, steps, offset, window, op, limits)
    end)
    |> case do
      {:ok, result, _, _} -> {:ok, result}
      error -> error
    end
  end

  defp evaluate_group(group, {:ok, result, work, points}, steps, offset, window, op, limits) do
    case evaluate_series(group, steps, offset, window, op, work, limits.work) do
      {:ok, {labels, values}, work} when points + length(values) <= limits.points ->
        result = if values == [], do: result, else: [{labels, values} | result]
        {:cont, {:ok, result, work, points + length(values)}}

      {:ok, _, _} ->
        {:halt, {:error, :query_result_limit}}

      error ->
        {:halt, error}
    end
  end

  defp evaluate_series({labels, ordered}, steps, offset, window, op, work, max_work) do
    state = %{remaining: ordered, bucket: :queue.new(), values: [], work: work}

    result =
      Enum.reduce_while(steps, {:ok, state}, fn ts, {:ok, state} ->
        step_bucket(ts, state, offset, window, op, max_work)
      end)

    case result do
      {:ok, state} ->
        labels = if op == :latest, do: labels, else: Map.delete(labels, "__name__")
        {:ok, {labels, Enum.reverse(state.values)}, state.work}

      error ->
        error
    end
  end

  defp step_bucket(ts, state, offset, window, op, max_work) do
    finish = ts - offset
    {arrivals, remaining} = Enum.split_while(state.remaining, &(&1.timestamp_ns <= finish))
    bucket = :queue.join(state.bucket, :queue.from_list(arrivals)) |> expire(finish - window)
    work = state.work + length(arrivals) + if(op == :latest, do: 1, else: max(1, :queue.len(bucket)))

    if work > max_work do
      {:halt, {:error, :query_work_limit}}
    else
      value = bucket_value(op, bucket, finish - window, finish)
      values = if is_nil(value), do: state.values, else: [{ts, value} | state.values]
      {:cont, {:ok, %{remaining: remaining, bucket: bucket, work: work, values: values}}}
    end
  end

  defp expire(bucket, start) do
    case :queue.peek(bucket) do
      {:value, sample} when sample.timestamp_ns <= start -> expire(:queue.drop(bucket), start)
      _ -> bucket
    end
  end

  defp bucket_value(:latest, bucket, _, _) do
    case :queue.peek_r(bucket) do
      {:value, sample} -> sample.value
      :empty -> nil
    end
  end

  defp bucket_value(op, bucket, start, finish), do: value(op, :queue.to_list(bucket), start, finish)

  # Both storage adapters use unanchored matching; Prometheus selectors anchor
  # the entire label and let dot match newline. Push the wrapped pattern down.
  defp anchored_matcher({name, op, pattern}) when op in [:re, :nre], do: {name, op, "(?s:\\A(?:#{pattern})\\z)"}
  defp anchored_matcher(matcher), do: matcher

  defp prepare_series(samples) do
    Enum.reduce_while(Enum.group_by(samples, & &1.labels), {:ok, [], []}, &prepare_group/2)
  end

  defp prepare_group({labels, group}, {:ok, acc, warnings}) do
    if Enum.any?(group, &(not is_integer(&1.timestamp_ns) or not is_number(&1.value))) do
      {:halt, {:error, :unsupported_sample_value}}
    else
      ordered = group |> Enum.sort_by(&{&1.timestamp_ns, -&1.value}) |> Enum.uniq_by(& &1.timestamp_ns)
      conflicts = length(Enum.uniq_by(group, &{&1.timestamp_ns, &1.value})) != length(ordered)

      warnings =
        if conflicts,
          do: ["Conflicting samples at the same timestamp were resolved using the maximum value." | warnings],
          else: warnings

      {:cont, {:ok, [{labels, ordered} | acc], warnings}}
    end
  end

  defp value(_, [], _, _), do: nil

  defp value(op, samples, start, finish) when op in [:rate, :increase, :delta] do
    if length(samples) >= 2, do: extrapolated_change(op, samples, start, finish)
  end

  defp value(:irate, samples, _, _) do
    case Enum.take(samples, -2) do
      [first, last] ->
        change = if last.value < first.value, do: last.value, else: last.value - first.value
        change / ((last.timestamp_ns - first.timestamp_ns) / 1_000_000_000)

      _ ->
        nil
    end
  end

  defp value(:sum_over_time, samples, _, _), do: aggregate(:sum, Enum.map(samples, & &1.value))
  defp value(:avg_over_time, samples, _, _), do: aggregate(:avg, Enum.map(samples, & &1.value))
  defp value(:min_over_time, samples, _, _), do: aggregate(:min, Enum.map(samples, & &1.value))
  defp value(:max_over_time, samples, _, _), do: aggregate(:max, Enum.map(samples, & &1.value))
  defp value(:count_over_time, samples, _, _), do: length(samples) * 1.0

  defp extrapolated_change(op, samples, start, finish) do
    first = hd(samples)
    last = List.last(samples)
    change = last.value - first.value

    {change, _} =
      Enum.reduce(tl(samples), {change, first.value}, fn sample, {delta, previous} ->
        reset = if op != :delta and sample.value < previous, do: previous, else: 0
        {delta + reset, sample.value}
      end)

    interval = (last.timestamp_ns - first.timestamp_ns) / 1_000_000_000
    average = interval / (length(samples) - 1)
    left = boundary_duration((first.timestamp_ns - start) / 1_000_000_000, average)
    right = boundary_duration((finish - last.timestamp_ns) / 1_000_000_000, average)

    left =
      if op != :delta and change > 0 and first.value >= 0, do: min(left, interval * first.value / change), else: left

    factor = (interval + left + right) / interval
    factor = if op == :rate, do: factor / ((finish - start) / 1_000_000_000), else: factor
    change * factor
  end

  defp boundary_duration(distance, average) do
    if distance >= average * 1.1, do: average / 2, else: distance
  end

  defp aggregate(:sum, values), do: Enum.sum(values) * 1.0
  defp aggregate(:avg, values), do: Enum.sum(values) / length(values)
  defp aggregate(:min, values), do: Enum.min(values)
  defp aggregate(:max, values), do: Enum.max(values)
  defp aggregate(:count, values), do: length(values) * 1.0
  defp group_key(_, nil), do: %{}
  defp group_key(labels, {:grouping, :by, names}), do: Map.take(labels, names)
  defp group_key(labels, {:grouping, :without, names}), do: Map.drop(labels, ["__name__" | names])

  # Match Prometheus's shortest-decimal wire formatting and exponent cutoffs.
  defp format_value(value) when is_integer(value), do: Integer.to_string(value)

  defp format_value(value) do
    text = Float.to_string(value)

    case String.split(text, "e") do
      [plain] -> String.trim_trailing(plain, ".0")
      [mantissa, exponent] -> format_exponent(mantissa, String.to_integer(exponent), abs(value))
    end
  end

  defp format_exponent(mantissa, exponent, magnitude) when magnitude < 1.0e-6 or magnitude >= 1.0e21 do
    sign = if exponent < 0, do: "-", else: "+"
    String.trim_trailing(mantissa, ".0") <> "e" <> sign <> String.pad_leading(Integer.to_string(abs(exponent)), 2, "0")
  end

  defp format_exponent(mantissa, exponent, _) do
    sign = if String.starts_with?(mantissa, "-"), do: "-", else: ""
    [whole, fraction] = mantissa |> String.trim_leading("-") |> String.split(".")
    digits = whole <> fraction
    position = byte_size(whole) + exponent

    decimal =
      cond do
        position <= 0 -> "0." <> String.duplicate("0", -position) <> digits
        position >= byte_size(digits) -> digits <> String.duplicate("0", position - byte_size(digits))
        true -> binary_part(digits, 0, position) <> "." <> binary_part(digits, position, byte_size(digits) - position)
      end

    trimmed =
      if String.contains?(decimal, "."),
        do: decimal |> String.trim_trailing("0") |> String.trim_trailing("."),
        else: decimal

    sign <> trimmed
  end

  defp envelope(kind, series) do
    result =
      series
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {labels, samples} ->
        values = Enum.map(samples, fn {ts, value} -> [ts / 1_000_000_000, format_value(value)] end)
        field = if kind == :vector, do: "value", else: "values"
        %{"metric" => labels, field => if(kind == :vector, do: hd(values), else: values)}
      end)

    %{"status" => "success", "data" => %{"resultType" => Atom.to_string(kind), "result" => result}}
  end
end
