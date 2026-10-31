defmodule Pulso.LogQL.NativeRange do
  @moduledoc """
  Native input for streamed range aggregations.

  For a range aggregation whose pipeline uses only line filters, `logfmt`,
  `regexp`, string/regex/number label filters and a plain `unwrap`, the
  storage scan runs the pipeline in Rust and returns one packed, ordered
  sample list per series, without building Erlang terms for log records.
  It answers exactly as `Pulso.LogQL.MetricEval`'s Elixir pipeline would;
  any query or row it cannot reproduce returns `:fallback` and the caller
  evaluates in Elixir. Adapters without native scans also fall back.
  """

  alias Pulso.Codec.NIF
  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.QueryValidation
  alias Pulso.LogQL.StreamMatchers
  alias Pulso.PromQL.Evaluator, as: Budget
  alias Pulso.Storage

  @ops ~w(count_over_time rate bytes_over_time bytes_rate sum_over_time avg_over_time min_over_time
          max_over_time stddev_over_time stdvar_over_time quantile_over_time)a

  @doc """
  `{:ok, %{labels => {:packed, samples}}}`, `{:error, reason}`, or
  `:fallback` when the query or its data needs the Elixir pipeline.
  """
  def collect(inner, spec, tenant, query_opts) do
    with true <- spec.op in @ops,
         true <- Map.get(spec, :post_filter, []) == [],
         false <- Map.get(spec, :preserve_errors, false),
         true <- Storage.native_log_metric?(),
         :ok <- QueryValidation.validate(inner),
         {storage_opts, stages} = Evaluator.push_down(inner, query_opts),
         {:ok, program} <- encode_stages(stages),
         {:ok, value} <- value(spec),
         {:ok, scan} <-
           native(
             NIF.log_metric_scan_new(
               program,
               value,
               drop(spec),
               grouping(spec.grouping),
               Budget.remaining(:log_samples)
             )
           ) do
      scan(tenant, storage_opts, scan)
    else
      {:error, _} = error -> error
      _ -> :fallback
    end
  end

  defp scan(tenant, storage_opts, scan) do
    case Storage.log_metric_scan(tenant, storage_opts, scan) do
      {:ok, {selected, kept}} ->
        Budget.charge(:log_samples, kept)
        Budget.charge(:work, selected)
        {:ok, decode(NIF.log_metric_scan_finish(scan))}

      {:error, :pipeline_error} ->
        throw({:logql_error, :pipeline_error})

      {:error, :query_resource_limit} ->
        _ = NIF.log_metric_scan_abort(scan)
        {:error, :query_sample_limit}

      {:error, :native_fallback} ->
        :fallback

      {:error, _} = error ->
        _ = NIF.log_metric_scan_abort(scan)
        error
    end
  end

  # Packed samples stay off the process heap; windows decode their slices.
  defp decode({:ok, series}), do: Map.new(series, fn {labels, packed} -> {labels, {:packed, packed}} end)

  defp native({:ok, scan}), do: {:ok, scan}
  defp native(_), do: :fallback

  defp value(%{op: op}) when op in [:bytes_over_time, :bytes_rate], do: {:ok, :bytes}
  defp value(%{op: :count_over_time}), do: {:ok, :count}
  defp value(%{unwrap: nil}), do: {:ok, :count}
  defp value(%{unwrap: %AST.Unwrap{label: label, conversion: :none}}), do: {:ok, {:unwrap, label}}
  defp value(_), do: :fallback

  defp drop(%{unwrap: %AST.Unwrap{label: label}}), do: label
  defp drop(_), do: nil

  defp grouping(nil), do: nil
  defp grouping(%AST.Grouping{mode: mode, labels: labels}), do: {mode, labels}

  defp encode_stages(stages) do
    Enum.reduce_while(stages, {:ok, []}, fn stage, {:ok, acc} ->
      case stage(stage) do
        nil -> {:halt, :fallback}
        encoded -> {:cont, {:ok, [encoded | acc]}}
      end
    end)
    |> case do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      other -> other
    end
  end

  defp stage(%AST.LineFilter{op: op, value: {:string, s}}) when op in [:contains, :not_contains], do: {op, s}
  defp stage(%AST.LineFilter{op: op, value: {:re, p}}) when op in [:match_re, :not_match_re], do: {op, p}
  defp stage(%AST.LogfmtParser{fields: [], flags: []}), do: :logfmt
  defp stage(%AST.RegexpParser{pattern: pattern}), do: {:regexp, pattern}

  defp stage(%AST.LabelFilter{expr: expr}) do
    case filter(expr) do
      nil -> nil
      encoded -> {:label_filter, encoded}
    end
  end

  defp stage(_), do: nil

  defp filter({op, left, right}) when op in [:and, :or] do
    with l when l != nil <- filter(left), r when r != nil <- filter(right), do: {op, l, r}
  end

  defp filter({:cmp, name, op, {:string, value}}) when op in [:eq, :neq], do: {op, name, value}
  defp filter({:cmp, name, op, {:re, pattern}}) when op in [:re, :nre], do: {op, name, StreamMatchers.anchor(pattern)}

  defp filter({:cmp, name, op, {:number, n}}) when op in [:eq, :neq, :lt, :lte, :gt, :gte] and is_number(n),
    do: {:num, op, name, n}

  defp filter(_), do: nil
end
