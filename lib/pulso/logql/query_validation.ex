defmodule Pulso.LogQL.QueryValidation do
  @moduledoc """
  Static validation for a LogQL AST before evaluation.

  Two categories of failure surface here as `{:error, reason}` instead
  of a crash or a silent wrong answer downstream:

    * **Invalid regex patterns** in selectors, line filters, label
      filters, `regexp`/`pattern` parser stages, and `drop`/`keep`
      entries. Patterns pushed into the native decoder are checked with
      its own regular-expression engine before reading storage.
    * **`ip(...)` line filters**, which no adapter evaluates yet.
      Returning them as an explicit `:unsupported` beats the pre-fix
      behaviour where the Elixir stub silently kept every line (for
      `|=`) or dropped every line (for `!=`, `|~`, `!~`).
  """

  alias Pulso.Codec.NIF
  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Pipeline.Pattern

  @spec validate(struct()) :: :ok | {:error, term()}
  def validate(%AST.LogQuery{selector: sel, stages: stages}) do
    with :ok <- validate_selector(sel) do
      validate_stages(stages)
    end
  end

  def validate(%AST.RangeAgg{inner: inner, op: op, grouping: grouping, at_ns: at, param: param, range_ns: range}) do
    with :ok <- validate_range_duration(range),
         :ok <- validate_range_parameter(op, param),
         :ok <- validate_at(at),
         :ok <- validate_range_grouping(grouping, op, param),
         :ok <- validate_range_unwrap(inner, op),
         do: validate(inner)
  end

  def validate(%AST.VectorAgg{op: :approx_topk, grouping: grouping}) when not is_nil(grouping),
    do: {:error, :unsupported_vector_grouping}

  def validate(%AST.VectorAgg{inner: inner, op: op, param: param}) do
    with :ok <- validate_vector_parameter(op, param), do: validate(inner)
  end

  def validate(%AST.BinaryOp{left: l, right: r} = binary) do
    with :ok <- validate_binary(binary), :ok <- validate(l), do: validate(r)
  end

  def validate(%AST.NumberLit{}), do: :ok
  def validate(%AST.VectorLit{}), do: :ok

  def validate(other), do: {:error, {:unsupported_expression, other}}

  # ---------------------------------------------------------------------------

  @comparisons [:eq, :neq, :lt, :lte, :gt, :gte]
  defp validate_binary(%AST.BinaryOp{left: left, right: right} = binary) do
    types = {scalar?(left), scalar?(right)}

    with :ok <- validate_set(binary, types),
         :ok <- validate_boolean(binary, types),
         do: validate_matching(binary.matching, types)
  end

  defp validate_set(%AST.BinaryOp{op: op, bool: bool, matching: matching}, types) when op in [:and, :or, :unless] do
    if bool or types != {false, false} or (matching != nil and matching.group != nil),
      do: {:error, :invalid_binary_operation},
      else: :ok
  end

  defp validate_set(_, _), do: :ok

  defp validate_boolean(%AST.BinaryOp{bool: true, op: op}, _) when op not in @comparisons,
    do: {:error, :invalid_binary_operation}

  defp validate_boolean(%AST.BinaryOp{bool: false, op: op}, {true, true}) when op in @comparisons,
    do: {:error, :invalid_binary_operation}

  defp validate_boolean(_, _), do: :ok
  defp validate_matching(nil, _), do: :ok
  defp validate_matching(_, {false, false}), do: :ok
  defp validate_matching(_, _), do: {:error, :invalid_binary_operation}

  defp scalar?(%AST.NumberLit{}), do: true
  defp scalar?(%AST.BinaryOp{left: left, right: right}), do: scalar?(left) and scalar?(right)
  defp scalar?(_), do: false

  defp validate_vector_parameter(:approx_topk, param)
       when is_integer(param) and param > 0 and param <= 9_223_372_036_854_775_807, do: :ok

  defp validate_vector_parameter(:approx_topk, _), do: {:error, :invalid_aggregation_parameter}
  defp validate_vector_parameter(op, param) when op in [:topk, :bottomk] and is_integer(param), do: :ok
  defp validate_vector_parameter(op, _) when op in [:topk, :bottomk], do: {:error, :invalid_aggregation_parameter}
  defp validate_vector_parameter(_, nil), do: :ok
  defp validate_vector_parameter(_, _), do: {:error, :invalid_aggregation_parameter}

  defp validate_range_duration(range) when is_integer(range) and range > 0, do: :ok
  defp validate_range_duration(_), do: {:error, :invalid_range_duration}
  defp validate_range_parameter(:quantile_over_time, param) when is_number(param), do: :ok
  defp validate_range_parameter(:quantile_over_time, _), do: {:error, :invalid_range_parameter}
  defp validate_range_parameter(:approx_count_distinct, param) when is_binary(param) and param != "", do: :ok
  defp validate_range_parameter(:approx_count_distinct, _), do: {:error, :invalid_range_parameter}
  defp validate_range_parameter(_, nil), do: :ok
  defp validate_range_parameter(_, _), do: {:error, :invalid_range_parameter}

  defp validate_at(nil), do: :ok
  defp validate_at(_), do: {:error, :unsupported_at_modifier}
  defp validate_range_grouping(nil, _, _), do: :ok

  defp validate_range_grouping(%AST.Grouping{mode: :by, labels: names}, :approx_count_distinct, param) do
    if param in names, do: {:error, :unsupported_range_grouping}, else: :ok
  end

  defp validate_range_grouping(%AST.Grouping{}, op, _)
       when op in [
              :avg_over_time,
              :stddev_over_time,
              :stdvar_over_time,
              :quantile_over_time,
              :max_over_time,
              :min_over_time,
              :first_over_time,
              :last_over_time
            ], do: :ok

  defp validate_range_grouping(_, _, _), do: {:error, :unsupported_range_grouping}
  defp validate_range_unwrap(_, :rate_counter), do: {:error, :unsupported_rate_counter}

  defp validate_range_unwrap(%AST.LogQuery{stages: stages}, op) do
    case Enum.split_while(stages, &(not match?(%AST.Unwrap{}, &1))) do
      {_, []} -> log_op(op)
      {_, [_]} -> unwrap_op(op)
      _ -> {:error, :unsupported_post_unwrap_stage}
    end
  end

  defp unwrap_op(op)
       when op in [
              :avg_over_time,
              :sum_over_time,
              :max_over_time,
              :min_over_time,
              :stddev_over_time,
              :stdvar_over_time,
              :quantile_over_time,
              :rate,
              :absent_over_time,
              :first_over_time,
              :last_over_time
            ], do: :ok

  defp unwrap_op(_), do: {:error, :invalid_range_aggregation}

  defp log_op(op)
       when op in [:bytes_over_time, :bytes_rate, :count_over_time, :rate, :absent_over_time, :approx_count_distinct],
       do: :ok

  defp log_op(_), do: {:error, :invalid_range_aggregation}

  defp validate_selector(%AST.Selector{matchers: matchers}) do
    Enum.reduce_while(matchers, :ok, fn m, :ok ->
      case validate_matcher(m) do
        :ok -> {:cont, :ok}
        err -> {:halt, err}
      end
    end)
  end

  defp validate_matcher(%AST.Matcher{op: op, value: v, name: name}) when op in [:re, :nre] do
    validate_native_pattern(name, v)
  end

  defp validate_matcher(_), do: :ok

  defp validate_stages(stages) do
    Enum.reduce_while(stages, {:ok, true}, fn stage, {:ok, native?} ->
      case validate_stage(stage, native?) do
        :ok -> {:cont, {:ok, native? and AST.LineFilter.native?(stage)}}
        err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, _} -> :ok
      error -> error
    end
  end

  defp validate_stage(%AST.LineFilter{value: {:ip, cidr}}, _native?) do
    {:error, {:unsupported, :ip_line_filter, cidr}}
  end

  defp validate_stage(%AST.LineFilter{value: {:pattern, pattern}}, _native?) do
    case Pattern.compile_filter(pattern) do
      {:ok, _parts} -> :ok
      {:error, reason} -> {:error, {:invalid_pattern, :line_filter, reason}}
    end
  end

  defp validate_stage(%AST.LineFilter{op: op, value: {:re, pat}}, true) when op in [:match_re, :not_match_re] do
    validate_native_pattern(:line_filter, pat)
  end

  defp validate_stage(%AST.LineFilter{op: op, value: {:re, pat}}, false) when op in [:match_re, :not_match_re] do
    validate_pattern(:line_filter, pat)
  end

  defp validate_stage(%AST.LabelFilter{expr: expr}, _native?), do: validate_label_expr(expr)

  defp validate_stage(%AST.RegexpParser{pattern: pat}, _native?), do: validate_pattern(:regexp_stage, pat)

  defp validate_stage(%AST.Drop{entries: entries}, _native?), do: validate_drop_entries(entries)
  defp validate_stage(%AST.Keep{entries: entries}, _native?), do: validate_drop_entries(entries)

  defp validate_stage(_, _native?), do: :ok

  defp validate_label_expr({:and, l, r}) do
    with :ok <- validate_label_expr(l), do: validate_label_expr(r)
  end

  defp validate_label_expr({:or, l, r}) do
    with :ok <- validate_label_expr(l), do: validate_label_expr(r)
  end

  defp validate_label_expr({:cmp, name, op, {:re, pat}}) when op in [:re, :nre] do
    validate_pattern({:label_filter, name}, pat)
  end

  defp validate_label_expr(_), do: :ok

  defp validate_drop_entries(entries) do
    Enum.reduce_while(entries, :ok, fn
      {_name, {op, pat}}, :ok when op in [:re, :nre] ->
        case validate_pattern(:drop_keep, pat) do
          :ok -> {:cont, :ok}
          err -> {:halt, err}
        end

      _, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_pattern(source, pattern) do
    case Pulso.QueryRegex.compile(pattern) do
      {:ok, _} ->
        case NIF.validate_log_regex(pattern) do
          :ok -> :ok
          :error -> {:error, {:invalid_regex, source, pattern, "unsupported native regular expression"}}
        end

      {:error, {reason, _}} ->
        {:error, {:invalid_regex, source, pattern, to_string(reason)}}
    end
  end

  defp validate_native_pattern(source, pattern), do: validate_pattern(source, pattern)
end
