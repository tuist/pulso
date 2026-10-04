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

  @spec validate(struct()) :: :ok | {:error, term()}
  def validate(%AST.LogQuery{selector: sel, stages: stages}) do
    with :ok <- validate_selector(sel) do
      validate_stages(stages)
    end
  end

  def validate(%AST.RangeAgg{inner: inner}), do: validate(inner)

  def validate(%AST.VectorAgg{inner: inner}), do: validate(inner)

  def validate(%AST.BinaryOp{left: l, right: r}) do
    with :ok <- validate(l) do
      validate(r)
    end
  end

  def validate(%AST.NumberLit{}), do: :ok

  def validate(other), do: {:error, {:unsupported_expression, other}}

  # ---------------------------------------------------------------------------

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
        :ok -> {:cont, {:ok, native? and match?(%AST.LineFilter{}, stage)}}
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
    case Regex.compile(pattern) do
      {:ok, _} -> :ok
      {:error, {reason, _}} -> {:error, {:invalid_regex, source, pattern, to_string(reason)}}
    end
  end

  defp validate_native_pattern(source, pattern) do
    with :ok <- validate_pattern(source, pattern) do
      case NIF.validate_log_regex(pattern) do
        :ok -> :ok
        :error -> {:error, {:invalid_regex, source, pattern, "unsupported native regular expression"}}
      end
    end
  end
end
