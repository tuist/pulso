defmodule Pulso.LogQL.Pipeline.Template do
  @moduledoc """
  Go-template-style interpolation for `| line_format` and
  `| label_format`.

  Only the subset LogQL uses:

    * `{{.label}}` — look up `label` in the label bag; missing → `""`.
    * `{{ .label }}` — with optional surrounding whitespace.
    * Literal text between placeholders is passed through verbatim.

  Templates are compiled to a list of tokens once and rendered by
  concatenating IO data — no per-render regex.
  """

  @type token :: {:literal, binary()} | {:label, String.t()}
  @type template :: [token()]

  @spec compile(String.t()) :: template()
  def compile(str) when is_binary(str) do
    do_compile(str, [])
  end

  defp do_compile("", acc), do: Enum.reverse(acc)

  defp do_compile(rest, acc) do
    case :binary.match(rest, "{{") do
      :nomatch ->
        Enum.reverse([{:literal, rest} | acc])

      {pos, 2} ->
        compile_placeholder(rest, pos, acc)
    end
  end

  defp compile_placeholder(rest, pos, acc) do
    {literal, after_open} = String.split_at(rest, pos)
    # Drop the leading `{{`.
    after_open = binary_part(after_open, 2, byte_size(after_open) - 2)

    case :binary.match(after_open, "}}") do
      :nomatch -> Enum.reverse([{:literal, rest} | acc])
      {end_pos, 2} -> continue_placeholder(literal, after_open, end_pos, acc)
    end
  end

  defp continue_placeholder(literal, after_open, end_pos, acc) do
    inner = binary_part(after_open, 0, end_pos) |> String.trim()
    tail = binary_part(after_open, end_pos + 2, byte_size(after_open) - end_pos - 2)
    acc = if literal == "", do: acc, else: [{:literal, literal} | acc]

    case parse_expr(inner) do
      {:label, name} -> do_compile(tail, [{:label, name} | acc])
      :ignored -> do_compile(tail, acc)
    end
  end

  defp parse_expr(<<".", name::binary>>), do: {:label, String.trim(name)}
  defp parse_expr(_), do: :ignored

  @spec render(template(), map()) :: String.t()
  def render(template, labels) when is_list(template) and is_map(labels) do
    template
    |> Enum.map(fn
      {:literal, s} -> s
      {:label, name} -> Map.get(labels, name, "")
    end)
    |> IO.iodata_to_binary()
  end
end
