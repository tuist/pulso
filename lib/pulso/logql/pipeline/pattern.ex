defmodule Pulso.LogQL.Pipeline.Pattern do
  @moduledoc """
  LogQL `| pattern` template compilation and matching.

  A template is literal text interleaved with `<name>` capture
  placeholders and `<_>` skip placeholders. Compilation returns a small
  representation the matcher walks in one linear scan of the log line.
  """

  @type token :: {:literal, binary()} | {:capture, String.t()} | :skip
  @type template :: [token()]

  @spec compile(String.t()) :: template()
  def compile(pattern) when is_binary(pattern) do
    do_compile(pattern, [])
  end

  defp do_compile("", acc), do: Enum.reverse(acc)

  defp do_compile(rest, acc) do
    case :binary.match(rest, "<") do
      :nomatch ->
        Enum.reverse([{:literal, rest} | acc])

      {lit_end, 1} ->
        {literal, after_lt} = String.split_at(rest, lit_end)

        case String.split(after_lt, ">", parts: 2) do
          [_only] ->
            do_compile("", [{:literal, rest} | acc])

          [<<"<", inner::binary>>, tail] ->
            acc = if literal == "", do: acc, else: [{:literal, literal} | acc]
            token = if inner == "_", do: :skip, else: {:capture, inner}
            do_compile(tail, [token | acc])
        end
    end
  end

  @spec match(template(), String.t()) :: {:ok, %{optional(String.t()) => String.t()}} | :nomatch
  def match(template, line) when is_binary(line) do
    match_template(template, line, %{})
  end

  defp match_template([], _rest, captures), do: {:ok, captures}

  defp match_template([{:literal, lit} | rest], line, captures) do
    case starts_with?(line, lit) do
      {:ok, remaining} -> match_template(rest, remaining, captures)
      :error -> :nomatch
    end
  end

  defp match_template([{:capture, name}, {:literal, next} | rest], line, captures) do
    case :binary.match(line, next) do
      :nomatch ->
        :nomatch

      {pos, len} ->
        value = binary_part(line, 0, pos)
        remaining = binary_part(line, pos + len, byte_size(line) - pos - len)
        match_template(rest, remaining, Map.put(captures, name, value))
    end
  end

  defp match_template([{:capture, name}], line, captures) do
    {:ok, Map.put(captures, name, line)}
  end

  defp match_template([:skip, {:literal, next} | rest], line, captures) do
    case :binary.match(line, next) do
      :nomatch ->
        :nomatch

      {pos, len} ->
        remaining = binary_part(line, pos + len, byte_size(line) - pos - len)
        match_template(rest, remaining, captures)
    end
  end

  defp match_template([:skip], _line, captures), do: {:ok, captures}

  defp starts_with?(line, prefix) do
    if byte_size(prefix) <= byte_size(line) and binary_part(line, 0, byte_size(prefix)) == prefix do
      {:ok, binary_part(line, byte_size(prefix), byte_size(line) - byte_size(prefix))}
    else
      :error
    end
  end
end
