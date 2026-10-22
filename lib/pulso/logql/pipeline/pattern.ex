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
      :nomatch -> Enum.reverse([{:literal, rest} | acc])
      {lit_end, 1} -> compile_placeholder(rest, lit_end, acc)
    end
  end

  defp compile_placeholder(rest, lit_end, acc) do
    {literal, after_lt} = String.split_at(rest, lit_end)

    case String.split(after_lt, ">", parts: 2) do
      [_only] -> do_compile("", [{:literal, rest} | acc])
      [<<"<", inner::binary>>, tail] -> continue_placeholder(literal, inner, tail, acc)
    end
  end

  defp continue_placeholder(literal, inner, tail, acc) do
    acc = if literal == "", do: acc, else: [{:literal, literal} | acc]
    token = if inner == "_", do: :skip, else: {:capture, inner}
    do_compile(tail, [token | acc])
  end

  @doc """
  Compile a `|>` / `!>` line filter pattern: literals and unnamed `<_>`
  wildcards. Like Loki, a placeholder needs an identifier (`<name>`), other
  `<...>` text is literal, and named or consecutive wildcards are invalid.
  """
  @spec compile_filter(String.t()) :: {:ok, [{:literal, binary()} | :skip]} | {:error, term()}
  def compile_filter(pattern) when is_binary(pattern) do
    parts =
      pattern
      |> compile()
      |> Enum.map(fn
        {:capture, name} ->
          if Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, name),
            do: {:capture, name},
            else: {:literal, "<" <> name <> ">"}

        part ->
          part
      end)
      |> merge_literals([])

    cond do
      Enum.any?(parts, &match?({:capture, _}, &1)) -> {:error, :named_capture}
      parts |> Enum.chunk_every(2, 1, :discard) |> Enum.member?([:skip, :skip]) -> {:error, :consecutive_captures}
      true -> {:ok, parts}
    end
  end

  defp merge_literals([], acc), do: Enum.reverse(acc)

  defp merge_literals([{:literal, b} | rest], [{:literal, a} | acc]),
    do: merge_literals(rest, [{:literal, a <> b} | acc])

  defp merge_literals([part | rest], acc), do: merge_literals(rest, [part | acc])

  @doc """
  Whether `line` matches a compiled filter pattern, with Loki's semantics:
  literals occur in order, a wildcard matches at least one byte, and the line
  must end where the pattern does (a trailing wildcard needs remaining text).
  An empty line matches only an empty pattern.
  """
  @spec filter_match?([{:literal, binary()} | :skip], binary()) :: boolean()
  def filter_match?([], line), do: line == ""
  def filter_match?(_parts, ""), do: false

  def filter_match?(parts, line) do
    result =
      parts
      |> Enum.with_index()
      |> Enum.reduce_while(0, fn
        {:skip, _i}, off ->
          {:cont, off}

        {{:literal, lit}, i}, off ->
          case :binary.match(line, lit, scope: {off, byte_size(line) - off}) do
            :nomatch -> {:halt, :nomatch}
            {pos, _len} when i != 0 and pos == off -> {:halt, :nomatch}
            {pos, len} -> {:cont, pos + len}
          end
      end)

    case result do
      :nomatch -> false
      off -> List.last(parts) == :skip == (off != byte_size(line))
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
