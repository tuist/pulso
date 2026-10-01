defmodule Pulso.LogQL.Pipeline.Logfmt do
  @moduledoc """
  Minimal logfmt parser. Splits a single line into `[{key, value}]`
  pairs. Understands quoted values with `\\"` and `\\\\` escapes; every
  other character passes through untouched. Bare keys without a value
  produce `{key, ""}` when `--keep-empty` is set, otherwise are skipped.

  This is a thousand times smaller than the full logfmt spec because
  the surface we need is only enough for LogQL's `| logfmt` stage: the
  full parser lives in Rust in the ingest path.
  """

  @spec parse(String.t(), boolean()) :: [{String.t(), String.t()}]
  def parse(line, keep_empty? \\ false) when is_binary(line) do
    parse_pairs(line, 0, [], keep_empty?)
  end

  defp parse_pairs(line, pos, acc, keep_empty?) do
    pos = skip_ws(line, pos)
    size = byte_size(line)

    if pos >= size do
      Enum.reverse(acc)
    else
      {key, pos} = read_key(line, pos)

      cond do
        key == "" ->
          Enum.reverse(acc)

        pos < size and :binary.at(line, pos) == ?= ->
          {value, pos} = read_value(line, pos + 1)
          parse_pairs(line, pos, [{key, value} | acc], keep_empty?)

        keep_empty? ->
          parse_pairs(line, pos, [{key, ""} | acc], keep_empty?)

        true ->
          parse_pairs(line, pos, acc, keep_empty?)
      end
    end
  end

  defp skip_ws(line, pos) do
    size = byte_size(line)

    if pos < size and :binary.at(line, pos) in [?\s, ?\t] do
      skip_ws(line, pos + 1)
    else
      pos
    end
  end

  defp read_key(line, pos) do
    size = byte_size(line)
    read_key(line, pos, pos, size)
  end

  defp read_key(line, start, pos, size) when pos >= size do
    {binary_part(line, start, pos - start), pos}
  end

  defp read_key(line, start, pos, size) do
    case :binary.at(line, pos) do
      c when c in [?=, ?\s, ?\t] ->
        {binary_part(line, start, pos - start), pos}

      _ ->
        read_key(line, start, pos + 1, size)
    end
  end

  defp read_value(line, pos) do
    size = byte_size(line)

    cond do
      pos >= size ->
        {"", pos}

      :binary.at(line, pos) == ?" ->
        read_quoted(line, pos + 1, [])

      true ->
        read_bare(line, pos, pos, size)
    end
  end

  defp read_bare(line, start, pos, size) when pos >= size do
    {binary_part(line, start, pos - start), pos}
  end

  defp read_bare(line, start, pos, size) do
    case :binary.at(line, pos) do
      c when c in [?\s, ?\t] -> {binary_part(line, start, pos - start), pos}
      _ -> read_bare(line, start, pos + 1, size)
    end
  end

  defp read_quoted(line, pos, acc) do
    size = byte_size(line)

    cond do
      pos >= size ->
        {IO.iodata_to_binary(Enum.reverse(acc)), pos}

      :binary.at(line, pos) == ?" ->
        {IO.iodata_to_binary(Enum.reverse(acc)), pos + 1}

      :binary.at(line, pos) == ?\\ and pos + 1 < size ->
        read_quoted(line, pos + 2, [<<:binary.at(line, pos + 1)>> | acc])

      true ->
        read_quoted(line, pos + 1, [<<:binary.at(line, pos)>> | acc])
    end
  end
end
