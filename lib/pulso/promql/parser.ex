defmodule Pulso.PromQL.Parser do
  @moduledoc """
  Parser for Pulso's initial Prometheus Query Language subset.

  Supports named and label-only selectors, range functions, nested vector
  aggregations with by/without grouping, parentheses, and positive offsets.
  Unsupported syntax fails rather than being interpreted as another language.
  """
  import NimbleParsec

  alias Pulso.Codec.NIF

  comment = string("#") |> repeat(utf8_char(not: ?\n))
  ws = ignore(repeat(choice([ascii_char([?\s, ?\t, ?\r, ?\n]), comment])))
  defcombinatorp(:ws, ws)

  identifier =
    ascii_char([?a..?z, ?A..?Z, ?_, ?:])
    |> repeat(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> reduce({List, :to_string, []})

  defcombinatorp(:identifier, identifier)

  escape =
    ignore(string("\\"))
    |> choice([
      string("n") |> replace(?\n),
      string("r") |> replace(?\r),
      string("t") |> replace(?\t),
      string("a") |> replace(7),
      string("b") |> replace(8),
      string("f") |> replace(12),
      string("v") |> replace(11),
      string("\\") |> replace(?\\),
      string("\"") |> replace(?"),
      string("'") |> replace(?'),
      ignore(string("x")) |> ascii_string([?0..?9, ?a..?f, ?A..?F], 2) |> reduce({__MODULE__, :hex_byte, []}),
      ignore(string("u")) |> ascii_string([?0..?9, ?a..?f, ?A..?F], 4) |> reduce({__MODULE__, :hex, []}),
      ignore(string("U")) |> ascii_string([?0..?9, ?a..?f, ?A..?F], 8) |> reduce({__MODULE__, :hex, []}),
      ascii_string([?0..?7], 3) |> reduce({__MODULE__, :octal, []})
    ])

  quoted =
    choice([
      ignore(string("\"")) |> repeat(choice([escape, utf8_char(not: ?", not: ?\\, not: ?\n)])) |> ignore(string("\"")),
      ignore(string("'")) |> repeat(choice([escape, utf8_char(not: ?', not: ?\\, not: ?\n)])) |> ignore(string("'")),
      ignore(string("`")) |> repeat(utf8_char(not: ?`)) |> ignore(string("`"))
    ])
    |> reduce({__MODULE__, :decode_string, []})

  matcher =
    parsec(:ws)
    |> concat(identifier)
    |> parsec(:ws)
    |> choice([
      string("=~") |> replace(:re),
      string("!~") |> replace(:nre),
      string("!=") |> replace(:neq),
      string("=") |> replace(:eq)
    ])
    |> parsec(:ws)
    |> concat(quoted)
    |> reduce({__MODULE__, :matcher, []})

  labels =
    ignore(string("{"))
    |> parsec(:ws)
    |> optional(
      matcher
      |> repeat(parsec(:ws) |> ignore(string(",")) |> concat(matcher))
      |> optional(parsec(:ws) |> ignore(string(",")))
    )
    |> parsec(:ws)
    |> ignore(string("}"))
    |> wrap()

  selector =
    choice([
      identifier |> optional(parsec(:ws) |> concat(labels)),
      labels
    ])
    |> reduce({__MODULE__, :selector, []})

  defcombinatorp(:selector, selector)

  component =
    ascii_string([?0..?9], min: 1)
    |> choice(Enum.map(["ms", "s", "m", "h", "d", "w", "y"], &string/1))
    |> reduce({__MODULE__, :duration_component, []})

  duration = times(component, min: 1) |> reduce({__MODULE__, :duration, []})
  defcombinatorp(:duration, duration)

  offset =
    parsec(:ws)
    |> ignore(string("offset"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> concat(duration)

  instant = selector |> optional(offset) |> reduce({__MODULE__, :instant, []})

  range =
    selector
    |> parsec(:ws)
    |> ignore(string("["))
    |> concat(duration)
    |> ignore(string("]"))
    |> optional(offset)
    |> reduce({__MODULE__, :range, []})

  functions = ~w(rate increase irate delta sum_over_time avg_over_time min_over_time max_over_time count_over_time)

  function =
    choice(Enum.map(functions, &(string(&1) |> replace(String.to_existing_atom(&1)))))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> ignore(string("("))
    |> parsec(:ws)
    |> concat(range)
    |> parsec(:ws)
    |> ignore(string(")"))
    |> reduce({__MODULE__, :function, []})

  grouping =
    choice([string("by") |> replace(:by), string("without") |> replace(:without)])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> ignore(string("("))
    |> parsec(:ws)
    |> optional(
      identifier
      |> repeat(parsec(:ws) |> ignore(string(",")) |> parsec(:ws) |> concat(identifier))
      |> optional(parsec(:ws) |> ignore(string(",")))
    )
    |> parsec(:ws)
    |> ignore(string(")"))
    |> reduce({__MODULE__, :grouping, []})

  aggregate =
    choice(
      Enum.map(~w(sum avg min max count), fn op ->
        op
        |> String.to_charlist()
        |> Enum.reduce(empty(), fn char, acc -> concat(acc, ascii_char([char, char - 32])) end)
        |> replace(String.to_existing_atom(op))
      end)
    )
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> optional(grouping |> parsec(:ws))
    |> ignore(string("("))
    |> parsec(:expression)
    |> parsec(:ws)
    |> ignore(string(")"))
    |> optional(parsec(:ws) |> concat(grouping))
    |> reduce({__MODULE__, :aggregate, []})

  expression =
    parsec(:ws)
    |> choice([
      aggregate,
      function,
      ignore(string("(")) |> parsec(:expression) |> parsec(:ws) |> ignore(string(")")),
      instant
    ])
    |> parsec(:ws)

  defcombinatorp(:expression, expression)
  defparsecp(:do_parse, expression |> eos())
  defparsecp(:do_parse_duration, duration |> eos())

  # These atoms are an explicit, closed operation set, never derived from input.
  @operations [
    :rate,
    :increase,
    :irate,
    :delta,
    :sum_over_time,
    :avg_over_time,
    :min_over_time,
    :max_over_time,
    :count_over_time,
    :sum,
    :avg,
    :min,
    :max,
    :count
  ]
  @factors %{
    "ms" => 1_000_000,
    "s" => 1_000_000_000,
    "m" => 60_000_000_000,
    "h" => 3_600_000_000_000,
    "d" => 86_400_000_000_000,
    "w" => 604_800_000_000_000,
    "y" => 31_536_000_000_000_000
  }

  def parse(input) when is_binary(input) and byte_size(input) <= 16_384 do
    case do_parse(input) do
      {:ok, [expr], "", _, _, _} -> validate(expr)
      _ -> {:error, :invalid_or_unsupported_query}
    end
  rescue
    ArgumentError -> {:error, :invalid_or_unsupported_query}
  end

  def parse(_), do: {:error, :invalid_or_unsupported_query}

  def parse_duration(input) when is_binary(input) and byte_size(input) <= 128 do
    case do_parse_duration(input) do
      {:ok, [duration], "", _, _, _} when duration > 0 -> {:ok, duration}
      _ -> {:error, :invalid_duration}
    end
  rescue
    ArgumentError -> {:error, :invalid_duration}
  end

  def parse_duration(_), do: {:error, :invalid_duration}

  @doc false
  def hex([s]), do: String.to_integer(s, 16)
  @doc false
  def octal([s]), do: escaped_byte(String.to_integer(s, 8))
  @doc false
  def hex_byte([s]), do: escaped_byte(String.to_integer(s, 16))
  defp escaped_byte(byte) when byte <= 255, do: <<byte>>
  defp escaped_byte(_), do: raise(ArgumentError)
  @doc false
  def decode_string(parts) do
    result =
      parts
      |> Enum.map(fn
        char when is_integer(char) -> <<char::utf8>>
        bytes -> bytes
      end)
      |> IO.iodata_to_binary()

    if String.valid?(result), do: result, else: raise(ArgumentError)
  end

  @doc false
  def matcher([name, op, value]), do: {name, op, value}
  @doc false
  def selector([name, matchers]), do: [{"__name__", :eq, name} | matchers]
  def selector([name]) when is_binary(name), do: [{"__name__", :eq, name}]
  def selector([matchers]), do: matchers
  @doc false
  def duration_component([n, unit]), do: {String.to_integer(n), unit}
  @doc false
  def duration(parts) do
    units = Enum.map(parts, fn {_, unit} -> Map.fetch!(@factors, unit) end)
    if units != Enum.sort(Enum.uniq(units), :desc), do: raise(ArgumentError)
    total = Enum.reduce(parts, 0, fn {n, unit}, sum -> sum + n * Map.fetch!(@factors, unit) end)
    if total > 31_536_000_000_000_000, do: raise(ArgumentError)
    total
  end

  @doc false
  def instant([matchers]), do: {:selector, matchers, 0}
  def instant([matchers, offset]), do: {:selector, matchers, offset}
  @doc false
  def range([matchers, window]), do: {:range, matchers, window, 0}
  def range([matchers, window, offset]), do: {:range, matchers, window, offset}
  @doc false
  def function([op, range]) when op in @operations, do: {:function, op, range}
  @doc false
  def grouping([mode | labels]), do: {:grouping, mode, labels}
  @doc false
  def aggregate([op, {:grouping, _, _} = group, inner]), do: {:aggregate, op, group, inner}
  def aggregate([op, inner, {:grouping, _, _} = group]), do: {:aggregate, op, group, inner}
  def aggregate([op, inner]), do: {:aggregate, op, nil, inner}
  def aggregate(_), do: raise(ArgumentError)

  defp validate({:aggregate, _, group, inner} = expr) do
    names =
      case group do
        nil -> []
        {:grouping, _, names} -> names
      end

    with true <- names == Enum.uniq(names) and Enum.all?(names, &label_name?/1),
         {:ok, _} <- validate(inner),
         do: {:ok, expr},
         else: (_ -> {:error, :invalid_grouping})
  end

  defp validate({:function, _, {:range, matchers, window, _}} = expr) when window > 0 do
    with :ok <- validate_matchers(matchers), do: {:ok, expr}
  end

  defp validate({:selector, matchers, _} = expr) do
    with :ok <- validate_matchers(matchers), do: {:ok, expr}
  end

  defp validate(_), do: {:error, :invalid_range}

  defp validate_matchers(matchers) do
    valid =
      length(matchers) <= 64 and
        Enum.all?(matchers, fn {name, op, value} ->
          label_name?(name) and
            (op not in [:re, :nre] or valid_regex?(value))
        end)

    nonempty = valid and Enum.any?(matchers, &excludes_empty?/1)

    if valid and nonempty, do: :ok, else: {:error, :invalid_selector}
  end

  defp valid_regex?(value) do
    byte_size(value) <= 1024 and NIF.validate_metric_regex(value) == :ok and
      NIF.validate_metric_regex("(?s:\\A(?:#{value})\\z)") == :ok
  end

  defp excludes_empty?({_, :eq, value}), do: value != ""
  defp excludes_empty?({_, :neq, value}), do: value == ""

  defp excludes_empty?({_, op, value}) do
    if NIF.validate_metric_regex(value) == :ok do
      matched = NIF.match_metric_regex("(?s:\\A(?:#{value})\\z)", "")
      if op == :re, do: not matched, else: matched
    else
      false
    end
  end

  defp label_name?(name), do: Regex.match?(~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/, name)
end
