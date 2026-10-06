defmodule Pulso.PromQL.Parser do
  @moduledoc """
  Parser for Pulso's initial Prometheus Query Language subset.

  Supports named and label-only selectors, range functions, nested vector
  aggregations with by/without grouping, parentheses, and positive offsets.
  Unsupported syntax fails rather than being interpreted as another language.
  """
  import NimbleParsec

  alias Pulso.Codec.NIF
  alias Pulso.PromQL.FloatParser

  comment = string("#") |> repeat(utf8_char(not: ?\n))
  ws = ignore(repeat(choice([ascii_char([?\s, ?\t, ?\r, ?\n]), comment])))
  defcombinatorp(:ws, ws)

  keyword = fn text ->
    text
    |> String.to_charlist()
    |> Enum.reduce(empty(), fn char, acc ->
      next = if char in ?a..?z, do: ascii_char([char, char - 32]), else: string(<<char>>)
      concat(acc, next)
    end)
    |> replace(text)
  end

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
    |> ignore(keyword.("offset"))
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
    choice([keyword.("by") |> replace(:by), keyword.("without") |> replace(:without)])
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
      Enum.map(~w(sum avg min max count group), fn op ->
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

  digits = ascii_string([?0..?9], 1) |> optional(ascii_string([?0..?9, ?_], min: 1))
  hex_digits = ascii_string([?0..?9, ?a..?f, ?A..?F, ?_], min: 1)
  exponent = optional(choice([string("+"), string("-")])) |> concat(digits)

  decimal_number =
    choice([
      digits |> optional(string(".") |> optional(digits)),
      string(".") |> concat(digits)
    ])
    |> optional(choice([string("e"), string("E")]) |> concat(exponent))

  hex_number = choice([string("0x"), string("0X")]) |> concat(hex_digits)

  number = choice([hex_number, decimal_number]) |> reduce({__MODULE__, :number, []})

  aggregation_prefix =
    choice(
      Enum.map(~w(sum avg min max count group topk bottomk), fn op ->
        keyword.(op) |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
      end)
    )

  generic_function =
    lookahead_not(aggregation_prefix)
    |> concat(identifier)
    |> parsec(:ws)
    |> ignore(string("("))
    |> parsec(:ws)
    |> optional(parsec(:argument) |> repeat(parsec(:ws) |> ignore(string(",")) |> parsec(:argument)))
    |> parsec(:ws)
    |> ignore(string(")"))
    |> reduce({__MODULE__, :call, []})

  parameter_aggregate =
    choice([keyword.("topk"), keyword.("bottomk")])
    |> parsec(:ws)
    |> optional(grouping |> parsec(:ws))
    |> ignore(string("("))
    |> parsec(:expression)
    |> ignore(string(","))
    |> parsec(:expression)
    |> ignore(string(")"))
    |> optional(parsec(:ws) |> concat(grouping))
    |> reduce({__MODULE__, :parameter_aggregate, []})

  special_number =
    choice([keyword.("nan") |> replace(:nan), keyword.("inf") |> replace(:infinity)])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> reduce({__MODULE__, :special_number, []})

  primary =
    parsec(:ws)
    |> choice([
      parameter_aggregate,
      aggregate,
      function,
      generic_function,
      special_number,
      ignore(string("(")) |> parsec(:expression) |> ignore(string(")")),
      number,
      instant
    ])
    |> parsec(:ws)

  defcombinatorp(:primary, primary)

  defcombinatorp(
    :argument,
    parsec(:ws) |> choice([quoted |> reduce({__MODULE__, :string_argument, []}), parsec(:expression)]) |> parsec(:ws)
  )

  # Power is right-associative; unary signs bind less tightly than power.
  defcombinatorp(
    :power,
    parsec(:primary)
    |> optional(string("^") |> parsec(:modifiers) |> parsec(:unary))
    |> reduce({__MODULE__, :power, []})
  )

  defcombinatorp(
    :unary,
    parsec(:ws)
    |> choice([
      choice([string("+"), string("-")]) |> parsec(:ws) |> parsec(:unary) |> reduce({__MODULE__, :unary, []}),
      parsec(:power)
    ])
  )

  label_list =
    ignore(string("("))
    |> parsec(:ws)
    |> optional(identifier |> repeat(parsec(:ws) |> ignore(string(",")) |> parsec(:ws) |> concat(identifier)))
    |> parsec(:ws)
    |> ignore(string(")"))
    |> wrap()

  matching =
    choice([keyword.("on"), keyword.("ignoring")])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> concat(label_list)
    |> wrap()

  cardinality =
    choice([keyword.("group_left"), keyword.("group_right")])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:]))
    |> parsec(:ws)
    |> optional(label_list)
    |> wrap()

  modifiers =
    parsec(:ws)
    |> optional(keyword.("bool") |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:])) |> parsec(:ws))
    |> optional(matching |> parsec(:ws))
    |> optional(cardinality |> parsec(:ws))
    |> wrap()

  defcombinatorp(:modifiers, modifiers)

  for {name, operand, operators} <- [
        {:product, :unary, ["*", "/", "%"]},
        {:addition, :product, ["+", "-"]},
        {:comparison, :addition, ["==", "!=", ">=", "<=", ">", "<"]},
        {:intersection, :comparison, ["and", "unless"]},
        {:expression, :intersection, ["or"]}
      ] do
    operator =
      case operators do
        [op] -> keyword.(op)
        _ -> choice(Enum.map(operators, keyword))
      end

    operator =
      if name in [:intersection, :expression],
        do: operator |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:])),
        else: operator

    defcombinatorp(
      name,
      parsec(operand)
      |> repeat(operator |> concat(modifiers) |> parsec(operand) |> wrap())
      |> reduce({__MODULE__, :binary, []})
    )
  end

  defparsecp(:do_parse, parsec(:expression) |> eos())
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
    :count,
    :group
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
      {:ok, [expr], "", _, _, _} -> if bounded?(expr), do: validate(expr), else: {:error, :query_expression_limit}
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

  @doc false
  def number(parts) do
    text = IO.iodata_to_binary(parts)

    case FloatParser.parse(text, :literal) do
      {:ok, value} -> {:scalar, value}
      :error -> raise ArgumentError
    end
  end

  @doc false
  def special_number([value]), do: {:scalar, value}
  @doc false
  def string_argument([text]), do: {:string, text}
  @doc false
  def call([name | args]), do: {:call, name, args}
  @doc false
  def parameter_aggregate([op, {:grouping, _, _} = group, parameter, inner]),
    do: {:parameter_aggregate, op, group, parameter, inner}

  def parameter_aggregate([op, parameter, inner, {:grouping, _, _} = group]),
    do: {:parameter_aggregate, op, group, parameter, inner}

  def parameter_aggregate([op, parameter, inner]), do: {:parameter_aggregate, op, nil, parameter, inner}
  def parameter_aggregate(_), do: raise(ArgumentError)
  @doc false
  def power([inner]), do: inner
  def power([left, "^", modifiers, right]), do: {:binary, "^", modifiers, left, right}
  @doc false
  def unary([op, inner]), do: {:unary, op, inner}
  @doc false
  def binary([left | rest]),
    do: Enum.reduce(rest, left, fn [op, modifiers, right], left -> {:binary, op, modifiers, left, right} end)

  @doc false
  def type({:scalar, _}), do: :scalar
  def type({:string, _}), do: :string
  def type({:unary, _, inner}), do: type(inner)
  def type({:call, name, _}) when name in ["time", "scalar"], do: :scalar

  def type({:binary, _, _, left, right}),
    do: if(type(left) == :scalar and type(right) == :scalar, do: :scalar, else: :vector)

  def type(_), do: :vector

  defp bounded?(expr), do: ast_size(expr, 0) <= 256
  defp ast_size(_expr, depth) when depth > 64, do: 257
  defp ast_size({:binary, _, _, left, right}, depth), do: 1 + ast_size(left, depth + 1) + ast_size(right, depth + 1)
  defp ast_size({:aggregate, _, _, inner}, depth), do: 1 + ast_size(inner, depth + 1)
  defp ast_size({:unary, _, inner}, depth), do: 1 + ast_size(inner, depth + 1)

  defp ast_size({:parameter_aggregate, _, _, parameter, inner}, depth),
    do: 1 + ast_size(parameter, depth + 1) + ast_size(inner, depth + 1)

  defp ast_size({:call, _, args}, depth), do: 1 + Enum.sum(Enum.map(args, &ast_size(&1, depth + 1)))
  defp ast_size(_, _), do: 1

  defp validate({:scalar, _} = expr), do: {:ok, expr}

  defp validate({:unary, _, inner} = expr) do
    with {:ok, _} <- validate(inner),
         true <- type(inner) in [:scalar, :vector],
         do: {:ok, expr},
         else: (_ -> {:error, :invalid_operand})
  end

  defp validate({:call, name, args} = expr) do
    signatures = %{
      "histogram_quantile" => [:scalar, :vector],
      "clamp_min" => [:vector, :scalar],
      "clamp_max" => [:vector, :scalar],
      "vector" => [:scalar],
      "scalar" => [:vector],
      "time" => [],
      "label_replace" => [:vector, :string, :string, :string, :string],
      "sort_desc" => [:vector],
      "sort" => [:vector],
      "round" => [:vector],
      "abs" => [:vector]
    }

    expected = if name == "round" and length(args) == 2, do: [:vector, :scalar], else: Map.get(signatures, name)

    valid =
      Enum.all?(args, fn
        {:string, _} -> true
        arg -> match?({:ok, _}, validate(arg))
      end)

    if valid and expected == Enum.map(args, &type/1),
      do: validate_call(expr),
      else: {:error, :invalid_function_arguments}
  end

  defp validate({:parameter_aggregate, _, group, parameter, inner} = expr) do
    with {:ok, _} <- validate(parameter),
         true <- type(parameter) == :scalar,
         {:ok, _} <- validate({:aggregate, :sum, group, inner}),
         do: {:ok, expr},
         else: (_ -> {:error, :invalid_aggregation})
  end

  defp validate({:binary, op, modifiers, left, right} = expr) do
    with {:ok, _} <- validate(left),
         {:ok, _} <- validate(right),
         true <- valid_binary?(op, modifiers, type(left), type(right)),
         do: {:ok, expr},
         else: (_ -> {:error, :invalid_binary_expression})
  end

  defp validate({:aggregate, _, group, inner} = expr) do
    names =
      case group do
        nil -> []
        {:grouping, _, names} -> names
      end

    with true <- names == Enum.uniq(names) and Enum.all?(names, &label_name?/1),
         {:ok, _} <- validate(inner),
         true <- type(inner) == :vector,
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

  defp validate_call({:call, "label_replace", [_, {:string, dst}, _, _, {:string, regex}]} = expr) do
    if label_name?(dst) and valid_regex?(regex), do: {:ok, expr}, else: {:error, :invalid_label_replace}
  end

  defp validate_call(expr), do: {:ok, expr}

  defp valid_binary?(op, modifiers, left, right) do
    boolean = "bool" in modifiers
    matching = Enum.find(modifiers, &match?([mode, _] when mode in ["on", "ignoring"], &1))
    grouping = Enum.find(modifiers, &match?([mode | _] when mode in ["group_left", "group_right"], &1))
    names = for [_mode, names] <- modifiers, name <- names, do: name

    valid_names =
      Enum.all?(names, &label_name?/1) and
        Enum.all?(modifiers, fn
          [_, names] -> names == Enum.uniq(names)
          _ -> true
        end)

    comparisons = op in ["==", "!=", ">", "<", ">=", "<="]
    sets = op in ["and", "or", "unless"]

    valid_names and valid_operator_types?(comparisons, sets, boolean, left, right) and
      valid_matching?(sets, matching, grouping, left, right)
  end

  defp valid_operator_types?(comparison?, set?, boolean?, left, right) do
    boolean_valid? = not boolean? or comparison?
    scalar_valid? = valid_scalar_comparison?(comparison?, boolean?, left, right)
    set_valid? = not set? or (left == :vector and right == :vector and not boolean?)
    boolean_valid? and scalar_valid? and set_valid?
  end

  defp valid_scalar_comparison?(true, false, :scalar, :scalar), do: false
  defp valid_scalar_comparison?(_, _, _, _), do: true

  defp valid_matching?(set?, matching, grouping, left, right) do
    vector_valid? = (is_nil(matching) and is_nil(grouping)) or (left == :vector and right == :vector)
    group_valid? = not set? or is_nil(grouping)
    vector_valid? and group_valid? and not overlapping_labels?(matching, grouping)
  end

  defp overlapping_labels?(["on", names], [_, include]), do: Enum.any?(include, &(&1 in names))
  defp overlapping_labels?(_, _), do: false

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
