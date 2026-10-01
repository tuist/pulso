defmodule Pulso.LogQL.Parser do
  @moduledoc """
  LogQL parser.

  `parse/1` turns a LogQL string into the AST rooted under
  `Pulso.LogQL.AST.*`. Log queries return a `LogQuery`; metric queries
  return a `RangeAgg`, `VectorAgg`, `BinaryOp`, or `NumberLit`. The
  discriminator is the returned struct's module.

  The grammar covers every construct documented at
  https://grafana.com/docs/loki/latest/query/, including:

    * Stream selectors with `=`, `!=`, `=~`, `!~`.
    * Line filters: `|=`, `!=`, `|~`, `!~`, with plain strings, backtick
      strings, or `ip("cidr")`.
    * Parser stages: `json`, `logfmt`, `regexp`, `pattern`, `unpack`;
      `json` and `logfmt` accept optional label extraction lists.
    * Label filters combining string, regex, numeric, duration, and byte
      comparisons with `and`/`or` — `and`/`or` bind tighter than the
      pipeline separator.
    * `line_format`, `label_format`, `drop`, `keep`, `decolorize`.
    * `unwrap label`, `unwrap duration(label)`, `unwrap bytes(label)`.
    * Range aggregations (`rate`, `count_over_time`, …) with optional
      `[range]`, `offset`, and `@` modifiers, and the `quantile_over_time`
      leading-scalar form.
    * Vector aggregations (`sum`, `topk`, …) with `by`/`without` on
      either side of the argument list.
    * Binary operators with PromQL precedence, right-associative `^`,
      unary `-`, `bool` modifier on comparisons, and `on`/`ignoring`
      matching with `group_left`/`group_right`.
    * Comments: `# ... EOL` and `// ... EOL`.

  Not accepted (deliberately, until the corresponding evaluator lands):

    * `sharding` and other query-planner hints.
  """

  import NimbleParsec

  alias Pulso.LogQL.AST

  # ---------------------------------------------------------------------------
  # Public entry point
  # ---------------------------------------------------------------------------

  @spec parse(String.t()) ::
          {:ok, AST.LogQuery.t() | AST.RangeAgg.t() | AST.VectorAgg.t() | AST.BinaryOp.t() | AST.NumberLit.t()}
          | {:error, {non_neg_integer(), non_neg_integer(), term()}}
  def parse(input) when is_binary(input) do
    case do_parse(input) do
      {:ok, [ast], "", _ctx, _line_col, _offset} ->
        {:ok, ast}

      {:ok, _acc, rest, _ctx, {line, col}, _offset} ->
        {:error, {line, col, {:trailing_input, String.slice(rest, 0, 40)}}}

      {:error, reason, _rest, _ctx, {line, col}, _offset} ->
        {:error, {line, col, reason}}
    end
  end

  # ---------------------------------------------------------------------------
  # Whitespace and comments
  # ---------------------------------------------------------------------------

  # `#`- and `//`-prefixed comments run to end-of-line. Both forms appear in
  # LogQL in the wild despite the docs only naming `#`.
  comment =
    choice([string("#"), string("//")])
    |> ignore()
    |> repeat(utf8_char(not: ?\n))
    |> ignore()

  ws =
    repeat(
      choice([
        ignore(ascii_char([?\s, ?\t, ?\r, ?\n])),
        comment
      ])
    )

  defcombinatorp(:ws, ws)

  # ---------------------------------------------------------------------------
  # Identifiers, strings, numbers
  # ---------------------------------------------------------------------------

  # Metric names in Loki can contain colons; label names cannot. We accept the
  # more permissive form for identifiers and let the semantic layer reject
  # invalid uses. Leading char: letter or underscore.
  identifier_start = ascii_char([?a..?z, ?A..?Z, ?_])
  identifier_cont = ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_, ?:])

  identifier =
    identifier_start
    |> repeat(identifier_cont)
    |> reduce({List, :to_string, []})

  defcombinatorp(:identifier, identifier)

  # LogQL supports three string forms: double-quoted with escapes,
  # single-quoted with escapes, and backtick-quoted raw. The AST stores the
  # decoded contents; the pretty-printer re-encodes with whichever quoting is
  # convenient.
  # LogQL string escapes: the C-style ones plus `\xHH` for a byte. For
  # everything else — most commonly regex escapes like `\d`, `\w`, `\s` inside
  # `|~ "..."` — we preserve both the backslash and the following character
  # verbatim so the pattern reaches the regex engine unchanged.
  escape_char =
    ignore(string("\\"))
    |> choice([
      string("n") |> replace(?\n),
      string("t") |> replace(?\t),
      string("r") |> replace(?\r),
      string("\\") |> replace(?\\),
      string("\"") |> replace(?"),
      string("'") |> replace(?'),
      string("0") |> replace(0),
      ignore(string("x"))
      |> ascii_char([?0..?9, ?a..?f, ?A..?F])
      |> ascii_char([?0..?9, ?a..?f, ?A..?F])
      |> reduce({__MODULE__, :hex_pair_to_byte, []}),
      utf8_char([]) |> reduce({__MODULE__, :keep_backslash, []})
    ])

  @doc false
  def keep_backslash([char]) do
    IO.iodata_to_binary([?\\, char])
  end

  double_string =
    ignore(string("\""))
    |> repeat(
      choice([
        escape_char,
        utf8_char(not: ?", not: ?\\)
      ])
    )
    |> ignore(string("\""))
    |> reduce({List, :to_string, []})

  single_string =
    ignore(string("'"))
    |> repeat(
      choice([
        escape_char,
        utf8_char(not: ?', not: ?\\)
      ])
    )
    |> ignore(string("'"))
    |> reduce({List, :to_string, []})

  raw_string =
    ignore(string("`"))
    |> repeat(utf8_char(not: ?`))
    |> ignore(string("`"))
    |> reduce({List, :to_string, []})

  string_literal = choice([raw_string, double_string, single_string])

  defcombinatorp(:string_literal, string_literal)

  @doc false
  def hex_pair_to_byte([hi, lo]) do
    List.to_integer([hi, lo], 16)
  end

  # Numeric literal: integer or float, optional sign. Metric queries also
  # accept the special tokens `+Inf`, `-Inf`, `NaN`, though we do not surface
  # them in the AST — they'd only appear on the right of a comparison, which
  # the evaluator does not yet need.
  int_digits = ascii_string([?0..?9], min: 1)

  number_literal =
    optional(ascii_char([?-, ?+]))
    |> concat(int_digits)
    |> optional(
      string(".")
      |> concat(int_digits)
    )
    |> optional(
      ascii_char([?e, ?E])
      |> optional(ascii_char([?-, ?+]))
      |> concat(int_digits)
    )
    |> reduce({__MODULE__, :chars_to_number, []})

  defcombinatorp(:number_literal, number_literal)

  @doc false
  def chars_to_number(chars) do
    str = IO.iodata_to_binary(chars)

    case Integer.parse(str) do
      {int, ""} ->
        int

      _ ->
        {float, ""} = Float.parse(str)
        float
    end
  end

  # Duration literal: sequence of `<int><unit>` pairs summed together.
  # Supported units: ns, us/µs, ms, s, m, h, d, w, y. Loki accepts fractional
  # values (`1.5h`) — we accept them too.
  duration_unit =
    choice([
      string("ns"),
      string("us"),
      string("µs"),
      string("ms"),
      string("s"),
      string("m"),
      string("h"),
      string("d"),
      string("w"),
      string("y")
    ])

  duration_component =
    int_digits
    |> optional(string(".") |> concat(int_digits))
    |> concat(duration_unit)
    |> reduce({__MODULE__, :duration_component_to_ns, []})

  duration_literal =
    times(duration_component, min: 1)
    |> reduce({__MODULE__, :sum_ints, []})

  defcombinatorp(:duration_literal, duration_literal)

  @doc false
  def sum_ints(list), do: Enum.sum(list)

  @duration_factors %{
    "ns" => 1,
    "us" => 1_000,
    "µs" => 1_000,
    "ms" => 1_000_000,
    "s" => 1_000_000_000,
    "m" => 60 * 1_000_000_000,
    "h" => 3600 * 1_000_000_000,
    "d" => 86_400 * 1_000_000_000,
    "w" => 7 * 86_400 * 1_000_000_000,
    "y" => 365 * 86_400 * 1_000_000_000
  }

  @doc false
  # Chars have been split as `[digits, ".", digits, unit]` or `[digits, unit]`.
  # Recombine the numeric portion, parse it, multiply by the unit factor.
  def duration_component_to_ns(chars) do
    [unit | rev] = Enum.reverse(chars)
    num_str = rev |> Enum.reverse() |> IO.iodata_to_binary()
    trunc(parse_number(num_str) * Map.fetch!(@duration_factors, unit))
  end

  defp parse_number(str) do
    case Integer.parse(str) do
      {int, ""} ->
        int

      _ ->
        {float, ""} = Float.parse(str)
        float
    end
  end

  # Byte size literal used in label filters: 5MB, 1KiB, 2GB, 100B.
  byte_unit =
    choice([
      string("PiB"),
      string("TiB"),
      string("GiB"),
      string("MiB"),
      string("KiB"),
      string("PB"),
      string("TB"),
      string("GB"),
      string("MB"),
      string("KB"),
      string("kB"),
      string("B")
    ])

  bytes_literal =
    int_digits
    |> optional(string(".") |> concat(int_digits))
    |> concat(byte_unit)
    |> reduce({__MODULE__, :bytes_component_to_bytes, []})

  defcombinatorp(:bytes_literal, bytes_literal)

  @byte_factors %{
    "B" => 1,
    "kB" => 1_000,
    "KB" => 1_000,
    "MB" => 1_000_000,
    "GB" => 1_000_000_000,
    "TB" => 1_000_000_000_000,
    "PB" => 1_000_000_000_000_000,
    "KiB" => 1024,
    "MiB" => 1024 * 1024,
    "GiB" => 1024 * 1024 * 1024,
    "TiB" => 1024 * 1024 * 1024 * 1024,
    "PiB" => 1024 * 1024 * 1024 * 1024 * 1024
  }

  @doc false
  def bytes_component_to_bytes(chars) do
    [unit | rev] = Enum.reverse(chars)
    num_str = rev |> Enum.reverse() |> IO.iodata_to_binary()
    trunc(parse_number(num_str) * Map.fetch!(@byte_factors, unit))
  end

  # ---------------------------------------------------------------------------
  # Selector
  # ---------------------------------------------------------------------------

  match_op =
    choice([
      string("=~") |> replace(:re),
      string("!~") |> replace(:nre),
      string("!=") |> replace(:neq),
      string("=") |> replace(:eq)
    ])

  matcher =
    parsec(:ws)
    |> parsec(:identifier)
    |> concat(parsec(:ws))
    |> concat(match_op)
    |> concat(parsec(:ws))
    |> parsec(:string_literal)
    |> reduce({__MODULE__, :build_matcher, []})

  @doc false
  def build_matcher([name, op, value]) do
    %AST.Matcher{name: name, op: op, value: value}
  end

  selector =
    ignore(string("{"))
    |> parsec(:ws)
    |> optional(
      matcher
      |> repeat(
        parsec(:ws)
        |> ignore(string(","))
        |> concat(matcher)
      )
    )
    |> concat(parsec(:ws))
    |> ignore(string("}"))
    |> reduce({__MODULE__, :build_selector, []})

  defcombinatorp(:selector, selector)

  @doc false
  def build_selector(matchers) do
    %AST.Selector{matchers: matchers}
  end

  # ---------------------------------------------------------------------------
  # Pipeline stages
  # ---------------------------------------------------------------------------

  # Line filter operator: order matters — `!~` before `!=`, `|~` before `|=`.
  line_filter_op =
    choice([
      string("|~") |> replace(:match_re),
      string("!~") |> replace(:not_match_re),
      string("|=") |> replace(:contains),
      string("!=") |> replace(:not_contains)
    ])

  ip_matcher =
    ignore(string("ip("))
    |> parsec(:ws)
    |> parsec(:string_literal)
    |> parsec(:ws)
    |> ignore(string(")"))
    |> unwrap_and_tag(:ip)

  line_filter_value =
    choice([
      ip_matcher,
      parsec(:string_literal) |> unwrap_and_tag(:string)
    ])

  line_filter =
    parsec(:ws)
    |> concat(line_filter_op)
    |> concat(parsec(:ws))
    |> concat(line_filter_value)
    |> reduce({__MODULE__, :build_line_filter, []})

  @doc false
  def build_line_filter([op, {:ip, ip}]) do
    %AST.LineFilter{op: op, value: {:ip, ip}}
  end

  # Regex ops keep the value tagged as `:re`; substring ops as `:string`.
  def build_line_filter([op, {:string, s}]) when op in [:match_re, :not_match_re] do
    %AST.LineFilter{op: op, value: {:re, s}}
  end

  def build_line_filter([op, {:string, s}]) do
    %AST.LineFilter{op: op, value: {:string, s}}
  end

  # --- Label filter ---------------------------------------------------------

  cmp_op =
    choice([
      string("=~") |> replace(:re),
      string("!~") |> replace(:nre),
      string("<=") |> replace(:lte),
      string(">=") |> replace(:gte),
      string("!=") |> replace(:neq),
      string("=") |> replace(:eq),
      string("<") |> replace(:lt),
      string(">") |> replace(:gt)
    ])

  # A single label predicate. The value's tag drives evaluator dispatch, so we
  # tag every branch here rather than reconstruct at eval time.
  label_filter_value =
    choice([
      parsec(:duration_literal) |> unwrap_and_tag(:duration_ns),
      parsec(:bytes_literal) |> unwrap_and_tag(:bytes),
      parsec(:number_literal) |> unwrap_and_tag(:number),
      parsec(:string_literal) |> unwrap_and_tag(:string)
    ])

  label_cmp =
    parsec(:ws)
    |> parsec(:identifier)
    |> concat(parsec(:ws))
    |> concat(cmp_op)
    |> concat(parsec(:ws))
    |> concat(label_filter_value)
    |> reduce({__MODULE__, :build_label_cmp, []})

  @doc false
  def build_label_cmp([name, op, {tag, value}]) do
    tagged =
      cond do
        op in [:re, :nre] -> {:re, value}
        tag == :string -> {:string, value}
        true -> {tag, value}
      end

    {:cmp, name, op, tagged}
  end

  # Label filter is a chain: `foo=1 and bar=2 or baz=3`. `and` binds tighter
  # than `or`. Parentheses group.
  label_filter_expr =
    parsec(:label_filter_or)
    |> reduce({__MODULE__, :wrap_label_filter, []})

  @doc false
  def wrap_label_filter([expr]), do: %AST.LabelFilter{expr: expr}

  label_filter_or =
    parsec(:label_filter_and)
    |> repeat(
      parsec(:ws)
      |> ignore(string("or"))
      |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
      |> concat(parsec(:label_filter_and))
      |> reduce({__MODULE__, :label_filter_or_pair, []})
    )
    |> reduce({__MODULE__, :fold_label_filter_left, []})

  defcombinatorp(:label_filter_or, label_filter_or)

  label_filter_and =
    parsec(:label_filter_atom)
    |> repeat(
      parsec(:ws)
      |> ignore(string("and"))
      |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
      |> concat(parsec(:label_filter_atom))
      |> reduce({__MODULE__, :label_filter_and_pair, []})
    )
    |> reduce({__MODULE__, :fold_label_filter_left, []})

  defcombinatorp(:label_filter_and, label_filter_and)

  label_filter_atom =
    parsec(:ws)
    |> choice([
      ignore(string("("))
      |> parsec(:label_filter_or)
      |> concat(parsec(:ws))
      |> ignore(string(")")),
      label_cmp
    ])

  defcombinatorp(:label_filter_atom, label_filter_atom)

  @doc false
  def label_filter_or_pair([expr]), do: {:or, expr}
  @doc false
  def label_filter_and_pair([expr]), do: {:and, expr}

  @doc false
  # Left-fold the accumulator into a properly-shaped tree. First element is
  # the head; subsequent elements are `{:and, expr}` / `{:or, expr}` markers.
  def fold_label_filter_left([head | tail]) do
    Enum.reduce(tail, head, fn
      {:and, rhs}, acc -> {:and, acc, rhs}
      {:or, rhs}, acc -> {:or, acc, rhs}
    end)
  end

  # A pipeline `|` may be either a label filter or a stage. Label filters and
  # stages both start with `|`. The parser tries stages first (they name a
  # keyword), then falls back to a label filter.

  # --- Parser stages --------------------------------------------------------

  # An optional field extractor list: `label`, `label="path"`, comma-separated.
  parser_field =
    parsec(:ws)
    |> parsec(:identifier)
    |> optional(
      parsec(:ws)
      |> ignore(string("="))
      |> concat(parsec(:ws))
      |> parsec(:string_literal)
    )
    |> reduce({__MODULE__, :build_parser_field, []})

  @doc false
  def build_parser_field([name]), do: {name, name}
  def build_parser_field([name, source]), do: {name, source}

  parser_fields =
    parser_field
    |> repeat(
      parsec(:ws)
      |> ignore(string(","))
      |> concat(parser_field)
    )

  json_stage =
    ignore(string("json"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> optional(concat(parsec(:ws), parser_fields))
    |> reduce({__MODULE__, :build_json_stage, []})

  @doc false
  def build_json_stage([]), do: %AST.JsonParser{fields: []}
  def build_json_stage(fields), do: %AST.JsonParser{fields: fields}

  logfmt_flag =
    choice([
      string("--strict") |> replace(:strict),
      string("--keep-empty") |> replace(:keep_empty)
    ])

  logfmt_flags =
    parsec(:ws) |> concat(logfmt_flag) |> repeat()

  logfmt_stage =
    ignore(string("logfmt"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> concat(logfmt_flags)
    |> optional(concat(parsec(:ws), parser_fields))
    |> reduce({__MODULE__, :build_logfmt_stage, []})

  @doc false
  def build_logfmt_stage(parts) do
    {flags, fields} = Enum.split_with(parts, &is_atom/1)
    %AST.LogfmtParser{fields: fields, flags: flags}
  end

  regexp_stage =
    ignore(string("regexp"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> parsec(:string_literal)
    |> reduce({__MODULE__, :build_regexp_stage, []})

  @doc false
  def build_regexp_stage([pattern]), do: %AST.RegexpParser{pattern: pattern}

  pattern_stage =
    ignore(string("pattern"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> parsec(:string_literal)
    |> reduce({__MODULE__, :build_pattern_stage, []})

  @doc false
  def build_pattern_stage([pattern]), do: %AST.PatternParser{pattern: pattern}

  unpack_stage =
    ignore(string("unpack"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> replace(%AST.UnpackParser{})

  # --- Format stages --------------------------------------------------------

  line_format_stage =
    ignore(string("line_format"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> parsec(:string_literal)
    |> reduce({__MODULE__, :build_line_format, []})

  @doc false
  def build_line_format([template]), do: %AST.LineFormat{template: template}

  label_format_entry =
    parsec(:ws)
    |> parsec(:identifier)
    |> concat(parsec(:ws))
    |> ignore(string("="))
    |> concat(parsec(:ws))
    |> choice([
      parsec(:string_literal) |> unwrap_and_tag(:template),
      parsec(:identifier) |> unwrap_and_tag(:rename)
    ])
    |> reduce({__MODULE__, :build_label_format_entry, []})

  @doc false
  def build_label_format_entry([name, {:template, s}]), do: {name, {:template, s}}
  def build_label_format_entry([name, {:rename, other}]), do: {name, {:rename, other}}

  label_format_stage =
    ignore(string("label_format"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> concat(label_format_entry)
    |> repeat(
      parsec(:ws)
      |> ignore(string(","))
      |> concat(label_format_entry)
    )
    |> reduce({__MODULE__, :build_label_format, []})

  @doc false
  def build_label_format(entries), do: %AST.LabelFormat{entries: entries}

  # --- Drop / Keep ----------------------------------------------------------

  drop_keep_match =
    optional(
      parsec(:ws)
      |> concat(cmp_op)
      |> concat(parsec(:ws))
      |> parsec(:string_literal)
    )
    |> reduce({__MODULE__, :build_drop_match, []})

  @doc false
  def build_drop_match([]), do: :any
  def build_drop_match([:eq, v]), do: {:eq, v}
  def build_drop_match([:neq, v]), do: {:neq, v}
  def build_drop_match([:re, v]), do: {:re, v}
  def build_drop_match([:nre, v]), do: {:nre, v}

  drop_keep_entry =
    parsec(:ws)
    |> parsec(:identifier)
    |> concat(drop_keep_match)
    |> reduce({__MODULE__, :build_drop_entry, []})

  @doc false
  def build_drop_entry([name, match]), do: {name, match}

  drop_stage =
    ignore(string("drop"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> concat(drop_keep_entry)
    |> repeat(
      parsec(:ws)
      |> ignore(string(","))
      |> concat(drop_keep_entry)
    )
    |> reduce({__MODULE__, :build_drop_stage, []})

  @doc false
  def build_drop_stage(entries), do: %AST.Drop{entries: entries}

  keep_stage =
    ignore(string("keep"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> concat(drop_keep_entry)
    |> repeat(
      parsec(:ws)
      |> ignore(string(","))
      |> concat(drop_keep_entry)
    )
    |> reduce({__MODULE__, :build_keep_stage, []})

  @doc false
  def build_keep_stage(entries), do: %AST.Keep{entries: entries}

  decolorize_stage =
    ignore(string("decolorize"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> replace(%AST.Decolorize{})

  # --- Unwrap ---------------------------------------------------------------

  unwrap_conversion =
    choice([
      string("duration_seconds") |> replace(:duration_seconds),
      string("duration") |> replace(:duration),
      string("bytes") |> replace(:bytes)
    ])

  unwrap_stage =
    ignore(string("unwrap"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> choice([
      unwrap_conversion
      |> ignore(string("("))
      |> parsec(:ws)
      |> parsec(:identifier)
      |> parsec(:ws)
      |> ignore(string(")"))
      |> reduce({__MODULE__, :build_unwrap_conv, []}),
      parsec(:identifier) |> reduce({__MODULE__, :build_unwrap_plain, []})
    ])

  @doc false
  def build_unwrap_conv([conv, label]), do: %AST.Unwrap{label: label, conversion: conv}
  @doc false
  def build_unwrap_plain([label]), do: %AST.Unwrap{label: label, conversion: :none}

  # A pipe-prefixed stage. Try named stages first, then fall back to a label
  # filter — a label filter begins with an identifier followed by a comparison
  # operator, which none of the named stages match.
  pipe_stage =
    parsec(:ws)
    |> ignore(string("|"))
    |> parsec(:ws)
    |> choice([
      json_stage,
      logfmt_stage,
      regexp_stage,
      pattern_stage,
      unpack_stage,
      line_format_stage,
      label_format_stage,
      drop_stage,
      keep_stage,
      decolorize_stage,
      unwrap_stage,
      parsec(:label_filter_expr)
    ])

  defcombinatorp(:label_filter_expr, label_filter_expr)

  # Standalone (non-pipe-prefixed) filter forms that follow the selector
  # without a leading `|`: `|=`, `!=`, `|~`, `!~`.
  bare_line_filter =
    parsec(:ws)
    |> concat(line_filter)

  pipeline_stage = choice([pipe_stage, bare_line_filter])

  defcombinatorp(:pipeline_stage, pipeline_stage)

  # ---------------------------------------------------------------------------
  # Log query
  # ---------------------------------------------------------------------------

  log_query =
    parsec(:ws)
    |> parsec(:selector)
    |> repeat(parsec(:pipeline_stage))
    |> reduce({__MODULE__, :build_log_query, []})

  defcombinatorp(:log_query, log_query)

  @doc false
  def build_log_query([%AST.Selector{} = selector | stages]) do
    %AST.LogQuery{selector: selector, stages: stages}
  end

  # ---------------------------------------------------------------------------
  # Metric expression
  # ---------------------------------------------------------------------------

  range_agg_name =
    choice([
      string("rate_counter") |> replace(:rate_counter),
      string("rate") |> replace(:rate),
      string("count_over_time") |> replace(:count_over_time),
      string("bytes_over_time") |> replace(:bytes_over_time),
      string("bytes_rate") |> replace(:bytes_rate),
      string("sum_over_time") |> replace(:sum_over_time),
      string("avg_over_time") |> replace(:avg_over_time),
      string("max_over_time") |> replace(:max_over_time),
      string("min_over_time") |> replace(:min_over_time),
      string("stddev_over_time") |> replace(:stddev_over_time),
      string("stdvar_over_time") |> replace(:stdvar_over_time),
      string("quantile_over_time") |> replace(:quantile_over_time),
      string("first_over_time") |> replace(:first_over_time),
      string("last_over_time") |> replace(:last_over_time),
      string("absent_over_time") |> replace(:absent_over_time)
    ])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))

  vector_agg_name =
    choice([
      string("sum") |> replace(:sum),
      string("avg") |> replace(:avg),
      string("min") |> replace(:min),
      string("max") |> replace(:max),
      string("count") |> replace(:count),
      string("stddev") |> replace(:stddev),
      string("stdvar") |> replace(:stdvar),
      string("topk") |> replace(:topk),
      string("bottomk") |> replace(:bottomk),
      string("sort_desc") |> replace(:sort_desc),
      string("sort") |> replace(:sort)
    ])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))

  identifier_list =
    parsec(:identifier)
    |> repeat(
      parsec(:ws)
      |> ignore(string(","))
      |> concat(parsec(:ws))
      |> parsec(:identifier)
    )

  grouping =
    parsec(:ws)
    |> choice([
      string("by") |> replace(:by),
      string("without") |> replace(:without)
    ])
    |> concat(parsec(:ws))
    |> ignore(string("("))
    |> parsec(:ws)
    |> optional(identifier_list)
    |> parsec(:ws)
    |> ignore(string(")"))
    |> reduce({__MODULE__, :build_grouping, []})

  @doc false
  def build_grouping([mode | labels]) do
    %AST.Grouping{mode: mode, labels: labels}
  end

  offset_modifier =
    parsec(:ws)
    |> ignore(string("offset"))
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
    |> parsec(:ws)
    |> parsec(:duration_literal)
    |> unwrap_and_tag(:offset)

  at_modifier =
    parsec(:ws)
    |> ignore(string("@"))
    |> parsec(:ws)
    |> parsec(:number_literal)
    |> unwrap_and_tag(:at)

  # `[duration]` bracket. Kept as its own combinator so `unwrap_and_tag(:range)`
  # sees only `[duration]` — the surrounding brackets and whitespace emit no
  # tokens.
  range_bracket =
    parsec(:ws)
    |> ignore(string("["))
    |> parsec(:ws)
    |> parsec(:duration_literal)
    |> parsec(:ws)
    |> ignore(string("]"))
    |> unwrap_and_tag(:range)

  # Optional leading scalar param, e.g. `0.99,` in `quantile_over_time(0.99, ...)`.
  range_leading_param =
    parsec(:number_literal)
    |> unwrap_and_tag(:param)
    |> parsec(:ws)
    |> ignore(string(","))

  # Range aggregator body: an optional leading scalar, the log query,
  # `[duration]`, and optional offset / @ modifiers. Each sub-part emits either
  # a `%LogQuery{}` or a tagged tuple so `build_range_body` can identify them.
  range_body =
    parsec(:ws)
    |> optional(range_leading_param)
    |> parsec(:ws)
    |> concat(parsec(:log_query))
    |> concat(range_bracket)
    |> optional(offset_modifier)
    |> optional(at_modifier)
    |> reduce({__MODULE__, :build_range_body, []})

  @doc false
  def build_range_body(parts) do
    Enum.reduce(parts, %{param: nil, range: nil, offset: nil, at: nil, inner: nil}, fn
      {:param, p}, acc -> %{acc | param: p}
      {:range, r}, acc -> %{acc | range: r}
      {:offset, o}, acc -> %{acc | offset: o}
      {:at, a}, acc -> %{acc | at: trunc(a * 1_000_000_000)}
      %AST.LogQuery{} = q, acc -> %{acc | inner: q}
    end)
  end

  range_agg =
    range_agg_name
    |> concat(parsec(:ws))
    |> ignore(string("("))
    |> concat(range_body)
    |> concat(parsec(:ws))
    |> ignore(string(")"))
    |> reduce({__MODULE__, :build_range_agg, []})

  @doc false
  def build_range_agg([op, body]) do
    %AST.RangeAgg{
      op: op,
      inner: body.inner,
      range_ns: body.range,
      offset_ns: body.offset,
      at_ns: body.at,
      param: body.param
    }
  end

  # Vector aggregator: `sum(...)`, `sum by (svc)(...)`, `sum(...) by (svc)`,
  # `topk(5, ...)` — the scalar param appears before the vector expression.
  vector_agg_call_body =
    parsec(:ws)
    |> ignore(string("("))
    |> parsec(:ws)
    |> optional(
      parsec(:number_literal)
      |> unwrap_and_tag(:param)
      |> concat(parsec(:ws))
      |> ignore(string(","))
    )
    |> parsec(:ws)
    |> concat(parsec(:metric_expr))
    |> concat(parsec(:ws))
    |> ignore(string(")"))
    |> reduce({__MODULE__, :build_vector_call_body, []})

  @doc false
  def build_vector_call_body(parts) do
    Enum.reduce(parts, %{param: nil, inner: nil}, fn
      {:param, p}, acc -> %{acc | param: trunc(p)}
      inner, acc -> %{acc | inner: inner}
    end)
  end

  vector_agg =
    vector_agg_name
    |> concat(
      choice([
        grouping
        |> concat(vector_agg_call_body)
        |> reduce({__MODULE__, :build_vector_agg_pre_grouping, []}),
        vector_agg_call_body
        |> optional(grouping)
        |> reduce({__MODULE__, :build_vector_agg_post_grouping, []})
      ])
    )
    |> reduce({__MODULE__, :attach_vector_op, []})

  @doc false
  def build_vector_agg_pre_grouping([grouping, body]) do
    %{grouping: grouping, body: body}
  end

  def build_vector_agg_post_grouping([body]) do
    %{grouping: nil, body: body}
  end

  def build_vector_agg_post_grouping([body, grouping]) do
    %{grouping: grouping, body: body}
  end

  @doc false
  def attach_vector_op([op, %{grouping: grouping, body: body}]) do
    %AST.VectorAgg{op: op, inner: body.inner, grouping: grouping, param: body.param}
  end

  # ---------------------------------------------------------------------------
  # Metric expression with operator precedence
  # ---------------------------------------------------------------------------

  # PromQL precedence, low to high:
  #   or
  #   and, unless
  #   ==, !=, <=, <, >=, >
  #   +, -
  #   *, /, %
  #   ^ (right-assoc)
  #   unary -
  #   atom

  metric_expr =
    parsec(:or_expr)

  defcombinatorp(:metric_expr, metric_expr)

  or_expr =
    parsec(:and_expr)
    |> repeat(
      parsec(:ws)
      |> ignore(string("or"))
      |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:and_expr))
      |> reduce({__MODULE__, :binop_marker, [:or]})
    )
    |> reduce({__MODULE__, :fold_binop_left, []})

  defcombinatorp(:or_expr, or_expr)

  and_expr =
    parsec(:cmp_expr)
    |> repeat(
      parsec(:ws)
      |> choice([
        string("and") |> replace(:and),
        string("unless") |> replace(:unless)
      ])
      |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:cmp_expr))
      |> reduce({__MODULE__, :binop_marker_dyn, []})
    )
    |> reduce({__MODULE__, :fold_binop_left, []})

  defcombinatorp(:and_expr, and_expr)

  cmp_op_atom =
    choice([
      string("==") |> replace(:eq),
      string("!=") |> replace(:neq),
      string("<=") |> replace(:lte),
      string(">=") |> replace(:gte),
      string("<") |> replace(:lt),
      string(">") |> replace(:gt)
    ])

  cmp_expr =
    parsec(:add_expr)
    |> repeat(
      parsec(:ws)
      |> concat(cmp_op_atom)
      |> optional(
        parsec(:ws)
        |> string("bool")
        |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))
        |> replace(:bool)
      )
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:add_expr))
      |> reduce({__MODULE__, :binop_marker_cmp, []})
    )
    |> reduce({__MODULE__, :fold_binop_left, []})

  defcombinatorp(:cmp_expr, cmp_expr)

  add_expr =
    parsec(:mul_expr)
    |> repeat(
      parsec(:ws)
      |> choice([
        string("+") |> replace(:add),
        string("-") |> replace(:sub)
      ])
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:mul_expr))
      |> reduce({__MODULE__, :binop_marker_dyn, []})
    )
    |> reduce({__MODULE__, :fold_binop_left, []})

  defcombinatorp(:add_expr, add_expr)

  mul_expr =
    parsec(:pow_expr)
    |> repeat(
      parsec(:ws)
      |> choice([
        string("*") |> replace(:mul),
        string("/") |> replace(:div),
        string("%") |> replace(:mod)
      ])
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:pow_expr))
      |> reduce({__MODULE__, :binop_marker_dyn, []})
    )
    |> reduce({__MODULE__, :fold_binop_left, []})

  defcombinatorp(:mul_expr, mul_expr)

  # `^` is right-associative, so recurse right rather than fold left.
  pow_expr =
    parsec(:unary_expr)
    |> optional(
      parsec(:ws)
      |> ignore(string("^"))
      |> optional(parsec(:vector_matching))
      |> concat(parsec(:pow_expr))
      |> reduce({__MODULE__, :pow_marker, []})
    )
    |> reduce({__MODULE__, :fold_pow_right, []})

  defcombinatorp(:pow_expr, pow_expr)

  @doc false
  def pow_marker([%AST.VectorMatching{} = m, rhs]), do: {:pow, m, rhs}
  def pow_marker([rhs]), do: {:pow, nil, rhs}

  @doc false
  def fold_pow_right([lhs]), do: lhs

  def fold_pow_right([lhs, {:pow, matching, rhs}]) do
    %AST.BinaryOp{op: :pow, left: lhs, right: rhs, matching: matching}
  end

  unary_expr =
    parsec(:ws)
    |> choice([
      ignore(string("-"))
      |> parsec(:atom_expr)
      |> reduce({__MODULE__, :apply_unary_minus, []}),
      parsec(:atom_expr)
    ])

  defcombinatorp(:unary_expr, unary_expr)

  @doc false
  def apply_unary_minus([%AST.NumberLit{value: v}]), do: %AST.NumberLit{value: -v}

  def apply_unary_minus([expr]) do
    %AST.BinaryOp{op: :sub, left: %AST.NumberLit{value: 0}, right: expr}
  end

  # `atom` in metric context: range agg, vector agg, number literal, or a
  # parenthesized metric expression. A bare `{selector}...` also counts as a
  # metric-context atom only when it appears inside a range-agg body; at the
  # top level a log query returns from `parse/1` on its own.
  atom_expr =
    parsec(:ws)
    |> choice([
      range_agg,
      vector_agg,
      ignore(string("("))
      |> parsec(:metric_expr)
      |> concat(parsec(:ws))
      |> ignore(string(")")),
      parsec(:number_literal) |> map({__MODULE__, :wrap_number, []})
    ])

  defcombinatorp(:atom_expr, atom_expr)

  @doc false
  def wrap_number(n), do: %AST.NumberLit{value: n}

  # --- Vector matching modifier --------------------------------------------

  match_mode =
    choice([
      string("on") |> replace(:on),
      string("ignoring") |> replace(:ignoring)
    ])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))

  group_side =
    choice([
      string("group_left") |> replace(:left),
      string("group_right") |> replace(:right)
    ])
    |> lookahead_not(ascii_char([?a..?z, ?A..?Z, ?0..?9, ?_]))

  vector_matching =
    parsec(:ws)
    |> concat(match_mode)
    |> concat(parsec(:ws))
    |> ignore(string("("))
    |> parsec(:ws)
    |> optional(identifier_list)
    |> parsec(:ws)
    |> ignore(string(")"))
    |> optional(
      parsec(:ws)
      |> concat(group_side)
      |> optional(
        parsec(:ws)
        |> ignore(string("("))
        |> parsec(:ws)
        |> optional(identifier_list)
        |> parsec(:ws)
        |> ignore(string(")"))
        |> reduce({__MODULE__, :collect_group_labels, []})
      )
      |> reduce({__MODULE__, :collect_group_side, []})
    )
    |> reduce({__MODULE__, :build_vector_matching, []})

  defcombinatorp(:vector_matching, vector_matching)

  @doc false
  def collect_group_labels(labels), do: {:group_labels, labels}

  @doc false
  def collect_group_side([side]), do: {:group, side, []}
  def collect_group_side([side, {:group_labels, labels}]), do: {:group, side, labels}

  @doc false
  def build_vector_matching([mode | rest]) do
    {labels, group_info} =
      case rest do
        [{:group, side, group_labels}] ->
          {[], {side, group_labels}}

        labels ->
          case Enum.split_while(labels, &is_binary/1) do
            {ls, []} -> {ls, nil}
            {ls, [{:group, side, glabels}]} -> {ls, {side, glabels}}
          end
      end

    base = %AST.VectorMatching{mode: mode, labels: labels}

    case group_info do
      nil -> base
      {side, glabels} -> %{base | group: side, group_labels: glabels}
    end
  end

  # --- BinaryOp folding -----------------------------------------------------

  # Each `_marker` helper returns a compact structure that `fold_binop_left`
  # then reduces into a `%BinaryOp{}`.

  @doc false
  def binop_marker([%AST.VectorMatching{} = m, rhs], op), do: {op, false, m, rhs}
  def binop_marker([rhs], op), do: {op, false, nil, rhs}

  @doc false
  def binop_marker_dyn([op, %AST.VectorMatching{} = m, rhs]), do: {op, false, m, rhs}
  def binop_marker_dyn([op, rhs]), do: {op, false, nil, rhs}

  @doc false
  def binop_marker_cmp([op, :bool, %AST.VectorMatching{} = m, rhs]), do: {op, true, m, rhs}
  def binop_marker_cmp([op, :bool, rhs]), do: {op, true, nil, rhs}
  def binop_marker_cmp([op, %AST.VectorMatching{} = m, rhs]), do: {op, false, m, rhs}
  def binop_marker_cmp([op, rhs]), do: {op, false, nil, rhs}

  @doc false
  def fold_binop_left([lhs | rest]) do
    Enum.reduce(rest, lhs, fn {op, bool, matching, rhs}, acc ->
      %AST.BinaryOp{op: op, left: acc, right: rhs, bool: bool, matching: matching}
    end)
  end

  # ---------------------------------------------------------------------------
  # Top-level dispatch
  # ---------------------------------------------------------------------------

  # A top-level expression is either a metric expression or a bare log query.
  # We try the metric expression first because it also accepts numbers and
  # parenthesized forms; if it succeeds but leaves a selector unconsumed the
  # log-query branch will run instead.
  expression =
    parsec(:ws)
    |> choice([
      parsec(:metric_expr),
      parsec(:log_query)
    ])
    |> concat(parsec(:ws))

  defparsecp(:do_parse, expression)
end
