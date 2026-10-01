defmodule Pulso.LogQL.Pipeline do
  @moduledoc """
  Pipeline runner: applies an ordered list of LogQL stage ASTs to a
  stream of `Pulso.LogQL.Entry` values.

  Every stage returns `{:keep, entry} | :drop`. The runner short-circuits
  on `:drop` and drops the record from the output stream. Stages have
  no cross-record state — anything they compute is folded into the
  entry's `labels` or `line`.

  Stages that come pre-pushed into the storage layer (selector matchers
  and line filters at the head of the pipeline) are removed by
  `Pulso.LogQL.Evaluator` before this runner sees them, so this module
  runs exactly the stages Rust does not.
  """

  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Entry
  alias Pulso.LogQL.Pipeline.Logfmt
  alias Pulso.LogQL.Pipeline.Pattern
  alias Pulso.LogQL.Pipeline.Template

  @type stage :: struct()
  @type result :: {:keep, Entry.t()} | :drop

  # ---------------------------------------------------------------------------
  # Runner
  # ---------------------------------------------------------------------------

  @doc """
  Compile a list of AST stages into an internal representation, resolving
  regex patterns, pre-parsing templates, etc. Compilation is done once
  per query; the compiled stages then run against every entry.
  """
  @spec compile([stage()]) :: [term()]
  def compile(stages) when is_list(stages) do
    Enum.map(stages, &compile_stage/1)
  end

  @doc "Apply every compiled stage to a stream of entries."
  @spec run([term()], Enumerable.t(Entry.t())) :: Enumerable.t(Entry.t())
  def run(compiled, entries) do
    entries
    |> Stream.transform(:ok, fn entry, acc ->
      case run_stages(compiled, entry) do
        {:keep, e} -> {[e], acc}
        :drop -> {[], acc}
      end
    end)
  end

  defp run_stages([], entry), do: {:keep, entry}

  defp run_stages([stage | rest], entry) do
    case apply_stage(stage, entry) do
      {:keep, updated} -> run_stages(rest, updated)
      :drop -> :drop
    end
  end

  # ---------------------------------------------------------------------------
  # Stage compilation — done once per query, before the record loop
  # ---------------------------------------------------------------------------

  defp compile_stage(%AST.LineFilter{op: op, value: value}) do
    {:line_filter, op, compile_line_filter_value(value)}
  end

  defp compile_stage(%AST.LabelFilter{expr: expr}) do
    {:label_filter, compile_label_filter(expr)}
  end

  defp compile_stage(%AST.JsonParser{fields: fields}) do
    {:json, fields}
  end

  defp compile_stage(%AST.LogfmtParser{fields: fields, flags: flags}) do
    {:logfmt, fields, :keep_empty in flags}
  end

  defp compile_stage(%AST.RegexpParser{pattern: pattern}) do
    {:ok, regex} = Regex.compile(pattern)
    {:regexp, regex}
  end

  defp compile_stage(%AST.PatternParser{pattern: pattern}) do
    {:pattern, Pattern.compile(pattern)}
  end

  defp compile_stage(%AST.UnpackParser{}), do: :unpack

  defp compile_stage(%AST.LineFormat{template: t}) do
    {:line_format, Template.compile(t)}
  end

  defp compile_stage(%AST.LabelFormat{entries: entries}) do
    compiled =
      Enum.map(entries, fn
        {name, {:template, t}} -> {name, {:template, Template.compile(t)}}
        {name, {:rename, src}} -> {name, {:rename, src}}
      end)

    {:label_format, compiled}
  end

  defp compile_stage(%AST.Drop{entries: entries}) do
    {:drop, Enum.map(entries, &compile_drop_entry/1)}
  end

  defp compile_stage(%AST.Keep{entries: entries}) do
    {:keep, Enum.map(entries, &compile_drop_entry/1)}
  end

  defp compile_stage(%AST.Decolorize{}), do: :decolorize

  defp compile_stage(%AST.Unwrap{} = unwrap), do: {:unwrap, unwrap}

  defp compile_line_filter_value({:string, s}), do: {:string, s}
  defp compile_line_filter_value({:re, pat}), do: {:re, compile_regex(pat)}
  # IP line filters are rejected by `Pulso.LogQL.QueryValidation` before
  # the pipeline compiles, so this clause exists only as a safety net.
  defp compile_line_filter_value({:ip, _cidr}) do
    raise ArgumentError, "ip(...) line filters are not yet supported"
  end

  defp compile_regex(pat) do
    {:ok, re} = Regex.compile(pat)
    re
  end

  defp compile_drop_entry({name, :any}), do: {name, :any}
  defp compile_drop_entry({name, {:eq, v}}), do: {name, {:eq, v}}
  defp compile_drop_entry({name, {:neq, v}}), do: {name, {:neq, v}}
  defp compile_drop_entry({name, {:re, pat}}), do: {name, {:re, compile_regex(pat)}}
  defp compile_drop_entry({name, {:nre, pat}}), do: {name, {:nre, compile_regex(pat)}}

  # Label-filter tree: recursively compile so leaf regexes precompile once.
  defp compile_label_filter({:and, l, r}), do: {:and, compile_label_filter(l), compile_label_filter(r)}
  defp compile_label_filter({:or, l, r}), do: {:or, compile_label_filter(l), compile_label_filter(r)}

  defp compile_label_filter({:cmp, name, op, {:re, pat}}) do
    {:cmp, name, op, {:re, compile_regex(pat)}}
  end

  defp compile_label_filter({:cmp, name, op, value}) do
    {:cmp, name, op, value}
  end

  # ---------------------------------------------------------------------------
  # Stage application
  # ---------------------------------------------------------------------------

  defp apply_stage({:line_filter, op, value}, %Entry{} = e) do
    if line_matches?(op, value, e.line), do: {:keep, e}, else: :drop
  end

  defp apply_stage({:label_filter, tree}, %Entry{} = e) do
    if eval_label_filter(tree, e.labels), do: {:keep, e}, else: :drop
  end

  defp apply_stage({:json, fields}, %Entry{} = e) do
    case decode_json(e.line) do
      {:ok, map} when is_map(map) ->
        extracted = extract_json_fields(map, fields)
        {:keep, %{e | labels: Map.merge(e.labels, extracted)}}

      _ ->
        {:keep, e}
    end
  end

  defp apply_stage({:logfmt, fields, keep_empty?}, %Entry{} = e) do
    pairs = Logfmt.parse(e.line, keep_empty?)
    extracted = extract_logfmt_fields(pairs, fields)
    {:keep, %{e | labels: Map.merge(e.labels, extracted)}}
  end

  defp apply_stage({:regexp, regex}, %Entry{} = e) do
    captures = Regex.named_captures(regex, e.line) || %{}
    {:keep, %{e | labels: Map.merge(e.labels, captures)}}
  end

  defp apply_stage({:pattern, tmpl}, %Entry{} = e) do
    case Pattern.match(tmpl, e.line) do
      {:ok, caps} -> {:keep, %{e | labels: Map.merge(e.labels, caps)}}
      :nomatch -> {:keep, e}
    end
  end

  defp apply_stage(:unpack, %Entry{} = e) do
    case decode_json(e.line) do
      {:ok, map} when is_map(map) ->
        {new_line, extras} = Map.pop(map, "_entry", e.line)
        new_line = if is_binary(new_line), do: new_line, else: Pulso.JSON.encode!(new_line)
        stringified = Map.new(extras, fn {k, v} -> {k, to_string_val(v)} end)
        {:keep, %{e | line: new_line, labels: Map.merge(e.labels, stringified)}}

      _ ->
        {:keep, e}
    end
  end

  defp apply_stage({:line_format, tmpl}, %Entry{} = e) do
    {:keep, %{e | line: Template.render(tmpl, e.labels)}}
  end

  defp apply_stage({:label_format, entries}, %Entry{} = e) do
    labels =
      Enum.reduce(entries, e.labels, fn
        {name, {:template, t}}, acc -> Map.put(acc, name, Template.render(t, acc))
        {name, {:rename, src}}, acc -> acc |> Map.put(name, Map.get(acc, src, "")) |> Map.delete(src)
      end)

    {:keep, %{e | labels: labels}}
  end

  defp apply_stage({:drop, entries}, %Entry{} = e) do
    labels =
      Enum.reduce(entries, e.labels, fn {name, match}, acc ->
        if drop_matches?(match, Map.get(acc, name)) do
          Map.delete(acc, name)
        else
          acc
        end
      end)

    {:keep, %{e | labels: labels}}
  end

  defp apply_stage({:keep, entries}, %Entry{} = e) do
    kept =
      for {name, match} <- entries, value = Map.get(e.labels, name), keep_matches?(match, value), into: %{} do
        {name, value}
      end

    {:keep, %{e | labels: kept}}
  end

  defp apply_stage(:decolorize, %Entry{} = e) do
    {:keep, %{e | line: strip_ansi(e.line)}}
  end

  defp apply_stage({:unwrap, _}, %Entry{} = e) do
    # Unwrap is a directive for the range aggregator; it does not filter or
    # transform records at the pipeline level. Pass-through here.
    {:keep, e}
  end

  # ---------------------------------------------------------------------------
  # Predicates
  # ---------------------------------------------------------------------------

  defp line_matches?(:contains, {:string, s}, line), do: String.contains?(line, s)
  defp line_matches?(:not_contains, {:string, s}, line), do: not String.contains?(line, s)
  defp line_matches?(:match_re, {:re, re}, line), do: Regex.match?(re, line)
  defp line_matches?(:not_match_re, {:re, re}, line), do: not Regex.match?(re, line)

  defp eval_label_filter({:and, l, r}, labels), do: eval_label_filter(l, labels) and eval_label_filter(r, labels)
  defp eval_label_filter({:or, l, r}, labels), do: eval_label_filter(l, labels) or eval_label_filter(r, labels)

  defp eval_label_filter({:cmp, name, op, value}, labels) do
    actual = Map.get(labels, name, "")
    eval_cmp(op, actual, value)
  end

  defp eval_cmp(:eq, actual, {:string, v}), do: actual == v
  defp eval_cmp(:neq, actual, {:string, v}), do: actual != v
  defp eval_cmp(:re, actual, {:re, re}), do: Regex.match?(re, actual)
  defp eval_cmp(:nre, actual, {:re, re}), do: not Regex.match?(re, actual)

  defp eval_cmp(op, actual, {:number, n}) when op in [:eq, :neq, :lt, :lte, :gt, :gte] do
    case parse_number(actual) do
      {:ok, num} -> compare_number(op, num, n)
      :error -> false
    end
  end

  defp eval_cmp(op, actual, {:duration_ns, ns}) when op in [:eq, :neq, :lt, :lte, :gt, :gte] do
    case parse_duration_ns(actual) do
      {:ok, dur} -> compare_number(op, dur, ns)
      :error -> false
    end
  end

  defp eval_cmp(op, actual, {:bytes, bytes}) when op in [:eq, :neq, :lt, :lte, :gt, :gte] do
    case parse_bytes(actual) do
      {:ok, b} -> compare_number(op, b, bytes)
      :error -> false
    end
  end

  defp eval_cmp(_, _, _), do: false

  defp compare_number(:eq, a, b), do: a == b
  defp compare_number(:neq, a, b), do: a != b
  defp compare_number(:lt, a, b), do: a < b
  defp compare_number(:lte, a, b), do: a <= b
  defp compare_number(:gt, a, b), do: a > b
  defp compare_number(:gte, a, b), do: a >= b

  defp drop_matches?(:any, _v), do: true
  defp drop_matches?(_, nil), do: false
  defp drop_matches?({:eq, v}, actual), do: actual == v
  defp drop_matches?({:neq, v}, actual), do: actual != v
  defp drop_matches?({:re, re}, actual), do: Regex.match?(re, actual)
  defp drop_matches?({:nre, re}, actual), do: not Regex.match?(re, actual)

  defp keep_matches?(:any, _), do: true
  defp keep_matches?({:eq, v}, actual), do: actual == v
  defp keep_matches?({:neq, v}, actual), do: actual != v
  defp keep_matches?({:re, re}, actual), do: Regex.match?(re, actual)
  defp keep_matches?({:nre, re}, actual), do: not Regex.match?(re, actual)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp decode_json(""), do: :error

  defp decode_json(line) do
    {:ok, Pulso.JSON.decode!(line)}
  rescue
    _ -> :error
  end

  # No field list means extract every top-level string-ish key.
  defp extract_json_fields(map, []) do
    Map.new(map, fn {k, v} -> {to_string(k), to_string_val(v)} end)
  end

  defp extract_json_fields(map, fields) do
    Map.new(fields, fn {label, path} ->
      {label, path |> String.split(".") |> Enum.reduce(map, &nested_get/2) |> to_string_val()}
    end)
  end

  defp nested_get(_key, nil), do: nil
  defp nested_get(key, %{} = m), do: Map.get(m, key)
  defp nested_get(_key, _), do: nil

  defp extract_logfmt_fields(pairs, []) do
    Map.new(pairs, fn {k, v} -> {k, v} end)
  end

  defp extract_logfmt_fields(pairs, fields) do
    map = Map.new(pairs)
    Map.new(fields, fn {label, source} -> {label, Map.get(map, source, "")} end)
  end

  defp to_string_val(v) when is_binary(v), do: v
  defp to_string_val(v) when is_integer(v), do: Integer.to_string(v)
  defp to_string_val(v) when is_float(v), do: Float.to_string(v)
  defp to_string_val(true), do: "true"
  defp to_string_val(false), do: "false"
  defp to_string_val(nil), do: ""
  defp to_string_val(v), do: Pulso.JSON.encode!(v)

  defp parse_number(str) do
    case Integer.parse(str) do
      {i, ""} ->
        {:ok, i}

      _ ->
        case Float.parse(str) do
          {f, ""} -> {:ok, f}
          _ -> :error
        end
    end
  end

  defp parse_duration_ns(str) do
    # Reuse the LogQL grammar's duration lexer indirectly via parsing a
    # wrapped label-filter expression. Simpler: implement the same
    # sub-set (int + unit, no fractions) here.
    parse_duration(str, 0)
  end

  defp parse_duration("", acc) when acc > 0, do: {:ok, acc}
  defp parse_duration("", _), do: :error

  defp parse_duration(str, acc) do
    case Integer.parse(str) do
      {n, rest} ->
        {factor, tail} = split_duration_unit(rest)
        if factor == 0, do: :error, else: parse_duration(tail, acc + n * factor)

      :error ->
        :error
    end
  end

  defp split_duration_unit("ns" <> rest), do: {1, rest}
  defp split_duration_unit("us" <> rest), do: {1_000, rest}
  defp split_duration_unit("µs" <> rest), do: {1_000, rest}
  defp split_duration_unit("ms" <> rest), do: {1_000_000, rest}
  defp split_duration_unit("s" <> rest), do: {1_000_000_000, rest}
  defp split_duration_unit("m" <> rest), do: {60 * 1_000_000_000, rest}
  defp split_duration_unit("h" <> rest), do: {3_600 * 1_000_000_000, rest}
  defp split_duration_unit("d" <> rest), do: {86_400 * 1_000_000_000, rest}
  defp split_duration_unit(_), do: {0, ""}

  defp parse_bytes(str) do
    case Integer.parse(str) do
      {n, rest} ->
        case split_bytes_unit(rest) do
          0 -> :error
          f -> {:ok, n * f}
        end

      :error ->
        :error
    end
  end

  defp split_bytes_unit(""), do: 1
  defp split_bytes_unit("B"), do: 1
  defp split_bytes_unit("kB"), do: 1_000
  defp split_bytes_unit("KB"), do: 1_000
  defp split_bytes_unit("MB"), do: 1_000_000
  defp split_bytes_unit("GB"), do: 1_000_000_000
  defp split_bytes_unit("TB"), do: 1_000_000_000_000
  defp split_bytes_unit("KiB"), do: 1024
  defp split_bytes_unit("MiB"), do: 1024 * 1024
  defp split_bytes_unit("GiB"), do: 1024 * 1024 * 1024
  defp split_bytes_unit("TiB"), do: 1024 * 1024 * 1024 * 1024
  defp split_bytes_unit(_), do: 0

  # Strip ANSI CSI sequences: ESC[ ... final-byte (a byte in 0x40-0x7E).
  defp strip_ansi(line) do
    strip_ansi(line, [])
  end

  defp strip_ansi(<<0x1B, "[", rest::binary>>, acc) do
    rest = skip_ansi_params(rest)
    strip_ansi(rest, acc)
  end

  defp strip_ansi(<<c::utf8, rest::binary>>, acc), do: strip_ansi(rest, [acc, <<c::utf8>>])
  defp strip_ansi(<<>>, acc), do: IO.iodata_to_binary(acc)

  defp skip_ansi_params(<<c, rest::binary>>) when c >= 0x40 and c <= 0x7E, do: rest
  defp skip_ansi_params(<<_c, rest::binary>>), do: skip_ansi_params(rest)
  defp skip_ansi_params(<<>>), do: <<>>
end
