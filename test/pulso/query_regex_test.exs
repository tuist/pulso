defmodule Pulso.QueryRegexTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.{Parser, QueryValidation}
  alias Pulso.Loki.Tail
  alias Pulso.Record.Log

  @controls Path.expand("../fixtures/regex/go-controls.json", __DIR__)
  @external_resource @controls

  test "independent PCRE and native byte engines replay actual Go answers" do
    for row <- @controls |> File.read!() |> Pulso.JSON.decode!() do
      %{"pattern" => pattern, "value" => value, "match" => expected} = row
      assert {:ok, reference} = Pulso.QueryRegex.compile(pattern)
      assert Regex.match?(reference, value) == expected, inspect(row)
      assert {:ok, native} = NIF.compile_log_regex(pattern)
      assert NIF.filter_log_regex(native, [{:kept, value}], false) == if(expected, do: [:kept], else: []), inspect(row)
    end
  end

  test "adversarial backtracking shapes agree before and after parsing, in tails and Parquet" do
    pattern = "^(?:(a|aa)+b|a+c)$"
    line = String.duplicate("a", 64) <> "c"
    record = %Log{timestamp_ns: 1, service: "api", body: line, resource: %{"x" => line}}
    assert :ok = Pulso.Storage.append(:logs, "regex-runtime", [record])
    assert {:ok, blob, _, _, _} = NIF.encode_log_segment_parquet([record])
    assert {:ok, [^record]} = NIF.decode_log_segment_parquet(blob, nil, nil, nil, [], [{:match_re, pattern}])
    quoted = Pulso.JSON.encode!(pattern)

    for {suffix, matches?} <- [
          {" |~ " <> quoted, true},
          {" | json |~ " <> quoted, true},
          {" | json !~ " <> quoted, false},
          {" | x=~" <> quoted, true},
          {" | x!~" <> quoted, false},
          {" | drop x=~" <> quoted <> " | x=\"\"", true},
          {" | keep x=~" <> quoted <> " | x!=\"\"", true}
        ] do
      assert {:ok, ast} = Parser.parse(~s({service="api"}) <> suffix)
      assert :ok = QueryValidation.validate(ast)
      assert {:ok, streams} = Evaluator.evaluate_log(ast, "regex-runtime", %{})
      assert streams != [] == matches?, suffix
      assert Tail.select(Tail.compile(ast), [record]) != [] == matches?, suffix
    end
  end

  test "native named captures retain unmatched groups and do not conceal a PCRE execution limit" do
    line = String.duplicate("a", 64) <> "c"
    pattern = "^(?:(?<first>a|aa)+b|(?<kept>a+c))$"
    assert {:ok, native} = NIF.compile_log_regex(pattern)
    assert %{"first" => "", "kept" => ^line} = NIF.log_regex_named_captures(native, line)
    assert nil == NIF.log_regex_named_captures(native, "unmatched")

    record = %Log{timestamp_ns: 1, service: "api", body: line}
    query = ~s({service="api"} | regexp ) <> Pulso.JSON.encode!(pattern) <> ~s( | kept!="")
    assert {:ok, ast} = Parser.parse(query)
    assert :ok = QueryValidation.validate(ast)

    assert [%{labels: %{"kept" => ^line, "first" => ""}}] =
             Tail.select(Tail.compile(ast), [record])
  end

  test "all pipeline regex positions reject unsupported ambiguous classes and quoted repairs" do
    for pattern <- ["[a[:b]]", "[a[]\\d", "\\Q\\d\\E", "a{2}{3}", "a*{2}", "a+{2}", "a?{1}"],
        stage <- ["|~ ", "| x=~", "| drop x=~", "| keep x=~", "| regexp "] do
      query = ~s({service="api"} ) <> stage <> Pulso.JSON.encode!(pattern)
      assert {:ok, ast} = Parser.parse(query)
      assert {:error, {:invalid_regex, _, _, _}} = QueryValidation.validate(ast), query
    end
  end
end
