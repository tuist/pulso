defmodule Pulso.LogQL.ParserTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Parser
  alias Pulso.LogQL.QueryValidation

  @tag :formatter_roundtrip_batch
  test "complete Loki formatter inputs and literal outputs retain query admission" do
    fixture = File.read!("test/fixtures/logs/loki-upstream/formatter-roundtrip.json") |> Pulso.JSON.decode!()
    assert fixture["revision"] == "5aa4b9212d64d723f63b2770452054aa23df114d"
    assert fixture["row_count"] == 196
    assert fixture["literal_query_count"] == 185
    assert fixture["literal_expected_count"] == 94

    for table <- fixture["tables"], row <- table["rows"], row["query_is_direct_input"] do
      query = Base.decode64!(row["query_bytes_base64"])
      result = with {:ok, ast} <- Parser.parse(query), do: QueryValidation.validate(ast)

      case {table["name"], row["source_index"]} do
        {"Test_SampleExpr_String", 49} ->
          # Outer grammar admits the string, but executable field expressions
          # reject unquoted dotted logfmt sources in the actual pinned parser.
          assert {:ok, _} = Parser.parse(query)
          assert result == {:error, {:invalid_logfmt_expression, "response.code"}}

        {"TestStringer", index} when index in [15, 36] ->
          assert {:ok, _} = Parser.parse(query)
          assert result == {:error, :invalid_ip_operation}

        {"TestJSONSerializationRoundTrip", 12} ->
          assert {:ok, _} = Parser.parse(query)
          assert result == {:error, {:invalid_ip_pattern, ""}}

        {"Test_SampleExpr_String_Fail", _} ->
          # Preserve Loki's rejection oracle while retaining Pulso's existing
          # ordinary nonpositive-rank empty-result policy, not a passing port.
          assert row["reject"]
          assert result == :ok

        {"Test_formatQueryHandlerResponse", 1} ->
          # The source asserts an invalid-query response, not a reject boolean.
          assert {:error, _} = result

        _ ->
          assert result == :ok, inspect({table["name"], row["source_index"], query, result})
      end

      if row["expected_is_same_expression"] and row["expected_bytes_base64"] do
        # These bytes come from Loki's literal oracle, not a Pulso formatter.
        # Admission of both spellings is not proof that Pulso formats queries.
        expected = Base.decode64!(row["expected_bytes_base64"])
        assert {:ok, ast} = Parser.parse(expected)
        assert QueryValidation.validate(ast) == result
      end
    end
  end

  @tag :matcher_admission_batch
  test "complete Loki matcher tables preserve tuples and explicit selector-policy differences" do
    fixture = File.read!("test/fixtures/logs/loki-upstream/matcher-admission.json") |> Pulso.JSON.decode!()
    assert fixture["revision"] == "5aa4b9212d64d723f63b2770452054aa23df114d"
    assert Enum.map(fixture["tables"], & &1["row_count"]) == [4, 19, 7]
    assert fixture["rejected_count"] == 15

    for table <- fixture["tables"], row <- table["rows"] do
      query = Base.decode64!(row["query_bytes_base64"])
      parsed = Parser.parse(query)

      case table["name"] do
        "TestParseMatchers" ->
          # This is a selector-only contract, not general LogQL admission.
          case parsed do
            {:ok, %AST.LogQuery{selector: selector, stages: []} = ast} ->
              refute row["reject"]
              assert QueryValidation.validate(ast) == :ok
              operators = %{eq: "=", neq: "!=", re: "=~", nre: "!~"}

              actual =
                Enum.map(selector.matchers, fn matcher ->
                  {matcher.name, Map.fetch!(operators, matcher.op), matcher.value}
                end)

              expected =
                Enum.map(row["expected_matchers"], fn matcher ->
                  {matcher["name"], matcher["op"], matcher["value"]}
                end)

              assert actual == expected

            _ ->
              assert row["reject"], inspect({query, parsed})
          end

        name ->
          result = with {:ok, ast} <- parsed, do: QueryValidation.validate(ast)

          policy_difference? =
            case name do
              "TestParseSampleExpr_equalityMatcher" -> row["source_index"] in [1, 4, 6, 7, 8, 10, 15, 16, 17]
              "TestParseLogSelectorExpr_equalityMatcher" -> row["source_index"] in [1, 4, 6]
            end

          if policy_difference? do
            # Keep the source rejection: Pulso deliberately permits these
            # empty/negative/empty-compatible selectors under server budgets.
            assert row["reject"]

            assert row["expected_error_expression"] ==
                     "logqlmodel.NewParseError(errAtleastOneEqualityMatcherRequired, 0, 0)"

            assert result == :ok, inspect(query)
          else
            assert result != :ok == row["reject"], inspect({name, query, result})
          end
      end
    end
  end

  @tag :syntax_admission_batch
  test "complete pinned Loki syntax table retains admission and explicit policy differences" do
    fixture = File.read!("test/fixtures/logs/loki-upstream/syntax-admission.json") |> Pulso.JSON.decode!()
    assert fixture["revision"] == "5aa4b9212d64d723f63b2770452054aa23df114d"
    assert length(fixture["rows"]) == 206
    assert Enum.count(fixture["rows"], & &1["reject"]) == 47

    for row <- fixture["rows"] do
      # Base64 preserves the original invalid UTF-8 input as well as raw strings.
      query = Base.decode64!(row["query_bytes_base64"])
      result = with {:ok, ast} <- Parser.parse(query), do: QueryValidation.validate(ast)

      case row["source_index"] do
        0 ->
          # Loki constructs this line-filter AST without compiling its trailing
          # backslash; Pulso deliberately validates patterns before storage.
          assert row["reject"] == false
          assert {:ok, _} = Parser.parse(query)
          assert {:error, {:invalid_regex, :line_filter, "error\\", _reason}} = result

        42 ->
          # Empty selectors are an existing Pulso policy, not a newly skipped row.
          assert row["reject"] == true
          assert query == "approx_count_distinct(mac, {}[1d]) by (version)"
          assert result == :ok

        _ ->
          assert result != :ok == row["reject"], inspect({row["source_index"], query, result})
      end
    end
  end

  # The primary parser test: every non-blank, non-comment line in the corpus
  # file MUST parse to `{:ok, _}`. This catches grammar regressions without
  # over-asserting on AST shape — the `describe` blocks below make the AST
  # guarantees explicit for representative cases.
  @corpus_path Path.expand("../../support/logql_corpus.txt", __DIR__)
  @external_resource @corpus_path
  @corpus @corpus_path
          |> File.read!()
          |> String.split("\n", trim: true)
          |> Enum.reject(&(String.starts_with?(&1, "#") or &1 == ""))

  describe "corpus" do
    for {query, idx} <- Enum.with_index(@corpus) do
      test "#{idx}: #{String.slice(query, 0, 60)}" do
        query = unquote(query)

        assert {:ok, _ast} = Parser.parse(query),
               "expected #{inspect(query)} to parse"
      end
    end
  end

  describe "selector" do
    @tag :byte_diagnostics_batch
    @tag :string_literals_batch
    test "single equality matcher" do
      assert {:ok, %AST.LogQuery{selector: sel, stages: []}} = Parser.parse("{svc=\"api\"}")

      assert %AST.Selector{matchers: [%AST.Matcher{name: "svc", op: :eq, value: "api"}]} = sel

      # Unicode rows from Go 1.27.3 strconv/quote_test.go:251-253,299,310-311,
      # Copyright 2009 The Go Authors, BSD-3-Clause; see Go-LICENSE in the
      # Loki fixtures. Surrogate expectations follow Loki's strutil decoder,
      # not Go's rejecting Unquote. NUL/boundary/malformed controls are local.
      for {escaped, expected} <- [
            {~S/\u1234/, "ሴ"},
            {~S/\U00010111/, <<0x10111::utf8>>},
            {~S/\U0001011111/, <<0x10111::utf8, "11">>},
            {~S/\u0000/, <<0>>},
            {~S/\uD7FF/, <<0xD7FF::utf8>>},
            {~S/\uE000/, <<0xE000::utf8>>},
            {~S/\udead/, "�"},
            {~S/\ud83d\ude4f/, "��"},
            {~S/\U0010FFFF/, <<0x10FFFF::utf8>>}
          ] do
        assert {:ok, %AST.LogQuery{selector: %{matchers: [%AST.Matcher{value: ^expected}]}}} =
                 Parser.parse("{svc=\"" <> escaped <> "\"}")
      end

      for escaped <- [~S/\u/, ~S/\u12/, ~S/\uGGGG/, ~S/\U0000000/, ~S/\U12345678/] do
        assert {:error, _reason} = Parser.parse("{svc=\"" <> escaped <> "\"}")
      end

      assert {:ok, %AST.LogQuery{selector: %{matchers: [%AST.Matcher{value: ~S/\u1234/}]}}} =
               Parser.parse(~S/{svc=`\u1234`}/)
    end

    test "all four match operators" do
      assert {:ok, %AST.LogQuery{selector: sel}} =
               Parser.parse(~s({a="1", b!="2", c=~"3", d!~"4"}))

      assert [
               %AST.Matcher{name: "a", op: :eq, value: "1"},
               %AST.Matcher{name: "b", op: :neq, value: "2"},
               %AST.Matcher{name: "c", op: :re, value: "3"},
               %AST.Matcher{name: "d", op: :nre, value: "4"}
             ] = sel.matchers
    end

    test "rejects a query without a selector" do
      assert {:error, _} = Parser.parse("|= \"foo\"")
    end
  end

  # Go table inputs; expectations from the actual pinned Loki Scanner and
  # Prometheus strutil.Unquote, not strconv.Unquote. See fixture provenance.
  @tag :string_literals_batch
  test "quoted and raw literals preserve decoder bytes and lexer rejection" do
    cases =
      "test/fixtures/logs/loki-upstream/string-literals.jsonl"
      |> File.stream!()
      |> Enum.map(&Pulso.JSON.decode!/1)

    assert length(cases) == 104

    for row <- cases do
      literal = Base.decode64!(row["literal_base64"])
      query = "{svc=" <> literal <> "}"

      case row["value_base64"] do
        nil ->
          assert {:error, _reason} = Parser.parse(query), inspect(literal)

        encoded ->
          expected = Base.decode64!(encoded)

          assert {:ok, %AST.LogQuery{selector: %{matchers: [%AST.Matcher{value: ^expected}]}}} =
                   Parser.parse(query)
      end
    end
  end

  describe "line filter" do
    test "distinguishes substring from regex ops in the value tag" do
      assert {:ok, %AST.LogQuery{stages: stages}} =
               Parser.parse(~s({s="a"} |= "x" |~ "y"))

      assert [
               %AST.LineFilter{op: :contains, value: {:string, "x"}},
               %AST.LineFilter{op: :match_re, value: {:re, "y"}}
             ] = stages
    end

    test "ip filter is tagged distinctly" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LineFilter{value: {:ip, "10.0.0.0/8"}}]}} =
               Parser.parse(~s[{s="a"} |= ip("10.0.0.0/8")])
    end
  end

  describe "label filter" do
    test "duration values are stored in nanoseconds" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: expr}]}} =
               Parser.parse("{s=\"a\"} | duration > 1s")

      assert {:cmp, "duration", :gt, {:duration_ns, 1_000_000_000}} = expr
    end

    test "byte values are stored in bytes" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: expr}]}} =
               Parser.parse("{s=\"a\"} | size < 5MB")

      assert {:cmp, "size", :lt, {:bytes, 5_000_000}} = expr
    end

    @tag :label_conversion_batch
    test "typed equality admits both spellings without changing string or selector grammar" do
      for {literal, expected} <- [
            {"1s", {:duration_ns, 1_000_000_000}},
            {"5MB", {:bytes, 5_000_000}},
            {"2", {:number, 2}}
          ],
          operator <- ["=", "=="] do
        assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: expr}]}} =
                 Parser.parse(~s/{s="a"} | n#{operator}#{literal}/)

        assert expr == {:cmp, "n", :eq, expected}
      end

      for text <- [~s/{s=="a"}/, ~s/{s="a"} | n=="2"/, ~s/{s="a"} | drop n=="2"/, ~s/{s="a"} | keep n=="2"/] do
        assert {:error, _} = Parser.parse(text)
      end
    end

    test "and binds tighter than or" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: expr}]}} =
               Parser.parse(~s({s="a"} | a="1" or b="2" and c="3"))

      # Expected tree: (a="1") or ((b="2") and (c="3"))
      assert {:or, {:cmp, "a", :eq, {:string, "1"}},
              {:and, {:cmp, "b", :eq, {:string, "2"}}, {:cmp, "c", :eq, {:string, "3"}}}} = expr
    end

    test "parentheses override precedence" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: expr}]}} =
               Parser.parse(~s[{s="a"} | (a="1" or b="2") and c="3"])

      assert {:and, {:or, {:cmp, "a", :eq, {:string, "1"}}, {:cmp, "b", :eq, {:string, "2"}}},
              {:cmp, "c", :eq, {:string, "3"}}} = expr
    end
  end

  describe "parser stages" do
    test "json with no args extracts everything" do
      assert {:ok, %AST.LogQuery{stages: [%AST.JsonParser{fields: []}]}} =
               Parser.parse("{s=\"a\"} | json")
    end

    test "json field shorthand duplicates the name" do
      assert {:ok, %AST.LogQuery{stages: [%AST.JsonParser{fields: fields}]}} =
               Parser.parse(~s({s="a"} | json foo, bar="path"))

      assert [{"foo", "foo"}, {"bar", "path"}] = fields
    end

    test "logfmt flags are captured as atoms" do
      assert {:ok, %AST.LogQuery{stages: [%AST.LogfmtParser{flags: flags}]}} =
               Parser.parse("{s=\"a\"} | logfmt --strict --keep-empty")

      assert :strict in flags
      assert :keep_empty in flags
    end
  end

  describe "range aggregation" do
    test "basic rate" do
      assert {:ok,
              %AST.RangeAgg{
                op: :rate,
                inner: %AST.LogQuery{},
                range_ns: 300_000_000_000
              }} = Parser.parse("rate({s=\"a\"}[5m])")
    end

    test "quantile_over_time carries a scalar param" do
      assert {:ok, %AST.RangeAgg{op: :quantile_over_time, param: 0.99}} =
               Parser.parse("quantile_over_time(0.99, {s=\"a\"} | json | unwrap x [5m])")
    end

    test "unwrap ends up inside the inner log query" do
      assert {:ok, %AST.RangeAgg{inner: %AST.LogQuery{stages: stages}}} =
               Parser.parse("sum_over_time({s=\"a\"} | json | unwrap duration(lat) [5m])")

      assert Enum.any?(stages, fn
               %AST.Unwrap{label: "lat", conversion: :duration} -> true
               _ -> false
             end)
    end

    test "offset stored in nanoseconds" do
      assert {:ok, %AST.RangeAgg{offset_ns: offset}} =
               Parser.parse("rate({s=\"a\"}[5m] offset 1h)")

      assert offset == 3_600_000_000_000
    end

    test "@ modifier stored in nanoseconds" do
      assert {:ok, %AST.RangeAgg{at_ns: at}} =
               Parser.parse("rate({s=\"a\"}[5m] @ 1700000000)")

      assert at == 1_700_000_000_000_000_000
    end
  end

  describe "vector aggregation" do
    test "grouping before args" do
      assert {:ok,
              %AST.VectorAgg{
                op: :sum,
                grouping: %AST.Grouping{mode: :by, labels: ["env"]}
              }} = Parser.parse("sum by (env) (rate({s=\"a\"}[5m]))")
    end

    test "grouping after args" do
      assert {:ok,
              %AST.VectorAgg{
                op: :sum,
                grouping: %AST.Grouping{mode: :by, labels: ["env"]}
              }} = Parser.parse("sum(rate({s=\"a\"}[5m])) by (env)")
    end

    test "topk captures the k param as integer" do
      assert {:ok, %AST.VectorAgg{op: :topk, param: 5}} =
               Parser.parse("topk(5, sum(rate({s=\"a\"}[5m])))")
    end
  end

  describe "binary operators" do
    test "left-associative arithmetic" do
      assert {:ok,
              %AST.BinaryOp{
                op: :sub,
                left: %AST.BinaryOp{
                  op: :add,
                  left: %AST.NumberLit{value: 1},
                  right: %AST.NumberLit{value: 2}
                },
                right: %AST.NumberLit{value: 3}
              }} = Parser.parse("1 + 2 - 3")
    end

    test "multiplication binds tighter than addition" do
      assert {:ok,
              %AST.BinaryOp{
                op: :add,
                left: %AST.NumberLit{value: 1},
                right: %AST.BinaryOp{
                  op: :mul,
                  left: %AST.NumberLit{value: 2},
                  right: %AST.NumberLit{value: 3}
                }
              }} = Parser.parse("1 + 2 * 3")
    end

    test "exponentiation is right-associative" do
      assert {:ok,
              %AST.BinaryOp{
                op: :pow,
                left: %AST.NumberLit{value: 2},
                right: %AST.BinaryOp{
                  op: :pow,
                  left: %AST.NumberLit{value: 3},
                  right: %AST.NumberLit{value: 4}
                }
              }} = Parser.parse("2 ^ 3 ^ 4")
    end

    test "bool modifier flips comparison to scalar" do
      assert {:ok, %AST.BinaryOp{op: :gt, bool: true}} =
               Parser.parse("sum(rate({s=\"a\"}[5m])) > bool 100")
    end

    test "vector matching modifier is attached to the op" do
      assert {:ok,
              %AST.BinaryOp{
                op: :div,
                matching: %AST.VectorMatching{mode: :on, labels: ["env"]}
              }} = Parser.parse(~s|rate({s="a"}[5m]) / on(env) rate({s="b"}[5m])|)
    end

    test "group_left with label list" do
      assert {:ok, %AST.BinaryOp{matching: matching}} =
               Parser.parse(~s|rate({s="a"}[5m]) * on(env) group_left(pod) rate({s="b"}[5m])|)

      assert %AST.VectorMatching{
               mode: :on,
               labels: ["env"],
               group: :left,
               group_labels: ["pod"]
             } = matching
    end

    test "unary minus on a number" do
      assert {:ok, %AST.NumberLit{value: -5}} = Parser.parse("-5")
    end

    test "a sign cannot negate a vector or parenthesized expression" do
      for query <- ["-rate({s=\"a\"}[5m])", "-vector(2)", "-(1 + 2)", "+(1 + 2)"] do
        assert {:error, _} = Parser.parse(query)
      end
    end
  end

  describe "errors" do
    test "trailing garbage" do
      assert {:error, {_line, _col, _}} = Parser.parse("{s=\"a\"} garbage")
    end

    test "unterminated selector" do
      assert {:error, _} = Parser.parse("{s=\"a\"")
    end

    test "unknown top-level construct" do
      assert {:error, _} = Parser.parse("not a query")
    end
  end
end
