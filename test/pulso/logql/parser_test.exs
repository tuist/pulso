defmodule Pulso.LogQL.ParserTest do
  use ExUnit.Case, async: true

  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Parser

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
    test "single equality matcher" do
      assert {:ok, %AST.LogQuery{selector: sel, stages: []}} = Parser.parse("{svc=\"api\"}")

      assert %AST.Selector{matchers: [%AST.Matcher{name: "svc", op: :eq, value: "api"}]} = sel
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

    test "unary minus on an expression rewrites as 0 - x" do
      assert {:ok, %AST.BinaryOp{op: :sub, left: %AST.NumberLit{value: 0}}} =
               Parser.parse("-rate({s=\"a\"}[5m])")
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
