defmodule Pulso.PromQL.ParserTest do
  use ExUnit.Case, async: true

  alias Pulso.PromQL.Parser

  test "matcher counts and pattern sizes are bounded before native compilation" do
    matchers = Enum.map_join(1..65, ",", &~s(a#{&1}="x"))
    assert {:error, _} = Parser.parse("m{" <> matchers <> "}")
    assert {:error, _} = Parser.parse(~s(m{job=~"#{String.duplicate("a", 1025)}"}))
  end

  test "unclosed nested aggregations cannot backtrack exponentially" do
    supervisor = start_supervised!({Task.Supervisor, []})

    for prefix <- ["sum(", "topk(1,"], depth <- [20, 30, 100] do
      task = Task.Supervisor.async_nolink(supervisor, fn -> Parser.parse(String.duplicate(prefix, depth) <> "x") end)
      assert {:ok, {:error, _}} = Task.yield(task, 1000)
    end
  end

  test "Prometheus keywords are case insensitive while label identifiers retain case" do
    for {upper, lower} <- [
          {"SUM BY(job)(a)", "sum by(job)(a)"},
          {"TOPK(1,a)", "topk(1,a)"},
          {"a AND ON(job) b", "a and on(job) b"},
          {"a > BOOL 1", "a > bool 1"},
          {"a OFFSET 1m", "a offset 1m"},
          {"a / ON(job) GROUP_LEFT(region) b", "a / on(job) group_left(region) b"}
        ] do
      assert Parser.parse(upper) == Parser.parse(lower)
    end

    assert {:ok, {:selector, [_, {"Job", :eq, "API"}], 0}} = Parser.parse(~s|a{Job="API"}|)
  end

  test "mutated untrusted input always returns a parse result" do
    :rand.seed(:exsss, {11, 37, 91})
    seeds = [~s|sum by(job) (rate(m{job=~"api.*"}[5m]))|, "m offset 1h", "# comment\nm"]

    for seed <- seeds, _ <- 1..100 do
      position = :rand.uniform(byte_size(seed)) - 1
      <<prefix::binary-size(^position), _byte, suffix::binary>> = seed
      query = prefix <> <<:rand.uniform(256) - 1>> <> suffix
      assert {status, _} = Parser.parse(query)
      assert status in [:ok, :error]
    end
  end

  test "aggregations ignore case and whitespace may include comments" do
    assert Parser.parse("SUM(m)") == Parser.parse("sum(m)")
    assert Parser.parse("sUm( # aggregate\n m) # trailing") == Parser.parse("sum(m)")
    assert {:ok, _} = Parser.parse(~s(m{job="#literal"}))
  end

  test "named selectors and offsets, grouped nested rate expressions" do
    assert {:ok, {:selector, [{"__name__", :eq, "requests_total"}, {"job", :eq, "api"}], 60_000_000_000}} =
             Parser.parse(~s(requests_total{job="api"} offset 1m))

    assert {:ok, {:aggregate, :sum, {:grouping, :by, ["job"]}, {:function, :rate, {:range, _, 300_000_000_000, 0}}}} =
             Parser.parse("sum by (job) (rate(requests_total[5m]))")

    assert {:ok, {:aggregate, :avg, {:grouping, :without, ["instance"]}, _}} =
             Parser.parse("avg(requests_total) without (instance)")

    assert {:ok, _} = Parser.parse(~s({__name__=~"requests_.+",job!="",}))
    assert {:ok, {:selector, [{"__name__", :eq, ":requests_total"}], 0}} = Parser.parse(":requests_total")
  end

  test "rejects invalid selectors, ranges, and unsupported syntax" do
    for query <- [
          "",
          "{}",
          ~s({job=~".*"}),
          ~s({job=~"["}),
          "rate(x[0s])",
          "rate(x[1m1h])",
          "rate(x[1.5h])",
          "sum by (a) (x) without (b)",
          "x[5m]",
          "rate(x[5m]) trailing",
          "x offset -1m",
          "rate(x[5m:1m])",
          "x @ 123",
          "sumby(job)(x)",
          "x offset1m",
          "sum by(a,a)(x)"
        ] do
      assert {:error, _} = Parser.parse(query), query
    end
  end

  test "quoted matcher contents do not become syntax" do
    assert {:ok, {:selector, [_, {"job", :eq, "a\"}b"}], 0}} = Parser.parse(~S(x{job="a\"}b"}))
    assert {:ok, _} = Parser.parse(~S(x{job=~`api\d+`}))
    assert {:error, _} = Parser.parse(~S(x{job="\q"}))
  end

  test "escaped bytes and Unicode preserve matcher values without silent transcoding" do
    assert {:ok, {:selector, [_, {"job", :eq, "é"}], 0}} = Parser.parse(~S(x{job="\xc3\xa9"}))
    assert {:ok, {:selector, [_, {"job", :eq, "é"}], 0}} = Parser.parse(~S(x{job="\u00e9"}))
    assert {:ok, {:selector, [_, {"job", :eq, "é"}], 0}} = Parser.parse(~S(x{job="é"}))
    assert {:error, _} = Parser.parse(~S(x{job="\xff"}))
    assert {:error, _} = Parser.parse(~S(x{job="\777"}))
  end

  test "malformed regexes cannot use the anchoring wrapper to become valid" do
    for op <- ["=~", "!~"] do
      assert {:error, :invalid_selector} = Parser.parse(~s/x{job#{op}"a)|.*(?:"}/)
    end
  end

  test "the execution engine rejects unsupported regexes and excessive durations" do
    for op <- ["=~", "!~"] do
      assert {:error, _} = Parser.parse(~s/m{job#{op}"(?!api).*"}/)
      assert {:error, _} = Parser.parse(~S/m{job=~"(a)\\1"}/)
    end

    assert {:error, _} = Parser.parse("rate(m[300y])")
    assert {:error, _} = Parser.parse("m offset 999999999999999999y")
  end
end
