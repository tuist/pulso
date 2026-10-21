defmodule Pulso.LogQL.QuantityTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.Quantity

  test "duration parsing reverses all 1047 independently produced public Go formatter controls" do
    cases = Pulso.JSON.decode!(File.read!("test/fixtures/traces/tempo-trace-duration-format-oracle.json"))
    assert length(cases) == 1047
    for [ns, text] <- cases, do: assert(Quantity.duration(text) == {:ok, String.to_integer(ns)})
  end

  test "fractional scaling is exact without decimal floats or unbounded fraction integers" do
    for {unit, factor} <- [{"B", 1}, {"KB", 1_000}, {"KiB", 1024}, {"PiB", 1_125_899_906_842_624}],
        length <- [1, 2, 15, 40, 200] do
      fraction = String.slice(String.duplicate("9876543210", 20), 0, length)
      # Independent bounded test-only rational oracle, never used in production.
      expected = div(String.to_integer("1" <> fraction) * factor, Integer.pow(10, length))
      assert Quantity.bytes("1." <> fraction <> unit) == {:ok, expected}
    end

    assert Quantity.duration("1h2m3.5s") == {:ok, 3_723_500_000_000}
    assert Quantity.duration("0s") == {:ok, 0}
    assert Quantity.duration("+0") == {:ok, 0}
    assert Quantity.duration("-0.0ns") == {:ok, 0}
    # Deliberate exact-decimal policy: Go1.25.10 ParseDuration rounds this to
    # 1_200_000_000_000 through float64; Pulso truncates the exact quantity.
    assert Quantity.duration("0.3333333333333333333h") == {:ok, 1_199_999_999_999}
    assert Quantity.bytes("-5MB") == {:ok, -5_000_000}
    assert Quantity.bytes("+1.5KB") == {:ok, 1_500}
    assert Quantity.duration("1.999999999999999999999ns") == {:ok, 1}
    assert Quantity.bytes("9007199254740993B") == {:ok, 9_007_199_254_740_993}
  end

  test "query literals use the same exact scaling, bounded sums and explicit week/year profile" do
    alias Pulso.LogQL.{AST, Parser}

    for {text, expected} <- [
          {"9007199254740993.0ns", 9_007_199_254_740_993},
          {"0.6ns0.6ns", 0},
          {"1.5w", 907_200_000_000_000}
        ] do
      assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: {:cmp, "latency", :eq, {:duration_ns, ^expected}}}]}} =
               Parser.parse(~s/{app="a"} | latency = #{text}/)
    end

    for text <- ["9223372036854775808ns", "9223372036854775807ns1ns", String.duplicate("9", 200) <> "s"] do
      assert {:error, :invalid_quantity} = Parser.parse(~s/{app="a"} | latency = #{text}/)
    end

    assert {:ok, %AST.LogQuery{stages: [%AST.LabelFilter{expr: {:cmp, "size", :eq, {:bytes, 9_007_199_254_740_993}}}]}} =
             Parser.parse(~s/{app="a"} | size = 9007199254740993.0B/)

    assert Quantity.duration("1w") == :error
    assert Quantity.duration_literal("1w") == {:ok, 604_800_000_000_000}
  end

  test "overflow, unsupported units, junk and internal signs reject without partial parsing" do
    for text <- [
          "",
          "+",
          "--1s",
          "1s-1ms",
          "1sx",
          "1e3s",
          "1w",
          "1y",
          "1.2.3s",
          "9223372036854775808ns",
          "-9223372036854775809ns",
          String.duplicate("9", 10_000) <> "s"
        ] do
      assert Quantity.duration(text) == :error
    end

    assert Quantity.duration("-9223372036854775808ns") == {:ok, -9_223_372_036_854_775_808}
    assert Quantity.duration("9223372036854775807ns1ns") == :error

    for text <- ["", "unknown", "1.5K", "1e3B", "1KBjunk", "9223372036854775808B"] do
      assert Quantity.bytes(text) == :error
    end
  end
end
