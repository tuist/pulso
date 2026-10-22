defmodule Pulso.LogQL.BinaryBudgetTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.Parser
  alias Pulso.PromQL.Evaluator, as: Budget
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()

    records =
      for {pod, ts} <- [{"one", 9}, {"one", 10}, {"two", 10}],
          do: %Log{timestamp_ns: ts, service: "api", body: "hello", resource: %{"pod" => pod}}

    assert :ok = Storage.append(:logs, "acme", records)
    :ok
  end

  defp run(query, opts \\ %{end_ts_ns: 10}) do
    with {:ok, ast} <- Parser.parse(query), do: Evaluator.evaluate_metric(ast, "acme", opts)
  end

  test "scalar broadcasts work at every matrix step" do
    assert {:ok, {:matrix, series}} =
             run(~s'count_over_time({service="api"}[1m]) / 2', %{start_ts_ns: 10, end_ts_ns: 11, step_ns: 1})

    values = Map.new(series, fn {labels, points} -> {labels["pod"], points} end)
    assert values == %{"one" => [{10, 1.0}, {11, 1.0}], "two" => [{10, 0.5}, {11, 0.5}]}

    assert {:ok, {:matrix, [{%{}, [{10, 3.0}, {11, 3.0}]}]}} =
             run("1 + 2", %{start_ts_ns: 10, end_ts_ns: 11, step_ns: 1})
  end

  test "set unions retain unmatched right values and do not manufacture boolean ones" do
    assert {:ok, {:vector, series}} =
             run(~s'count_over_time({service="absent"}[1m]) or count_over_time({service="api"}[1m])')

    assert Map.new(series, fn {labels, {ts, value}} -> {labels["pod"], {ts, value}} end) == %{
             "one" => {10, 2.0},
             "two" => {10, 1.0}
           }

    assert {:ok, {:vector, both}} =
             run(~s'count_over_time({service="api"}[1m]) and count_over_time({service="api"}[1m])')

    assert Map.new(series) == Map.new(both)
  end

  test "empty-label aggregation is a vector and ambiguous matching rejects, never overwrites" do
    assert {:error, :many_to_many_matching} =
             run(~s'sum(count_over_time({service="api"}[1m])) + on () count_over_time({service="api"}[1m])')
  end

  test "empty matrix windows are missing, not zero samples" do
    assert {:ok, {:matrix, series}} =
             run(~s'count_over_time({service="api"}[1ns])', %{start_ts_ns: 10, end_ts_ns: 12, step_ns: 1})

    assert Enum.all?(series, fn {_, points} -> points == [{10, 1.0}] end)
  end

  test "absence reports one only when the whole window is empty" do
    assert {:ok, {:vector, [{%{"service" => "missing"}, {10, 1.0}}]}} =
             run(~s'absent_over_time({service="missing", pod=~".*"}[1m])')

    assert {:ok, {:vector, []}} = run(~s'absent_over_time({service="api"}[1m])')

    assert {:ok, {:matrix, [{%{"service" => "api"}, [{11, 1.0}, {12, 1.0}]}]}} =
             run(~s'absent_over_time({service="api"}[1ns])', %{start_ts_ns: 10, end_ts_ns: 12, step_ns: 1})
  end

  test "invalid unwrapped points fail the whole query instead of averaging a valid subset" do
    assert :ok =
             Storage.append(:logs, "acme", [
               %Log{timestamp_ns: 10, service: "value", body: "x=1"},
               %Log{timestamp_ns: 10, service: "value", body: "x=invalid"}
             ])

    assert {:error, :sample_extraction_error} =
             run(~s'quantile_over_time(0.5, {service="value"} | logfmt | unwrap x [1m]) by ()')

    assert {:error, :unsupported_post_unwrap_stage} =
             run(~s'quantile_over_time(0.5, {service="value"} | logfmt | unwrap x | __error__="" [1m])')

    assert {:error, :invalid_range_duration} = run(~s'count_over_time({service="api"}[0s])')
  end

  test "nonfinite extracted values propagate through range and vector folds without arithmetic crashes" do
    assert :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: "special", body: "x=NaN"}])

    for operation <- ~w(sum_over_time avg_over_time min_over_time max_over_time stddev_over_time stdvar_over_time) do
      assert {:ok, {:vector, [{_, {10, :nan}}]}} = run(~s'#{operation}({service="special"} | logfmt | unwrap x [1m])')
    end

    for operation <- ~w(sum avg min max stddev stdvar) do
      assert {:ok, {:vector, [{%{}, {10, :nan}}]}} =
               run(~s'#{operation}(quantile_over_time(0.5, {service="special"} | logfmt | unwrap x [1m]))')
    end
  end

  test "rank and sort put NaNs last, preserve matrix series and reject fractional ranks" do
    records =
      for value <- ["NaN", "1", "+Inf", "-Inf"],
          do: %Log{timestamp_ns: 10, service: "rank", body: "x=#{value}", resource: %{"pod" => value}}

    assert :ok = Storage.append(:logs, "acme", records)
    inner = ~s'sum_over_time({service="rank"} | logfmt | unwrap x [1m])'
    assert {:ok, {:vector, top}} = run("topk(3, #{inner})")
    assert Enum.map(top, fn {_, {_, value}} -> value end) == [:infinity, 1.0, :negative_infinity]
    assert {:ok, {:vector, sorted}} = run("sort(#{inner})")
    assert Enum.map(sorted, fn {_, {_, value}} -> value end) == [:negative_infinity, 1.0, :infinity, :nan]
    assert {:ok, {:matrix, matrix}} = run("sort(#{inner})", %{start_ts_ns: 10, end_ts_ns: 11, step_ns: 1})
    assert length(matrix) == 4
    assert {:ok, {:vector, []}} = run("topk(-1, #{inner})")
    assert {:error, :invalid_aggregation_parameter} = run("topk(1.5, #{inner})")
  end

  test "unwrap number grammar follows Go ParseFloat, not query integer literals" do
    for {text, expected} <- [{"1_000", 1000.0}, {"0x1_0p0", 16.0}] do
      assert :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: text, body: "x=#{text}"}])

      assert {:ok, {:vector, [{_, {10, ^expected}}]}} =
               run(~s'sum_over_time({service="#{text}"} | logfmt | unwrap x [1m])')
    end

    assert :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: "hex_integer", body: "x=0x10"}])
    assert {:error, :sample_extraction_error} = run(~s'sum_over_time({service="hex_integer"} | logfmt | unwrap x [1m])')
  end

  test "selectors share retained log sample and work quotas, not the PromQL sample quota" do
    config = Runtime.get_env(:pulso, Budget, [])
    Runtime.put_env(:pulso, Budget, Keyword.put(config, :max_samples, 1))
    assert {:ok, _} = run(~s'count_over_time({service="api"}[1m])')

    Runtime.put_env(:pulso, Budget, Keyword.put(config, :max_log_samples, 5))

    assert {:error, :query_sample_limit} =
             run(~s'count_over_time({service="api"}[1m]) + count_over_time({service="api"}[1m])')

    Runtime.put_env(:pulso, Budget, Keyword.put(config, :max_work, 8))

    assert {:error, :query_work_limit} =
             run(~s'count_over_time({service="api"}[1m])', %{start_ts_ns: 10, end_ts_ns: 12, step_ns: 1})
  end
end
