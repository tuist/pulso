defmodule Pulso.LogQL.NativeRangeParityTest do
  # The native range scan must answer exactly like the Elixir pipeline.
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.MetricEval
  alias Pulso.LogQL.Parser
  alias Pulso.PromQL.Evaluator
  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Test.MetricEvalReference
  alias Pulso.Test.NativeQueryStorage

  @second 1_000_000_000

  setup do
    Pulso.Runtime.put_env(:pulso, Storage, adapter: NativeQueryStorage)
    tenant = "native-range-#{System.unique_integer([:positive])}"

    records =
      for i <- 1..400 do
        ts = Enum.random(0..120) * @second
        dur = Enum.random(["#{Enum.random(1..900)}", "#{Enum.random(1..900)}.5", "oops", nil])
        quoted = Enum.random(["", ~s( msg="hello \\"x\\" world")])

        body =
          "level=info " <>
            if(dur, do: "dur=#{dur} ", else: "") <> "code=#{Enum.random([200, 404, 503])}#{quoted} n=#{i}"

        %Log{
          timestamp_ns: ts,
          service: Enum.random(["api", "web", nil]),
          severity_text: Enum.random(["info", "error", nil]),
          body: body,
          attributes: Enum.random([%{}, %{"trace" => "t#{rem(i, 5)}"}]),
          resource: %{"app" => Enum.random(["a", "b"]), "pod" => Enum.random(["p1", "p2", "p3"])}
        }
      end

    :ok = Storage.append(:logs, tenant, records)
    {:ok, tenant: tenant}
  end

  defp query do
    unwrap? = :rand.uniform(2) == 1

    op =
      Enum.random(
        ~w(count_over_time rate bytes_over_time bytes_rate) ++
          if(unwrap?,
            do: ~w(sum_over_time avg_over_time min_over_time max_over_time stddev_over_time quantile_over_time rate),
            else: []
          )
      )

    stages =
      Enum.random([
        ~s/| logfmt/,
        ~s/|= "level" | logfmt | code=~"5.." or dur > 300/,
        ~s/| regexp "dur=(?P<d>[0-9.]+)(?P<rest> .*)?" | d != ""/,
        ~s/!~ "n=1[0-9]" | logfmt | code != "404" and pod != "p3"/,
        ~s/| logfmt | msg =~ "hello.*" or level="info"/
      ])

    unwrap = if unwrap?, do: Enum.random([" | unwrap dur", " | logfmt | unwrap dur"]), else: ""
    selector = Enum.random([~s/{app=~"a|b"}/, ~s/{app="a", service_name="api"}/, ~s/{pod!="p2"}/])
    param = if op == "quantile_over_time", do: "0.75, ", else: ""
    grouping = Enum.random(["", " by (pod)", " without (n, msg, trace)"])
    inner = "#{op}(#{param}#{selector} #{stages}#{unwrap} [#{Enum.random(~w(10s 30s 1m))}])#{grouping}"
    if :rand.uniform(2) == 1, do: "sum by (app) (#{inner})", else: inner
  end

  defp evaluate(module, ast, tenant, opts) do
    Process.delete(:promql_budget)
    Process.delete(:promql_deadline_ms)
    module.evaluate(ast, tenant, opts)
  end

  defp canonical({:ok, {kind, series}}),
    do: {:ok, kind, series |> Enum.map(fn {labels, samples} -> {labels, approx(samples)} end) |> Enum.sort()}

  defp canonical(other), do: other
  defp approx(samples) when is_list(samples), do: Enum.map(samples, fn {ts, v} -> {ts, approx(v)} end)
  defp approx({ts, v}), do: {ts, approx(v)}
  defp approx(v) when is_float(v), do: Float.round(v, 9)
  defp approx(v), do: v

  test "random native range scans match the materializing evaluator", %{tenant: tenant} do
    for _ <- 1..120 do
      text = query()
      {:ok, ast} = Parser.parse(text)

      opts =
        Enum.random([
          %{start_ts_ns: 10 * @second, end_ts_ns: 120 * @second, step_ns: Enum.random([5, 15]) * @second},
          %{end_ts_ns: Enum.random(30..130) * @second}
        ])

      expected = evaluate(MetricEvalReference, ast, tenant, opts)
      actual = evaluate(MetricEval, ast, tenant, opts)
      assert canonical(actual) == canonical(expected), "#{text} #{inspect(opts)}"
    end

    native = Stream.repeatedly(fn -> receive do: (:native_log_metric_scan -> 1), after: (0 -> nil) end)
    native_scans = native |> Enum.take_while(& &1) |> length()
    # Most queries must take the native path rather than fall back.
    assert native_scans >= 80
  end

  test "native scans stop at the retained log sample budget", %{tenant: tenant} do
    config = Pulso.Runtime.get_env(:pulso, Evaluator, [])
    Pulso.Runtime.put_env(:pulso, Evaluator, Keyword.put(config, :max_log_samples, 10))
    {:ok, ast} = Parser.parse(~s/count_over_time({app=~"a|b"} [2m])/)

    assert evaluate(MetricEval, ast, tenant, %{end_ts_ns: 130 * @second}) == {:error, :query_sample_limit}
  end

  test "rows the native scan cannot reproduce fall back to the Elixir pipeline" do
    tenant = "native-fallback-#{System.unique_integer([:positive])}"

    :ok =
      Storage.append(:logs, tenant, [
        %Log{timestamp_ns: 5 * @second, body: "dur=10", resource: %{"app" => "a", "replicas" => 3}},
        %Log{timestamp_ns: 6 * @second, body: "dur=0x10", resource: %{"app" => "a"}}
      ])

    for text <- [~s/count_over_time({app="a"} | logfmt [1m])/, ~s/sum_over_time({app="a"} | logfmt | unwrap dur [1m])/] do
      {:ok, ast} = Parser.parse(text)
      opts = %{end_ts_ns: 30 * @second}

      assert canonical(evaluate(MetricEval, ast, tenant, opts)) ==
               canonical(evaluate(MetricEvalReference, ast, tenant, opts))
    end

    refute_received :native_log_metric_scan
  end
end
