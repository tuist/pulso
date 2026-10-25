defmodule PulsoWeb.PrometheusMathConformanceTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Record.MetricSample
  alias Pulso.Record.NativeHistogram
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Test.CompactionStore

  @directory Path.expand("../../fixtures/promql/mimir", __DIR__)

  setup %{adapter: adapter} do
    Runtime.put_env(:pulso, Storage, adapter: adapter)

    if adapter == S3 do
      store =
        start_supervised!(
          {Agent,
           fn ->
             %{
               objects: %{},
               reads: [],
               version: 0,
               hook: nil,
               faults: %{},
               barriers: %{},
               lists: 0,
               deletes: [],
               requests: []
             }
           end}
        )

      server = start_supervised!({Bandit, plug: {CompactionStore, agent: store}, port: 0})
      {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

      Runtime.put_env(:pulso, S3, %{
        bucket: "pulso",
        endpoint: "http://localhost:#{port}",
        region: "us-east-1",
        access_key_id: "test-#{System.unique_integer([:positive])}",
        secret_access_key: "test",
        allow_http: true,
        refresh_stale_ms: 0
      })

      start_supervised!(ManifestSupervision)
    end

    :ok
  end

  for adapter <- [Memory, S3],
      {file, count} <- [{"upstream-math.test", 21}, {"mimir-math.test", 7}, {"collision.test", 2}] do
    @tag adapter: adapter
    test "Mimir #{file} through HTTP and #{inspect(adapter)}" do
      blocks =
        @directory
        |> Path.join(unquote(file))
        |> File.read!()
        |> String.replace(~r/^#.*\n/m, "")
        |> String.trim()
        |> then(&Regex.split(~r/(?=^(?:load|clear|eval(?:_fail)?)\b)/m, &1, trim: true))

      {_, count} =
        Enum.reduce(blocks, {"fixture-0", 0}, fn block, {tenant, count} ->
          [command | body] = block |> String.split("\n", trim: true) |> Enum.map(&String.trim/1)

          case String.split(command, " ", parts: 2) do
            ["clear"] ->
              {"fixture-#{count}", count}

            ["load", interval] ->
              records = Enum.flat_map(body, &records(&1, seconds(interval)))
              assert :ok = Storage.append(:metrics, tenant, records)
              {tenant, count}

            [eval, _] when eval in ["eval", "eval_fail"] ->
              check(command, body, tenant)
              {tenant, count + 1}
          end
        end)

      assert count == unquote(count)
    end
  end

  # Only the syntax present in the selected upstream cases is supported.
  # Unknown commands, values and series forms fail rather than being skipped.
  defp series(line) do
    [_, metric, values] = Regex.run(~r/^(\{[^}]*\}|[\w:]+(?:\{[^}]*\})?)\s+(.+)$/, line)
    [_, name, attributes] = Regex.run(~r/^([\w:]*)(.*)$/, metric)

    labels =
      case attributes do
        "" ->
          %{}

        "{}" ->
          %{}

        _ ->
          [_, key, quoted] = Regex.run(~r/^\{(\w+)=("(?:[^"\\]|\\.)*")\}$/, attributes)
          %{key => JSON.decode!(quoted)}
      end

    labels = if name == "", do: labels, else: Map.put(labels, "__name__", name)
    {labels, Regex.scan(~r/\{\{[^}]*\}\}(?:x\d+)?|\S+/, values) |> Enum.flat_map(fn [token] -> expand(token) end)}
  end

  defp expand(token) do
    case Regex.run(~r/^(.+?)\+([\d.]+)x(\d+)$/, token) do
      [_, first, increment, count] ->
        for index <- 0..String.to_integer(count), do: value(first) + index * value(increment)

      nil ->
        case Regex.run(~r/^(.+)x(\d+)$/, token) do
          [_, sample, count] -> List.duplicate(value(sample), String.to_integer(count) + 1)
          nil -> [value(token)]
        end
    end
  end

  defp value("_"), do: :absent
  defp value(nan) when nan in ["NaN", "-NaN"], do: :nan
  defp value(infinity) when infinity in ["Inf", "+Inf"], do: :infinity
  defp value("-Inf"), do: :negative_infinity

  defp value("{{" <> rest) do
    fields = rest |> String.trim_trailing("}}") |> String.split() |> Map.new(&List.to_tuple(String.split(&1, ":")))
    assert Map.keys(fields) -- ["schema", "sum", "count"] == []

    %NativeHistogram{
      schema: String.to_integer(Map.get(fields, "schema", "0")),
      sum: value(Map.fetch!(fields, "sum")),
      count: value(Map.fetch!(fields, "count"))
    }
  end

  defp value(token) do
    {number, ""} = Float.parse(token)
    number
  end

  defp records(line, interval) do
    {labels, values} = series(line)

    for {value, index} <- Enum.with_index(values), value != :absent do
      sample = %MetricSample{labels: labels, timestamp_ns: index * interval * 1_000_000_000}
      if is_struct(value, NativeHistogram), do: %{sample | histogram: value}, else: %{sample | value: value}
    end
  end

  defp seconds("0"), do: 0

  defp seconds(duration) do
    [_, count, unit] = Regex.run(~r/^(\d+)(s|m)$/, duration)
    String.to_integer(count) * if(unit == "m", do: 60, else: 1)
  end

  defp check(command, expected, tenant) do
    [_, mode, query] = Regex.run(~r/^eval(?:_fail)? (instant at \S+|range from \S+ to \S+ step \S+) (.+)$/, command)

    case String.split(mode) do
      ["instant", "at", time] ->
        time = seconds(time)
        request("/api/v1/query", %{query: query, time: "#{time}"}, expected, tenant, command, time, 1)

        # The upstream runner also checks instant cases as range queries.
        request(
          "/api/v1/query_range",
          %{query: query, start: "#{time}", end: "#{time}", step: "1s"},
          expected,
          tenant,
          command,
          time,
          1
        )

      ["range", "from", first, "to", last, "step", step] ->
        params = %{query: query, start: "#{seconds(first)}", end: "#{seconds(last)}", step: step}
        request("/api/v1/query_range", params, expected, tenant, command, seconds(first), seconds(step))
    end
  end

  defp request(path, params, expected, tenant, command, first, step) do
    conn = build_conn() |> put_req_header("x-scope-orgid", tenant) |> get(path, params)

    if String.starts_with?(command, "eval_fail") do
      assert expected == ["expected_fail_message vector cannot contain metrics with the same labelset"]

      # Pulso's HTTP diagnostic differs; assert its corresponding error exactly,
      # not just any 422 (which could conceal budgets or unrelated failures).
      assert %{
               "status" => "error",
               "errorType" => "execution",
               "error" => "Vector matching produced ambiguous or duplicate series."
             } == json_response(conn, 422)
    else
      assert %{"status" => "success", "data" => data} = json_response(conn, 200)
      assert data["resultType"] == if(path == "/api/v1/query", do: "vector", else: "matrix")
      assert_samples(data["result"], expected, first, step, command)
    end
  end

  defp assert_samples(actual, expected, first, step, command) do
    expected =
      Map.new(expected, fn line ->
        {labels, values} = series(line)
        points = for {v, i} <- Enum.with_index(values), v != :absent, do: {first + i * step, v}
        {labels, points}
      end)

    assert length(actual) == map_size(expected), command
    assert MapSet.new(actual, & &1["metric"]) == MapSet.new(Map.keys(expected)), command

    for sample <- actual do
      points = Map.fetch!(expected, sample["metric"])
      values = sample["values"] || [sample["value"]]
      assert length(values) == length(points), command

      for {[timestamp, actual], {expected_timestamp, expected}} <- Enum.zip(values, points) do
        assert timestamp == expected_timestamp, command
        assert_number(value(actual), expected, command)
      end
    end
  end

  defp assert_number(actual, expected, command) when is_number(actual) and is_number(expected) do
    # Upstream uses almost.Equal with relative epsilon 1e-6, treating signed
    # zeros as equal. The separate IEEE corpus checks zero signs exactly.
    assert actual == expected or abs(actual - expected) < 1.0e-6 * (abs(actual) + abs(expected)), command
  end

  defp assert_number(actual, expected, command), do: assert(actual == expected, command)
end
