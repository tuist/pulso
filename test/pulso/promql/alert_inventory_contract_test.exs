defmodule Pulso.PromQL.AlertInventoryContractTest do
  use Pulso.Test.Case, async: true

  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.Parser

  test "all complete inventoried Prometheus source expressions parse and evaluate with empty inputs" do
    inventory =
      "../../../plans/alerting/grafana-rule-inventory.json"
      |> Path.expand(__DIR__)
      |> File.read!()
      |> JSON.decode!()

    queries =
      for rule <- inventory["rules"],
          node <- rule["data"],
          node["source_type"] == "prometheus",
          do: {rule["uid"], node["model"]["expr"]}

    assert length(Enum.uniq_by(queries, &elem(&1, 1))) == 104

    failures =
      Enum.flat_map(queries, fn {uid, query} ->
        # One paused Grafana rule contains a client macro, not PromQL.
        query = String.replace(query, "$__rate_interval", "5m")

        case Parser.parse(query) do
          {:ok, _} ->
            case Evaluator.query(query, "empty", %{end_ts_ns: 1_700_000_000_000_000_000}) do
              {:ok, _} -> []
              error -> [{uid, query, error}]
            end

          error ->
            [{uid, query, error}]
        end
      end)

    assert failures == []
  end
end
