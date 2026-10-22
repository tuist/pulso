defmodule Pulso.Traces.WorkloadContractTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.Parser
  alias Pulso.Traces.Selector
  alias Pulso.Traces.TempoSearch

  @workflows Path.expand("../../fixtures/traces/query-workflows.json", __DIR__)

  test "bounded original-string workflow parses without losing owner or regex predicates" do
    workflow = Pulso.JSON.decode!(File.read!(@workflows))
    assert {:ok, _} = Selector.parse(workflow["accepted"])
    for query <- workflow["logs"], do: assert({:ok, _} = Parser.parse(query))
  end

  test "observed negation, intrinsic OR and span-duration workflows parse without widening discovery or window search" do
    workflow = Pulso.JSON.decode!(File.read!(@workflows))

    for query <- workflow["expressions"] do
      assert {:ok, {:expression, _}} = TempoSearch.parse(query)
      assert {:error, :unsupported_trace_selector} = Selector.parse(query)
    end
  end

  test "remaining unsupported operators fail rather than dropping filters" do
    workflow = Pulso.JSON.decode!(File.read!(@workflows))
    Pulso.Runtime.put_env(:pulso, TempoSearch, enabled: true)

    for query <- workflow["unsupported"] do
      assert {:error, :unsupported_trace_selector} = Selector.parse(query)
      assert {:error, :unsupported_trace_selector} = TempoSearch.query("fixture", query, [])
    end
  end
end
