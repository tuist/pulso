defmodule Pulso.Storage.SortOrderTest do
  # SortOrder is the shared tie-breaker for every storage adapter. A client
  # that swaps adapters must never see the order change when timestamps are
  # equal, so this coverage runs against the pure sort — no adapter needed.

  use ExUnit.Case, async: true

  alias Pulso.Record.Log
  alias Pulso.Storage.SortOrder

  defp log(ts, opts \\ []) do
    %Log{
      timestamp_ns: ts,
      observed_timestamp_ns: Keyword.get(opts, :observed),
      trace_id: Keyword.get(opts, :trace_id),
      span_id: Keyword.get(opts, :span_id),
      body: Keyword.get(opts, :body)
    }
  end

  test "primary sort is timestamp_ns descending" do
    result = SortOrder.sort([log(10), log(30), log(20)])
    assert Enum.map(result, & &1.timestamp_ns) == [30, 20, 10]
  end

  test "equal timestamp_ns falls through to observed_timestamp_ns descending" do
    result =
      SortOrder.sort([
        log(10, observed: 100),
        log(10, observed: 300),
        log(10, observed: 200)
      ])

    assert Enum.map(result, & &1.observed_timestamp_ns) == [300, 200, 100]
  end

  test "trace_id and body break further ties deterministically" do
    a = log(10, observed: 5, trace_id: "aaa", body: "one")
    b = log(10, observed: 5, trace_id: "aaa", body: "two")
    c = log(10, observed: 5, trace_id: "bbb", body: "one")

    # The precise order matters less than the fact that repeated sorts of
    # the same input produce the same output.
    assert SortOrder.sort([a, b, c]) == SortOrder.sort([c, b, a])
    assert SortOrder.sort([a, b, c]) == SortOrder.sort([b, a, c])
  end

  test "nil fields sort last within their position" do
    with_trace = log(10, observed: 5, trace_id: "aaa")
    without_trace = log(10, observed: 5, trace_id: nil)

    result = SortOrder.sort([without_trace, with_trace])
    assert Enum.map(result, & &1.trace_id) == ["aaa", nil]
  end
end
