defmodule Pulso.Storage.MemoryTest do
  use ExUnit.Case, async: false

  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  defp record(ts, opts \\ []) do
    %Log{
      timestamp_ns: ts,
      severity_text: Keyword.get(opts, :severity_text),
      service: Keyword.get(opts, :service),
      body: Keyword.get(opts, :body)
    }
  end

  test "append then query returns the records for the tenant" do
    :ok = Storage.append("t1", [record(10), record(20)])
    assert {:ok, records} = Storage.query("t1")
    assert Enum.map(records, & &1.timestamp_ns) == [20, 10]
  end

  test "records for one tenant are invisible to another" do
    :ok = Storage.append("t1", [record(1)])
    :ok = Storage.append("t2", [record(2)])

    assert {:ok, [%Log{timestamp_ns: 1}]} = Storage.query("t1")
    assert {:ok, [%Log{timestamp_ns: 2}]} = Storage.query("t2")
  end

  test "filters by time range and service" do
    :ok =
      Storage.append("t", [
        record(10, service: "api"),
        record(20, service: "web"),
        record(30, service: "api"),
        record(40, service: "api")
      ])

    assert {:ok, records} = Storage.query("t", start_ts: 15, end_ts: 35, service: "api")
    assert Enum.map(records, & &1.timestamp_ns) == [30]
  end

  test "applies limit" do
    :ok = Storage.append("t", [record(1), record(2), record(3), record(4)])
    assert {:ok, records} = Storage.query("t", limit: 2)
    assert length(records) == 2
    assert Enum.map(records, & &1.timestamp_ns) == [4, 3]
  end

  test "populates observed_timestamp_ns when the record does not carry one" do
    before_append = System.system_time(:nanosecond)
    :ok = Storage.append("t", [record(1)])
    after_append = System.system_time(:nanosecond)

    assert {:ok, [%Log{observed_timestamp_ns: observed}]} = Storage.query("t")
    assert observed >= before_append and observed <= after_append
  end

  test "backfills a nil timestamp_ns from the observed timestamp" do
    # OTLP allows `time_unix_nano` to be absent. Storage picks up the
    # observed timestamp when the caller supplied one.
    :ok = Storage.append("t", [%Log{timestamp_ns: nil, observed_timestamp_ns: 42}])
    assert {:ok, [%Log{timestamp_ns: 42, observed_timestamp_ns: 42}]} = Storage.query("t")
  end

  test "backfills a nil timestamp_ns from the wall clock when observed is absent too" do
    before_append = System.system_time(:nanosecond)
    :ok = Storage.append("t", [%Log{timestamp_ns: nil, observed_timestamp_ns: nil}])
    after_append = System.system_time(:nanosecond)

    assert {:ok, [%Log{timestamp_ns: ts}]} = Storage.query("t")
    assert ts >= before_append and ts <= after_append
  end

  test "equal timestamps are broken by observed_timestamp_ns then trace_id" do
    # Guard against a limit response depending on adapter-internal insertion
    # order. Two adapters must sort ties the same way; both delegate to
    # Pulso.Storage.SortOrder.
    a = %Log{timestamp_ns: 10, observed_timestamp_ns: 100, trace_id: "aaa"}
    b = %Log{timestamp_ns: 10, observed_timestamp_ns: 300, trace_id: "aaa"}
    c = %Log{timestamp_ns: 10, observed_timestamp_ns: 200, trace_id: "bbb"}

    :ok = Storage.append("t", [a, b, c])
    assert {:ok, sorted} = Storage.query("t")
    assert Enum.map(sorted, & &1.observed_timestamp_ns) == [300, 200, 100]
  end
end
