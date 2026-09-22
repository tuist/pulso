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
end
