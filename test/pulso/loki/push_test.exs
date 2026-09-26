defmodule Pulso.Loki.PushTest do
  use ExUnit.Case, async: true

  alias Pulso.Loki.Push
  alias Pulso.Record.Log

  test "returns {[], 0} for a payload without streams" do
    assert Push.decode(%{}) == {[], 0}
    assert Push.decode(%{"streams" => "not-a-list"}) == {[], 0}
  end

  test "decodes a stream with one value tuple" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api", "level" => "info"},
          "values" => [["1700000000000000000", "hello"]]
        }
      ]
    }

    assert {[
              %Log{
                timestamp_ns: 1_700_000_000_000_000_000,
                observed_timestamp_ns: nil,
                severity_text: "info",
                service: "api",
                body: "hello",
                trace_id: nil,
                span_id: nil,
                attributes: %{},
                resource: %{"service_name" => "api", "level" => "info"}
              }
            ], 0} = Push.decode(payload)
  end

  test "falls back to `service` label when `service_name` is absent" do
    # Older Alloy configs and hand-rolled clients emit `service` rather
    # than the Grafana 3.0 `service_name` convention. Storing the value
    # on `Log.service` under either name keeps queries uniform.
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service" => "legacy"},
          "values" => [["1", "line"]]
        }
      ]
    }

    assert {[%Log{service: "legacy"}], 0} = Push.decode(payload)
  end

  test "prefers service_name over the fallback service label" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "new", "service" => "old"},
          "values" => [["1", "line"]]
        }
      ]
    }

    assert {[%Log{service: "new"}], 0} = Push.decode(payload)
  end

  test "falls back to detected_level when level is absent" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"detected_level" => "warn"},
          "values" => [["1", "line"]]
        }
      ]
    }

    assert {[%Log{severity_text: "warn"}], 0} = Push.decode(payload)
  end

  test "keeps the raw label map in resource, including lifted service and level" do
    # Lifting into dedicated Log fields is a query convenience — the
    # source labels stay in `resource` so a caller looking at the stored
    # record can see exactly what the sender emitted.
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api", "level" => "info", "k8s.namespace" => "prod"},
          "values" => [["1", "line"]]
        }
      ]
    }

    assert {[%Log{resource: resource}], 0} = Push.decode(payload)
    assert resource["service_name"] == "api"
    assert resource["level"] == "info"
    assert resource["k8s.namespace"] == "prod"
  end

  test "lifts trace_id and span_id out of structured metadata" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api"},
          "values" => [
            [
              "1",
              "line",
              %{"trace_id" => "abc", "span_id" => "def", "user_id" => "u1"}
            ]
          ]
        }
      ]
    }

    assert {[
              %Log{
                trace_id: "abc",
                span_id: "def",
                attributes: %{"user_id" => "u1"}
              }
            ], 0} = Push.decode(payload)
  end

  test "counts an entry with a missing timestamp as rejected" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api"},
          "values" => [
            ["1", "ok"],
            [nil, "no ts"]
          ]
        }
      ]
    }

    assert {[%Log{body: "ok"}], 1} = Push.decode(payload)
  end

  test "accepts numeric timestamps too" do
    # The Loki wire spec calls for strings; a handful of clients emit
    # integers. Accepting both avoids a class of "why is nothing landing"
    # bugs while keeping the string form as the canonical.
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api"},
          "values" => [[1_700_000_000_000_000_000, "hi"]]
        }
      ]
    }

    assert {[%Log{timestamp_ns: 1_700_000_000_000_000_000}], 0} = Push.decode(payload)
  end

  test "counts a values entry with a non-string line as rejected" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api"},
          "values" => [
            ["1", "ok"],
            ["2", 42],
            ["3", %{"json" => "line"}]
          ]
        }
      ]
    }

    assert {[%Log{body: "ok"}], 2} = Push.decode(payload)
  end

  test "counts a whole stream as rejected when `stream` is not a map" do
    # A malformed stream still names its values; counting them as
    # rejects gives the sender an honest tally rather than pretending
    # nothing arrived.
    payload = %{
      "streams" => [
        %{
          "stream" => "not-a-map",
          "values" => [["1", "a"], ["2", "b"]]
        }
      ]
    }

    assert {[], 2} = Push.decode(payload)
  end

  test "flattens multiple streams" do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "a"},
          "values" => [["1", "a1"], ["2", "a2"]]
        },
        %{
          "stream" => %{"service_name" => "b"},
          "values" => [["3", "b1"]]
        }
      ]
    }

    assert {records, 0} = Push.decode(payload)
    assert length(records) == 3
    assert Enum.map(records, & &1.service) == ["a", "a", "b"]
    assert Enum.map(records, & &1.body) == ["a1", "a2", "b1"]
    assert Enum.map(records, & &1.timestamp_ns) == [1, 2, 3]
  end

  test "empty values list produces no records and no rejects" do
    payload = %{
      "streams" => [
        %{"stream" => %{"service_name" => "api"}, "values" => []}
      ]
    }

    assert {[], 0} = Push.decode(payload)
  end
end
