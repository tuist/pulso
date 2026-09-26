defmodule Pulso.Loki.PushTest do
  use ExUnit.Case, async: true

  alias Pulso.Loki.Push
  alias Pulso.Loki.PushProto
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

  describe "decode_proto/1" do
    test "returns {[], 0} for anything that is not a PushRequest" do
      assert Push.decode_proto(%{}) == {[], 0}
      assert Push.decode_proto(nil) == {[], 0}
    end

    test "decodes a well-formed stream with structured metadata" do
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: ~s({service_name="api", level="info"}),
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 1_700_000_000, nanos: 42},
                line: "hello",
                structured_metadata: [
                  %PushProto.LabelPair{name: "trace_id", value: "abc"},
                  %PushProto.LabelPair{name: "span_id", value: "def"},
                  %PushProto.LabelPair{name: "user_id", value: "u1"}
                ]
              }
            ]
          }
        ]
      }

      assert {[
                %Log{
                  timestamp_ns: 1_700_000_000_000_000_042,
                  observed_timestamp_ns: nil,
                  severity_text: "info",
                  service: "api",
                  body: "hello",
                  trace_id: "abc",
                  span_id: "def",
                  attributes: %{"user_id" => "u1"},
                  resource: %{"service_name" => "api", "level" => "info"}
                }
              ], 0} = Push.decode_proto(request)
    end

    test "combines seconds and nanos into a single nanosecond timestamp" do
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: ~s({service_name="api"}),
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 5, nanos: 123_456_789},
                line: "x",
                structured_metadata: []
              }
            ]
          }
        ]
      }

      assert {[%Log{timestamp_ns: 5_123_456_789}], 0} = Push.decode_proto(request)
    end

    test "counts a missing timestamp as one reject" do
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: ~s({service_name="api"}),
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 1, nanos: 0},
                line: "ok",
                structured_metadata: []
              },
              %PushProto.Entry{
                timestamp: nil,
                line: "no ts",
                structured_metadata: []
              }
            ]
          }
        ]
      }

      assert {[%Log{body: "ok"}], 1} = Push.decode_proto(request)
    end

    test "counts every entry as rejected when the labels string is malformed" do
      # Without labels we cannot attribute records to a service or
      # resource, so a whole-stream reject is honest. Counting entries
      # gives the sender the same tally the JSON path would.
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: "not a valid label string",
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 1, nanos: 0},
                line: "a",
                structured_metadata: []
              },
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 2, nanos: 0},
                line: "b",
                structured_metadata: []
              }
            ]
          }
        ]
      }

      assert {[], 2} = Push.decode_proto(request)
    end

    test "flattens multiple streams" do
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: ~s({service_name="a"}),
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 1, nanos: 0},
                line: "a1",
                structured_metadata: []
              },
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 2, nanos: 0},
                line: "a2",
                structured_metadata: []
              }
            ]
          },
          %PushProto.Stream{
            labels: ~s({service_name="b"}),
            entries: [
              %PushProto.Entry{
                timestamp: %PushProto.Timestamp{seconds: 3, nanos: 0},
                line: "b1",
                structured_metadata: []
              }
            ]
          }
        ]
      }

      assert {records, 0} = Push.decode_proto(request)
      assert Enum.map(records, & &1.service) == ["a", "a", "b"]
      assert Enum.map(records, & &1.body) == ["a1", "a2", "b1"]
    end

    test "an empty entries list produces no records and no rejects" do
      request = %PushProto.PushRequest{
        streams: [
          %PushProto.Stream{
            labels: ~s({service_name="api"}),
            entries: []
          }
        ]
      }

      assert {[], 0} = Push.decode_proto(request)
    end
  end
end
