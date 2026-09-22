defmodule Pulso.OTLP.LogsTest do
  use ExUnit.Case, async: true

  alias Pulso.OTLP.Logs
  alias Pulso.Record.Log

  test "returns [] for a payload without resourceLogs" do
    assert Logs.decode(%{}) == []
    assert Logs.decode(%{"resourceLogs" => "not-a-list"}) == []
  end

  test "decodes a full record with resource, attributes, and body" do
    payload = %{
      "resourceLogs" => [
        %{
          "resource" => %{
            "attributes" => [
              %{"key" => "service.name", "value" => %{"stringValue" => "api"}},
              %{"key" => "deploy.env", "value" => %{"stringValue" => "prod"}}
            ]
          },
          "scopeLogs" => [
            %{
              "scope" => %{"name" => "my.lib"},
              "logRecords" => [
                %{
                  "timeUnixNano" => "1700000000000000000",
                  "observedTimeUnixNano" => "1700000000000000001",
                  "severityNumber" => 9,
                  "severityText" => "INFO",
                  "body" => %{"stringValue" => "hello"},
                  "attributes" => [
                    %{"key" => "user.id", "value" => %{"stringValue" => "u1"}}
                  ],
                  "traceId" => "abc",
                  "spanId" => "def"
                }
              ]
            }
          ]
        }
      ]
    }

    assert [
             %Log{
               timestamp_ns: 1_700_000_000_000_000_000,
               observed_timestamp_ns: 1_700_000_000_000_000_001,
               severity_number: 9,
               severity_text: "INFO",
               service: "api",
               body: "hello",
               trace_id: "abc",
               span_id: "def",
               attributes: %{"user.id" => "u1"},
               resource: %{"service.name" => "api", "deploy.env" => "prod"}
             }
           ] = Logs.decode(payload)
  end

  test "skips records without a timestamp" do
    payload = %{
      "resourceLogs" => [
        %{
          "scopeLogs" => [
            %{"logRecords" => [%{"body" => %{"stringValue" => "no ts"}}]}
          ]
        }
      ]
    }

    assert Logs.decode(payload) == []
  end

  test "decodes AnyValue variants in attributes" do
    payload = %{
      "resourceLogs" => [
        %{
          "scopeLogs" => [
            %{
              "logRecords" => [
                %{
                  "timeUnixNano" => "1",
                  "attributes" => [
                    %{"key" => "s", "value" => %{"stringValue" => "x"}},
                    %{"key" => "i", "value" => %{"intValue" => "42"}},
                    %{"key" => "b", "value" => %{"boolValue" => true}},
                    %{"key" => "d", "value" => %{"doubleValue" => 3.14}},
                    %{
                      "key" => "arr",
                      "value" => %{
                        "arrayValue" => %{
                          "values" => [%{"stringValue" => "a"}, %{"intValue" => "1"}]
                        }
                      }
                    },
                    %{
                      "key" => "kv",
                      "value" => %{
                        "kvlistValue" => %{
                          "values" => [%{"key" => "n", "value" => %{"intValue" => "7"}}]
                        }
                      }
                    }
                  ]
                }
              ]
            }
          ]
        }
      ]
    }

    assert [%Log{attributes: attrs}] = Logs.decode(payload)
    assert attrs["s"] == "x"
    assert attrs["i"] == 42
    assert attrs["b"] == true
    assert attrs["d"] == 3.14
    assert attrs["arr"] == ["a", 1]
    assert attrs["kv"] == %{"n" => 7}
  end

  test "flattens multiple resourceLogs and scopeLogs entries" do
    payload = %{
      "resourceLogs" => [
        %{
          "resource" => %{
            "attributes" => [%{"key" => "service.name", "value" => %{"stringValue" => "a"}}]
          },
          "scopeLogs" => [
            %{"logRecords" => [%{"timeUnixNano" => "1"}, %{"timeUnixNano" => "2"}]},
            %{"logRecords" => [%{"timeUnixNano" => "3"}]}
          ]
        },
        %{
          "resource" => %{
            "attributes" => [%{"key" => "service.name", "value" => %{"stringValue" => "b"}}]
          },
          "scopeLogs" => [%{"logRecords" => [%{"timeUnixNano" => "4"}]}]
        }
      ]
    }

    records = Logs.decode(payload)
    assert length(records) == 4
    assert Enum.map(records, & &1.service) == ["a", "a", "a", "b"]
    assert Enum.map(records, & &1.timestamp_ns) == [1, 2, 3, 4]
  end
end
