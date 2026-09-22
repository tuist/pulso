defmodule Pulso.MCP.Tools do
  @moduledoc """
  Registry of read-only MCP tools Pulso exposes.

  Every tool in this module is read-only against `Pulso.Storage`. Write and
  remediation tools live in separate surfaces by design — see
  `docs/architecture.md`.
  """

  alias Pulso.Record.Log
  alias Pulso.Storage

  @tools [
    %{
      "name" => "query_logs",
      "description" => "Return log records for a tenant, optionally filtered by time range and service.",
      "inputSchema" => %{
        "type" => "object",
        "properties" => %{
          "tenant" => %{
            "type" => "string",
            "description" => "Tenant identifier (matches the X-Scope-OrgID used at ingest)."
          },
          "service" => %{
            "type" => "string",
            "description" => "Optional service.name filter."
          },
          "start_ts_ns" => %{
            "type" => "integer",
            "description" => "Inclusive lower bound on log timestamp, Unix nanoseconds."
          },
          "end_ts_ns" => %{
            "type" => "integer",
            "description" => "Inclusive upper bound on log timestamp, Unix nanoseconds."
          },
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000}
        },
        "required" => ["tenant"]
      }
    }
  ]

  @spec list() :: [map()]
  def list, do: @tools

  @spec call(String.t(), map()) :: {:ok, [map()]} | {:error, term()}
  def call("query_logs", %{"tenant" => tenant} = args) when is_binary(tenant) do
    opts =
      []
      |> put_opt(:start_ts, args["start_ts_ns"])
      |> put_opt(:end_ts, args["end_ts_ns"])
      |> put_opt(:limit, args["limit"])
      |> put_opt(:service, args["service"])

    with {:ok, records} <- Storage.query(tenant, opts) do
      {:ok, [%{"type" => "text", "text" => Jason.encode!(Enum.map(records, &encode_record/1))}]}
    end
  end

  def call("query_logs", _args), do: {:error, {:invalid_arguments, "tenant is required"}}
  def call(name, _args), do: {:error, {:unknown_tool, name}}

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp encode_record(%Log{} = record) do
    %{
      "timestamp_ns" => record.timestamp_ns,
      "observed_timestamp_ns" => record.observed_timestamp_ns,
      "severity_number" => record.severity_number,
      "severity_text" => record.severity_text,
      "service" => record.service,
      "body" => record.body,
      "trace_id" => record.trace_id,
      "span_id" => record.span_id,
      "attributes" => record.attributes,
      "resource" => record.resource
    }
  end
end
