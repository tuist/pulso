defmodule Pulso.MCP.Tools do
  @moduledoc """
  Registry of read-only MCP tools Pulso exposes.

  Every tool in this module is read-only against `Pulso.Storage`. Write and
  remediation tools live in separate surfaces by design — see
  `docs/architecture.md`.
  """

  alias Pulso.Auth
  alias Pulso.Codec.NIF
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
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
    },
    %{
      "name" => "query_metrics",
      "description" =>
        "Return metric samples for a tenant, optionally filtered by time range and PromQL-style label matchers.",
      "inputSchema" => %{
        "type" => "object",
        "properties" => %{
          "tenant" => %{
            "type" => "string",
            "description" => "Tenant identifier (matches the X-Scope-OrgID used at ingest)."
          },
          "matchers" => %{
            "type" => "array",
            "description" =>
              "PromQL-style label matchers. Each element is `{name, op, value}` with op ∈ {=, !=, =~, !~}.",
            "items" => %{
              "type" => "object",
              "properties" => %{
                "name" => %{"type" => "string"},
                "op" => %{"type" => "string", "enum" => ["=", "!=", "=~", "!~"]},
                "value" => %{"type" => "string"}
              },
              "required" => ["name", "op", "value"]
            }
          },
          "start_ts_ns" => %{
            "type" => "integer",
            "description" => "Inclusive lower bound on sample timestamp, Unix nanoseconds."
          },
          "end_ts_ns" => %{
            "type" => "integer",
            "description" => "Inclusive upper bound on sample timestamp, Unix nanoseconds."
          },
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000}
        },
        "required" => ["tenant"]
      }
    }
  ]

  @spec list() :: [map()]
  def list, do: @tools

  @spec call(String.t(), map(), Pulso.MCP.context()) :: {:ok, [map()]} | {:error, term()}
  def call(name, args, context \\ %{})

  def call("query_logs", %{"tenant" => tenant} = args, context) when is_binary(tenant) do
    opts =
      []
      |> put_opt(:start_ts, args["start_ts_ns"])
      |> put_opt(:end_ts, args["end_ts_ns"])
      |> put_opt(:limit, args["limit"])
      |> put_opt(:service, args["service"])

    with :ok <- verify(context, tenant),
         {:ok, records} <- Storage.query(:logs, tenant, opts) do
      {:ok, [%{"type" => "text", "text" => encode_records(records)}]}
    end
  end

  def call("query_logs", _args, _context), do: {:error, {:invalid_arguments, "tenant is required"}}

  def call("query_metrics", %{"tenant" => tenant} = args, context) when is_binary(tenant) do
    with {:ok, matcher_tuples} <- parse_matchers(args["matchers"]) do
      opts =
        []
        |> put_opt(:start_ts, args["start_ts_ns"])
        |> put_opt(:end_ts, args["end_ts_ns"])
        |> put_opt(:limit, args["limit"])
        |> put_opt(:matchers, matcher_tuples)

      with :ok <- verify(context, tenant),
           {:ok, samples} <- Storage.query(:metrics, tenant, opts) do
        {:ok, [%{"type" => "text", "text" => encode_samples(samples)}]}
      end
    end
  end

  def call("query_metrics", _args, _context), do: {:error, {:invalid_arguments, "tenant is required"}}

  def call(name, _args, _context), do: {:error, {:unknown_tool, name}}

  # Translate the public PromQL-style operators (`=`, `!=`, `=~`, `!~`)
  # into the atom shape the storage layer and the Rust decoder expect.
  defp parse_matchers(nil), do: {:ok, nil}

  defp parse_matchers(list) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn
      %{"name" => n, "op" => op, "value" => v}, {:ok, acc}
      when is_binary(n) and is_binary(v) ->
        case translate_op(op) do
          {:ok, atom_op} -> {:cont, {:ok, [{n, atom_op, v} | acc]}}
          err -> {:halt, err}
        end

      _, _ ->
        {:halt, {:error, {:invalid_arguments, "matchers must be [{name, op, value}]"}}}
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  defp parse_matchers(_), do: {:error, {:invalid_arguments, "matchers must be a list"}}

  defp translate_op("="), do: {:ok, :eq}
  defp translate_op("!="), do: {:ok, :neq}
  defp translate_op("=~"), do: {:ok, :re}
  defp translate_op("!~"), do: {:ok, :nre}
  defp translate_op(op), do: {:error, {:invalid_arguments, "unknown matcher op: #{inspect(op)}"}}

  # Every tool that names a tenant runs it through `Pulso.Auth.verify/2`.
  # MCP is the same JSON-RPC transport for read and write; without this hop
  # the ingest boundary's auth check would be bypassable via the read path.
  #
  # A missing conn falls through to a fresh `%Plug.Conn{}`. When the active
  # auth module is `Pulso.Auth.Open` (dev/test default) that still returns
  # :ok. When it is `Pulso.Auth.SharedSecret` (prod) it fails, closed —
  # there is no in-process caller that legitimately reaches this path
  # without a conn under real auth.
  defp verify(context, tenant) do
    conn = Map.get(context, :conn) || %Plug.Conn{}

    case Auth.verify(conn, tenant) do
      :ok -> :ok
      {:error, reason} -> {:error, {:unauthorized, reason}}
    end
  end

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  # Rust encodes the record list directly (no intermediate maps); it defers
  # to the Elixir encoding below whenever it cannot produce the same JSON.
  defp encode_records(records) do
    case NIF.encode_log_segment(records, :plain, :array) do
      {:ok, json, _min_ts, _max_ts, _count} -> json
      :fallback -> JSON.encode!(Enum.map(records, &encode_record/1))
    end
  end

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

  # Metrics have no Rust fast-path on the response-encoding boundary
  # yet; keeping this in Elixir until there is one keeps the hot path
  # tight for logs and lets the first metric response ship with the
  # obvious shape.
  defp encode_samples(samples) do
    JSON.encode!(Enum.map(samples, &encode_sample/1))
  end

  defp encode_sample(%MetricSample{} = s) do
    %{
      "series_id" => s.series_id,
      "timestamp_ns" => s.timestamp_ns,
      "value" => s.value,
      "labels" => s.labels
    }
  end
end
