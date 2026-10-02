defmodule Pulso.MCP.Tools do
  @moduledoc """
  Registry of read-only MCP tools Pulso exposes.

  Every tool in this module is read-only against `Pulso.Storage`. Write and
  remediation tools live in separate surfaces by design — see
  `docs/architecture.md`.

  Calls validate the schema vocabulary used by this registry before execution.
  Unknown properties remain allowed; explicit null does not omit a property.
  Read-only annotations describe tool behavior and do not replace tenant
  authorization, which every tool enforces before querying storage.
  """

  alias Pulso.Auth
  alias Pulso.Codec.NIF
  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Envelope
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.Parser
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage

  @timestamp_schema %{
    "type" => "integer",
    "minimum" => -9_223_372_036_854_775_808,
    "maximum" => 9_223_372_036_854_775_807
  }

  @tools [
    %{
      "name" => "query_promql",
      "description" =>
        "Evaluate the supported Prometheus Query Language subset: selectors, rate/increase/irate/delta, over-time functions, and sum/avg/min/max/count with by/without grouping. Returns a Prometheus vector or matrix envelope.",
      "annotations" => %{"readOnlyHint" => true},
      "inputSchema" => %{
        "type" => "object",
        "properties" => %{
          "tenant" => %{"type" => "string"},
          "query" => %{"type" => "string"},
          "start_ts_ns" => @timestamp_schema,
          "end_ts_ns" => @timestamp_schema,
          "step_ms" => %{
            "type" => "integer",
            "minimum" => 1,
            "description" =>
              "Range-query step in milliseconds. Requires start_ts_ns and end_ts_ns; omit for an instant query."
          }
        },
        "required" => ["tenant", "query"]
      }
    },
    %{
      "name" => "query_logs",
      "description" => "Return log records for a tenant, optionally filtered by time range and service.",
      "annotations" => %{"readOnlyHint" => true},
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
          "start_ts_ns" =>
            Map.put(@timestamp_schema, "description", "Inclusive lower bound on log timestamp, Unix nanoseconds."),
          "end_ts_ns" =>
            Map.put(@timestamp_schema, "description", "Inclusive upper bound on log timestamp, Unix nanoseconds."),
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000}
        },
        "required" => ["tenant"]
      }
    },
    %{
      "name" => "query_metrics",
      "description" =>
        "Return metric samples for a tenant, optionally filtered by time range and PromQL-style label matchers.",
      "annotations" => %{"readOnlyHint" => true},
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
          "start_ts_ns" =>
            Map.put(@timestamp_schema, "description", "Inclusive lower bound on sample timestamp, Unix nanoseconds."),
          "end_ts_ns" =>
            Map.put(@timestamp_schema, "description", "Inclusive upper bound on sample timestamp, Unix nanoseconds."),
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000}
        },
        "required" => ["tenant"]
      }
    },
    %{
      "name" => "query_logql",
      "description" =>
        "Run a LogQL query and return the Loki-shaped JSON envelope. Supports log queries (streams result) and metric queries (matrix or vector result). The envelope is identical to /loki/api/v1/query_range so agent tooling that already understands Loki works unchanged.",
      "annotations" => %{"readOnlyHint" => true},
      "inputSchema" => %{
        "type" => "object",
        "properties" => %{
          "tenant" => %{"type" => "string"},
          "query" => %{"type" => "string", "description" => "LogQL expression."},
          "start_ts_ns" => @timestamp_schema,
          "end_ts_ns" => @timestamp_schema,
          "step_ms" => %{
            "type" => "integer",
            "minimum" => 1,
            "description" =>
              "Step interval in milliseconds for range metric queries. Required for matrix output; ignored for log queries."
          },
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000},
          "direction" => %{"type" => "string", "enum" => ["forward", "backward"]}
        },
        "required" => ["tenant", "query"]
      }
    }
  ]

  @spec list() :: [map()]
  def list, do: @tools

  @spec call(String.t(), map(), Pulso.MCP.context()) :: {:ok, [map()]} | {:error, term()}
  def call(name, args, context \\ %{}) do
    case Enum.find(@tools, &(&1["name"] == name)) do
      nil ->
        {:error, {:unknown_tool, name}}

      tool ->
        args = normalize_integers(args, tool["inputSchema"])

        with :ok <- validate_value(args, tool["inputSchema"], "arguments"),
             :ok <- validate_range(args) do
          execute(name, args, context)
        end
    end
  end

  # Schema integers include numbers written with a zero fractional part.
  # Normalize those known fields before passing them to integer-only backends.
  defp normalize_integers(args, schema) when is_map(args) do
    Enum.reduce(Map.get(schema, "properties", %{}), args, fn
      {key, %{"type" => "integer"}}, acc ->
        case Map.fetch(acc, key) do
          {:ok, value} -> Map.put(acc, key, normalize_integer(value))
          :error -> acc
        end

      _, acc ->
        acc
    end)
  end

  defp normalize_integers(args, _schema), do: args

  defp normalize_integer(value) when is_float(value) do
    integer = trunc(value)
    if value == integer, do: integer, else: value
  end

  defp normalize_integer(value), do: value

  # Validate the schema vocabulary used by this registry. Unknown properties
  # remain allowed, as in the published schemas; explicit null is not omission.
  defp validate_value(value, %{"type" => "object"} = schema, path) when is_map(value) do
    required = Map.get(schema, "required", [])

    case Enum.find(required, &(not Map.has_key?(value, &1))) do
      nil ->
        Enum.reduce_while(Map.get(schema, "properties", %{}), :ok, fn {key, child}, :ok ->
          validation_result(validate_property(value, key, child, path))
        end)

      key ->
        invalid("#{path}.#{key} is required")
    end
  end

  defp validate_value(value, %{"type" => "array", "items" => item}, path) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {entry, index}, :ok ->
      validation_result(validate_value(entry, item, "#{path}[#{index}]"))
    end)
  end

  defp validate_value(value, %{"type" => "string"} = schema, path) when is_binary(value) do
    if Map.has_key?(schema, "enum") and value not in schema["enum"] do
      invalid("#{path} must be one of #{inspect(schema["enum"])}")
    else
      :ok
    end
  end

  defp validate_value(value, %{"type" => "integer"} = schema, path) when is_integer(value) do
    cond do
      Map.has_key?(schema, "minimum") and value < schema["minimum"] ->
        invalid("#{path} must be at least #{schema["minimum"]}")

      Map.has_key?(schema, "maximum") and value > schema["maximum"] ->
        invalid("#{path} must be at most #{schema["maximum"]}")

      true ->
        :ok
    end
  end

  defp validate_value(_value, %{"type" => type}, path), do: invalid("#{path} must be #{type}")

  defp validate_value(_value, _schema, path), do: invalid("#{path} uses an unsupported schema")

  defp validate_property(value, key, schema, path) do
    case Map.fetch(value, key) do
      :error -> :ok
      {:ok, item} -> validate_value(item, schema, "#{path}.#{key}")
    end
  end

  defp validation_result(:ok), do: {:cont, :ok}
  defp validation_result(error), do: {:halt, error}

  defp validate_range(%{"start_ts_ns" => start, "end_ts_ns" => finish}) when start > finish,
    do: invalid("start_ts_ns must not exceed end_ts_ns")

  defp validate_range(_args), do: :ok

  defp invalid(message), do: {:error, {:invalid_arguments, message}}

  defp execute("query_logs", %{"tenant" => tenant} = args, context) when is_binary(tenant) do
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

  defp execute("query_metrics", %{"tenant" => tenant} = args, context) when is_binary(tenant) do
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

  defp execute("query_logql", %{"tenant" => tenant, "query" => query} = args, context)
       when is_binary(tenant) and is_binary(query) do
    opts = build_logql_opts(args)

    with :ok <- verify(context, tenant),
         {:ok, ast} <- parse_query(query),
         {:ok, envelope} <- run_logql(ast, tenant, opts) do
      {:ok, [%{"type" => "text", "text" => Pulso.JSON.encode!(envelope)}]}
    end
  end

  defp execute("query_promql", %{"tenant" => tenant, "query" => query} = args, context)
       when is_binary(tenant) and is_binary(query) do
    opts =
      %{}
      |> maybe_put(:start_ts_ns, args["start_ts_ns"])
      |> maybe_put(:end_ts_ns, args["end_ts_ns"])

    opts = maybe_put(opts, :step_ns, from_ms(args["step_ms"]))

    with :ok <- verify(context, tenant) do
      query |> Pulso.PromQL.Evaluator.query(tenant, opts) |> promql_result()
    end
  end

  defp execute(name, _args, _context), do: {:error, {:unknown_tool, name}}

  defp promql_result({:ok, result}), do: {:ok, [%{"type" => "text", "text" => Pulso.JSON.encode!(result)}]}
  defp promql_result({:error, {:storage_error, _}}), do: {:error, :metric_storage_unavailable}
  defp promql_result({:error, :query_execution_failed}), do: {:error, :metric_query_execution_failed}
  defp promql_result({:error, :query_overloaded}), do: {:error, :query_overloaded}

  defp promql_result({:error, reason})
       when reason in [
              :query_sample_limit,
              :query_scan_limit,
              :query_work_limit,
              :query_result_limit,
              :query_resource_limit,
              :query_timeout
            ], do: {:error, :query_execution_limit}

  defp promql_result({:error, reason}), do: {:error, {:invalid_arguments, reason}}

  # -- query_metrics helpers -----------------------------------------------

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

  # -- query_logql helpers -------------------------------------------------

  defp parse_query(query) do
    case Parser.parse(query) do
      {:ok, ast} -> {:ok, ast}
      {:error, reason} -> {:error, {:invalid_arguments, {:logql_parse_error, reason}}}
    end
  end

  defp build_logql_opts(args) do
    %{}
    |> maybe_put(:start_ts_ns, args["start_ts_ns"])
    |> maybe_put(:end_ts_ns, args["end_ts_ns"])
    |> maybe_put(:limit, args["limit"])
    |> maybe_put(:step_ns, from_ms(args["step_ms"]))
    |> maybe_put(:direction, from_direction(args["direction"]))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  # These conversions only receive values accepted by the registry schema.
  defp from_ms(nil), do: nil
  defp from_ms(ms) when is_integer(ms) and ms > 0, do: ms * 1_000_000

  defp from_direction("forward"), do: :forward
  defp from_direction("backward"), do: :backward
  defp from_direction(nil), do: nil

  defp run_logql(%AST.LogQuery{} = ast, tenant, opts) do
    case Evaluator.evaluate_log(ast, tenant, opts) do
      {:ok, streams} -> {:ok, Envelope.streams(streams)}
      {:error, _} = err -> err
    end
  end

  defp run_logql(metric_expr, tenant, opts) do
    case Evaluator.evaluate_metric(metric_expr, tenant, opts) do
      {:ok, {:matrix, series}} -> {:ok, Envelope.matrix(series)}
      {:ok, {:vector, series}} -> {:ok, Envelope.vector(series)}
      {:error, _} = err -> err
    end
  end

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

  # Rust fast-path JSON encoder; falls back to Elixir on any shape the
  # Rust side cannot guarantee to emit identically. Keeps the hot path
  # (10k-sample MCP responses) off the general-purpose encoder, which
  # has to build one intermediate string-keyed map per sample first.
  defp encode_samples(samples) do
    case NIF.encode_metric_samples(samples) do
      {:ok, json} -> json
      :fallback -> JSON.encode!(Enum.map(samples, &encode_sample/1))
    end
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
