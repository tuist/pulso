defmodule Pulso.LogQL.Envelope do
  @moduledoc """
  Marshal `Pulso.LogQL.Evaluator` results into the Loki HTTP wire
  format: `{status: "success", data: {resultType, result: [...]}}`.

  Shared by `PulsoWeb.LokiQueryController` and `Pulso.MCP.Tools`
  (`query_logql`) so both surfaces emit byte-identical payloads.

    * Streams entries use nanosecond timestamp strings.
    * Matrix and vector samples use second-precision timestamp floats
      as strings, matching Grafana's Loki wire format.
  """

  @spec streams([{map(), [{integer(), String.t()}]}]) :: map()
  def streams(streams) when is_list(streams) do
    result =
      Enum.map(streams, fn {labels, entries} ->
        %{
          "stream" => stringify(labels),
          "values" => Enum.map(entries, fn {ts, line} -> [Integer.to_string(ts), line] end)
        }
      end)

    %{
      "status" => "success",
      "data" => %{
        "resultType" => "streams",
        "result" => result,
        "stats" => %{}
      }
    }
  end

  @spec matrix([{map(), [{integer(), float()}]}]) :: map()
  def matrix(series) when is_list(series) do
    result =
      Enum.map(series, fn {labels, samples} ->
        %{
          "metric" => stringify(labels),
          "values" => Enum.map(samples, fn {ts, v} -> [seconds_str(ts), value_str(v)] end)
        }
      end)

    %{
      "status" => "success",
      "data" => %{
        "resultType" => "matrix",
        "result" => result,
        "stats" => %{}
      }
    }
  end

  @spec vector([{map(), {integer(), float()}}]) :: map()
  def vector(series) when is_list(series) do
    result =
      Enum.map(series, fn {labels, {ts, v}} ->
        %{
          "metric" => stringify(labels),
          "value" => [seconds_str(ts), value_str(v)]
        }
      end)

    %{
      "status" => "success",
      "data" => %{
        "resultType" => "vector",
        "result" => result,
        "stats" => %{}
      }
    }
  end

  defp stringify(map) when is_map(map), do: Map.new(map, fn {k, v} -> {to_string(k), to_string(v)} end)

  defp seconds_str(ts_ns) when is_integer(ts_ns) do
    seconds = ts_ns / 1_000_000_000
    Float.to_string(seconds)
  end

  defp value_str(v) when is_float(v), do: Float.to_string(v)
  defp value_str(v) when is_integer(v), do: Integer.to_string(v)
end
