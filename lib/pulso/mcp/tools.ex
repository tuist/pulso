defmodule Pulso.MCP.Tools do
  @moduledoc """
  Registry of read-only MCP tools Pulso exposes.
  """

  @tools [
    %{
      "name" => "query_logs",
      "description" => "Run a LogQL query against the configured Loki backend and return the matching log lines.",
      "inputSchema" => %{
        "type" => "object",
        "properties" => %{
          "query" => %{
            "type" => "string",
            "description" => ~s(LogQL expression, for example: {app="web"} |= "error")
          },
          "limit" => %{"type" => "integer", "minimum" => 1, "maximum" => 5000},
          "direction" => %{"type" => "string", "enum" => ["backward", "forward"]}
        },
        "required" => ["query"]
      }
    }
  ]

  @spec list() :: [map()]
  def list, do: @tools

  @spec call(String.t(), map()) :: {:ok, [map()]} | {:error, term()}
  def call("query_logs", %{"query" => logql} = args) do
    opts =
      []
      |> put_opt(:limit, args["limit"])
      |> put_opt(:direction, parse_direction(args["direction"]))

    with {:ok, body} <- Pulso.Loki.query_range(logql, opts) do
      {:ok, [%{"type" => "text", "text" => Jason.encode!(body)}]}
    end
  end

  def call(name, _args), do: {:error, {:unknown_tool, name}}

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp parse_direction("backward"), do: :backward
  defp parse_direction("forward"), do: :forward
  defp parse_direction(_), do: nil
end
