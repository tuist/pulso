defmodule Pulso.MCP do
  @moduledoc """
  Minimal Model Context Protocol server surface.

  Handles the JSON-RPC 2.0 messages Pulso currently supports: the `initialize`
  handshake, `tools/list`, and `tools/call`. Only read-only tools are exposed
  from this module by design; write and remediation tools live in separate,
  narrowly-scoped surfaces (see the project brief).
  """

  alias Pulso.MCP.Tools

  @protocol_version "2025-06-18"
  @server_info %{"name" => "pulso", "version" => "0.1.0"}

  @doc """
  Dispatch a single JSON-RPC message.

  Returns `{:reply, response}` for requests and `:noreply` for notifications
  (messages without an `id`).
  """
  @spec dispatch(map()) :: {:reply, map()} | :noreply
  def dispatch(%{"method" => method} = msg) do
    id = Map.get(msg, "id")
    params = Map.get(msg, "params", %{})

    case {id, handle(method, params)} do
      {nil, _} -> :noreply
      {id, {:ok, result}} -> {:reply, ok(id, result)}
      {id, {:error, code, message}} -> {:reply, error(id, code, message)}
    end
  end

  def dispatch(_), do: {:reply, error(nil, -32_600, "Invalid Request")}

  defp handle("initialize", _params) do
    {:ok,
     %{
       "protocolVersion" => @protocol_version,
       "serverInfo" => @server_info,
       "capabilities" => %{"tools" => %{"listChanged" => false}}
     }}
  end

  defp handle("tools/list", _params) do
    {:ok, %{"tools" => Tools.list()}}
  end

  defp handle("tools/call", %{"name" => name} = params) do
    arguments = Map.get(params, "arguments", %{})

    case Tools.call(name, arguments) do
      {:ok, content} ->
        {:ok, %{"content" => content, "isError" => false}}

      {:error, reason} ->
        {:ok,
         %{
           "content" => [%{"type" => "text", "text" => inspect(reason)}],
           "isError" => true
         }}
    end
  end

  defp handle("ping", _params), do: {:ok, %{}}
  defp handle(_unknown, _params), do: {:error, -32_601, "Method not found"}

  defp ok(id, result), do: %{"jsonrpc" => "2.0", "id" => id, "result" => result}

  defp error(id, code, message),
    do: %{"jsonrpc" => "2.0", "id" => id, "error" => %{"code" => code, "message" => message}}
end
