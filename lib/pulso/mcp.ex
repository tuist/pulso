defmodule Pulso.MCP do
  @moduledoc """
  Minimal Model Context Protocol server surface.

  Handles the JSON-RPC 2.0 messages Pulso currently supports: the `initialize`
  handshake, `tools/list`, and `tools/call`. Only read-only tools are exposed
  from this module by design; write and remediation tools live in separate,
  narrowly-scoped surfaces (see the project brief).
  """

  alias Pulso.MCP.Tools

  @supported_protocol_versions ["2025-06-18", "2025-03-26"]
  @latest_protocol_version hd(@supported_protocol_versions)
  @server_info %{"name" => "pulso", "version" => "0.1.0"}

  @type context :: %{optional(:conn) => Plug.Conn.t()}

  @doc "Protocol versions Pulso can speak, newest first."
  @spec supported_protocol_versions() :: [String.t()]
  def supported_protocol_versions, do: @supported_protocol_versions

  @doc """
  Dispatch a single JSON-RPC message.

  `context` carries per-request state that individual tools need to run
  authorization or other checks. Today only `:conn` is populated (from
  `PulsoWeb.MCPController`); the shape is intentionally open for future
  fields (tenant hint, feature flags).

  Returns `{:reply, response}` for requests and `:noreply` for notifications
  (messages without an `id`).
  """
  @spec dispatch(map(), context()) :: {:reply, map()} | :noreply
  def dispatch(msg, context \\ %{})

  def dispatch(%{"method" => method} = msg, context) when is_map(context) do
    id = Map.get(msg, "id")
    params = Map.get(msg, "params") || %{}

    case {id, if(is_map(params), do: handle(method, params, context), else: invalid_params())} do
      {nil, _} -> :noreply
      {id, {:ok, result}} -> {:reply, ok(id, result)}
      {id, {:error, code, message}} -> {:reply, error(id, code, message)}
    end
  end

  # Responses to server-initiated requests (and their errors) carry no method.
  # Pulso never sends requests, so there is nothing to correlate; accept them.
  def dispatch(%{"id" => _, "result" => _}, _), do: :noreply
  def dispatch(%{"id" => _, "error" => _}, _), do: :noreply
  def dispatch(_, _), do: {:reply, error(nil, -32_600, "Invalid Request")}

  # Version negotiation: echo the client's version when supported, otherwise
  # answer with the latest version and let the client decide whether to proceed.
  defp handle("initialize", params, _context) do
    case Map.get(params, "protocolVersion") do
      version when version in @supported_protocol_versions -> {:ok, initialize_result(version)}
      version when is_binary(version) -> {:ok, initialize_result(@latest_protocol_version)}
      _ -> {:error, -32_602, "Invalid params: protocolVersion must be a string"}
    end
  end

  defp handle("tools/list", _params, _context) do
    {:ok, %{"tools" => Tools.list()}}
  end

  defp handle("tools/call", %{"name" => name} = params, context) do
    arguments = Map.get(params, "arguments", %{})

    case Tools.call(name, arguments, context) do
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

  defp handle("ping", _params, _context), do: {:ok, %{}}
  defp handle(_unknown, _params, _context), do: {:error, -32_601, "Method not found"}

  defp invalid_params, do: {:error, -32_602, "Invalid params: expected an object"}

  defp initialize_result(version) do
    %{
      "protocolVersion" => version,
      "serverInfo" => @server_info,
      "capabilities" => %{"tools" => %{"listChanged" => false}}
    }
  end

  defp ok(id, result), do: %{"jsonrpc" => "2.0", "id" => id, "result" => result}

  defp error(id, code, message),
    do: %{"jsonrpc" => "2.0", "id" => id, "error" => %{"code" => code, "message" => message}}
end
