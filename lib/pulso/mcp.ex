defmodule Pulso.MCP do
  @moduledoc """
  Stateless Model Context Protocol server surface, revision `2026-07-28`.

  Every request carries its protocol version and client capabilities in
  `params._meta`, so the server keeps no handshake or session state and
  answers each request independently. Supported methods are
  `server/discover`, `tools/list`, `tools/call`, and `subscriptions/listen`.
  The legacy `initialize` handshake and `ping` were removed by this revision
  and return method-not-found.

  Only read-only tools are exposed from this module by design; write and
  remediation tools live in separate, narrowly-scoped surfaces (see
  `docs/architecture.md`).

  This module is transport-agnostic. HTTP concerns (status codes, mirrored
  request headers, and origin checks) live in `PulsoWeb.MCPController`.
  """

  alias Pulso.MCP.Tools

  @protocol_version "2026-07-28"
  @supported_versions [@protocol_version]

  @meta_protocol_version "io.modelcontextprotocol/protocolVersion"
  @meta_client_capabilities "io.modelcontextprotocol/clientCapabilities"
  @meta_client_info "io.modelcontextprotocol/clientInfo"
  @meta_server_info "io.modelcontextprotocol/serverInfo"
  @meta_subscription_id "io.modelcontextprotocol/subscriptionId"

  @list_ttl_ms 60_000
  @cache_scope "private"

  @invalid_request -32_600
  @method_not_found -32_601
  @invalid_params -32_602
  @unsupported_protocol_version -32_022

  @subscription_flags ["toolsListChanged", "promptsListChanged", "resourcesListChanged"]

  @type context :: %{optional(:conn) => Plug.Conn.t()}
  @type id :: String.t() | integer()
  @type message ::
          {:request, id(), String.t(), map()}
          | {:notification, String.t()}
          | {:invalid, id() | nil}

  @doc "Protocol versions this server implements."
  @spec supported_versions() :: [String.t()]
  def supported_versions, do: @supported_versions

  @doc """
  Classify a decoded JSON-RPC message without executing it.

  Requests have a string or integer `id`; notifications have no `id` key.
  Anything else, including batches, responses, a `null` identifier, or
  non-object `params`, is invalid.
  """
  @spec classify(term()) :: message()
  def classify(%{"jsonrpc" => "2.0", "method" => method} = msg) when is_binary(method) do
    cond do
      Map.has_key?(msg, "params") and not is_map(msg["params"]) -> {:invalid, readable_id(msg)}
      not Map.has_key?(msg, "id") -> {:notification, method}
      valid_id?(msg["id"]) -> {:request, msg["id"], method, Map.get(msg, "params", %{})}
      true -> {:invalid, nil}
    end
  end

  def classify(msg), do: {:invalid, readable_id(msg)}

  @doc """
  Dispatch a single JSON-RPC message.

  Returns `{:reply, response}` for requests, `{:stream, messages}` when the
  response is a sequence of messages ending in the final response (used by
  `subscriptions/listen`), and `:noreply` for notifications. Notifications
  are never executed: this revision defines no client-to-server
  notification over HTTP.
  """
  @spec dispatch(term(), context()) :: {:reply, map()} | {:stream, [map()]} | :noreply
  def dispatch(msg, context \\ %{}) do
    case classify(msg) do
      {:notification, _method} ->
        :noreply

      {:invalid, id} ->
        {:reply, error_response(id, @invalid_request, "Invalid Request")}

      {:request, id, method, params} ->
        with :ok <- validate_meta(params),
             :ok <- validate_version(params),
             {:ok, result} <- handle(method, params, id, context) do
          {:reply, %{"jsonrpc" => "2.0", "id" => id, "result" => complete(result)}}
        else
          {:stream, messages} -> {:stream, messages}
          {:error, code, message, data} -> {:reply, error_response(id, code, message, data)}
        end
    end
  end

  @doc "Build a JSON-RPC error response, omitting an unreadable identifier."
  @spec error_response(id() | nil, integer(), String.t(), term()) :: map()
  def error_response(id, code, message, data \\ nil) do
    error = %{"code" => code, "message" => message}
    error = if is_nil(data), do: error, else: Map.put(error, "data", data)
    response = %{"jsonrpc" => "2.0", "error" => error}
    if is_nil(id), do: response, else: Map.put(response, "id", id)
  end

  defp validate_meta(%{"_meta" => meta}) when is_map(meta) do
    cond do
      not is_binary(meta[@meta_protocol_version]) ->
        invalid_params("_meta.#{@meta_protocol_version} must be a string")

      not is_map(meta[@meta_client_capabilities]) ->
        invalid_params("_meta.#{@meta_client_capabilities} must be an object")

      Map.has_key?(meta, @meta_client_info) and not implementation?(meta[@meta_client_info]) ->
        invalid_params("_meta.#{@meta_client_info} must have string name and version")

      true ->
        :ok
    end
  end

  defp validate_meta(_params), do: invalid_params("params._meta is required")

  defp validate_version(%{"_meta" => %{@meta_protocol_version => version}}) do
    if version in @supported_versions do
      :ok
    else
      {:error, @unsupported_protocol_version, "Unsupported protocol version",
       %{"supported" => @supported_versions, "requested" => version}}
    end
  end

  defp implementation?(%{"name" => name, "version" => version}), do: is_binary(name) and is_binary(version)
  defp implementation?(_), do: false

  defp handle("server/discover", _params, _id, _context) do
    {:ok,
     %{
       "supportedVersions" => @supported_versions,
       "capabilities" => capabilities(),
       "ttlMs" => @list_ttl_ms,
       "cacheScope" => @cache_scope
     }}
  end

  defp handle("tools/list", _params, _id, _context) do
    {:ok, %{"tools" => Tools.list(), "ttlMs" => @list_ttl_ms, "cacheScope" => @cache_scope}}
  end

  defp handle("tools/call", %{"name" => name} = params, _id, context) when is_binary(name) do
    arguments = Map.get(params, "arguments", %{})

    cond do
      not is_map(arguments) ->
        invalid_params("params.arguments must be an object")

      not Tools.known?(name) ->
        invalid_params("Unknown tool: #{name}")

      true ->
        case Tools.call(name, arguments, context) do
          {:ok, content} ->
            {:ok, %{"content" => content, "isError" => false}}

          {:error, reason} ->
            {:ok, %{"content" => [%{"type" => "text", "text" => inspect(reason)}], "isError" => true}}
        end
    end
  end

  defp handle("tools/call", _params, _id, _context), do: invalid_params("params.name must be a string")

  defp handle("subscriptions/listen", params, id, _context) do
    with :ok <- validate_subscription_filter(params["notifications"]) do
      # Pulso supports no change notifications (`listChanged: false`), so it
      # acknowledges an empty filter and ends the subscription gracefully.
      # Nothing outlives the request.
      meta = Map.put(server_meta(), @meta_subscription_id, id)

      acknowledgement = %{
        "jsonrpc" => "2.0",
        "method" => "notifications/subscriptions/acknowledged",
        "params" => %{"_meta" => meta, "notifications" => %{}}
      }

      completion = %{"jsonrpc" => "2.0", "id" => id, "result" => %{"resultType" => "complete", "_meta" => meta}}

      {:stream, [acknowledgement, completion]}
    end
  end

  defp handle("initialize", _params, _id, _context) do
    {:error, @method_not_found, "Method not found: initialize was removed in protocol #{@protocol_version}",
     %{"supported" => @supported_versions}}
  end

  defp handle(method, _params, _id, _context), do: {:error, @method_not_found, "Method not found: #{method}", nil}

  defp validate_subscription_filter(filter) when is_map(filter) do
    Enum.reduce_while(filter, :ok, fn
      {key, value}, :ok when key in @subscription_flags and not is_boolean(value) ->
        {:halt, invalid_params("params.notifications.#{key} must be a boolean")}

      {"resourceSubscriptions", value}, :ok ->
        if is_list(value) and Enum.all?(value, &is_binary/1),
          do: {:cont, :ok},
          else: {:halt, invalid_params("params.notifications.resourceSubscriptions must be an array of strings")}

      _entry, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_subscription_filter(_filter), do: invalid_params("params.notifications must be an object")

  defp capabilities, do: %{"tools" => %{"listChanged" => false}}

  defp complete(result) do
    result
    |> Map.put("resultType", "complete")
    |> Map.update("_meta", server_meta(), &Map.merge(&1, server_meta()))
  end

  defp server_meta do
    %{@meta_server_info => %{"name" => "pulso", "version" => to_string(Application.spec(:pulso, :vsn))}}
  end

  defp invalid_params(message), do: {:error, @invalid_params, message, nil}

  defp valid_id?(id), do: is_binary(id) or is_integer(id)

  defp readable_id(%{"id" => id}) do
    if valid_id?(id), do: id
  end

  defp readable_id(_msg), do: nil
end
