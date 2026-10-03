defmodule PulsoWeb.MCPController do
  @moduledoc """
  Stateless MCP streamable HTTP endpoint.

  `POST /mcp` carries JSON-RPC messages. Requests are answered with a JSON
  body; messages that need no reply (notifications and client responses) are
  acknowledged with `202 Accepted` and no body. Pulso keeps no sessions and
  never pushes server-initiated messages, so `GET` (the optional SSE stream)
  and `DELETE` (session termination) answer `405 Method Not Allowed`, as the
  specification prescribes for servers that do not offer them.
  """

  use PulsoWeb, :controller

  def rpc(conn, %{"_json" => messages}) when is_list(messages) do
    # JSON-RPC batching was removed in 2025-06-18; only older clients may use it.
    if conn.assigns[:mcp_protocol_version] == "2025-06-18",
      do: invalid_request(conn),
      else: batch(conn, messages)
  end

  def rpc(conn, params) when is_map(params), do: respond(conn, Pulso.MCP.dispatch(params, %{conn: conn}))

  def unsupported(conn, _params) do
    conn
    |> put_resp_header("allow", "POST")
    |> send_resp(405, "")
  end

  defp batch(conn, []), do: invalid_request(conn)

  defp batch(conn, messages) do
    context = %{conn: conn}

    responses =
      Enum.flat_map(messages, fn message ->
        case Pulso.MCP.dispatch(message, context) do
          {:reply, response} -> [response]
          :noreply -> []
        end
      end)

    case responses do
      [] -> send_resp(conn, 202, "")
      list -> json(conn, list)
    end
  end

  defp invalid_request(conn) do
    conn
    |> put_status(400)
    |> json(%{"jsonrpc" => "2.0", "id" => nil, "error" => %{"code" => -32_600, "message" => "Invalid Request"}})
  end

  defp respond(conn, {:reply, response}), do: json(conn, response)
  defp respond(conn, :noreply), do: send_resp(conn, 202, "")
end
