defmodule PulsoWeb.MCPController do
  @moduledoc """
  Streamable HTTP transport for `Pulso.MCP` (MCP revision `2026-07-28`).

  Each POST carries exactly one JSON-RPC request or notification; batches
  are rejected. Notifications are acknowledged with `202 Accepted` and an
  empty body. Requests must carry the mirrored metadata headers validated
  by `PulsoWeb.MCPHeaders`. The transport is stateless: it never mints or
  echoes `Mcp-Session-Id`, and ignores `Mcp-Session-Id` and `Last-Event-ID`.
  GET and DELETE return `405 Method Not Allowed` because this revision
  removed the standalone stream and session termination.

  Origin and media-type checks run earlier, before body parsing, in
  `PulsoWeb.MCPRequestGate`.
  """

  use PulsoWeb, :controller

  alias Pulso.MCP
  alias PulsoWeb.MCPHeaders

  @header_mismatch -32_020

  def rpc(conn, _params) do
    # Read the parsed body, not merged params, so query strings cannot
    # supply protocol fields.
    message = conn.body_params

    case MCP.classify(message) do
      {:notification, _method} ->
        send_resp(conn, 202, "")

      {:invalid, _id} ->
        respond(conn, MCP.dispatch(message))

      {:request, id, _method, _params} = request ->
        case MCPHeaders.validate(conn, request) do
          :ok -> respond(conn, MCP.dispatch(message, %{conn: conn}))
          {:error, reason} -> respond(conn, {:reply, MCP.error_response(id, @header_mismatch, reason)})
        end
    end
  end

  def method_not_allowed(conn, _params) do
    conn
    |> put_resp_header("allow", "POST")
    |> send_resp(405, "")
  end

  defp respond(conn, {:reply, response}) do
    conn
    |> put_status(status(response))
    |> json(response)
  end

  defp respond(conn, {:stream, messages}) do
    body = Enum.map_join(messages, fn message -> "data: #{Pulso.JSON.encode!(message)}\n\n" end)

    conn
    |> put_resp_content_type("text/event-stream")
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("x-accel-buffering", "no")
    |> send_resp(200, body)
  end

  defp status(%{"error" => %{"code" => -32_601}}), do: 404
  defp status(%{"error" => %{"code" => -32_603}}), do: 500
  defp status(%{"error" => _}), do: 400
  defp status(_response), do: 200
end
