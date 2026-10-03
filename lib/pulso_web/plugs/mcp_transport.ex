defmodule PulsoWeb.Plugs.MCPTransport do
  @moduledoc """
  Validates the transport-level requirements of MCP streamable HTTP
  (2025-06-18) before a request reaches `PulsoWeb.MCPController`.

    * `Origin` — when present, it must be listed in
      `config :pulso, PulsoWeb.Plugs.MCPTransport, allowed_origins: [...]`,
      otherwise the request is rejected with 403 (DNS-rebinding protection).
      Non-browser clients send no `Origin` header and are unaffected.
    * `MCP-Protocol-Version` — when present, it must name a version in
      `Pulso.MCP.supported_protocol_versions/0`, otherwise 400. An absent
      header is accepted, as the specification asks servers to assume
      `2025-03-26` for clients that omit it.

  The effective version is stored in `conn.assigns.mcp_protocol_version`.
  """

  @behaviour Plug

  import Plug.Conn

  @assumed_version "2025-03-26"

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    conn = check_origin(conn)
    if conn.halted, do: conn, else: check_protocol_version(conn)
  end

  defp check_origin(conn) do
    case get_req_header(conn, "origin") do
      [] -> conn
      [origin] -> if origin in allowed_origins(), do: conn, else: reject(conn, 403, "Origin not allowed")
      _many -> reject(conn, 403, "Origin not allowed")
    end
  end

  defp check_protocol_version(conn) do
    case get_req_header(conn, "mcp-protocol-version") do
      [] ->
        assign(conn, :mcp_protocol_version, @assumed_version)

      [version] ->
        if version in Pulso.MCP.supported_protocol_versions() do
          assign(conn, :mcp_protocol_version, version)
        else
          reject(conn, 400, "Unsupported MCP-Protocol-Version: #{version}")
        end

      _many ->
        reject(conn, 400, "Multiple MCP-Protocol-Version headers")
    end
  end

  defp reject(conn, status, message) do
    body = JSON.encode!(%{"jsonrpc" => "2.0", "id" => nil, "error" => %{"code" => -32_600, "message" => message}})

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, body)
    |> halt()
  end

  defp allowed_origins do
    :pulso |> Application.get_env(__MODULE__, []) |> Keyword.get(:allowed_origins, [])
  end
end
