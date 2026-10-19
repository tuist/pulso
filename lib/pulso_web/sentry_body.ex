defmodule PulsoWeb.SentryBody do
  @moduledoc "Preserve raw Sentry bytes before form/JSON parsers, including browser simple requests."
  @behaviour Plug

  import Plug.Conn

  alias Plug.Parsers.RequestTooLargeError

  def init(opts), do: opts

  def call(conn, _opts) do
    case Enum.map(conn.path_info, &URI.decode/1) do
      ["api", _project, endpoint] when endpoint in ["envelope", "store"] -> prepare(conn)
      _ -> conn
    end
  end

  defp prepare(conn) do
    conn =
      conn
      |> put_resp_header("access-control-allow-origin", "*")
      |> put_resp_header("access-control-allow-methods", "POST, OPTIONS")
      |> put_resp_header("access-control-allow-headers", "content-type, x-sentry-auth, sentry-trace, baggage")
      |> put_resp_header("access-control-expose-headers", "x-pulso-discarded-items, retry-after")

    case conn.method do
      "OPTIONS" -> conn |> send_resp(204, "") |> halt()
      "POST" -> read(conn)
      _ -> conn
    end
  end

  defp read(conn) do
    if get_req_header(conn, "content-encoding") in [[], ["identity"], ["gzip"]] do
      case PulsoWeb.CompressedBodyReader.read_body(conn, length: 4 * 1024 * 1024) do
        {:ok, body, conn} ->
          # Plug.Parsers skips a fetched body, regardless of its media type.
          conn |> Map.put(:body_params, %{}) |> put_private(:sentry_body, body)

        {:more, _, conn} ->
          error(conn, 413, "payload_too_large")

        {:error, _} ->
          error(conn, 400, "invalid_body")
      end
    else
      error(conn, 415, "unsupported_content_encoding")
    end
  rescue
    RequestTooLargeError -> error(conn, 413, "payload_too_large")
  end

  defp error(conn, status, reason) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Pulso.JSON.encode!(%{error: reason}))
    |> halt()
  end
end
