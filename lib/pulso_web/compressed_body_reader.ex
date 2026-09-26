defmodule PulsoWeb.CompressedBodyReader do
  @moduledoc """
  Body reader for `Plug.Parsers` that transparently decompresses a
  `Content-Encoding: gzip` request body before parsing.

  Both Loki push and Prometheus `remote_write` agents (and OTLP/HTTP
  senders configured to compress) commonly ship gzipped payloads, and
  Plug's built-in JSON parser needs plain bytes to hand to the decoder.

  Snappy is intentionally not handled here — Loki's snappy-framed
  protobuf variant sits on a different content-type and needs its own
  decoder anyway.

  The reader forwards a `{:more, ...}` return unchanged. If the body
  exceeds Plug.Parsers's configured length limit, the parser rejects
  it with 413 — a decompressor cannot invent the missing bytes.
  """

  @spec read_body(Plug.Conn.t(), keyword()) ::
          {:ok, binary(), Plug.Conn.t()}
          | {:more, binary(), Plug.Conn.t()}
          | {:error, term()}
  def read_body(conn, opts) do
    case Plug.Conn.read_body(conn, opts) do
      {:ok, body, conn} ->
        case maybe_decompress(conn, body) do
          {:ok, decompressed} -> {:ok, decompressed, conn}
          {:error, _} = err -> err
        end

      other ->
        other
    end
  end

  defp maybe_decompress(conn, body) do
    case Plug.Conn.get_req_header(conn, "content-encoding") do
      ["gzip"] -> gunzip(body)
      _ -> {:ok, body}
    end
  end

  defp gunzip(body) do
    {:ok, :zlib.gunzip(body)}
  rescue
    _ -> {:error, :invalid_gzip}
  end
end
