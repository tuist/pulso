defmodule PulsoWeb.CompressedBodyReaderTest do
  use ExUnit.Case, async: true

  alias PulsoWeb.CompressedBodyReader

  defp conn_with_body(body, headers \\ []) do
    conn = Plug.Test.conn(:post, "/", body)

    Enum.reduce(headers, conn, fn {k, v}, acc ->
      Plug.Conn.put_req_header(acc, k, v)
    end)
  end

  test "passes an uncompressed body through untouched" do
    body = "not gzipped"
    conn = conn_with_body(body)

    assert {:ok, ^body, %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
  end

  test "gunzips a body when content-encoding is gzip" do
    original = String.duplicate("hello world\n", 100)
    gzipped = :zlib.gzip(original)
    conn = conn_with_body(gzipped, [{"content-encoding", "gzip"}])

    assert {:ok, ^original, %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
  end

  test "returns :invalid_gzip when the body is not actually gzipped" do
    # A client that advertises `content-encoding: gzip` but sends plain
    # bytes has a bug — surfacing it as an error beats decoding garbage
    # and confusing the downstream parser with the failure.
    conn = conn_with_body("not gzip data", [{"content-encoding", "gzip"}])

    assert {:error, :invalid_gzip} = CompressedBodyReader.read_body(conn, [])
  end

  test "ignores content-encoding values other than gzip" do
    # Snappy needs its own path (framing differs) and any unknown
    # encoding is best left to a purpose-built reader; passing the raw
    # bytes through lets a later plug fail loudly rather than double-
    # decoding.
    body = "opaque"
    conn = conn_with_body(body, [{"content-encoding", "snappy"}])

    assert {:ok, ^body, %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
  end
end
