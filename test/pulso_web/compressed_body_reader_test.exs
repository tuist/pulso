defmodule PulsoWeb.CompressedBodyReaderTest do
  use ExUnit.Case, async: false

  alias Plug.Parsers.RequestTooLargeError
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

  describe "gzip semantics" do
    test "inflates concatenated members as one document" do
      body = :zlib.gzip("{\"streams\":") <> :zlib.gzip("[]}")
      conn = conn_with_body(body, [{"content-encoding", "gzip"}])

      assert {:ok, "{\"streams\":[]}", %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
    end

    test "inflates identical concatenated members" do
      member = :zlib.gzip("abc")
      conn = conn_with_body(member <> member <> member, [{"content-encoding", "gzip"}])

      assert {:ok, "abcabcabc", %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
    end

    test "rejects a truncated stream and trailing garbage" do
      full = :zlib.gzip("{\"streams\":[]}")

      for body <- [binary_part(full, 0, byte_size(full) - 8), binary_part(full, 0, 12), full <> "junk", ""] do
        conn = conn_with_body(body, [{"content-encoding", "gzip"}])
        assert {:error, :invalid_gzip} = CompressedBodyReader.read_body(conn, [])
      end
    end
  end

  describe "decompressed size limit" do
    setup do
      original = Application.get_env(:pulso, CompressedBodyReader)
      Application.put_env(:pulso, CompressedBodyReader, max_decompressed_bytes: 1_000)

      on_exit(fn ->
        if original,
          do: Application.put_env(:pulso, CompressedBodyReader, original),
          else: Application.delete_env(:pulso, CompressedBodyReader)
      end)
    end

    test "inflates a body exactly at the limit" do
      original = String.duplicate("a", 1_000)
      conn = conn_with_body(:zlib.gzip(original), [{"content-encoding", "gzip"}])

      assert {:ok, ^original, %Plug.Conn{}} = CompressedBodyReader.read_body(conn, [])
    end

    test "applies one budget across concatenated members" do
      member = :zlib.gzip(String.duplicate("a", 600))
      conn = conn_with_body(member <> member, [{"content-encoding", "gzip"}])

      assert_raise RequestTooLargeError, fn -> CompressedBodyReader.read_body(conn, []) end
    end

    test "aborts a gzip bomb with a 413 instead of inflating it" do
      bomb = :zlib.gzip(String.duplicate("a", 50_000_000))
      assert byte_size(bomb) < 100_000
      conn = conn_with_body(bomb, [{"content-encoding", "gzip"}])

      assert_raise RequestTooLargeError, fn -> CompressedBodyReader.read_body(conn, []) end
    end
  end
end
