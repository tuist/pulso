defmodule PulsoWeb.CompressedBodyReaderTest do
  use Pulso.Test.Case, async: true

  alias PulsoWeb.CompressedBodyReader

  defp gzip(body) do
    conn = Plug.Test.conn(:post, "/v1/traces") |> Plug.Conn.put_req_header("content-encoding", "gzip")
    CompressedBodyReader.decompress(conn, body)
  end

  test "empty gzip members and overlapping zero trailers decode correctly" do
    empty = :zlib.gzip("")
    assert {:ok, ""} = gzip(empty)
    assert {:ok, "payload"} = gzip(empty <> :zlib.gzip("payload") <> empty)
    assert {:ok, ""} = gzip(:binary.copy(empty, 64))
  end

  test "even empty members consume the fixed gzip work ceiling" do
    empty = :zlib.gzip("")
    assert {:error, :payload_too_large} = gzip(:binary.copy(empty, 65))
    assert {:error, :payload_too_large} = gzip(:binary.copy(empty, 10_000))
  end

  test "member caps do not weaken CRC, trailer and cumulative output validation" do
    assert {:error, :invalid_gzip} = gzip("")
    assert {:error, :invalid_gzip} = gzip(binary_part(:zlib.gzip("hello"), 0, 15))
    Pulso.Runtime.put_env(:pulso, CompressedBodyReader, max_decompressed_bytes: 5)
    assert {:ok, "hello"} = gzip(:zlib.gzip("hello"))
    assert {:error, :payload_too_large} = gzip(:zlib.gzip("hello") <> :zlib.gzip("!"))
  end
end
