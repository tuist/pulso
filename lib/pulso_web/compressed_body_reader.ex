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

  The compressed length limit alone does not bound memory, since gzip
  expands by orders of magnitude. Inflation therefore runs incrementally and
  aborts with `Plug.Parsers.RequestTooLargeError` (413) once the output
  passes `:max_decompressed_bytes`, so a gzip bomb is never fully
  materialised. The limit is the `:max_decompressed_bytes` option of
  `config :pulso, PulsoWeb.CompressedBodyReader` and defaults to 16 MiB, the
  same ceiling the Snappy receivers apply.
  """

  alias Plug.Parsers.RequestTooLargeError

  @default_max_decompressed_bytes 16 * 1024 * 1024

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

  # `:zlib.gunzip/1` semantics, bounded: every concatenated member is
  # inflated, a truncated stream or missing trailer is an error, and one
  # cumulative budget covers all members.
  defp gunzip(<<>>), do: {:error, :invalid_gzip}

  defp gunzip(body) do
    {:ok, IO.iodata_to_binary(inflate_members(body, [], max_decompressed_bytes()))}
  rescue
    e in RequestTooLargeError -> reraise e, __STACKTRACE__
    _ -> {:error, :invalid_gzip}
  end

  defp inflate_members(<<>>, acc, _budget), do: acc

  defp inflate_members(body, acc, budget) do
    output = inflate_member(body, budget)
    size = IO.iodata_length(output)
    consumed = member_size(body, output)
    rest = binary_part(body, consumed, byte_size(body) - consumed)
    inflate_members(rest, [acc, output], budget - size)
  end

  defp inflate_member(body, budget) do
    z = :zlib.open()

    try do
      :zlib.inflateInit(z, 16 + 15)
      output = inflate(z, :zlib.safeInflate(z, body), [], 0, budget)
      # Raises `:data_error` when the stream ended early (no trailer).
      :zlib.inflateEnd(z)
      output
    after
      :zlib.close(z)
    end
  end

  defp inflate(z, {status, chunk}, acc, size, max) do
    size = size + IO.iodata_length(chunk)

    cond do
      size > max -> raise RequestTooLargeError
      status == :continue -> inflate(z, :zlib.safeInflate(z, []), [acc, chunk], size, max)
      true -> [acc, chunk]
    end
  end

  # zlib does not report how much input a member consumed, but a gzip member
  # always ends with `CRC32 || ISIZE` of its own output. The member ends at the
  # earliest occurrence of that trailer that is followed by end of input or
  # the next member's magic bytes.
  defp member_size(body, output) do
    trailer = <<:erlang.crc32(output)::little-32, rem(IO.iodata_length(output), 4_294_967_296)::little-32>>

    body
    |> :binary.matches(trailer)
    |> Enum.find_value(fn {pos, len} ->
      member_end = pos + len

      case binary_part(body, member_end, min(2, byte_size(body) - member_end)) do
        <<>> -> member_end
        <<0x1F, 0x8B>> -> member_end
        _ -> nil
      end
    end) || raise "gzip member boundary not found"
  end

  defp max_decompressed_bytes do
    :pulso
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:max_decompressed_bytes, @default_max_decompressed_bytes)
  end
end
