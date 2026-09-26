defmodule PulsoWeb.LokiController do
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.Loki.Push
  alias Pulso.Loki.PushProto
  alias Pulso.Storage

  @default_tenant "default"

  # Cap on the decompressed protobuf body. Loki's default upstream limit
  # is a few MiB per push; 16 MiB is comfortably above that and small
  # enough that a single misconfigured or malicious sender cannot
  # exhaust the request process's heap. Enforced twice: once against
  # the length varint in the Snappy block header (before we allocate
  # any output buffer), then once against the actual decompressed
  # bytes as a belt-and-braces check.
  @max_decompressed_bytes 16 * 1024 * 1024

  # Hard cap on the compressed body read off the socket. Set well
  # above @max_decompressed_bytes / 32× (a plausible upper bound for
  # protobuf-in-snappy ratios) so a legitimate 16 MiB uncompressed
  # payload always fits, and low enough that a runaway sender cannot
  # buffer arbitrary amounts of memory before we see the length header.
  @max_compressed_bytes 4 * 1024 * 1024

  @doc """
  Loki push receiver at `POST /loki/api/v1/push`.

  Accepts two wire formats:

    * `application/json` — the JSON push, optionally with
      `Content-Encoding: gzip`. Gzip is unwrapped by
      `PulsoWeb.CompressedBodyReader` before `Plug.Parsers` runs, and
      the parsed map lands here as `params`.

    * `application/x-protobuf` — Alloy and Promtail's default. The body
      is a Snappy-compressed (raw block format) protobuf `PushRequest`.
      `Content-Encoding: snappy` and an absent content-encoding both
      mean "snappy" per the Loki convention; gzip/deflate over
      protobuf is rejected with 415.

  Tenant, auth, and idempotency semantics match the OTLP receiver:
  tenant from `X-Scope-OrgID` (default `"default"`), verified via
  `Pulso.Auth`, and `Idempotency-Key` propagated to the storage backend
  so a retry does not duplicate.

  On success the response is `204 No Content` per the Loki push
  convention — no body, no partial-success envelope. If any records
  were rejected during decoding, the count is surfaced in the
  `X-Pulso-Rejected-Records` response header so senders that want to
  monitor decode drops can, without breaking clients that expect a
  bodyless 204.
  """
  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/

  def push(conn, params) do
    tenant = tenant_from(conn)
    opts = append_opts(conn)

    with :ok <- validate_tenant(tenant),
         :ok <- Auth.verify(conn, tenant),
         {:ok, records, rejected, conn} <- decode_body(conn, params),
         :ok <- Storage.append(tenant, records, opts) do
      conn
      |> put_rejected_header(rejected)
      |> send_resp(:no_content, "")
    else
      {:error, :unsupported_content_type, conn} ->
        conn
        |> put_status(:unsupported_media_type)
        |> json(%{error: "unsupported_content_type"})

      {:error, :invalid_snappy, conn} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "invalid_snappy"})

      {:error, :invalid_protobuf, conn} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "invalid_protobuf"})

      {:error, :payload_too_large, conn} ->
        conn
        |> put_status(:request_entity_too_large)
        |> json(%{error: "payload_too_large"})

      {:error, {:invalid_tenant, _}} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "invalid_tenant"})

      {:error, reason} when reason in [:missing_token, :invalid_token, :unknown_tenant] ->
        conn
        |> put_status(:unauthorized)
        |> json(%{error: to_string(reason)})

      {:error, {:encode_failed, _}} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "encode_failed"})

      {:error, {:attribute_key_collision, _}} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: "attribute_key_collision"})

      {:error, reason} ->
        conn
        |> put_status(:internal_server_error)
        |> json(%{error: inspect(reason)})
    end
  end

  # Content-type dispatch. `Plug.Parsers` already handled JSON (and its
  # gzip variant via CompressedBodyReader) — for that path the map is
  # already in `params`. Protobuf is not parsed by Plug.Parsers, so its
  # bytes are still on the socket and we read them here.
  defp decode_body(conn, params) do
    case content_type(conn) do
      :json ->
        {records, rejected} = Push.decode(params)
        {:ok, records, rejected, conn}

      :protobuf ->
        with :ok <- validate_protobuf_encoding(conn),
             {:ok, body, conn} <- read_full_body(conn),
             :ok <- guard_advertised_size(body),
             {:ok, decoded} <- snappy_decode(body),
             :ok <- guard_size(decoded),
             {:ok, request} <- proto_decode(decoded) do
          {records, rejected} = Push.decode_proto(request)
          {:ok, records, rejected, conn}
        else
          {:error, reason} -> {:error, reason, conn}
        end

      :unsupported ->
        {:error, :unsupported_content_type, conn}
    end
  end

  # JSON when the header is absent — Plug.Parsers picks its parser off
  # the header, and with none we treat the request as JSON to match
  # the pre-existing behavior.
  defp content_type(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [] ->
        :json

      [ct | _] ->
        cond do
          json_content_type?(ct) -> :json
          protobuf_content_type?(ct) -> :protobuf
          true -> :unsupported
        end
    end
  end

  defp json_content_type?(ct) do
    ct
    |> String.downcase()
    |> String.starts_with?("application/json")
  end

  defp protobuf_content_type?(ct) do
    ct
    |> String.downcase()
    |> String.starts_with?("application/x-protobuf")
  end

  # Loki's protobuf path treats both `Content-Encoding: snappy` and an
  # absent content-encoding as snappy. Gzip and deflate are declared
  # unsupported: we could add them, but Alloy never sends them, and
  # returning 415 makes the "missing feature" case explicit rather than
  # letting an unexpected encoding surface as an invalid_snappy 400.
  defp validate_protobuf_encoding(conn) do
    case Plug.Conn.get_req_header(conn, "content-encoding") do
      [] -> :ok
      [enc] -> if String.downcase(enc) == "snappy", do: :ok, else: :unsupported_content_type
      _ -> :unsupported_content_type
    end
    |> case do
      :ok -> :ok
      :unsupported_content_type -> {:error, :unsupported_content_type}
    end
  end

  defp read_full_body(conn) do
    case Plug.Conn.read_body(conn, length: @max_compressed_bytes) do
      {:ok, body, conn} ->
        {:ok, body, conn}

      {:more, _chunk, _conn} ->
        # Plug returns `:more` only when a single read filled its
        # `length` window — i.e. the compressed body is above our cap.
        # A snappy-compressed Loki push above this cap has no
        # legitimate shape, so refuse rather than buffer arbitrary
        # amounts.
        {:error, :payload_too_large}

      {:error, _} = err ->
        err
    end
  end

  # Snappy's block format begins with a varint that carries the
  # uncompressed size. Peeking at it before `decompress/1` lets us
  # refuse a "snappy bomb" — a highly compressible payload whose
  # decompressed output would blow through @max_decompressed_bytes —
  # before we allocate an output buffer.
  defp guard_advertised_size(body) do
    case :snappyer.uncompressed_length(body) do
      {:ok, size} when size <= @max_decompressed_bytes -> :ok
      {:ok, _size} -> {:error, :payload_too_large}
      {:error, _} -> {:error, :invalid_snappy}
    end
  rescue
    _ -> {:error, :invalid_snappy}
  end

  defp snappy_decode(body) do
    case :snappyer.decompress(body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _} -> {:error, :invalid_snappy}
    end
  rescue
    _ -> {:error, :invalid_snappy}
  end

  # Belt-and-braces check on the actual decompressed size. In practice
  # `guard_advertised_size/1` already rejected anything above the cap,
  # but the length header is untrusted input and this guarantees the
  # buffer we hand to the protobuf decoder is bounded.
  defp guard_size(body) when byte_size(body) > @max_decompressed_bytes, do: {:error, :payload_too_large}

  defp guard_size(_), do: :ok

  defp proto_decode(bytes) do
    case PushProto.PushRequest.decode(bytes) do
      {:ok, request} -> {:ok, request}
      {:error, _} -> {:error, :invalid_protobuf}
    end
  rescue
    _ -> {:error, :invalid_protobuf}
  end

  defp validate_tenant(tenant) do
    if Regex.match?(@tenant_regex, tenant),
      do: :ok,
      else: {:error, {:invalid_tenant, tenant}}
  end

  defp tenant_from(conn) do
    case Plug.Conn.get_req_header(conn, "x-scope-orgid") do
      [tenant | _] when is_binary(tenant) and tenant != "" -> tenant
      _ -> @default_tenant
    end
  end

  defp append_opts(conn) do
    case Plug.Conn.get_req_header(conn, "idempotency-key") do
      [key | _] when is_binary(key) and byte_size(key) > 0 -> [idempotency_key: key]
      _ -> []
    end
  end

  defp put_rejected_header(conn, 0), do: conn

  defp put_rejected_header(conn, rejected) do
    Plug.Conn.put_resp_header(conn, "x-pulso-rejected-records", Integer.to_string(rejected))
  end
end
