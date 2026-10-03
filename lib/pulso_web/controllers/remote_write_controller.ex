defmodule PulsoWeb.RemoteWriteController do
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.RemoteWrite.Push
  alias Pulso.Storage

  @default_tenant "default"
  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/

  # Cap on the decompressed protobuf body. Prometheus's upstream
  # `remote_write` server defaults to a few MiB; 16 MiB sits comfortably
  # above that and small enough that a single misconfigured or malicious
  # sender cannot exhaust the request process's heap. The NIF checks it
  # against the length varint in the Snappy block header before
  # allocating any output buffer.
  @max_decompressed_bytes 16 * 1024 * 1024

  # Hard cap on the compressed body read off the socket.
  @max_compressed_bytes 4 * 1024 * 1024

  @doc """
  Prometheus remote_write v1 receiver at `POST /api/v1/write`.

  Protocol contract (what Alloy / Grafana Agent / vanilla Prometheus
  will send unmodified):

    * `Content-Type: application/x-protobuf` (required, 415 otherwise).
    * `Content-Encoding: snappy` (required, 415 otherwise — the
      Prometheus spec is stricter than Loki's push here: an absent
      content-encoding is NOT treated as snappy).
    * `X-Prometheus-Remote-Write-Version: 0.1.0` (treated as advisory —
      we accept any `0.*` or missing header; a non-zero major is
      rejected with 400 since the wire shape is version-locked).
    * `X-Scope-OrgID: <tenant>` (optional; defaults to `"default"`).
    * `Idempotency-Key: <token>` (optional; propagated to storage).

  Status codes:

    * `204 No Content` on success. Prometheus retries on anything else.
    * `400` on invalid snappy, invalid protobuf, or invalid tenant name.
    * `401` on auth failures.
    * `413` if the body, record count, or attributes exceed ingest budgets.
    * `415` on wrong `Content-Type` or `Content-Encoding`.
    * `429` on storage backpressure (`:owner_overloaded`) — Prometheus
      interprets this as a retryable signal and backs off.
    * `503` on other transient storage failures.

  Rejected-sample count is surfaced in `X-Pulso-Rejected-Records` to
  mirror the Loki push convention.
  """
  def write(conn, _params) do
    tenant = tenant_from(conn)
    opts = append_opts(conn)

    with :ok <- validate_tenant(tenant),
         :ok <- Auth.verify(conn, tenant),
         :ok <- validate_content_type(conn),
         :ok <- validate_content_encoding(conn),
         :ok <- validate_version_header(conn),
         {:ok, body, conn} <- read_full_body(conn),
         {:ok, samples, rejected} <- Push.decode_protobuf(body, @max_decompressed_bytes),
         :ok = Pulso.SelfMetrics.records(:metrics, :rejected, rejected),
         :ok <- Storage.append(:metrics, tenant, samples, opts) do
      conn
      |> put_rejected_header(rejected)
      |> send_resp(:no_content, "")
    else
      {:error, :unsupported_content_type} ->
        conn |> put_status(:unsupported_media_type) |> json(%{error: "unsupported_content_type"})

      {:error, :unsupported_content_encoding} ->
        conn |> put_status(:unsupported_media_type) |> json(%{error: "unsupported_content_encoding"})

      {:error, :unsupported_remote_write_version} ->
        conn |> put_status(:bad_request) |> json(%{error: "unsupported_remote_write_version"})

      {:error, :invalid_snappy} ->
        conn |> put_status(:bad_request) |> json(%{error: "invalid_snappy"})

      {:error, :invalid_protobuf} ->
        conn |> put_status(:bad_request) |> json(%{error: "invalid_protobuf"})

      {:error, :payload_too_large} ->
        conn |> put_status(:request_entity_too_large) |> json(%{error: "payload_too_large"})

      {:error, reason} when reason in [:too_many_records, :attributes_too_large] ->
        conn |> put_status(:request_entity_too_large) |> json(%{error: to_string(reason)})

      {:error, {:invalid_tenant, _}} ->
        conn |> put_status(:bad_request) |> json(%{error: "invalid_tenant"})

      {:error, reason} when reason in [:missing_token, :invalid_token, :unknown_tenant] ->
        conn |> put_status(:unauthorized) |> json(%{error: to_string(reason)})

      {:error, :owner_overloaded} ->
        # 429 — Prometheus retries on 5xx and 429 with backoff. The
        # typed `:owner_overloaded` from `ManifestOwner.register_segments`
        # is explicit backpressure, not a server fault, so 429 is the
        # right surface.
        conn |> put_status(:too_many_requests) |> json(%{error: "backpressure"})

      {:error, {:encode_failed, _}} ->
        conn |> put_status(:bad_request) |> json(%{error: "encode_failed"})

      {:error, {:decode_failed, _}} ->
        conn |> put_status(:bad_request) |> json(%{error: "decode_failed"})

      {:error, reason} ->
        conn |> put_status(:service_unavailable) |> json(%{error: inspect(reason)})
    end
  end

  defp validate_content_type(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [] ->
        {:error, :unsupported_content_type}

      [ct | _] ->
        if protobuf?(ct), do: :ok, else: {:error, :unsupported_content_type}
    end
  end

  defp protobuf?(ct) do
    ct |> String.downcase() |> String.starts_with?("application/x-protobuf")
  end

  # The Prometheus remote_write v1 spec requires `Content-Encoding: snappy`.
  # Unlike Loki's push, an absent header is NOT accepted — rejecting it
  # keeps us conformant and surfaces a misconfigured sender immediately.
  defp validate_content_encoding(conn) do
    case Plug.Conn.get_req_header(conn, "content-encoding") do
      [enc] -> if String.downcase(enc) == "snappy", do: :ok, else: {:error, :unsupported_content_encoding}
      _ -> {:error, :unsupported_content_encoding}
    end
  end

  # `X-Prometheus-Remote-Write-Version: 0.1.0`. We accept any `0.*`
  # (and an absent header, since older forwarders omit it); a non-zero
  # major is a different wire shape so refuse rather than silently
  # mis-parse. Future v2 (OpenMetrics) support will key off this value.
  defp validate_version_header(conn) do
    case Plug.Conn.get_req_header(conn, "x-prometheus-remote-write-version") do
      [] ->
        :ok

      [v | _] ->
        if String.starts_with?(v, "0."), do: :ok, else: {:error, :unsupported_remote_write_version}
    end
  end

  defp read_full_body(conn) do
    case Plug.Conn.read_body(conn, length: @max_compressed_bytes) do
      {:ok, body, conn} -> {:ok, body, conn}
      {:more, _, _} -> {:error, :payload_too_large}
      {:error, _} = err -> err
    end
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
