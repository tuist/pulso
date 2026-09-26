defmodule PulsoWeb.LokiController do
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.Loki.Push
  alias Pulso.Storage

  @default_tenant "default"

  @doc """
  Loki push receiver at `POST /loki/api/v1/push`.

  Accepts the JSON push wire format Alloy and Promtail can be configured
  to emit. Gzip content encoding is decompressed transparently by
  `PulsoWeb.CompressedBodyReader` before this action runs. Snappy-framed
  protobuf (Alloy's default push_config) is not yet supported and is
  rejected with 415.

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

    with :ok <- validate_content_type(conn),
         :ok <- validate_tenant(tenant),
         :ok <- Auth.verify(conn, tenant),
         {records, rejected} = Push.decode(params),
         :ok <- Storage.append(tenant, records, opts) do
      conn
      |> put_rejected_header(rejected)
      |> send_resp(:no_content, "")
    else
      {:error, :unsupported_content_type} ->
        conn
        |> put_status(:unsupported_media_type)
        |> json(%{error: "unsupported_content_type"})

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

  # We only speak JSON on this path today. A request with
  # `application/x-protobuf` (Alloy/Promtail's default) is rejected
  # explicitly rather than silently treated as JSON, which would
  # otherwise 400 on the parser — the 415 tells the operator this is a
  # missing feature, not a malformed body.
  defp validate_content_type(conn) do
    case Plug.Conn.get_req_header(conn, "content-type") do
      [] -> :ok
      [ct | _] -> if json_content_type?(ct), do: :ok, else: {:error, :unsupported_content_type}
    end
  end

  defp json_content_type?(ct) do
    ct
    |> String.downcase()
    |> String.starts_with?("application/json")
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
