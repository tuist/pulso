defmodule PulsoWeb.OTLPController do
  use PulsoWeb, :controller

  alias Pulso.Auth
  alias Pulso.OTLP.Logs
  alias Pulso.Storage

  @default_tenant "default"

  @doc """
  OTLP/HTTP JSON logs receiver at `POST /v1/logs`.

  Tenant is taken from the `X-Scope-OrgID` header (Loki/Cortex convention) and
  defaults to `"default"` when absent. The caller is then verified against
  the configured `Pulso.Auth` module: `Pulso.Auth.Open` in dev/test accepts
  everything; `Pulso.Auth.SharedSecret` in prod requires a bearer token.

  On success, the response body is the empty `ExportLogsServiceResponse`
  object per the OTLP spec.
  """
  # A conservative tenant charset; mirrors `Pulso.Storage.S3`. Validating
  # here (before auth) is what makes a `bad/name` tenant come back as 400
  # rather than 401 or 500 — the storage layer would still reject it, but
  # by then the request has already spent an auth check on a value we know
  # is invalid.
  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/

  def logs(conn, params) do
    tenant = tenant_from(conn)
    opts = append_opts(conn)

    with :ok <- validate_tenant(tenant),
         :ok <- Auth.verify(conn, tenant),
         records = Logs.decode(params),
         :ok <- Storage.append(tenant, records, opts) do
      json(conn, %{})
    else
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
end
