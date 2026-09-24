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
  def logs(conn, params) do
    tenant = tenant_from(conn)

    with :ok <- Auth.verify(conn, tenant),
         records = Logs.decode(params),
         :ok <- Storage.append(tenant, records) do
      json(conn, %{})
    else
      {:error, reason} when reason in [:missing_token, :invalid_token, :unknown_tenant] ->
        conn
        |> put_status(:unauthorized)
        |> json(%{error: to_string(reason)})

      {:error, {:invalid_tenant, _} = reason} ->
        conn
        |> put_status(:bad_request)
        |> json(%{error: inspect(reason)})

      {:error, reason} ->
        conn
        |> put_status(:internal_server_error)
        |> json(%{error: inspect(reason)})
    end
  end

  defp tenant_from(conn) do
    case Plug.Conn.get_req_header(conn, "x-scope-orgid") do
      [tenant | _] when is_binary(tenant) and tenant != "" -> tenant
      _ -> @default_tenant
    end
  end
end
