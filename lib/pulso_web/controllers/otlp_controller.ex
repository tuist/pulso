defmodule PulsoWeb.OTLPController do
  use PulsoWeb, :controller

  alias Pulso.OTLP.Logs
  alias Pulso.Storage

  @default_tenant "default"

  @doc """
  OTLP/HTTP JSON logs receiver at `POST /v1/logs`.

  Tenant is taken from the `X-Scope-OrgID` header (Loki/Cortex convention) and
  defaults to `"default"` when absent. The success response is the empty
  `ExportLogsServiceResponse` object per the OTLP spec.
  """
  def logs(conn, params) do
    tenant = tenant_from(conn)
    records = Logs.decode(params)

    case Storage.append(tenant, records) do
      :ok -> json(conn, %{})
      {:error, reason} -> conn |> put_status(:internal_server_error) |> json(%{error: inspect(reason)})
    end
  end

  defp tenant_from(conn) do
    case Plug.Conn.get_req_header(conn, "x-scope-orgid") do
      [tenant | _] when is_binary(tenant) and tenant != "" -> tenant
      _ -> @default_tenant
    end
  end
end
