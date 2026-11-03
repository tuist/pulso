defmodule Pulso.Auth do
  @moduledoc """
  Boundary for tenant authorization checks.

  `verify/3` is called from ingest and read paths before Pulso attributes a
  request to a tenant. Implementations (the `verify/2` callback) decide
  whether the caller is allowed to speak for the tenant they named; they do **not** decide which tenant a
  request belongs to (that is a routing concern, e.g. `X-Scope-OrgID`).

  Two implementations ship in-tree:

    * `Pulso.Auth.Open` — accepts everything. Default for dev and test.
    * `Pulso.Auth.SharedSecret` — requires an `Authorization: Bearer <token>`
      header that matches a per-tenant token from application env. Default
      for prod.

  `Pulso.Auth.WorkloadIdentity` additionally accepts Kubernetes projected
  ServiceAccount tokens bound to tenants and capabilities, alongside either
  module.

  The active implementation is read from `Runtime.get_env(:pulso,
  Pulso.Auth)[:module]` at call time so tests can swap it without
  recompiling.
  """

  alias Plug.Conn
  alias Pulso.Auth.WorkloadIdentity
  alias Pulso.Runtime

  @type reason :: :missing_token | :invalid_token | :unknown_tenant | :forbidden | term()

  @callback verify(Conn.t(), tenant :: String.t()) :: :ok | {:error, reason()}

  @doc """
  Authorizes the request for `tenant` and `capability`: `:read` for query and
  MCP paths, `:ingest` for push/write paths.

  A Bearer JWT is verified by `Pulso.Auth.WorkloadIdentity` when workload
  identity trust is configured; its bindings decide the capability. Every other
  credential goes to the configured module, whose tenant tokens grant both
  capabilities as they always have.
  """
  @spec verify(Conn.t(), String.t(), WorkloadIdentity.capability()) :: :ok | {:error, reason()}
  def verify(conn, tenant, capability) when is_binary(tenant) and capability in [:read, :ingest] do
    with trust when trust != [] <- WorkloadIdentity.trust(),
         {:ok, token} <- bearer(conn),
         true <- WorkloadIdentity.jwt?(token) do
      WorkloadIdentity.verify(token, tenant, capability, trust)
    else
      _ -> module().verify(conn, tenant)
    end
  end

  defp bearer(conn) do
    with [header] <- Conn.get_req_header(conn, "authorization"),
         [scheme, token] when token != "" <- String.split(header, " ", parts: 2),
         "bearer" <- String.downcase(scheme) do
      {:ok, token}
    end
  end

  @doc "Decoded HTTP Basic credentials, or `:error` when the request has none."
  @spec basic_credentials(Conn.t()) :: {:ok, String.t(), String.t()} | :error
  def basic_credentials(conn) do
    with [header] <- Conn.get_req_header(conn, "authorization"),
         [scheme, encoded] <- String.split(header, " ", parts: 2),
         "basic" <- String.downcase(scheme),
         {:ok, decoded} <- Base.decode64(String.trim(encoded)),
         [username, password] when username != "" and password != "" <- :binary.split(decoded, ":") do
      {:ok, username, password}
    else
      _ -> :error
    end
  end

  @spec module() :: module()
  def module do
    case Runtime.get_env(:pulso, __MODULE__) do
      nil ->
        raise """
        Pulso.Auth is not configured. This should never happen — config/config.exs
        sets `Pulso.Auth.Open` as the compile-time default. Refusing to accept
        traffic rather than fail open.
        """

      env ->
        case Keyword.get(env, :module) do
          nil ->
            raise "Pulso.Auth :module key is not set. See config/config.exs."

          :must_configure_at_runtime ->
            raise """
            Pulso.Auth is still set to the prod sentinel `:must_configure_at_runtime`.
            runtime.exs must set `config :pulso, Pulso.Auth, module: Pulso.Auth.SharedSecret, tokens: %{...}`
            before the app accepts traffic.
            """

          mod when is_atom(mod) ->
            mod
        end
    end
  end
end
