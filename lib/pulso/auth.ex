defmodule Pulso.Auth do
  @moduledoc """
  Boundary for tenant authorization checks.

  `verify/2` is called from ingest and read paths before Pulso attributes a
  request to a tenant. Implementations decide whether the caller is allowed
  to speak for the tenant they named; they do **not** decide which tenant a
  request belongs to (that is a routing concern, e.g. `X-Scope-OrgID`).

  Two implementations ship in-tree:

    * `Pulso.Auth.Open` — accepts everything. Default for dev and test.
    * `Pulso.Auth.SharedSecret` — requires an `Authorization: Bearer <token>`
      header that matches a per-tenant token from application env. Default
      for prod.

  The active implementation is read from `Application.get_env(:pulso,
  Pulso.Auth)[:module]` at call time so tests can swap it without
  recompiling.
  """

  alias Plug.Conn
  alias Pulso.Auth.Open

  @type reason :: :missing_token | :invalid_token | :unknown_tenant | term()

  @callback verify(Conn.t(), tenant :: String.t()) :: :ok | {:error, reason()}

  @spec verify(Conn.t(), String.t()) :: :ok | {:error, reason()}
  def verify(conn, tenant) when is_binary(tenant) do
    module().verify(conn, tenant)
  end

  @spec module() :: module()
  def module do
    case Application.get_env(:pulso, __MODULE__) do
      nil -> Open
      env -> Keyword.get(env, :module, Open)
    end
  end
end
