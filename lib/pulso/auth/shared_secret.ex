defmodule Pulso.Auth.SharedSecret do
  @moduledoc """
  Per-tenant Bearer-token authentication. A minimal but real auth surface
  suitable for prod until Pulso grows a real accounts service.

  Tokens are configured as:

      config :pulso, Pulso.Auth,
        module: Pulso.Auth.SharedSecret,
        tokens: %{"acme" => "sha256$<hex>", "beta" => "sha256$<hex>"}

  Values are of the form `"<algo>$<hex>"`. Only `sha256` is accepted today.
  Comparison is constant-time (`Plug.Crypto.secure_compare/2`) so token
  presence cannot be inferred from response timing.

  A tenant with no configured token is rejected as `:unknown_tenant`. Empty
  tokens are rejected as `:invalid_token` so an accidentally-blank env var
  cannot turn into "auth off for this tenant".
  """

  @behaviour Pulso.Auth

  alias Plug.Conn

  @impl Pulso.Auth
  def verify(conn, tenant) when is_binary(tenant) do
    with {:ok, presented} <- extract_token(conn),
         {:ok, stored_hash} <- fetch_stored_hash(tenant) do
      compare_hash(presented, stored_hash)
    end
  end

  defp extract_token(conn) do
    case Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] when byte_size(token) > 0 -> {:ok, token}
      ["bearer " <> token] when byte_size(token) > 0 -> {:ok, token}
      _ -> {:error, :missing_token}
    end
  end

  defp fetch_stored_hash(tenant) do
    tokens =
      case Application.get_env(:pulso, Pulso.Auth) do
        nil -> %{}
        env -> Keyword.get(env, :tokens, %{})
      end

    case Map.fetch(tokens, tenant) do
      {:ok, "sha256$" <> hex} when byte_size(hex) == 64 -> {:ok, hex}
      {:ok, _malformed} -> {:error, :invalid_token}
      :error -> {:error, :unknown_tenant}
    end
  end

  defp compare_hash(presented, stored_hex) do
    computed_hex =
      :sha256
      |> :crypto.hash(presented)
      |> Base.encode16(case: :lower)

    if Plug.Crypto.secure_compare(computed_hex, stored_hex) do
      :ok
    else
      {:error, :invalid_token}
    end
  end
end
