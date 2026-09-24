defmodule Pulso.AuthTest do
  use ExUnit.Case, async: false

  alias Pulso.Auth
  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret

  setup do
    on_exit(fn -> Application.delete_env(:pulso, Auth) end)
    :ok
  end

  defp conn(headers \\ []) do
    Enum.reduce(headers, %Plug.Conn{}, fn {k, v}, c ->
      Plug.Conn.put_req_header(c, k, v)
    end)
  end

  describe "Pulso.Auth.module/0" do
    test "falls back to Pulso.Auth.Open when nothing is configured" do
      Application.delete_env(:pulso, Auth)
      assert Auth.module() == Open
    end

    test "returns the configured module" do
      Application.put_env(:pulso, Auth, module: SharedSecret)
      assert Auth.module() == SharedSecret
    end
  end

  describe "Pulso.Auth.Open" do
    test "accepts any tenant on any conn" do
      assert Open.verify(conn(), "acme") == :ok
      assert Open.verify(conn(), "") == :ok
    end
  end

  describe "Pulso.Auth.SharedSecret" do
    setup do
      token = "the-secret"
      hex = Base.encode16(:crypto.hash(:sha256, token), case: :lower)
      Application.put_env(:pulso, Auth, tokens: %{"acme" => "sha256$#{hex}"})
      {:ok, token: token}
    end

    test "accepts the correct bearer token", %{token: token} do
      assert SharedSecret.verify(conn([{"authorization", "Bearer #{token}"}]), "acme") == :ok
    end

    test "accepts lowercase 'bearer' too", %{token: token} do
      assert SharedSecret.verify(conn([{"authorization", "bearer #{token}"}]), "acme") == :ok
    end

    test "rejects a wrong token with :invalid_token" do
      assert SharedSecret.verify(conn([{"authorization", "Bearer wrong"}]), "acme") ==
               {:error, :invalid_token}
    end

    test "rejects a missing header with :missing_token" do
      assert SharedSecret.verify(conn(), "acme") == {:error, :missing_token}
    end

    test "rejects an empty bearer with :missing_token" do
      assert SharedSecret.verify(conn([{"authorization", "Bearer "}]), "acme") ==
               {:error, :missing_token}
    end

    test "rejects a tenant with no configured token as :unknown_tenant", %{token: token} do
      assert SharedSecret.verify(conn([{"authorization", "Bearer #{token}"}]), "other") ==
               {:error, :unknown_tenant}
    end

    test "rejects a malformed stored value as :invalid_token" do
      Application.put_env(:pulso, Auth, tokens: %{"acme" => "plaintext-not-hashed"})

      assert SharedSecret.verify(conn([{"authorization", "Bearer whatever"}]), "acme") ==
               {:error, :invalid_token}
    end
  end
end
