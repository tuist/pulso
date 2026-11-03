defmodule Pulso.Auth.WorkloadIdentityTest do
  use PulsoWeb.ConnCase, async: true

  import ExUnit.CaptureLog

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.Auth.WorkloadIdentity
  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  @moduletag :capture_log

  @issuer "https://kubernetes.default.svc.cluster.local"
  @subject "system:serviceaccount:atlas-production:atlas"
  @kid "atlas-key"

  setup_all do
    # Two clusters sharing kubeadm's default issuer: only the pinned key differs.
    {:ok,
     trusted: :public_key.generate_key({:rsa, 2048, 65_537}), other: :public_key.generate_key({:rsa, 2048, 65_537})}
  end

  setup %{trusted: trusted} do
    Memory.reset()
    trust = WorkloadIdentity.parse!([entry(trusted)])
    Pulso.Runtime.put_env(:pulso, WorkloadIdentity, trust: trust)

    digest = Base.encode16(:crypto.hash(:sha256, "shared-token"), case: :lower)
    Pulso.Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{"tuist" => "sha256$" <> digest})
    {:ok, trust: trust}
  end

  defp entry(key, bindings \\ [%{"subject" => @subject, "tenant" => "tuist", "capabilities" => ["read"]}]) do
    %{
      "name" => "atlas-production",
      "issuer" => @issuer,
      "audiences" => ["pulso.tuist.dev"],
      "max_token_ttl_seconds" => 3600,
      "keys" => %{"type" => "inline", "jwks" => %{"keys" => [jwk(key, @kid)]}},
      "bindings" => bindings
    }
  end

  defp jwk({:RSAPrivateKey, _, n, e, _, _, _, _, _, _, _}, kid) do
    encode = &Base.url_encode64(:binary.encode_unsigned(&1), padding: false)
    %{"kty" => "RSA", "kid" => kid, "alg" => "RS256", "use" => "sig", "n" => encode.(n), "e" => encode.(e)}
  end

  defp now, do: System.os_time(:second)

  defp claims(overrides \\ %{}) do
    iat = now()

    Map.merge(
      %{
        "iss" => @issuer,
        "aud" => ["pulso.tuist.dev"],
        "sub" => @subject,
        "iat" => iat,
        "nbf" => iat,
        "exp" => iat + 3600,
        "kubernetes.io" => %{
          "namespace" => "atlas-production",
          "serviceaccount" => %{"name" => "atlas", "uid" => "c0ffee"},
          "pod" => %{"name" => "atlas-0", "uid" => "f00d"}
        }
      },
      overrides
    )
  end

  defp sign(key, claims, header \\ %{"alg" => "RS256", "kid" => @kid}) do
    input = b64(JSON.encode!(header)) <> "." <> b64(JSON.encode!(claims))
    input <> "." <> b64(:public_key.sign(input, :sha256, key))
  end

  defp b64(data), do: Base.url_encode64(data, padding: false)

  defp conn_with(token, tenant \\ "tuist") do
    build_conn()
    |> put_req_header("authorization", "Bearer " <> token)
    |> put_req_header("x-scope-orgid", tenant)
  end

  defp reason(token, trust, at \\ now()) do
    {:error, {reason, _trust}} = WorkloadIdentity.authenticate(token, trust, at)
    reason
  end

  test "a valid token reads its bound tenant only", %{trusted: key} do
    token = sign(key, claims())
    assert Auth.verify(conn_with(token), "tuist", :read) == :ok
    assert Auth.verify(conn_with(token), "other", :read) == {:error, :forbidden}
  end

  test "a read-only principal is refused on ingest, over the API too", %{trusted: key} do
    token = sign(key, claims())
    assert Auth.verify(conn_with(token), "tuist", :ingest) == {:error, :forbidden}

    conn =
      token
      |> conn_with()
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", %{
        "streams" => [%{"stream" => %{"service" => "api"}, "values" => [["1700000000000000000", "hi"]]}]
      })

    assert json_response(conn, 403) == %{"error" => "forbidden"}
    assert {:ok, []} = Storage.query(:logs, "tuist")
  end

  test "shared-secret tokens keep granting both capabilities alongside JWT principals" do
    conn =
      "shared-token"
      |> conn_with()
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", %{
        "streams" => [%{"stream" => %{"service" => "api"}, "values" => [["1700000000000000000", "hi"]]}]
      })

    assert conn.status == 204
    assert {:ok, [%Log{body: "hi"}]} = Storage.query(:logs, "tuist")
    assert Auth.verify(conn_with("shared-token"), "tuist", :read) == :ok
    assert Auth.verify(conn_with("wrong"), "tuist", :read) == {:error, :invalid_token}
  end

  test "ingest bindings authorize ingest", %{trusted: key} do
    binding = %{"subject" => @subject, "tenant" => "tuist", "capabilities" => ["ingest"]}
    trust = WorkloadIdentity.parse!([entry(key, [binding])])
    token = sign(key, claims())
    assert WorkloadIdentity.verify(token, "tuist", :ingest, trust) == :ok
    assert WorkloadIdentity.verify(token, "tuist", :read, trust) == {:error, :forbidden}
  end

  test "the pinned key, not the shared issuer, selects the trust entry", %{trusted: a, other: b} do
    bind = fn tenant -> [%{"subject" => @subject, "tenant" => tenant, "capabilities" => ["read"]}] end
    entry_b = put_in(entry(b, bind.("b")), ["keys", "jwks", "keys"], [jwk(b, "other-key")])
    trust = WorkloadIdentity.parse!([entry(a, bind.("a")), Map.put(entry_b, "name", "other-cluster")])
    token_a = sign(a, claims())
    token_b = sign(b, claims(), %{"alg" => "RS256", "kid" => "other-key"})

    assert WorkloadIdentity.verify(token_a, "a", :read, trust) == :ok
    assert WorkloadIdentity.verify(token_a, "b", :read, trust) == {:error, :forbidden}
    assert WorkloadIdentity.verify(token_b, "b", :read, trust) == :ok
    assert WorkloadIdentity.verify(token_b, "a", :read, trust) == {:error, :forbidden}
    # Key B signing under entry A's kid does not borrow entry A's bindings.
    assert reason(sign(b, claims()), trust) == :invalid_signature
  end

  test "read paths answer 403 for an authenticated principal without a read binding", %{trusted: key} do
    binding = %{"subject" => @subject, "tenant" => "tuist", "capabilities" => ["ingest"]}
    Pulso.Runtime.put_env(:pulso, WorkloadIdentity, trust: WorkloadIdentity.parse!([entry(key, [binding])]))
    token = sign(key, claims())

    for path <- ["/api/v1/query?query=1", "/api/v1/labels", "/loki/api/v1/labels"] do
      assert token |> conn_with() |> get(path) |> Map.fetch!(:status) == 403, path
    end

    assert "wrong" |> conn_with() |> get("/api/v1/query?query=1") |> Map.fetch!(:status) == 401
  end

  test "wrong audience and issuer are rejected", %{trusted: key, trust: trust} do
    token = sign(key, claims(%{"aud" => ["https://kubernetes.default.svc"]}))
    assert Auth.verify(conn_with(token), "tuist", :read) == {:error, :invalid_token}
    assert reason(token, trust) == :bad_audience
    assert reason(sign(key, claims(%{"aud" => "pulso.tuist.dev.evil"})), trust) == :bad_audience
    assert reason(sign(key, claims(%{"iss" => "https://other"})), trust) == :bad_issuer
  end

  test "expiry, not-before and lifetime are enforced with 60 s of skew", %{trusted: key, trust: trust} do
    token = sign(key, claims())
    exp = now() + 3600

    assert {:ok, _} = WorkloadIdentity.authenticate(token, trust, exp + 59)
    assert reason(token, trust, exp + 60) == :token_expired
    assert Auth.verify(conn_with(sign(key, claims(%{"exp" => now() - 61}))), "tuist", :read) == {:error, :invalid_token}

    assert reason(sign(key, claims(%{"nbf" => now() + 120})), trust) == :token_not_yet_valid
    assert reason(sign(key, claims(%{"iat" => now() + 120, "exp" => now() + 600})), trust) == :token_not_yet_valid
    assert reason(sign(key, claims(%{"exp" => now() + 3601})), trust) == :token_ttl_exceeded
    assert reason(sign(key, Map.delete(claims(), "exp")), trust) == :bad_time_claims
    assert reason(sign(key, claims(%{"exp" => (now() + 60) * 1.0})), trust) == :bad_time_claims
  end

  test "a key from another cluster with the same issuer string is rejected", %{other: other, trust: trust} do
    # Same issuer, audience, subject and even kid: only the signing key differs.
    forged = sign(other, claims())
    assert Auth.verify(conn_with(forged), "tuist", :read) == {:error, :invalid_token}
    assert reason(forged, trust) == :invalid_signature

    assert reason(sign(other, claims(), %{"alg" => "RS256", "kid" => "other-cluster"}), trust) == :unknown_kid
    assert reason(sign(other, claims(), %{"alg" => "RS256"}), trust) == :missing_kid
  end

  test "tampered payloads and non-RS256 algorithms are rejected", %{trusted: key, trust: trust} do
    [h, _p, s] = String.split(sign(key, claims()), ".")

    tampered =
      Enum.join([h, b64(JSON.encode!(claims(%{"sub" => "system:serviceaccount:atlas-production:admin"}))), s], ".")

    assert reason(tampered, trust) == :invalid_signature

    payload = b64(JSON.encode!(claims()))

    for alg <- ["none", "HS256", "RS512", "ES256", "PS256"] do
      header = b64(JSON.encode!(%{"alg" => alg, "kid" => @kid}))
      token = header <> "." <> payload <> "." <> b64("sig")
      assert reason(token, trust) == :unsupported_algorithm
      assert Auth.verify(conn_with(token), "tuist", :read) == {:error, :invalid_token}
    end

    assert reason(sign(key, claims(), %{"alg" => "RS256", "kid" => @kid, "crit" => ["exp"]}), trust) ==
             :unsupported_header
  end

  test "unknown subjects and inconsistent ServiceAccount claims are rejected", %{trusted: key, trust: trust} do
    other = "system:serviceaccount:atlas-production:worker"

    token =
      sign(
        key,
        claims(%{
          "sub" => other,
          "kubernetes.io" => %{"namespace" => "atlas-production", "serviceaccount" => %{"name" => "worker"}}
        })
      )

    assert reason(token, trust) == :unknown_subject
    assert Auth.verify(conn_with(token), "tuist", :read) == {:error, :invalid_token}

    assert reason(sign(key, claims(%{"sub" => "atlas"})), trust) == :not_service_account
    assert reason(sign(key, claims(%{"sub" => "system:serviceaccount:a:b:c"})), trust) == :not_service_account

    assert reason(
             sign(key, claims(%{"kubernetes.io" => %{"namespace" => "x", "serviceaccount" => %{"name" => "atlas"}}})),
             trust
           ) ==
             :bad_kubernetes_claims
  end

  test "rejections never log the token", %{other: other} do
    forged = sign(other, claims())
    log = capture_log(fn -> Auth.verify(conn_with(forged), "tuist", :read) end)
    assert log =~ "invalid_signature"
    refute log =~ forged
    refute log =~ hd(String.split(forged, "."))
  end

  test "without trust configuration JWT-shaped bearers go to the shared secret", %{trusted: key} do
    Pulso.Runtime.put_env(:pulso, WorkloadIdentity, trust: [])
    assert Auth.verify(conn_with(sign(key, claims())), "tuist", :read) == {:error, :invalid_token}
  end

  test "configuration fails closed at boot", %{trusted: key} do
    valid = entry(key)
    jwk = jwk(key, @kid)
    with_keys = fn keys -> put_in(valid, ["keys", "jwks", "keys"], keys) end
    {:RSAPrivateKey, _, _n, _e, d, _, _, _, _, _, _} = key
    {:RSAPrivateKey, _, small_n, _, _, _, _, _, _, _, _} = :public_key.generate_key({:rsa, 1024, 65_537})

    invalid = [
      Map.delete(valid, "audiences"),
      Map.put(valid, "audiences", []),
      Map.put(valid, "unexpected", true),
      Map.put(valid, "max_token_ttl_seconds", 31_536_000),
      Map.put(valid, "keys", %{"type" => "url", "url" => "https://kubernetes.default.svc/openid/v1/jwks"}),
      Map.put(valid, "bindings", []),
      Map.put(valid, "bindings", [%{"subject" => @subject, "tenant" => "tuist", "capabilities" => ["admin"]}]),
      Map.put(valid, "bindings", [%{"subject" => "atlas", "tenant" => "tuist", "capabilities" => ["read"]}]),
      with_keys.([Map.put(jwk, "d", Base.url_encode64(:binary.encode_unsigned(d), padding: false))]),
      with_keys.([Map.put(jwk, "alg", "HS256")]),
      # e = 1 would make every signature trivially forgeable.
      with_keys.([Map.put(jwk, "e", "AQ")]),
      with_keys.([Map.put(jwk, "n", Base.url_encode64(:binary.encode_unsigned(small_n), padding: false))]),
      with_keys.([jwk, jwk])
    ]

    for config <- invalid do
      assert_raise ArgumentError, ~r/PULSO_WORKLOAD_IDENTITY_JSON/, fn -> WorkloadIdentity.parse!([config]) end
    end

    assert_raise ArgumentError, ~r/unique/, fn -> WorkloadIdentity.parse!([valid, Map.put(valid, "name", "b")]) end

    assert [%{max_token_ttl_seconds: 3600, keys: [%{size: 256}]}] =
             WorkloadIdentity.parse!([Map.delete(valid, "max_token_ttl_seconds")])

    # The JWKS can be pasted verbatim from /openid/v1/jwks as a JSON string.
    as_string = put_in(valid, ["keys", "jwks"], JSON.encode!(%{"keys" => [jwk]}))
    assert [%{keys: [%{kid: @kid}]} = parsed] = WorkloadIdentity.parse!([as_string])
    assert WorkloadIdentity.verify(sign(key, claims()), "tuist", :read, [parsed]) == :ok
    assert_raise ArgumentError, fn -> WorkloadIdentity.parse!([put_in(valid, ["keys", "jwks"], "{")]) end

    # YAML reads a bare `n` key as false.
    yaml_n = jwk |> Map.delete("n") |> Map.put("false", jwk["n"])
    assert_raise ArgumentError, ~r/quote "n"/, fn -> WorkloadIdentity.parse!([with_keys.([yaml_n])]) end
  end

  test "runtime.exs reads PULSO_WORKLOAD_IDENTITY_JSON", %{trusted: key} do
    path = Path.expand("../../../config/runtime.exs", __DIR__)
    Pulso.Runtime.put_env("PULSO_WORKLOAD_IDENTITY_JSON", JSON.encode!([entry(key)]))
    config = Config.Reader.read!(path, env: :test, target: :host)
    assert [%{name: "atlas-production", issuer: @issuer}] = config[:pulso][WorkloadIdentity][:trust]

    Pulso.Runtime.put_env("PULSO_WORKLOAD_IDENTITY_JSON", "{")
    assert_raise RuntimeError, fn -> Config.Reader.read!(path, env: :test, target: :host) end
  end
end
