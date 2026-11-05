defmodule Pulso.Alerting.PrincipalTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Alerting.Principal

  @runtime Path.expand("../../../config/runtime.exs", __DIR__)
  @docs Path.expand("../../../docs/alerting.md", __DIR__)

  test "runtime.exs accepts every documented capability and Principal authorizes each" do
    documented =
      Regex.scan(~r/^\| `(alert:[a-z:]+)` \|/m, File.read!(@docs), capture: :all_but_first)
      |> List.flatten()

    assert "alert:record" in documented
    assert Enum.sort(documented) == Enum.sort(Principal.capabilities())

    # The deployed canary principal shape: Helm alerting.principals plus evaluation.
    principal = %{
      "tenant" => "tuist",
      "id" => "amp-operator",
      "type" => "agent",
      "token_hash" => String.duplicate("ab", 32),
      "capabilities" => documented
    }

    Pulso.Runtime.put_env("PULSO_ALERTING_PRINCIPALS_JSON", JSON.encode!([principal]))
    Pulso.Runtime.put_env("PULSO_ALERTING_EVALUATION_ENABLED", "true")
    Pulso.Runtime.put_env("PULSO_ALERTING_POLL_INTERVAL_MS", "15000")
    config = Config.Reader.read!(@runtime, env: :test, target: :host)

    assert [configured] = config[:pulso][Pulso.Alerting][:principals]
    assert configured.capabilities == documented

    for capability <- documented do
      assert Principal.authorize(configured, capability) == :ok
    end

    assert Principal.authorize(configured, "alert:rules:write", ["alert:record"]) == :ok
  end

  test "runtime.exs still rejects unknown capabilities and fields" do
    base = %{
      "tenant" => "tuist",
      "id" => "operator",
      "type" => "agent",
      "token_hash" => String.duplicate("cd", 32),
      "capabilities" => ["alert:read"]
    }

    for invalid <- [
          %{base | "capabilities" => ["alert:read", "alert:admin"]},
          Map.put(base, "name", "extra field"),
          %{base | "token_hash" => String.duplicate("CD", 32)}
        ] do
      Pulso.Runtime.put_env("PULSO_ALERTING_PRINCIPALS_JSON", JSON.encode!([invalid]))

      assert_raise RuntimeError, ~r/invalid principal/, fn ->
        Config.Reader.read!(@runtime, env: :test, target: :host)
      end
    end

    Pulso.Runtime.put_env("PULSO_ALERTING_PRINCIPALS_JSON", JSON.encode!([base, %{base | "id" => "other"}]))

    assert_raise RuntimeError, ~r/duplicate/, fn -> Config.Reader.read!(@runtime, env: :test, target: :host) end
  end
end
