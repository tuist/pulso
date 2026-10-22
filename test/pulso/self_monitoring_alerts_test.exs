defmodule Pulso.SelfMonitoringAlertsTest do
  # The self-monitoring rules the Helm chart ships must stay evaluable by
  # Pulso's own PromQL, since operators may store these metrics in Pulso.
  use Pulso.Test.Case, async: true

  alias Pulso.PromQL.Evaluator
  alias Pulso.Record.MetricSample
  alias Pulso.Storage

  @rules Path.expand("../../charts/pulso/files/alerts.json", __DIR__)
  @external_resource @rules

  defp rules do
    @rules |> File.read!() |> Pulso.JSON.decode!() |> Map.fetch!("groups") |> Enum.flat_map(& &1["rules"])
  end

  test "every rule is a uniquely named alert with a severity, summary and description" do
    rules = rules()
    assert length(rules) == length(Enum.uniq_by(rules, & &1["alert"]))

    for rule <- rules do
      assert rule["alert"] =~ ~r/\APulso[A-Za-z]+\z/
      assert rule["labels"]["severity"] in ["warning", "critical"]
      assert is_binary(rule["annotations"]["summary"]) and is_binary(rule["annotations"]["description"])
      assert is_nil(rule["for"]) or rule["for"] =~ ~r/\A\d+[mh]\z/
    end
  end

  test "every expression evaluates with Pulso's PromQL over self-monitoring samples" do
    tenant = "self-#{System.unique_integer([:positive])}"
    now = System.os_time(:nanosecond)
    hour = 3_600_000_000_000

    names =
      rules()
      |> Enum.flat_map(&Regex.scan(~r/pulso_[a-z_]+/, &1["expr"]))
      |> List.flatten()
      |> Enum.uniq()

    # Two weeks of hourly points, so offsets and long ranges see data.
    samples =
      for name <- names,
          ago <- 0..(14 * 24),
          do: %MetricSample{
            timestamp_ns: now - ago * hour,
            value: ago * 1.0,
            labels: %{"__name__" => name, "signal" => "logs", "outcome" => "accepted"}
          }

    assert :ok = Storage.append(:metrics, tenant, samples)

    for rule <- rules() do
      assert {:ok, _} = Evaluator.query(rule["expr"], tenant, %{end_ts_ns: now}), rule["alert"]
    end
  end
end
