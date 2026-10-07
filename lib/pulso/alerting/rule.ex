defmodule Pulso.Alerting.Rule do
  @moduledoc "Validated native threshold rules and lossless, disabled Grafana definitions."
  alias Pulso.Alerting.Canonical
  alias Pulso.Alerting.Graph
  alias Pulso.Alerting.Principal
  alias Pulso.PromQL.Parser

  @native_fields ~w(kind name query threshold enabled cadence_ms for_ms keep_firing_ms labels notification_targets)
  @defaults %{
    "kind" => "promql_threshold",
    "enabled" => false,
    "cadence_ms" => 60_000,
    "for_ms" => 0,
    "keep_firing_ms" => 0,
    "labels" => %{},
    "notification_targets" => []
  }

  def validate(%{"kind" => "grafana", "original" => original} = input) when is_map(original) do
    if Map.keys(input) -- ~w(kind original enabled) == [] and Map.get(input, "enabled", false) == false and
         is_binary(original["uid"]) and Graph.validate(original) == :ok and safe_json?(original, 0) and
         byte_size(Canonical.encode(input)) <= 262_144 do
      {:ok, Map.put(input, "enabled", false)}
    else
      {:error, :invalid_rule}
    end
  end

  def validate(input) when is_map(input) do
    rule = Map.merge(@defaults, input)

    with true <- Map.keys(input) -- @native_fields == [],
         "promql_threshold" <- rule["kind"],
         true <- text?(rule["name"], 256),
         true <- text?(rule["query"], 16_384),
         true <- is_boolean(rule["enabled"]),
         true <- target_ids?(rule["notification_targets"]),
         true <- duration?(rule["cadence_ms"], 1_000, 86_400_000),
         true <- duration?(rule["for_ms"], 0, 604_800_000),
         true <- duration?(rule["keep_firing_ms"], 0, 604_800_000),
         true <- labels?(rule["labels"]) and not Enum.any?(["__name__", "alertname"], &Map.has_key?(rule["labels"], &1)),
         %{"op" => op, "value" => value} = threshold <- rule["threshold"],
         true <- op in ["gt", "lt", "eq"] and is_number(value) and map_size(threshold) == 2,
         {:ok, _} <- Parser.parse(rule["query"]),
         true <- byte_size(Canonical.encode(rule)) <= 65_536 do
      {:ok, rule}
    else
      _ -> {:error, :invalid_rule}
    end
  end

  def validate(_), do: {:error, :invalid_rule}
  def classification(%{"kind" => "grafana"}), do: ["alert:import"]
  def classification(_), do: []

  def labels?(labels) when is_map(labels) do
    map_size(labels) <= 32 and
      Enum.all?(labels, fn {key, value} ->
        text?(key, 128) and
          is_binary(value) and byte_size(value) <= 512 and String.valid?(value)
      end)
  end

  def labels?(_), do: false

  defp target_ids?(ids) when is_list(ids),
    do: length(ids) <= 8 and length(Enum.uniq(ids)) == length(ids) and Enum.all?(ids, &Principal.valid_id?/1)

  defp target_ids?(_), do: false

  defp duration?(value, low, high), do: is_integer(value) and value >= low and value <= high
  defp text?(value, max), do: is_binary(value) and byte_size(value) in 1..max and String.valid?(value)

  # Import preserves unknown fields, but credential-bearing fields are not rule data.
  defp safe_json?(_, depth) when depth > 32, do: false

  defp safe_json?(map, depth) when is_map(map) do
    map_size(map) <= 1024 and
      Enum.all?(map, fn {key, value} ->
        is_binary(key) and String.downcase(key) not in ~w(password token secret authorization api_key securejsondata) and
          safe_json?(value, depth + 1)
      end)
  end

  defp safe_json?(list, depth) when is_list(list),
    do: length(list) <= 4096 and Enum.all?(list, &safe_json?(&1, depth + 1))

  defp safe_json?(value, _), do: is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value)
end
