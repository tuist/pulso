defmodule Pulso.IngestLimits do
  @moduledoc """
  Per-request ingest budgets, shared by the JSON and native receivers.

  Limits apply to supplied records and attribute entries, including malformed
  records and duplicate keys, before decoding can drop or collapse them. An
  over-budget request is rejected in full; nothing is appended to storage.
  """

  alias Pulso.Runtime

  @defaults %{
    max_records: 10_000,
    max_attributes: 128,
    max_key_bytes: 256,
    max_value_bytes: 16_384,
    max_attribute_bytes: 65_536,
    max_depth: 16,
    max_nodes: 1_024
  }

  def config do
    overrides = Runtime.get_env(:pulso, __MODULE__, [])

    Map.new(@defaults, fn {key, default} ->
      value = Keyword.get(overrides, key, default)

      if !(is_integer(value) and value in 1..2_147_483_647) do
        raise ArgumentError, "#{inspect(__MODULE__)} #{key} must be an integer in 1..2147483647"
      end

      {key, value}
    end)
  end

  def native_options do
    limits = config()

    {limits.max_records, limits.max_attributes, limits.max_key_bytes, limits.max_value_bytes,
     limits.max_attribute_bytes}
  end

  @doc "Validate a parsed JSON request before expanding it into records."
  def validate(protocol, payload) when protocol in [:otlp, :loki] do
    limits = config()

    case protocol do
      :otlp -> validate_otlp(payload, limits)
      :loki -> validate_loki(payload, limits)
    end
  end

  defp validate_loki(payload, limits) do
    field(payload, "streams")
    |> reduce_list({0, 0}, &loki_stream(&1, limits, &2))
    |> finish()
  end

  defp loki_stream(stream, limits, {records, groups}) do
    with :ok <- group_limit(groups + 1, limits),
         :ok <- attributes(field(stream, "stream"), limits, false) do
      reduce_list(field(stream, "values"), {records, groups + 1}, &loki_record(&1, limits, &2))
    end
  end

  defp loki_record(value, limits, {count, groups}) do
    with :ok <- record_limit(count + 1, limits),
         :ok <- loki_metadata(value, limits) do
      {:ok, {count + 1, groups}}
    end
  end

  defp loki_metadata([_, _, metadata], limits), do: attributes(metadata, limits, false)
  defp loki_metadata(_, _), do: :ok

  defp validate_otlp(payload, limits) do
    field(payload, "resourceLogs")
    |> reduce_list({0, 0}, &otlp_resource(&1, limits, &2))
    |> finish()
  end

  defp otlp_resource(resource, limits, {records, groups}) do
    with :ok <- otlp_group_limit(groups + 1, limits),
         :ok <- attributes(field(field(resource, "resource"), "attributes"), limits, true) do
      reduce_list(field(resource, "scopeLogs"), {records, groups + 1}, &otlp_scope(&1, limits, &2))
    end
  end

  defp otlp_scope(scope, limits, {count, groups}) do
    with :ok <- otlp_group_limit(groups + 1, limits),
         :ok <- attributes(field(field(scope, "scope"), "attributes"), limits, true) do
      reduce_list(field(scope, "logRecords"), {count, groups + 1}, &otlp_record(&1, limits, &2))
    end
  end

  defp otlp_record(record, limits, {count, groups}) do
    with :ok <- record_limit(count + 1, limits),
         :ok <- attributes(field(record, "attributes"), limits, true),
         :ok <- body_structure(field(record, "body"), limits) do
      {:ok, {count + 1, groups}}
    end
  end

  defp field(map, key) when is_map(map), do: Map.get(map, key)
  defp field(_, _), do: nil

  defp reduce_list(list, initial, fun) when is_list(list) do
    Enum.reduce_while(list, {:ok, initial}, fn item, {:ok, state} ->
      case fun.(item, state) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp reduce_list(_, initial, _), do: {:ok, initial}
  defp finish({:ok, _}), do: :ok
  defp finish(error), do: error

  defp record_limit(count, limits) do
    if count <= limits.max_records, do: :ok, else: {:error, :too_many_records}
  end

  # Empty containers also consume work, independently of record count.
  defp group_limit(count, limits), do: record_limit(count, limits)

  # A single OTLP record needs both a resource and a scope group.
  defp otlp_group_limit(count, limits), do: record_limit(count, %{limits | max_records: 2 * limits.max_records})

  defp attributes(nil, _, _), do: :ok

  defp attributes(value, limits, true) do
    value |> otlp_pairs(limits, 0, {0, 0}) |> finish()
  end

  defp attributes(value, limits, false) do
    with :ok <- attribute_count(value, limits),
         {:ok, _} <- walk(value, limits, 0, {0, 0}) do
      :ok
    end
  end

  defp otlp_pairs(pairs, limits, depth, {bytes, nodes}) when is_list(pairs) do
    with :ok <- structure_limit(limits, depth, nodes),
         :ok <- attribute_count(pairs, limits) do
      reduce_list(pairs, {bytes, nodes + 1}, &otlp_pair(&1, limits, depth, &2))
    end
  end

  defp otlp_pairs(value, limits, depth, state), do: walk(value, limits, depth, state)

  defp otlp_pair(pair, limits, depth, state) do
    with {:ok, next} <- walk_attribute_key(field(pair, "key"), limits, depth + 1, state) do
      walk_otlp(field(pair, "value"), limits, depth + 1, next)
    end
  end

  defp walk_attribute_key(key, limits, depth, {bytes, nodes}) when is_binary(key) do
    with :ok <- structure_limit(limits, depth, nodes),
         :ok <- key_limit(key, limits) do
      add_bytes({bytes, nodes + 1}, byte_size(key), limits)
    end
  end

  defp walk_attribute_key(key, limits, depth, state), do: walk(key, limits, depth, state)

  defp walk_otlp(value, limits, depth, {_, nodes} = state) do
    with :ok <- structure_limit(limits, depth, nodes) do
      case value do
        %{"stringValue" => v} ->
          walk(v, limits, depth, state)

        %{"boolValue" => v} ->
          walk(v, limits, depth, state)

        %{"doubleValue" => v} ->
          walk(v, limits, depth, state)

        %{"bytesValue" => v} ->
          walk(v, limits, depth, state)

        %{"intValue" => v} ->
          walk(v, limits, depth, state)

        %{"arrayValue" => %{"values" => values}} when is_list(values) ->
          {bytes, nodes} = state
          reduce_list(values, {bytes, nodes + 1}, &walk_otlp(&1, limits, depth + 1, &2))

        %{"kvlistValue" => %{"values" => pairs}} when is_list(pairs) ->
          otlp_pairs(pairs, limits, depth, state)

        other ->
          walk(other, limits, depth, state)
      end
    end
  end

  defp attribute_count(value, limits) when is_map(value) do
    if map_size(value) <= limits.max_attributes, do: :ok, else: {:error, :attributes_too_large}
  end

  defp attribute_count(value, limits) when is_list(value) do
    value
    |> Enum.reduce_while(0, fn _, count ->
      if count < limits.max_attributes, do: {:cont, count + 1}, else: {:halt, :over}
    end)
    |> case do
      :over -> {:error, :attributes_too_large}
      _ -> :ok
    end
  end

  defp attribute_count(_, _), do: :ok

  # Bodies can contain AnyValue recursion too. Bound structure without applying
  # the attribute string/byte caps to ordinary log messages.
  defp body_structure(value, limits) do
    body_limits = %{limits | max_value_bytes: 16 * 1024 * 1024, max_attribute_bytes: 16 * 1024 * 1024}
    value |> walk_otlp(body_limits, 0, {0, 0}) |> finish()
  end

  defp structure_limit(limits, depth, nodes) do
    if depth <= limits.max_depth and nodes < limits.max_nodes,
      do: :ok,
      else: {:error, :attributes_too_large}
  end

  defp walk(_, limits, depth, {_, nodes}) when depth > limits.max_depth or nodes >= limits.max_nodes,
    do: {:error, :attributes_too_large}

  defp walk(value, limits, depth, {bytes, nodes}) do
    state = {bytes, nodes + 1}

    cond do
      is_binary(value) ->
        walk_string(value, limits, state)

      is_map(value) ->
        with :ok <- attribute_count(value, limits) do
          reduce_map(value, state, &walk_map_entry(&1, limits, depth, &2))
        end

      is_list(value) ->
        reduce_list(value, state, &walk(&1, limits, depth + 1, &2))

      true ->
        add_bytes(state, 8, limits)
    end
  end

  defp walk_string(value, limits, state) do
    if byte_size(value) <= limits.max_value_bytes,
      do: add_bytes(state, byte_size(value), limits),
      else: {:error, :attributes_too_large}
  end

  defp walk_map_entry({key, child}, limits, depth, state) do
    with :ok <- key_limit(key, limits),
         {:ok, next} <- add_bytes(state, if(is_binary(key), do: byte_size(key), else: 8), limits) do
      walk(child, limits, depth + 1, next)
    end
  end

  defp reduce_map(map, state, fun) do
    Enum.reduce_while(map, {:ok, state}, fn pair, {:ok, state} ->
      case fun.(pair, state) do
        {:ok, next} -> {:cont, {:ok, next}}
        error -> {:halt, error}
      end
    end)
  end

  defp key_limit(key, limits) when is_binary(key) and byte_size(key) > limits.max_key_bytes,
    do: {:error, :attributes_too_large}

  defp key_limit(_, _), do: :ok

  defp add_bytes({bytes, nodes}, amount, limits) do
    if bytes + amount <= limits.max_attribute_bytes,
      do: {:ok, {bytes + amount, nodes}},
      else: {:error, :attributes_too_large}
  end
end
