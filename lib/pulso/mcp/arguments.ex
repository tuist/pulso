defmodule Pulso.MCP.Arguments do
  @moduledoc false

  # Validate the schema vocabulary used by Pulso's tool registry. Keeping
  # validation tied to the published schemas prevents discovery and calls
  # from disagreeing about types, required fields, or numeric limits.
  def normalize(value, schema) do
    required = schema["required"] || []

    Enum.reduce(schema["properties"] || %{}, value, fn {key, child_schema}, args ->
      normalize_property(args, key, child_schema, required)
    end)
  end

  defp normalize_property(args, key, schema, required) do
    case Map.fetch(args, key) do
      {:ok, nil} -> if key in required, do: args, else: Map.delete(args, key)
      {:ok, value} -> Map.put(args, key, normalize_integer(value, schema))
      :error -> args
    end
  end

  defp normalize_integer(value, %{"type" => "integer"}) when is_float(value) do
    integer = trunc(value)
    if value == integer, do: integer, else: value
  end

  defp normalize_integer(value, _schema), do: value

  def validate(value, schema, path \\ "arguments") do
    with :ok <- validate_type(value, schema["type"], path),
         :ok <- validate_constraints(value, schema, path) do
      validate_children(value, schema, path)
    end
  end

  defp validate_type(value, "object", _path) when is_map(value), do: :ok
  defp validate_type(value, "array", _path) when is_list(value), do: :ok
  defp validate_type(value, "string", _path) when is_binary(value), do: :ok
  defp validate_type(value, "integer", _path) when is_integer(value), do: :ok
  defp validate_type(_value, type, path), do: invalid("#{path} must be #{type}")

  defp validate_constraints(value, schema, path) do
    with :ok <- validate_enum(value, schema, path),
         :ok <- validate_bounds(value, schema, path) do
      validate_length(value, schema, path)
    end
  end

  defp validate_enum(value, %{"enum" => allowed}, path) do
    if value in allowed, do: :ok, else: invalid("#{path} must be one of #{inspect(allowed)}")
  end

  defp validate_enum(_value, _schema, _path), do: :ok

  defp validate_bounds(value, schema, path) when is_integer(value) do
    cond do
      Map.has_key?(schema, "minimum") and value < schema["minimum"] ->
        invalid("#{path} must be at least #{schema["minimum"]}")

      Map.has_key?(schema, "maximum") and value > schema["maximum"] ->
        invalid("#{path} must be at most #{schema["maximum"]}")

      true ->
        :ok
    end
  end

  defp validate_bounds(_value, _schema, _path), do: :ok

  defp validate_length(value, %{"minLength" => minimum}, path) when is_binary(value) do
    if String.length(value) >= minimum,
      do: :ok,
      else: invalid("#{path} must contain at least #{minimum} characters")
  end

  defp validate_length(_value, _schema, _path), do: :ok

  defp validate_children(value, %{"type" => "object"} = schema, path) do
    with :ok <- validate_required(value, schema, path) do
      Enum.reduce_while(schema["properties"] || %{}, :ok, fn {key, child_schema}, :ok ->
        continue(validate_property(value, key, child_schema, path))
      end)
    end
  end

  defp validate_children(value, %{"type" => "array", "items" => schema}, path) do
    value
    |> Stream.with_index()
    |> Enum.reduce_while(:ok, fn {child, index}, :ok ->
      continue(validate(child, schema, "#{path}[#{index}]"))
    end)
  end

  defp validate_children(_value, _schema, _path), do: :ok

  defp validate_required(value, schema, path) do
    case Enum.find(schema["required"] || [], &(not Map.has_key?(value, &1))) do
      nil -> :ok
      missing -> invalid("#{path}.#{missing} is required")
    end
  end

  defp validate_property(value, key, schema, path) do
    case Map.fetch(value, key) do
      :error -> :ok
      {:ok, child} -> validate(child, schema, "#{path}.#{key}")
    end
  end

  defp continue(:ok), do: {:cont, :ok}
  defp continue(error), do: {:halt, error}
  defp invalid(message), do: {:error, {:invalid_arguments, message}}
end
