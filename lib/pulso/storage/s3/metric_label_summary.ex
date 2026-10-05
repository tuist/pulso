defmodule Pulso.Storage.S3.MetricLabelSummary do
  @moduledoc false

  # Each retained label has a COMPLETE value set over the segment. Large or
  # invalid sets are omitted independently, never truncated. Choosing bounded
  # names from the first record is an optimization only: later-only names are
  # simply unknown. Missing labels have the Prometheus value "".
  @max_names 16
  @max_values 32
  @max_bytes 4096

  def build([]), do: nil

  def build([first | _] = records) do
    names =
      first.labels
      |> Map.keys()
      |> Enum.filter(&(valid_name?(&1) and &1 != "__name__"))
      |> Enum.sort()
      |> Enum.take(@max_names)

    sets = Map.new(names, &{&1, MapSet.new()})

    sets = Enum.reduce_while(records, sets, &collect_record/2)

    {summary, _bytes} =
      Enum.reduce(names, {%{}, 2}, fn name, acc -> add_label(name, Map.get(sets, name), acc) end)

    if map_size(summary) > 0, do: summary
  end

  defp collect_record(record, sets) do
    if Enum.all?(Map.keys(record.labels), &valid_utf8?/1) do
      sets = Map.new(sets, fn {name, values} -> {name, collect(values, Map.get(record.labels, name, ""))} end)
      {:cont, sets}
    else
      # Native JSON materialization normalizes invalid UTF-8 keys, which
      # could collide with a selected name. Never summarize that case.
      {:halt, %{}}
    end
  end

  defp add_label(name, %MapSet{} = values, {summary, bytes}) do
    values = values |> Enum.map(&:binary.copy/1) |> Enum.sort()
    added = IO.iodata_length(Pulso.JSON.encode_to_iodata!(%{name => values}))

    if bytes + added <= @max_bytes,
      do: {Map.put(summary, :binary.copy(name), values), bytes + added},
      else: {summary, bytes}
  end

  defp add_label(_name, _unknown, acc), do: acc

  defp collect(nil, _value), do: nil

  defp collect(values, value) do
    if valid_value?(value) do
      values = MapSet.put(values, value)
      if MapSet.size(values) <= @max_values, do: values
    end
  end

  def parse(summary) when is_map(summary) and map_size(summary) in 1..@max_names do
    if Enum.all?(summary, fn {name, values} ->
         valid_name?(name) and is_list(values) and length(values) in 1..@max_values and
           Enum.all?(values, &valid_value?/1)
       end) and IO.iodata_length(Pulso.JSON.encode_to_iodata!(summary)) <= @max_bytes,
       do: summary
  end

  def parse(_), do: nil

  def matches?(nil, _matchers), do: true

  def matches?(summary, matchers) do
    Enum.all?(matchers, fn
      {name, :eq, value} ->
        case Map.fetch(summary, name) do
          {:ok, values} -> value in values
          :error -> true
        end

      _ ->
        true
    end)
  end

  defp valid_name?(name), do: is_binary(name) and byte_size(name) in 1..128 and String.valid?(name)
  defp valid_value?(value), do: is_binary(value) and byte_size(value) <= 256 and String.valid?(value)
  defp valid_utf8?(value), do: is_binary(value) and String.valid?(value)
end
