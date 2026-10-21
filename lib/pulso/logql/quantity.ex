defmodule Pulso.LogQL.Quantity do
  @moduledoc false

  # Filter/unwrap quantities and query literals. Decimal scaling stays integer-only and
  # bounded to int64: long fractions never build an exponent-sized integer.
  @max 9_223_372_036_854_775_807
  @duration_units [
    {"ns", 1},
    {"us", 1_000},
    {"µs", 1_000},
    {"ms", 1_000_000},
    {"s", 1_000_000_000},
    {"m", 60_000_000_000},
    {"h", 3_600_000_000_000},
    {"d", 86_400_000_000_000}
  ]
  @byte_units %{
    "" => 1,
    "B" => 1,
    "kB" => 1_000,
    "KB" => 1_000,
    "MB" => 1_000_000,
    "GB" => 1_000_000_000,
    "TB" => 1_000_000_000_000,
    "PB" => 1_000_000_000_000_000,
    "KiB" => 1024,
    "MiB" => 1_048_576,
    "GiB" => 1_073_741_824,
    "TiB" => 1_099_511_627_776,
    "PiB" => 1_125_899_906_842_624
  }

  def duration(text), do: parse(text, :duration)
  def duration_literal(text), do: parse(text, :duration_literal)
  def bytes(text), do: parse(text, :bytes)

  def sum_components(values) do
    Enum.reduce_while(values, {:ok, 0}, fn value, {:ok, total} ->
      if value <= @max - total, do: {:cont, {:ok, total + value}}, else: {:halt, :error}
    end)
  end

  defp parse(text, kind) when is_binary(text) do
    {sign, text} =
      case text do
        "-" <> rest -> {-1, rest}
        "+" <> rest -> {1, rest}
        _ -> {1, text}
      end

    limit = if sign == -1, do: @max + 1, else: @max

    case magnitude(text, kind, limit) do
      {:ok, value} -> {:ok, sign * value}
      :error -> :error
    end
  end

  defp parse(_, _), do: :error

  defp magnitude("", _kind, _limit), do: :error
  defp magnitude("0", :duration, _limit), do: {:ok, 0}
  defp magnitude(text, :bytes, limit), do: byte_value(text, limit)
  defp magnitude(text, kind, limit), do: components(text, 0, limit, kind)

  defp components("", total, _limit, _kind), do: {:ok, total}

  defp components(text, total, limit, kind) do
    with {:ok, whole, fraction, rest} <- number(text),
         {factor, rest} <- duration_unit(rest, kind),
         {:ok, value} <- scale(whole, fraction, factor, limit - total) do
      components(rest, total + value, limit, kind)
    else
      _ -> :error
    end
  end

  defp byte_value(text, limit) do
    with {:ok, whole, fraction, unit} <- number(text),
         factor when is_integer(factor) <- Map.get(@byte_units, unit) do
      scale(whole, fraction, factor, limit)
    else
      _ -> :error
    end
  end

  # Prometheus-style query literals additionally allow weeks/years. Runtime
  # producer values deliberately do not infer those units from literal syntax.
  defp duration_unit("w" <> rest, :duration_literal), do: {604_800_000_000_000, rest}
  defp duration_unit("y" <> rest, :duration_literal), do: {31_536_000_000_000_000, rest}
  defp duration_unit(text, _kind), do: duration_unit(text)

  for {unit, factor} <- @duration_units do
    defp duration_unit(unquote(unit) <> rest), do: {unquote(factor), rest}
  end

  defp duration_unit(_), do: :error

  defp number(text) do
    length = digits(text, 0)
    <<whole::binary-size(^length), rest::binary>> = text

    {fraction, rest} =
      case rest do
        "." <> tail ->
          length = digits(tail, 0)
          <<fraction::binary-size(^length), rest::binary>> = tail
          {fraction, rest}

        _ ->
          {"", rest}
      end

    if whole == "" and fraction == "", do: :error, else: {:ok, whole, fraction, rest}
  end

  defp digits(<<digit, rest::binary>>, n) when digit in ?0..?9, do: digits(rest, n + 1)
  defp digits(_, n), do: n

  defp scale(whole, fraction, factor, limit) do
    with {:ok, integral} <- integer(whole, div(limit, factor), 0),
         value = integral * factor + fractional(fraction, byte_size(fraction) - 1, factor, 0),
         true <- value <= limit do
      {:ok, value}
    else
      _ -> :error
    end
  end

  defp integer("", _limit, value), do: {:ok, value}

  defp integer(<<digit, rest::binary>>, limit, value) do
    digit = digit - ?0
    if digit <= limit and value <= div(limit - digit, 10), do: integer(rest, limit, value * 10 + digit), else: :error
  end

  # Right-to-left long multiplication computes trunc(fraction * factor).
  # Carry is always smaller than factor, independent of fraction length.
  defp fractional(_text, index, _factor, carry) when index < 0, do: carry

  defp fractional(text, index, factor, carry),
    do: fractional(text, index - 1, factor, div((:binary.at(text, index) - ?0) * factor + carry, 10))
end
