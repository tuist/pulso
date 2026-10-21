defmodule Pulso.QueryRegex do
  @moduledoc false

  # Independent PCRE reference for the native Go-style Perl classes. Explicit
  # ranges also work inside existing classes; in particular Go \s excludes VT,
  # whereas PCRE \s includes it. Keep Unicode codepoints and strict end anchors.
  @ranges %{
    ?d => "0-9",
    ?D => "\\x00-\\x2f\\x3a-\\x{10ffff}",
    ?w => "A-Za-z0-9_",
    ?s => "\\t\\n\\f\\r ",
    ?S => "\\x00-\\x08\\x0b\\x0e-\\x1f\\x21-\\x{10ffff}"
  }

  def compile(pattern) do
    pattern |> translate(0, []) |> IO.iodata_to_binary() |> Regex.compile([:unicode, :dollar_endonly])
  end

  defp translate(<<>>, _depth, acc), do: Enum.reverse(acc)

  defp translate(<<"[^]", rest::binary>>, 0, acc), do: translate(<<"[^\\]", rest::binary>>, 0, acc)
  defp translate(<<"[]", rest::binary>>, 0, acc), do: translate(<<"[\\]", rest::binary>>, 0, acc)

  defp translate(<<"[", rest::binary>>, 0, acc) do
    case take_class(rest, 1, []) do
      {contents, tail} -> translate(tail, 0, [class(contents) | acc])
      :unterminated -> Enum.reverse([["[", rest] | acc])
    end
  end

  defp translate(<<"\\", char, rest::binary>>, depth, acc) do
    replacement =
      case Map.fetch(@ranges, char) do
        {:ok, range} -> if depth > 0, do: range, else: ["[", range, "]"]
        :error when char == ?W -> "[^A-Za-z0-9_]"
        :error when char == ?v -> "\\x0b"
        :error -> ["\\", <<char>>]
      end

    translate(rest, depth, [replacement | acc])
  end

  defp translate(<<char, rest::binary>>, depth, acc) do
    next =
      case char do
        ?[ -> depth + 1
        ?] -> max(0, depth - 1)
        _ -> depth
      end

    translate(rest, next, [<<char>> | acc])
  end

  # A positive complement range would incorrectly re-admit Unicode folds of
  # ASCII letters under (?i). Lift \W out of a class as a negated class instead.
  # Lookahead here is only an independent reference implementation detail.
  defp class(contents) do
    {negative?, body} =
      case contents do
        <<"^", rest::binary>> -> {true, rest}
        _ -> {false, contents}
      end

    {body, word_complement?} = strip_word_complement(body, [], false)

    if word_complement? do
      # Removing a leading \W must not turn a following literal ^ into a
      # new class-negation marker.
      literal =
        case body do
          <<"^", tail::binary>> -> <<"\\^", tail::binary>>
          _ -> body
        end

      other = if literal == "", do: [], else: ["|[", translate(literal, 1, []), "]"]
      union = ["[^A-Za-z0-9_]", other]
      if negative?, do: ["(?:(?!(?:", union, "))(?s:.))"], else: ["(?:", union, ")"]
    else
      ["[", translate(contents, 1, []), "]"]
    end
  end

  defp strip_word_complement(<<>>, acc, found), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), found}
  # Do not join two literal neighbours into a new range when removing \W.
  defp strip_word_complement(<<"\\W-", rest::binary>>, acc, _found),
    do: strip_word_complement(rest, ["\\-" | acc], true)

  defp strip_word_complement(<<"\\W", rest::binary>>, acc, _found), do: strip_word_complement(rest, acc, true)

  defp strip_word_complement(<<"\\", c, rest::binary>>, acc, found),
    do: strip_word_complement(rest, [["\\", <<c>>] | acc], found)

  defp strip_word_complement(<<c, rest::binary>>, acc, found), do: strip_word_complement(rest, [<<c>> | acc], found)

  defp take_class(<<>>, _depth, _acc), do: :unterminated
  defp take_class(<<"\\", c, rest::binary>>, depth, acc), do: take_class(rest, depth, [["\\", <<c>>] | acc])
  defp take_class(<<"]", rest::binary>>, 1, acc), do: {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}
  defp take_class(<<"[", rest::binary>>, depth, acc), do: take_class(rest, depth + 1, ["[" | acc])
  defp take_class(<<"]", rest::binary>>, depth, acc), do: take_class(rest, depth - 1, ["]" | acc])
  defp take_class(<<c, rest::binary>>, depth, acc), do: take_class(rest, depth, [<<c>> | acc])
end
