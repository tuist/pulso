defmodule Pulso.Loki.LabelString do
  @moduledoc """
  Parse the Prometheus-style label string that Loki carries inside its
  push protobuf `Stream.labels` field.

  The wire format is `{name="value",other="v2"}`: an opening `{`, a
  comma-separated list of `name="value"` pairs, and a closing `}`.
  Names are unquoted identifiers (`[A-Za-z_][A-Za-z0-9_]*`). Values are
  Go-quoted double-quoted strings — Prometheus's serializer uses
  `strconv.Quote`, so the full set of Go escape forms may appear:

    * `\\a`, `\\b`, `\\f`, `\\n`, `\\r`, `\\t`, `\\v`, `\\\\`, `\\"`, `\\'`
    * `\\xNN` — two-digit hex byte
    * `\\uNNNN` — four-digit hex Unicode codepoint (encoded as UTF-8)
    * `\\UNNNNNNNN` — eight-digit hex Unicode codepoint (encoded as UTF-8)
    * `\\NNN` — three-digit octal byte

  An unknown or truncated escape is treated as a parse error rather
  than silently dropped: label identity is load-bearing (it feeds
  `resource` and drives service routing), and silently changing a byte
  is worse than surfacing the reject to the sender.

  The JSON push path receives labels as an already-decoded map, so this
  module exists only for the protobuf path.
  """

  @doc """
  Parse a label string into a `%{name => value}` map.

  Returns `{:ok, map}` on success or `:error` on any malformed input
  (missing braces, non-string quote body, unterminated quote, trailing
  content after the closing brace).
  """
  @spec parse(binary()) :: {:ok, map()} | :error
  def parse(<<"{", rest::binary>>) do
    case pairs(trim_leading_ws(rest), %{}) do
      {:ok, map, ""} -> {:ok, map}
      {:ok, _map, _leftover} -> :error
      :error -> :error
    end
  end

  def parse(_), do: :error

  # Empty `{}` label set is legal — a stream with no labels.
  defp pairs(<<"}", rest::binary>>, acc), do: {:ok, acc, rest}

  defp pairs(rest, acc) do
    with {:ok, name, rest} <- name(rest),
         rest = trim_leading_ws(rest),
         {:ok, rest} <- eat_char(rest, ?=),
         rest = trim_leading_ws(rest),
         {:ok, value, rest} <- quoted(rest),
         rest = trim_leading_ws(rest),
         {:ok, cont, rest} <- separator(rest) do
      acc = Map.put(acc, name, value)

      case cont do
        :more -> pairs(trim_leading_ws(rest), acc)
        :done -> {:ok, acc, rest}
      end
    end
  end

  defp name(rest), do: name(rest, <<>>)

  defp name(<<c, rest::binary>>, acc) when c in ?a..?z or c in ?A..?Z or c == ?_ do
    name_tail(rest, <<acc::binary, c>>)
  end

  defp name(_, <<>>), do: :error

  defp name_tail(<<c, rest::binary>>, acc) when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_ do
    name_tail(rest, <<acc::binary, c>>)
  end

  defp name_tail(rest, acc), do: {:ok, acc, rest}

  defp eat_char(<<c, rest::binary>>, c), do: {:ok, rest}
  defp eat_char(_, _), do: :error

  defp quoted(<<"\"", rest::binary>>), do: quoted_body(rest, <<>>)
  defp quoted(_), do: :error

  defp quoted_body(<<"\"", rest::binary>>, acc), do: {:ok, acc, rest}
  defp quoted_body(<<"\\", rest::binary>>, acc), do: escape(rest, acc)
  defp quoted_body(<<c, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, c>>)
  defp quoted_body(<<>>, _acc), do: :error

  # Go string escapes. `strconv.Quote` (which Prometheus uses to
  # serialize label values) emits any of these, so we decode the full
  # set. Anything unrecognized is a parse error rather than a silent
  # byte drop — label identity is load-bearing.
  defp escape(<<?a, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, 0x07>>)
  defp escape(<<?b, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, 0x08>>)
  defp escape(<<?f, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, 0x0C>>)
  defp escape(<<?n, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?\n>>)
  defp escape(<<?r, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?\r>>)
  defp escape(<<?t, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?\t>>)
  defp escape(<<?v, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, 0x0B>>)
  defp escape(<<?\\, rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?\\>>)
  defp escape(<<?", rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?">>)
  defp escape(<<?', rest::binary>>, acc), do: quoted_body(rest, <<acc::binary, ?'>>)

  defp escape(<<?x, a, b, rest::binary>>, acc) do
    with {:ok, byte} <- hex(<<a, b>>) do
      quoted_body(rest, <<acc::binary, byte>>)
    end
  end

  defp escape(<<?u, a, b, c, d, rest::binary>>, acc) do
    with {:ok, cp} <- hex(<<a, b, c, d>>),
         {:ok, encoded} <- encode_codepoint(cp) do
      quoted_body(rest, <<acc::binary, encoded::binary>>)
    end
  end

  defp escape(<<?U, a, b, c, d, e, f, g, h, rest::binary>>, acc) do
    with {:ok, cp} <- hex(<<a, b, c, d, e, f, g, h>>),
         {:ok, encoded} <- encode_codepoint(cp) do
      quoted_body(rest, <<acc::binary, encoded::binary>>)
    end
  end

  defp escape(<<a, b, c, rest::binary>>, acc) when a in ?0..?7 and b in ?0..?7 and c in ?0..?7 do
    byte = (a - ?0) * 64 + (b - ?0) * 8 + (c - ?0)
    if byte <= 0xFF, do: quoted_body(rest, <<acc::binary, byte>>), else: :error
  end

  defp escape(_, _), do: :error

  defp hex(binary) do
    case Integer.parse(binary, 16) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp encode_codepoint(cp) when cp in 0..0x10FFFF and (cp < 0xD800 or cp > 0xDFFF) do
    {:ok, <<cp::utf8>>}
  rescue
    _ -> :error
  end

  defp encode_codepoint(_), do: :error

  defp separator(<<",", rest::binary>>), do: {:ok, :more, rest}
  defp separator(<<"}", rest::binary>>), do: {:ok, :done, rest}
  defp separator(_), do: :error

  defp trim_leading_ws(<<c, rest::binary>>) when c in [?\s, ?\t], do: trim_leading_ws(rest)
  defp trim_leading_ws(rest), do: rest
end
