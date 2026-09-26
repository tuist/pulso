defmodule Pulso.Loki.LabelStringTest do
  use ExUnit.Case, async: true

  alias Pulso.Loki.LabelString

  test "returns an empty map for `{}`" do
    assert LabelString.parse("{}") == {:ok, %{}}
  end

  test "parses a single pair" do
    assert LabelString.parse(~s({service="api"})) == {:ok, %{"service" => "api"}}
  end

  test "parses multiple pairs" do
    assert LabelString.parse(~s({service_name="api", level="info"})) ==
             {:ok, %{"service_name" => "api", "level" => "info"}}
  end

  test "accepts whitespace around pairs and the `=`" do
    # Loki's serializer emits `{ k = "v" , k2 = "v2" }` in a handful of
    # code paths; being generous about whitespace beats round-tripping
    # a false negative on a real-world label string.
    assert LabelString.parse(~s({  service = "api" ,  level = "info"  })) ==
             {:ok, %{"service" => "api", "level" => "info"}}
  end

  test "unescapes the common Go string escapes" do
    # \\", \\\\, \\n, \\t, \\r are the escapes most Loki senders emit.
    input = "{k=\"a\\\"b\\\\c\\nd\\te\\rf\"}"
    assert LabelString.parse(input) == {:ok, %{"k" => "a\"b\\c\nd\te\rf"}}
  end

  test "unescapes the rarer Go control-char escapes" do
    input = "{k=\"\\a\\b\\f\\v\"}"
    assert LabelString.parse(input) == {:ok, %{"k" => <<0x07, 0x08, 0x0C, 0x0B>>}}
  end

  test "unescapes hex byte escapes" do
    assert LabelString.parse(~s({k="\\x00\\x7f\\xff"})) == {:ok, %{"k" => <<0x00, 0x7F, 0xFF>>}}
  end

  test "unescapes 4-digit Unicode escapes into UTF-8" do
    # é is é (U+00E9), 中 is 中.
    assert LabelString.parse(~s({k="caf\\u00e9 \\u4e2d"})) ==
             {:ok, %{"k" => "café 中"}}
  end

  test "unescapes 8-digit Unicode escapes into UTF-8" do
    # \U0001F600 is 😀.
    assert LabelString.parse(~s({k="\\U0001F600"})) == {:ok, %{"k" => "😀"}}
  end

  test "unescapes 3-digit octal escapes" do
    assert LabelString.parse(~s({k="\\000\\177\\377"})) == {:ok, %{"k" => <<0x00, 0x7F, 0xFF>>}}
  end

  test "rejects a truncated hex escape" do
    assert LabelString.parse(~s({k="\\x0"})) == :error
    assert LabelString.parse(~s({k="\\xzz"})) == :error
  end

  test "rejects a truncated Unicode escape" do
    assert LabelString.parse(~s({k="\\u00"})) == :error
    assert LabelString.parse(~s({k="\\U0001"})) == :error
  end

  test "rejects a surrogate codepoint in a Unicode escape" do
    # U+D800..U+DFFF are UTF-16 surrogate halves and are not valid
    # standalone codepoints in UTF-8.
    assert LabelString.parse(~s({k="\\ud800"})) == :error
  end

  test "rejects a truly unknown escape rather than silently keeping the byte" do
    # Old behavior swallowed the leading backslash and kept the char
    # literally, silently changing the label value. The current
    # behavior surfaces the reject to the sender.
    assert LabelString.parse(~s({k="\\q"})) == :error
    assert LabelString.parse(~s({k="\\z"})) == :error
  end

  test "rejects a body without braces" do
    assert LabelString.parse("service=\"api\"") == :error
  end

  test "rejects an unclosed brace" do
    assert LabelString.parse(~s({service="api")) == :error
  end

  test "rejects an unterminated quoted value" do
    assert LabelString.parse(~s({service="api)) == :error
  end

  test "rejects an empty name" do
    assert LabelString.parse(~s({="api"})) == :error
  end

  test "rejects an unquoted value" do
    assert LabelString.parse(~s({service=api})) == :error
  end

  test "rejects trailing content after the closing brace" do
    assert LabelString.parse(~s({service="api"} extra)) == :error
  end
end
