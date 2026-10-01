defmodule Pulso.LogQL.EvaluatorTest do
  use ExUnit.Case, async: false

  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.Parser
  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  # -- Test helpers ----------------------------------------------------------

  defp log(tenant, records) do
    :ok = Storage.append(:logs, tenant, records)
  end

  defp record(fields) do
    struct!(Log, Map.new(fields))
  end

  defp run(query, tenant \\ "acme", opts \\ %{}) do
    {:ok, ast} = Parser.parse(query)
    Evaluator.evaluate_log(ast, tenant, opts)
  end

  # ---------------------------------------------------------------------------
  # Selector matchers
  # ---------------------------------------------------------------------------

  describe "selector matchers" do
    test "matches on service via the dedicated column" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "a"),
        record(timestamp_ns: 20, service: "db", body: "b")
      ])

      assert {:ok, streams} = run(~s({service="api"}))
      assert [{_labels, [{10, "a"}]}] = streams
    end

    test "matches on stream label carried in resource" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "a", resource: %{"env" => "prod"}),
        record(timestamp_ns: 20, service: "api", body: "b", resource: %{"env" => "dev"})
      ])

      assert {:ok, streams} = run(~s({env="prod"}))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert lines == ["a"]
    end

    test "regex matcher on stream label" do
      log("acme", [
        record(timestamp_ns: 10, body: "a", resource: %{"env" => "prod"}),
        record(timestamp_ns: 20, body: "b", resource: %{"env" => "stg"}),
        record(timestamp_ns: 30, body: "c", resource: %{"env" => "dev"})
      ])

      assert {:ok, streams} = run(~s({env=~"prod|stg"}))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["a", "b"]
    end

    test "negated matcher and absent-label semantic" do
      log("acme", [
        record(timestamp_ns: 10, body: "a", resource: %{"env" => "prod"}),
        record(timestamp_ns: 20, body: "b", resource: %{}),
        record(timestamp_ns: 30, body: "c", resource: %{"env" => "dev"})
      ])

      assert {:ok, streams} = run(~s({env!="prod"}))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["b", "c"]
    end
  end

  # ---------------------------------------------------------------------------
  # Line filters
  # ---------------------------------------------------------------------------

  describe "line filters" do
    setup do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "connection timeout"),
        record(timestamp_ns: 20, service: "api", body: "connection ok"),
        record(timestamp_ns: 30, service: "api", body: "timeout at gateway")
      ])

      :ok
    end

    test "substring |=" do
      assert {:ok, streams} = run(~s({service="api"} |= "timeout"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["connection timeout", "timeout at gateway"]
    end

    test "substring !=" do
      assert {:ok, streams} = run(~s({service="api"} != "gateway"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["connection ok", "connection timeout"]
    end

    test "regex |~" do
      assert {:ok, streams} = run(~s({service="api"} |~ "^connection"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["connection ok", "connection timeout"]
    end

    test "chained line filters compose as AND" do
      assert {:ok, streams} = run(~s({service="api"} |= "connection" != "ok"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert lines == ["connection timeout"]
    end
  end

  # ---------------------------------------------------------------------------
  # Parsers, formats, drop/keep
  # ---------------------------------------------------------------------------

  describe "json parser" do
    test "extracts fields into labels" do
      log("acme", [
        record(
          timestamp_ns: 10,
          service: "api",
          body: ~s({"code":"500","msg":"boom"})
        ),
        record(
          timestamp_ns: 20,
          service: "api",
          body: ~s({"code":"200","msg":"ok"})
        )
      ])

      assert {:ok, streams} = run(~s({service="api"} | json | code = "500"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert lines == [~s({"code":"500","msg":"boom"})]
    end
  end

  describe "logfmt parser" do
    test "extracts pairs into labels" do
      log("acme", [
        record(
          timestamp_ns: 10,
          service: "api",
          body: ~s(method=GET path="/users" code=200)
        ),
        record(
          timestamp_ns: 20,
          service: "api",
          body: ~s(method=POST path="/users" code=500)
        )
      ])

      assert {:ok, streams} = run(~s({service="api"} | logfmt | code = "500"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert length(lines) == 1
    end
  end

  describe "label_filter with duration and bytes" do
    test "duration comparison" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "a", attributes: %{"dur" => "5s"}),
        record(timestamp_ns: 20, service: "api", body: "b", attributes: %{"dur" => "500ms"})
      ])

      assert {:ok, streams} = run(~s({service="api"} | dur > 1s))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert lines == ["a"]
    end
  end

  describe "line_format and label_format" do
    test "line_format rewrites the log line" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "orig", resource: %{"env" => "prod"})
      ])

      assert {:ok, streams} = run(~s({service="api"} | line_format "hi {{.env}}"))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert lines == ["hi prod"]
    end

    test "label_format renames labels" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "x", resource: %{"env" => "prod"})
      ])

      assert {:ok, streams} = run(~s({service="api"} | label_format e=env))
      [{labels, _}] = streams
      assert labels["e"] == "prod"
      refute Map.has_key?(labels, "env")
    end
  end

  describe "drop and keep" do
    test "drop removes labels" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "x", resource: %{"env" => "prod", "region" => "us"})
      ])

      assert {:ok, streams} = run(~s({service="api"} | drop region))
      [{labels, _}] = streams
      refute Map.has_key?(labels, "region")
      assert labels["env"] == "prod"
    end

    test "keep filters to named labels" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "x", resource: %{"env" => "prod", "region" => "us"})
      ])

      assert {:ok, streams} = run(~s({service="api"} | keep env))
      [{labels, _}] = streams
      assert labels == %{"env" => "prod"}
    end
  end

  # ---------------------------------------------------------------------------
  # Direction and limit
  # ---------------------------------------------------------------------------

  describe "direction and limit" do
    test "backward is descending time" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "old"),
        record(timestamp_ns: 20, service: "api", body: "new")
      ])

      assert {:ok, [{_l, entries}]} = run(~s({service="api"}), "acme", %{direction: :backward})
      assert entries == [{20, "new"}, {10, "old"}]
    end

    test "forward reverses" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "old"),
        record(timestamp_ns: 20, service: "api", body: "new")
      ])

      assert {:ok, [{_l, entries}]} = run(~s({service="api"}), "acme", %{direction: :forward})
      assert entries == [{10, "old"}, {20, "new"}]
    end

    test "limit caps total entries across streams" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "a"),
        record(timestamp_ns: 20, service: "api", body: "b"),
        record(timestamp_ns: 30, service: "api", body: "c")
      ])

      assert {:ok, streams} = run(~s({service="api"}), "acme", %{limit: 2})
      total = Enum.map(streams, fn {_, e} -> length(e) end) |> Enum.sum()
      assert total == 2
    end

    test "limit: 0 is treated as no limit, not a crash" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", body: "a"),
        record(timestamp_ns: 20, service: "api", body: "b")
      ])

      assert {:ok, streams} = run(~s({service="api"}), "acme", %{limit: 0})
      total = Enum.map(streams, fn {_, e} -> length(e) end) |> Enum.sum()
      assert total == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Static validation: bad regex and ip(...) get clean errors, not silent
  # wrong answers or crashes.
  # ---------------------------------------------------------------------------

  describe "validation" do
    test "invalid regex in selector returns :invalid_regex" do
      assert {:error, {:invalid_regex, "env", "(", _reason}} =
               run(~s({env=~"("}))
    end

    test "invalid regex in line filter returns :invalid_regex" do
      assert {:error, {:invalid_regex, :line_filter, _, _}} =
               run(~s({service="api"} |~ "("))
    end

    test "invalid regex in label filter returns :invalid_regex" do
      assert {:error, {:invalid_regex, {:label_filter, "code"}, _, _}} =
               run(~s({service="api"} | code =~ "("))
    end

    test "invalid regex in regexp parser returns :invalid_regex" do
      assert {:error, {:invalid_regex, :regexp_stage, _, _}} =
               run(~s({service="api"} | regexp "("))
    end

    test "ip line filter returns :unsupported" do
      assert {:error, {:unsupported, :ip_line_filter, _cidr}} =
               run(~s[{service="api"} |= ip("10.0.0.0/8")])
    end
  end

  # ---------------------------------------------------------------------------
  # Adapter parity: attributes are NOT stream labels for selector matchers.
  # ---------------------------------------------------------------------------

  describe "selector-matcher parity" do
    test "attributes are NOT matched by stream selector" do
      log("acme", [
        record(
          timestamp_ns: 10,
          service: "api",
          body: "a",
          # http_method is per-record structured metadata, not a stream label
          attributes: %{"http_method" => "GET"}
        )
      ])

      # {http_method="GET"} looks in resource + promoted fields only,
      # never attributes — matches Loki semantics and the S3 Rust decoder.
      assert {:ok, streams} = run(~s({http_method="GET"}))
      assert streams == []

      # The label filter after `|` DOES see attributes.
      assert {:ok, [{_labels, entries}]} = run(~s({service="api"} | http_method = "GET"))
      assert length(entries) == 1
    end

    test "regex on promoted service field works" do
      log("acme", [
        record(timestamp_ns: 10, service: "api-users", body: "a"),
        record(timestamp_ns: 20, service: "api-orders", body: "b"),
        record(timestamp_ns: 30, service: "db", body: "c")
      ])

      assert {:ok, streams} = run(~s({service=~"api-.*"}))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["a", "b"]
    end

    test "regex on promoted level field works" do
      log("acme", [
        record(timestamp_ns: 10, service: "api", severity_text: "INFO", body: "a"),
        record(timestamp_ns: 20, service: "api", severity_text: "WARN", body: "b"),
        record(timestamp_ns: 30, service: "api", severity_text: "ERROR", body: "c")
      ])

      assert {:ok, streams} = run(~s({level=~"INFO|WARN"}))
      lines = for {_, entries} <- streams, {_, line} <- entries, do: line
      assert Enum.sort(lines) == ["a", "b"]
    end
  end
end
