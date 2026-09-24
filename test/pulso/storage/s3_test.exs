defmodule Pulso.Storage.S3Test do
  # Round-trips logs through the real S3-compatible endpoint (RustFS via
  # docker-compose). Only runs with PULSO_INTEGRATION=1; plain `mix test`
  # skips it. See test/test_helper.exs.

  use ExUnit.Case, async: false

  alias Pulso.ObjectStore
  alias Pulso.Record.Log
  alias Pulso.Storage.S3

  @moduletag :integration

  setup do
    config = %{
      bucket: System.get_env("PULSO_S3_BUCKET", "pulso"),
      endpoint: System.get_env("PULSO_S3_ENDPOINT", "http://localhost:11100"),
      region: System.get_env("PULSO_S3_REGION", "us-east-1"),
      access_key_id: System.get_env("PULSO_S3_ACCESS_KEY_ID", "rustfsadmin"),
      secret_access_key: System.get_env("PULSO_S3_SECRET_ACCESS_KEY", "rustfsadmin"),
      allow_http: true
    }

    Application.put_env(:pulso, S3, config)

    tenant = "test-#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # The adapter writes objects under `tenants/<tenant>/v2/logs/`; clean up so
      # a re-run starts empty.
      case ObjectStore.list(config, "tenants/#{tenant}/v2/logs/") do
        {:ok, keys} -> Enum.each(keys, &ObjectStore.delete(config, &1))
        _ -> :ok
      end
    end)

    {:ok, config: config, tenant: tenant}
  end

  defp record(ts, opts \\ []) do
    %Log{
      timestamp_ns: ts,
      severity_text: Keyword.get(opts, :severity_text),
      service: Keyword.get(opts, :service),
      body: Keyword.get(opts, :body),
      attributes: Keyword.get(opts, :attributes, %{}),
      resource: Keyword.get(opts, :resource, %{})
    }
  end

  test "append then query round-trips log records", %{tenant: tenant} do
    assert :ok =
             S3.append(tenant, [
               record(10, service: "api", body: "hello"),
               record(20, service: "web", body: "world")
             ])

    assert {:ok, records} = S3.query(tenant, [])
    assert Enum.map(records, & &1.timestamp_ns) == [20, 10]
    assert Enum.map(records, & &1.service) == ["web", "api"]
    assert Enum.map(records, & &1.body) == ["world", "hello"]
  end

  test "records for one tenant are invisible to another", %{tenant: tenant, config: config} do
    other = "test-other-#{System.unique_integer([:positive])}"

    on_exit(fn ->
      case ObjectStore.list(config, "tenants/#{other}/v2/logs/") do
        {:ok, keys} -> Enum.each(keys, &ObjectStore.delete(config, &1))
        _ -> :ok
      end
    end)

    assert :ok = S3.append(tenant, [record(1)])
    assert :ok = S3.append(other, [record(2)])

    assert {:ok, [%Log{timestamp_ns: 1}]} = S3.query(tenant, [])
    assert {:ok, [%Log{timestamp_ns: 2}]} = S3.query(other, [])
  end

  test "filters by time range and service", %{tenant: tenant} do
    assert :ok =
             S3.append(tenant, [
               record(10, service: "api"),
               record(20, service: "web"),
               record(30, service: "api"),
               record(40, service: "api")
             ])

    assert {:ok, records} = S3.query(tenant, start_ts: 15, end_ts: 35, service: "api")
    assert Enum.map(records, & &1.timestamp_ns) == [30]
  end

  test "applies limit", %{tenant: tenant} do
    assert :ok = S3.append(tenant, [record(1), record(2), record(3), record(4)])
    assert {:ok, records} = S3.query(tenant, limit: 2)
    assert length(records) == 2
    assert Enum.map(records, & &1.timestamp_ns) == [4, 3]
  end

  test "stores records verbatim without injecting a wall-clock timestamp", %{tenant: tenant} do
    # Wall-clock backfill would defeat retry idempotency (the second call
    # under the same idempotency key would overwrite the first with a
    # later observed_ts). Records that arrive without timestamps are
    # stored as they came in; queries with time bounds naturally skip
    # them, unbounded queries return them.
    assert :ok =
             S3.append(tenant, [
               %Log{timestamp_ns: nil, observed_timestamp_ns: nil, body: "no ts"}
             ])

    assert {:ok, [%Log{timestamp_ns: nil, observed_timestamp_ns: nil, body: "no ts"}]} =
             S3.query(tenant, [])
  end

  test "preserves NDJSON-hostile bodies through the round trip", %{tenant: tenant} do
    tricky = "line1\nline2\t\"quoted\"\r\nline3"
    assert :ok = S3.append(tenant, [record(1, body: tricky)])
    assert {:ok, [%Log{body: ^tricky}]} = S3.query(tenant, [])
  end

  test "append with an empty batch is a no-op", %{tenant: tenant, config: config} do
    assert :ok = S3.append(tenant, [])
    assert {:ok, keys} = ObjectStore.list(config, "tenants/#{tenant}/v2/logs/")
    assert keys == []
  end

  test "rejects tenant names that could escape the prefix" do
    for bad <- ["../evil", "foo/bar", "foo bar", "", String.duplicate("a", 200)] do
      assert {:error, {:invalid_tenant, ^bad}} = S3.append(bad, [record(1)])
      assert {:error, {:invalid_tenant, ^bad}} = S3.query(bad, [])
    end
  end

  test "same idempotency_key deduplicates identical retries", %{
    tenant: tenant,
    config: config
  } do
    # Opt-in idempotency: a caller that wants retry safety passes the same
    # idempotency_key on the retry. The object key becomes deterministic,
    # so the second PUT overwrites the first with identical content and no
    # duplicate appears at query time.
    batch = [record(1, body: "same", service: "api")]

    assert :ok = S3.append(tenant, batch, idempotency_key: "req-1")
    assert :ok = S3.append(tenant, batch, idempotency_key: "req-1")

    assert {:ok, keys} = ObjectStore.list(config, "tenants/#{tenant}/v2/logs/")
    assert length(keys) == 1

    assert {:ok, records} = S3.query(tenant, [])
    assert length(records) == 1
  end

  test "without an idempotency_key identical batches remain distinct writes", %{
    tenant: tenant,
    config: config
  } do
    # If a caller doesn't opt in, two producers with byte-identical payloads
    # must not silently collapse — that would drop data.
    batch = [record(1, body: "same", service: "api")]

    assert :ok = S3.append(tenant, batch)
    assert :ok = S3.append(tenant, batch)

    assert {:ok, keys} = ObjectStore.list(config, "tenants/#{tenant}/v2/logs/")
    assert length(keys) == 2
  end

  test "a key deleted after listing does not fail the query", %{
    tenant: tenant,
    config: config
  } do
    assert :ok = S3.append(tenant, [record(1), record(2)])
    assert :ok = S3.append(tenant, [record(3)])

    # Delete one of the objects between our own list and get, mimicking a
    # compaction / retention job racing with a query.
    assert {:ok, [first | _]} = ObjectStore.list(config, "tenants/#{tenant}/v2/logs/")
    assert :ok = ObjectStore.delete(config, first)

    # Query should still return the surviving records, not error.
    assert {:ok, remaining} = S3.query(tenant, [])
    assert remaining != []
  end

  test "equal timestamps sort deterministically across adapters", %{tenant: tenant} do
    a = %Log{timestamp_ns: 10, observed_timestamp_ns: 100, trace_id: "aaa"}
    b = %Log{timestamp_ns: 10, observed_timestamp_ns: 300, trace_id: "aaa"}
    c = %Log{timestamp_ns: 10, observed_timestamp_ns: 200, trace_id: "bbb"}

    assert :ok = S3.append(tenant, [a, b, c])
    assert {:ok, sorted} = S3.query(tenant, [])
    assert Enum.map(sorted, & &1.observed_timestamp_ns) == [300, 200, 100]
  end
end
