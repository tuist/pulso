defmodule Pulso.Storage.S3.MetricMetadataStoreTest do
  use Pulso.Test.Case, async: true

  alias Pulso.MetricMetadata
  alias Pulso.Storage.S3.MetricMetadataStore
  alias Pulso.Test.CompactionStore

  setup do
    store =
      start_supervised!(
        {Agent,
         fn ->
           %{
             objects: %{},
             reads: [],
             version: 0,
             hook: nil,
             faults: %{},
             barriers: %{},
             lists: 0,
             deletes: [],
             requests: []
           }
         end}
      )

    # Under concurrent load the S3 client opens spare connections it may never
    # use; Bandit waits in a blocking read on those, so close them at teardown
    # instead of waiting out the 60 s read timeout.
    server =
      start_supervised!(
        {Bandit,
         plug: {CompactionStore, agent: store},
         ip: {127, 0, 0, 1},
         port: 0,
         thousand_island_options: [shutdown_timeout: 0]}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{port}",
      region: "us-east-1",
      access_key_id: "test-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true
    }

    {:ok, config: config, tenant: "metadata-#{System.unique_integer([:positive])}"}
  end

  test "concurrent writers merge rather than overwrite each other's variants", ctx do
    today = MetricMetadata.today()

    put = fn i ->
      MetricMetadataStore.put(ctx.config, ctx.tenant, [{"metric_#{rem(i, 4)}", "gauge", "v#{i}", ""}], today)
    end

    1..32
    |> Task.async_stream(put, max_concurrency: 32, timeout: :infinity)
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert {:ok, set} = MetricMetadataStore.get(ctx.config, ctx.tenant)
    assert set |> Map.values() |> Enum.map(&map_size/1) |> Enum.sum() == 32
  end

  test "oversized stored metadata fails before unbounded download or merge", ctx do
    body = String.duplicate("x", MetricMetadata.max_bytes() + 1)
    assert {:ok, _etag} = Pulso.ObjectStore.put(ctx.config, MetricMetadataStore.key(ctx.tenant), body)
    assert {:error, :response_too_large} = MetricMetadataStore.get(ctx.config, ctx.tenant)

    assert {:error, :response_too_large} =
             MetricMetadataStore.put(ctx.config, ctx.tenant, [{"up", "gauge", "", ""}], 100)
  end

  test "an unchanged set is not written again, and a missing object is an empty set", ctx do
    assert {:ok, %{}} = MetricMetadataStore.get(ctx.config, ctx.tenant)
    entries = [{"up", "gauge", "Target up.", ""}]
    assert :ok = MetricMetadataStore.put(ctx.config, ctx.tenant, entries, 100)
    {:ok, etag, _body} = Pulso.ObjectStore.get_if_none_match(ctx.config, MetricMetadataStore.key(ctx.tenant), nil)
    assert :ok = MetricMetadataStore.put(ctx.config, ctx.tenant, entries, 100)
    assert :not_modified = Pulso.ObjectStore.get_if_none_match(ctx.config, MetricMetadataStore.key(ctx.tenant), etag)
  end
end
