defmodule Pulso.Storage.S3.ManifestCacheTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestCache

  setup do
    start_supervised!(ManifestCache)
    :ok
  end

  test "late 304 and 200 refreshes cannot regress a newer acknowledged owner publication" do
    old = Manifest.new()
    new = %{old | version: 2}
    assert :ok = ManifestCache.put("tenant", "logs", old, "v1")
    starting = ManifestCache.get("tenant", "logs")
    assert :ok = ManifestCache.put("tenant", "logs", new, "v2")

    # A delayed conditional GET may return either unchanged v1 or another
    # snapshot read before v2 was published. Neither may regress the cache.
    for response_etag <- ["v1", "intermediate"] do
      assert :ok = ManifestCache.put_if_current("tenant", "logs", starting.manifest, response_etag, starting.etag)
      assert %{manifest: ^new, etag: "v2"} = ManifestCache.get("tenant", "logs")
    end

    assert :ok = ManifestCache.put_if_current("tenant", "logs", new, "v3", "v2")
    assert %{etag: "v3"} = ManifestCache.get("tenant", "logs")
  end

  test "publication installs on its base ETag and invalidates incomparable intervening snapshots" do
    old = Manifest.new()
    published = %{old | version: 2}
    ManifestCache.put("tenant", "logs", old, "base")
    base = ManifestCache.publication_base("tenant", "logs")
    assert :ok = ManifestCache.publish("tenant", "logs", published, "published", "base", base)
    assert %{etag: "published"} = ManifestCache.get("tenant", "logs")

    # Either a delayed older publisher or a newer publisher whose cache base
    # was superseded must force a reload; tags themselves cannot decide order.
    assert :ok = ManifestCache.publish("tenant", "logs", old, "delayed", "base", base)
    assert ManifestCache.get("tenant", "logs") == nil
    assert :ok = ManifestCache.publish("tenant", "logs", published, "fresh", "uncached-base", base)
    assert ManifestCache.get("tenant", "logs") == nil
    {nil, token} = ManifestCache.snapshot("tenant", "logs")
    assert :ok = ManifestCache.put_snapshot("tenant", "logs", published, "fresh", token)
    assert %{etag: "fresh"} = ManifestCache.get("tenant", "logs")
  end

  test "cold publication is fenced by the empty generation observed before CAS" do
    manifest = Manifest.new()
    base = ManifestCache.publication_base("tenant", "logs")
    ManifestCache.publish("tenant", "logs", manifest, "first", nil, base)
    assert %{etag: "first"} = ManifestCache.get("tenant", "logs")
    ManifestCache.drop("tenant", "logs")
    ManifestCache.publish("tenant", "logs", manifest, "delayed-first", nil, base)
    assert ManifestCache.get("tenant", "logs") == nil
    fresh = ManifestCache.publication_base("tenant", "logs")
    ManifestCache.publish("tenant", "logs", manifest, "latest", "first", fresh)
    assert %{etag: "latest"} = ManifestCache.get("tenant", "logs")
  end

  test "invalidation fences delayed publications and reads that began with an absent cache" do
    old = Manifest.new()
    {nil, absent} = ManifestCache.snapshot("tenant", "logs")
    ManifestCache.put("tenant", "logs", old, "X")
    base = ManifestCache.publication_base("tenant", "logs")
    # Z committed after Y, but its predecessor Y was never installed locally.
    ManifestCache.publish("tenant", "logs", old, "Z", "Y", base)
    assert ManifestCache.get("tenant", "logs") == nil
    ManifestCache.publish("tenant", "logs", old, "Y", "X", base)
    ManifestCache.put_snapshot("tenant", "logs", old, "pre-ACK-read", absent)
    assert ManifestCache.get("tenant", "logs") == nil
    {nil, token} = ManifestCache.snapshot("tenant", "logs")
    ManifestCache.put_snapshot("tenant", "logs", old, "fresh-Z", token)
    assert %{etag: "fresh-Z"} = ManifestCache.get("tenant", "logs")
  end

  test "publication generations also fence repeated predecessor ETags" do
    manifest = Manifest.new()

    for initially_warm? <- [false, true] do
      ManifestCache.drop("tenant", "logs")
      if initially_warm?, do: ManifestCache.put("tenant", "logs", manifest, "A")
      base = ManifestCache.publication_base("tenant", "logs")
      ManifestCache.put("tenant", "logs", manifest, "B")
      ManifestCache.put("tenant", "logs", manifest, "A")
      ManifestCache.publish("tenant", "logs", manifest, "delayed-B", "A", base)
      assert ManifestCache.get("tenant", "logs") == nil
    end
  end

  test "cold readers insert only while the cache is still absent" do
    manifest = Manifest.new()
    assert :ok = ManifestCache.put_if_current("tenant", "metrics", manifest, "initial", nil)
    assert :ok = ManifestCache.put("tenant", "metrics", manifest, "published")
    assert :ok = ManifestCache.put_if_current("tenant", "metrics", manifest, "late-initial", nil)
    assert %{etag: "published"} = ManifestCache.get("tenant", "metrics")
  end
end
