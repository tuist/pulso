defmodule Pulso.Storage.S3.ManifestTest do
  use ExUnit.Case, async: true

  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment

  defp seg(key, min_ts, max_ts, rows \\ 1) do
    Segment.build(key, min_ts, max_ts, rows)
  end

  test "manifest_key/2 encodes tenant and signal into the S3 path" do
    assert Manifest.manifest_key("acme", "logs") == "tenants/acme/v2/logs/manifest.json"
    assert Manifest.manifest_key("acme") == "tenants/acme/v2/logs/manifest.json"
  end

  test "encode/decode round-trips with all fields" do
    manifest = %Manifest{
      version: 1,
      segments: [
        seg("tenants/t/v2/logs/00-10-a.ndjson", 5, 10, 4),
        seg("tenants/t/v2/logs/11-20-b.ndjson", 11, 20, 8)
      ]
    }

    {:ok, decoded} =
      manifest
      |> Manifest.encode()
      |> IO.iodata_to_binary()
      |> Manifest.decode()

    # Segments come back sorted by max_ts desc regardless of input order.
    assert Enum.map(decoded.segments, & &1.max_ts) == [20, 10]
    assert Enum.map(decoded.segments, & &1.min_ts) == [11, 5]
    assert Enum.map(decoded.segments, & &1.row_count) == [8, 4]
    assert decoded.version == 1
  end

  test "decode enforces sorted invariant even on scrambled input" do
    scrambled = %{
      "v" => 1,
      "s" => [
        %{"k" => "a", "mn" => 5, "mx" => 10, "r" => 1},
        %{"k" => "c", "mn" => 30, "mx" => 40, "r" => 1},
        %{"k" => "b", "mn" => 15, "mx" => 20, "r" => 1}
      ]
    }

    {:ok, decoded} =
      scrambled
      |> JSON.encode!()
      |> Manifest.decode()

    assert Enum.map(decoded.segments, & &1.max_ts) == [40, 20, 10]
  end

  test "decode rejects a payload missing required fields" do
    bad = ~s({"v": 1, "s": [{"k": "a"}]})
    assert {:error, :invalid_segment} = Manifest.decode(bad)
  end

  test "decode rejects a payload that is not JSON" do
    assert {:error, {:decode_failed, _}} = Manifest.decode("not json at all")
  end

  test "merge/2 preserves the sorted invariant and dedupes on key" do
    manifest = %Manifest{
      segments: [
        seg("b", 10, 20, 2),
        seg("a", 0, 5, 1)
      ]
    }

    new = [
      # Supersedes the existing "b" with fresh row_count
      seg("b", 10, 20, 99),
      # Slots between "b" and "a" by max_ts desc
      seg("c", 6, 8, 3)
    ]

    merged = Manifest.merge(manifest, new)

    assert Enum.map(merged.segments, & &1.key) == ["b", "c", "a"]
    assert Enum.map(merged.segments, & &1.max_ts) == [20, 8, 5]
    # b was replaced by the incoming segment (its row_count updated to 99).
    assert Enum.find(merged.segments, &(&1.key == "b")).row_count == 99
  end

  test "merge/2 with an empty batch is identity" do
    manifest = %Manifest{segments: [seg("a", 0, 5, 1)]}
    assert Manifest.merge(manifest, []) == manifest
  end

  test "merge/2 dedups an incoming batch that repeats a key" do
    # The retry-idempotency path: two register_segments calls carrying
    # the same idempotency key produce two segments with an identical
    # S3 key. Both may arrive in the same flush window. If merge lets
    # both through, the manifest ends up with two entries pointing at
    # one S3 object — a later query would fetch that object twice.
    manifest = %Manifest{segments: []}
    dup = seg("same-key", 10, 20, 2)
    merged = Manifest.merge(manifest, [dup, dup])

    assert Enum.map(merged.segments, & &1.key) == ["same-key"]
  end

  test "prune_by_time drops segments outside the requested range" do
    manifest = %Manifest{
      segments: [
        seg("c", 30, 40, 1),
        seg("b", 10, 20, 1),
        seg("a", 0, 5, 1)
      ]
    }

    kept = Manifest.prune_by_time(manifest, 15, 35)
    assert Enum.map(kept, & &1.key) == ["c", "b"]

    kept_open_start = Manifest.prune_by_time(manifest, nil, 12)
    assert Enum.map(kept_open_start, & &1.key) == ["b", "a"]
  end

  test "prune_by_time with nil bounds returns everything" do
    manifest = %Manifest{
      segments: [seg("a", 0, 5), seg("b", 10, 20)]
    }

    assert Manifest.prune_by_time(manifest, nil, nil) == manifest.segments
  end
end
