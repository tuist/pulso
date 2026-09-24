defmodule Pulso.Storage.S3UnitTest do
  # Pure-Elixir tests of Pulso.Storage.S3's pre-network guards and key
  # construction. Anything that actually talks to an S3 endpoint lives in
  # s3_test.exs behind the `:integration` tag.

  use ExUnit.Case, async: true

  alias Pulso.Record.Log
  alias Pulso.Storage.S3

  describe "tenant validation" do
    test "rejects adversarial tenant names before touching the object store" do
      for bad <- ["", "../evil", "foo/bar", "foo bar", String.duplicate("a", 200)] do
        assert {:error, {:invalid_tenant, ^bad}} = S3.append(bad, [%Log{timestamp_ns: 1}])
        assert {:error, {:invalid_tenant, ^bad}} = S3.query(bad, [])
      end
    end

    test "rejects adversarial tenant names even for empty batches" do
      # Prior version bailed early on `[]` and returned :ok, letting an
      # attacker probe the auth surface with a no-op payload.
      assert {:error, {:invalid_tenant, "../evil"}} = S3.append("../evil", [])
      assert {:error, {:invalid_tenant, ""}} = S3.append("", [])
    end

    test "accepts common tenant name shapes" do
      for good <- ["default", "customer-42", "team.alpha", "svc_web"] do
        assert :ok = S3.append(good, [])
      end
    end
  end

  describe "encode failures" do
    test "returns an error tuple instead of raising on non-UTF-8 body bytes" do
      record = %Log{timestamp_ns: 1, body: <<0xFF, 0xFE>>}

      assert {:error, {:encode_failed, _}} = S3.append("default", [record])
    end
  end

  describe "content-addressed object keys" do
    test "the same payload maps to the same key so retries do not duplicate" do
      # Two callers PUT the same batch; if the response of the first is lost
      # and the second retries, the deterministic key means both PUTs land on
      # the same object and the second is a no-op overwrite.
      k1 = S3.object_key("acme", 100, "same-batch")
      k2 = S3.object_key("acme", 100, "same-batch")
      assert k1 == k2
    end

    test "different payloads map to different keys" do
      k1 = S3.object_key("acme", 100, "batch-a")
      k2 = S3.object_key("acme", 100, "batch-b")
      refute k1 == k2
    end

    test "different tenants never share a prefix" do
      # Substring is enough — S3 list uses prefix, so any escape would show
      # up as a shared prefix here.
      k1 = S3.object_key("alpha", 100, "shared")
      k2 = S3.object_key("beta", 100, "shared")
      refute String.starts_with?(k1, "tenants/beta/")
      refute String.starts_with?(k2, "tenants/alpha/")
    end

    test "keys sort chronologically by sort_ns within a tenant" do
      k_early = S3.object_key("acme", 100, "x")
      k_late = S3.object_key("acme", 200, "x")
      # S3 list returns keys in UTF-8 byte order; zero-padding puts earlier
      # timestamps first.
      assert k_early < k_late
    end
  end

  describe "attribute sanitization" do
    test "coerces non-string map keys to strings recursively" do
      sanitized =
        S3.sanitize_map(%{
          :status => 200,
          "nested" => %{404 => "missing", :ref => "abc"},
          "list" => [%{true => 1}]
        })

      assert Map.has_key?(sanitized, "status")
      assert sanitized["nested"] == %{"404" => "missing", "ref" => "abc"}
      assert sanitized["list"] == [%{"true" => 1}]
    end

    test "collapses keys that stringify to the same value" do
      # This is a hazard the sanitizer surfaces rather than hides: if two
      # keys collapse, the map loses a pair. Documenting it as expected here
      # means a future callsite that relies on distinct integer/string keys
      # will have to model that at its own layer.
      sanitized = S3.sanitize_map(%{1 => "int", "1" => "string"})
      assert map_size(sanitized) == 1
    end
  end
end
