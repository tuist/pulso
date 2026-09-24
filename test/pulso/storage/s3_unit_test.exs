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

  describe "object key construction" do
    test "with an idempotency key, the same call maps to the same key" do
      k1 = S3.object_key("acme", 100, "payload", "req-1")
      k2 = S3.object_key("acme", 100, "payload", "req-1")
      assert k1 == k2
    end

    test "with an idempotency key, different keys map to different objects" do
      k1 = S3.object_key("acme", 100, "payload", "req-1")
      k2 = S3.object_key("acme", 100, "payload", "req-2")
      refute k1 == k2
    end

    test "without an idempotency key, identical payloads still map to distinct objects" do
      # This is the "two legitimate producers happen to have identical bytes"
      # case Codex flagged in review 2: content-addressing alone would
      # silently drop one. With no idempotency key we always take a random
      # suffix so distinct calls always land on distinct objects.
      k1 = S3.object_key("acme", 100, "payload", nil)
      k2 = S3.object_key("acme", 100, "payload", nil)
      refute k1 == k2
    end

    test "an empty-string idempotency key is treated as absent" do
      # Prevents a client that sends `Idempotency-Key: ` from accidentally
      # collapsing every batch onto one key.
      k1 = S3.object_key("acme", 100, "payload", "")
      k2 = S3.object_key("acme", 100, "payload", "")
      refute k1 == k2
    end

    test "the same idempotency key with different content produces different objects" do
      # A caller who reuses an idempotency key with a different payload is
      # almost certainly buggy. We must not silently overwrite the earlier
      # write with the newer one; distinct content should land on distinct
      # keys.
      k1 = S3.object_key("acme", 100, "payload-a", "req-1")
      k2 = S3.object_key("acme", 100, "payload-b", "req-1")
      refute k1 == k2
    end

    test "the idempotency key is scoped by tenant" do
      # A shared idempotency key across tenants must not collide.
      k1 = S3.object_key("alpha", 100, "p", "req-1")
      k2 = S3.object_key("beta", 100, "p", "req-1")
      refute String.replace(k1, "alpha", "beta") == k2
    end

    test "different tenants never share a prefix" do
      k1 = S3.object_key("alpha", 100, "p", "req-1")
      k2 = S3.object_key("beta", 100, "p", "req-1")
      refute String.starts_with?(k1, "tenants/beta/")
      refute String.starts_with?(k2, "tenants/alpha/")
    end

    test "keys sort chronologically by sort_ns within a tenant" do
      k_early = S3.object_key("acme", 100, "p", "req-1")
      k_late = S3.object_key("acme", 200, "p", "req-1")
      assert k_early < k_late
    end
  end

  describe "attribute sanitization" do
    test "coerces non-string map keys to strings recursively" do
      assert {:ok, sanitized} =
               S3.sanitize_map(%{
                 :status => 200,
                 "nested" => %{404 => "missing", :ref => "abc"},
                 "list" => [%{true => 1}]
               })

      assert Map.has_key?(sanitized, "status")
      assert sanitized["nested"] == %{"404" => "missing", "ref" => "abc"}
      assert sanitized["list"] == [%{"true" => 1}]
    end

    test "rejects rather than collapses keys that stringify to the same value" do
      # A previous version silently dropped one value on collision. Codex
      # flagged the risk (map size decreases without a diagnostic). Now we
      # return an explicit error so the caller can surface it.
      assert {:error, {:attribute_key_collision, _}} =
               S3.sanitize_map(%{1 => "int", "1" => "string"})
    end
  end
end
