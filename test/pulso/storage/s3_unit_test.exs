defmodule Pulso.Storage.S3UnitTest do
  # Pure-Elixir tests of Pulso.Storage.S3's pre-network guards. Anything that
  # actually talks to an S3 endpoint lives in s3_test.exs behind the
  # `:integration` tag.

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
      # These do not touch the object store because the batch is empty.
      for good <- ["default", "customer-42", "team.alpha", "svc_web"] do
        assert :ok = S3.append(good, [])
      end
    end
  end

  describe "encode failures" do
    test "returns an error tuple instead of raising on non-UTF-8 body bytes" do
      # A Jason encoder that meets non-UTF-8 bytes in a string field must not
      # crash the ingest process; it must surface an error the OTLP controller
      # can map to a 4xx/5xx response.
      record = %Log{timestamp_ns: 1, body: <<0xFF, 0xFE>>}

      # `append` on a well-formed tenant with a non-encodable body should
      # bail before any network I/O.
      assert {:error, {:encode_failed, _}} = S3.append("default", [record])
    end
  end
end
