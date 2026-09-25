defmodule Pulso.ObjectStoreTest do
  # This module talks to a live S3-compatible endpoint (RustFS by default via
  # docker-compose.yml) through the Rust NIF. It only runs when the caller
  # opts in with PULSO_INTEGRATION=1 (see test/test_helper.exs), so plain
  # `mix test` on a machine without docker-compose still passes.

  use ExUnit.Case, async: false

  alias Pulso.ObjectStore

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

    key = "pulso-object-store-test/#{System.unique_integer([:positive])}.bin"

    on_exit(fn -> ObjectStore.delete(config, key) end)

    {:ok, config: config, key: key}
  end

  test "put then get returns the same bytes", %{config: config, key: key} do
    payload = :crypto.strong_rand_bytes(512)

    assert {:ok, etag} = ObjectStore.put(config, key, payload)
    assert is_binary(etag) and etag != ""
    assert {:ok, ^payload} = ObjectStore.get(config, key)
  end

  test "list returns keys under a prefix and delete removes them", %{config: config, key: key} do
    prefix = "pulso-object-store-test/list-#{System.unique_integer([:positive])}"
    key_a = "#{prefix}/a.txt"
    key_b = "#{prefix}/b.txt"

    on_exit(fn ->
      ObjectStore.delete(config, key_a)
      ObjectStore.delete(config, key_b)
    end)

    assert {:ok, _} = ObjectStore.put(config, key_a, "one")
    assert {:ok, _} = ObjectStore.put(config, key_b, "two")
    assert {:ok, _} = ObjectStore.put(config, key, "three")

    assert {:ok, keys} = ObjectStore.list(config, prefix)
    assert Enum.sort(keys) == [key_a, key_b]

    assert :ok = ObjectStore.delete(config, key_a)
    assert {:ok, [^key_b]} = ObjectStore.list(config, prefix)
  end

  test "get on a missing key returns an error", %{config: config} do
    missing = "pulso-object-store-test/missing-#{System.unique_integer([:positive])}"
    assert {:error, _reason} = ObjectStore.get(config, missing)
  end

  # Conditional-write coverage. These are what makes the manifest CAS work.
  describe "conditional writes" do
    test "put_if_none_match creates then rejects a duplicate create", %{config: config, key: key} do
      assert {:ok, etag} = ObjectStore.put_if_none_match(config, key, "one")
      assert is_binary(etag) and etag != ""
      assert {:error, :already_exists} = ObjectStore.put_if_none_match(config, key, "two")
      assert {:ok, "one"} = ObjectStore.get(config, key)
    end

    test "put_if_match updates on matching etag and rejects a stale one", %{
      config: config,
      key: key
    } do
      assert {:ok, etag1} = ObjectStore.put(config, key, "one")
      assert {:ok, etag2} = ObjectStore.put_if_match(config, key, "two", etag1)
      assert etag2 != etag1
      assert {:ok, "two"} = ObjectStore.get(config, key)
      assert {:error, :precondition_failed} = ObjectStore.put_if_match(config, key, "three", etag1)
      assert {:ok, "two"} = ObjectStore.get(config, key)
    end

    test "get_if_none_match returns :not_modified for a matching etag", %{config: config, key: key} do
      assert {:ok, etag} = ObjectStore.put(config, key, "one")
      assert :not_modified = ObjectStore.get_if_none_match(config, key, etag)
      assert {:ok, new_etag, "one"} = ObjectStore.get_if_none_match(config, key, nil)
      assert new_etag == etag
    end

    test "get_if_none_match returns :not_found for a missing key", %{config: config} do
      missing = "pulso-object-store-test/missing-cond-#{System.unique_integer([:positive])}"
      assert {:error, :not_found} = ObjectStore.get_if_none_match(config, missing, nil)
    end
  end
end
