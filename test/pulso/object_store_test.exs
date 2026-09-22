defmodule Pulso.ObjectStoreTest do
  # This module talks to a live MinIO endpoint via the Rust NIF.
  # It only runs when the caller opts in with PULSO_INTEGRATION=1 (see
  # test/test_helper.exs), so plain `mix test` on a machine without
  # docker-compose still passes.

  use ExUnit.Case, async: false

  alias Pulso.ObjectStore

  @moduletag :integration

  setup do
    config = %{
      bucket: System.get_env("PULSO_MINIO_BUCKET", "pulso"),
      endpoint: System.get_env("PULSO_MINIO_ENDPOINT", "http://localhost:9000"),
      region: System.get_env("PULSO_MINIO_REGION", "us-east-1"),
      access_key_id: System.get_env("PULSO_MINIO_ACCESS_KEY_ID", "minioadmin"),
      secret_access_key: System.get_env("PULSO_MINIO_SECRET_ACCESS_KEY", "minioadmin"),
      allow_http: true
    }

    key = "pulso-object-store-test/#{System.unique_integer([:positive])}.bin"

    on_exit(fn -> ObjectStore.delete(config, key) end)

    {:ok, config: config, key: key}
  end

  test "put then get returns the same bytes", %{config: config, key: key} do
    payload = :crypto.strong_rand_bytes(512)

    assert :ok = ObjectStore.put(config, key, payload)
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

    assert :ok = ObjectStore.put(config, key_a, "one")
    assert :ok = ObjectStore.put(config, key_b, "two")
    assert :ok = ObjectStore.put(config, key, "three")

    assert {:ok, keys} = ObjectStore.list(config, prefix)
    assert Enum.sort(keys) == [key_a, key_b]

    assert :ok = ObjectStore.delete(config, key_a)
    assert {:ok, [^key_b]} = ObjectStore.list(config, prefix)
  end

  test "get on a missing key returns an error", %{config: config} do
    missing = "pulso-object-store-test/missing-#{System.unique_integer([:positive])}"
    assert {:error, _reason} = ObjectStore.get(config, missing)
  end
end
