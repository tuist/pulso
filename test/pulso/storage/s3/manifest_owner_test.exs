defmodule Pulso.Storage.S3.ManifestOwnerTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Runtime
  alias Pulso.Runtime.Registry
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.ManifestRegistry
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.ManifestSupervisor

  test "an exited owner is replaced before asynchronous registry cleanup completes" do
    start_supervised!(ManifestSupervision)
    tenant = "stale-owner"
    key = {tenant, "logs"}
    {:ok, partition} = Registry.register(ManifestRegistry, :cleanup_barrier, nil)
    Registry.unregister(ManifestRegistry, :cleanup_barrier)
    assert {:ok, owner} = ManifestOwner.ensure_started(tenant, "logs", %{})
    ref = Process.monitor(owner)

    :ok = :sys.suspend(partition)

    try do
      :ok = DynamicSupervisor.terminate_child(Runtime.name(ManifestSupervisor), owner)
      assert_receive {:DOWN, ^ref, :process, ^owner, _reason}
      assert [{^owner, _}] = Registry.lookup(ManifestRegistry, key)

      assert {:ok, replacement} = ManifestOwner.ensure_started(tenant, "logs", %{})
      assert replacement != owner
      assert [{^replacement, _}] = Registry.lookup(ManifestRegistry, key)
      assert :sys.get_state(replacement).tenant == tenant
    after
      :sys.resume(partition)
    end
  end
end
