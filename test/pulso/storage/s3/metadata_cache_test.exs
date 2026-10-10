defmodule Pulso.Storage.S3.MetadataCacheTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Runtime
  alias Pulso.Storage.S3.MetadataCache

  setup do
    start_supervised!(MetadataCache)
    :ok
  end

  test "decoded heap occupancy triggers eviction independently of encoded byte accounting" do
    data = List.duplicate({1, 2}, 150_000)
    for n <- 1..4, do: assert(:ok = MetadataCache.put(%{"k" => "page-#{n}", "b" => 64}, data))
    table = Runtime.table(MetadataCache)
    assert :ets.info(table, :memory) * :erlang.system_info(:wordsize) <= 16_777_216
    assert :ets.info(table, :size) < 4
  end

  test "cached references do not pin the root from which they were decoded" do
    root = :binary.copy("x", 524_288)
    key = binary_part(root, 100, 128)
    ref = %{"k" => key, "b" => 64}
    assert :ok = MetadataCache.put(ref, :data)
    assert {:ok, :data} = MetadataCache.get(ref)
    [{stored_key, stored_ref, _, _, _}] = :ets.lookup(Runtime.table(MetadataCache), key)
    assert :binary.referenced_byte_size(stored_key) == byte_size(stored_key)
    assert :binary.referenced_byte_size(stored_ref["k"]) == byte_size(stored_ref["k"])
  end
end
