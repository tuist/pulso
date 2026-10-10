defmodule Pulso.Runtime.Registry do
  @moduledoc false
  def child_spec(opts), do: Registry.child_spec(Keyword.update!(opts, :name, &Pulso.Runtime.name/1))
  def register_name({registry, key}, pid), do: Registry.register_name({Pulso.Runtime.name(registry), key}, pid)
  def unregister_name({registry, key}), do: Registry.unregister_name({Pulso.Runtime.name(registry), key})
  def whereis_name({registry, key}), do: Registry.whereis_name({Pulso.Runtime.name(registry), key})
  def send({registry, key}, message), do: Registry.send({Pulso.Runtime.name(registry), key}, message)
  def lookup(registry, key), do: Registry.lookup(Pulso.Runtime.name(registry), key)
  def select(registry, spec), do: Registry.select(Pulso.Runtime.name(registry), spec)
  def register(registry, key, value), do: Registry.register(Pulso.Runtime.name(registry), key, value)
  def unregister(registry, key), do: Registry.unregister(Pulso.Runtime.name(registry), key)
  def update_value(registry, key, fun), do: Registry.update_value(Pulso.Runtime.name(registry), key, fun)
  def count(registry), do: Registry.count(Pulso.Runtime.name(registry))
end
