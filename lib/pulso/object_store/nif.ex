defmodule Pulso.ObjectStore.NIF do
  @moduledoc false

  use Rustler, otp_app: :pulso, crate: "pulso_object_store"

  def put(_config, _key, _data), do: :erlang.nif_error(:nif_not_loaded)
  def get(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def list(_config, _prefix), do: :erlang.nif_error(:nif_not_loaded)
end
