defmodule Pulso.Runtime.ProcessGroup do
  @moduledoc false
  def join(scope, group, pid), do: :pg.join(Pulso.Runtime.name(scope), group, pid)
  def leave(scope, group, pid), do: :pg.leave(Pulso.Runtime.name(scope), group, pid)
  def get_members(scope, group), do: :pg.get_members(Pulso.Runtime.name(scope), group)
  def get_local_members(scope, group), do: :pg.get_local_members(Pulso.Runtime.name(scope), group)
end
