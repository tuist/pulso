defmodule Pulso.Runtime.Supervisor do
  @moduledoc false
  use Supervisor

  @impl true
  def init({runtime, module, args}) do
    Pulso.Runtime.install(runtime)
    module.init(args)
  end
end
