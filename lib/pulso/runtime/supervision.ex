defmodule Pulso.Runtime.Supervision do
  @moduledoc false
  defmacro __using__(opts) do
    quote do: use(Elixir.Supervisor, unquote(opts))
  end

  def start_link(module, args, opts), do: Pulso.Runtime.supervisor_start(module, args, opts)

  def start_link(children, opts),
    do:
      Elixir.Supervisor.start_link(
        Enum.map(children, &Pulso.Runtime.child_spec/1),
        Keyword.update(opts, :name, nil, &Pulso.Runtime.name/1)
      )

  def init(children, opts), do: Elixir.Supervisor.init(Enum.map(children, &Pulso.Runtime.child_spec/1), opts)
  def child_spec(child, opts), do: Elixir.Supervisor.child_spec(child, opts)
  def restart_child(supervisor, child), do: Elixir.Supervisor.restart_child(Pulso.Runtime.name(supervisor), child)
  def terminate_child(supervisor, child), do: Elixir.Supervisor.terminate_child(Pulso.Runtime.name(supervisor), child)
end
