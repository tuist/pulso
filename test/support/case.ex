defmodule Pulso.Test.Case do
  @moduledoc "Test-owned dependency context and isolated service instances."
  use ExUnit.CaseTemplate

  import ExUnit.Callbacks,
    except: [start_supervised: 1, start_supervised: 2, start_supervised!: 1, start_supervised!: 2]

  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Storage.Memory

  using do
    quote do
      import ExUnit.Callbacks,
        except: [start_supervised: 1, start_supervised: 2, start_supervised!: 1, start_supervised!: 2]

      import Pulso.Test.Case,
        only: [start_supervised: 1, start_supervised: 2, start_supervised!: 1, start_supervised!: 2]
    end
  end

  setup tags do
    setup_runtime(tags)
  end

  def setup_runtime(%{module: module}) do
    runtime =
      Pulso.Runtime.new(
        instance: module,
        configs: Map.new(Application.get_all_env(:pulso), fn {key, value} -> {{:pulso, key}, value} end)
      )

    Pulso.Runtime.install(runtime)
    start_supervised!(Memory)
    start_supervised!(Pulso.Metrics)
    start_supervised!(Pulso.SelfMetrics)
    start_supervised!({Task.Supervisor, name: TaskSupervisor, max_children: 4})
    start_supervised!({Registry, keys: :unique, name: QuerySlots})
    {:ok, runtime: runtime}
  end

  def start_supervised(child, opts \\ []), do: ExUnit.Callbacks.start_supervised(Pulso.Runtime.child_spec(child), opts)

  def start_supervised!(child, opts \\ []),
    do: ExUnit.Callbacks.start_supervised!(Pulso.Runtime.child_spec(child), opts)
end
