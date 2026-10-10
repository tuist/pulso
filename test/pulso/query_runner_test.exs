defmodule Pulso.QueryRunnerTest do
  use Pulso.Test.Case, async: true

  alias Pulso.QueryRunner
  alias Pulso.Runtime.Task

  test "tenant admission is shared across query classes and released after workers finish" do
    caller = self()
    supervisor = start_supervised!({Task.Supervisor, name: __MODULE__.Callers})

    workers =
      for class <- [:promql, :logql] do
        {:ok, _pid} =
          Task.Supervisor.start_child(supervisor, fn ->
            result =
              QueryRunner.run("shared", class, fn _deadline ->
                send(caller, {:started, self()})
                receive do: (:release -> {:ok, class})
              end)

            send(caller, {:finished, result})
          end)

        # Admission happens in supervised tasks; allow for a loaded async suite.
        assert_receive {:started, worker}, 5_000
        worker
      end

    assert {:error, :query_overloaded} = QueryRunner.run("shared", :raw, fn _ -> {:ok, :unexpected} end)
    assert Pulso.Metrics.render() =~ "pulso_query_occupied_slots 2\n"
    assert {:ok, :other} = QueryRunner.run("other", :raw, fn _ -> {:ok, :other} end)

    for pid <- workers do
      ref = Process.monitor(pid)
      send(pid, :release)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    assert_receive {:finished, {:ok, :promql}}
    assert_receive {:finished, {:ok, :logql}}
    assert {:ok, :released} = QueryRunner.run("shared", :raw, fn _ -> {:ok, :released} end)
  end
end
