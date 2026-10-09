defmodule Pulso.RuntimeTest do
  use Pulso.Test.Case, async: true

  alias Pulso.ObjectStore.NIF
  alias Pulso.Runtime
  alias Pulso.Runtime.Supervision
  alias Pulso.Storage.Memory
  alias Pulso.Test.RuntimeStatusServer

  test "unscoped child specifications preserve production start modules" do
    child = {Memory, []}
    expected = Supervisor.child_spec(child, [])
    assert Runtime.with(nil, fn -> Runtime.child_spec(child) end) == expected

    assert Runtime.with(nil, fn -> Supervision.init([child], strategy: :one_for_one) end) ==
             Supervisor.init([child], strategy: :one_for_one)
  end

  test "owned child specifications preserve module metadata" do
    spec = Runtime.child_spec({Memory, []})
    assert spec.modules == [Memory]
    assert {Runtime, :start_child, [_runtime, _start]} = spec.start
  end

  test "owned servers delegate status redaction" do
    server = start_supervised!({RuntimeStatusServer, secret: "must-not-appear"})
    assert :sys.get_status(server) |> inspect() =~ "redacted"
    refute :sys.get_status(server) |> inspect() =~ "must-not-appear"
  end

end
