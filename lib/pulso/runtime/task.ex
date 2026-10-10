defmodule Pulso.Runtime.Task do
  @moduledoc false
  def child_spec(fun), do: Task.child_spec(Pulso.Runtime.capture(fun))
  def start_link(fun), do: Task.start_link(Pulso.Runtime.capture(fun))
  def yield(task, timeout \\ 5000), do: Task.yield(task, timeout)
  def await(task, timeout \\ 5000), do: Task.await(task, timeout)
  def shutdown(task, reason \\ 5000), do: Task.shutdown(task, reason)
  def async(fun), do: Task.async(Pulso.Runtime.capture(fun))

  def async_stream(collection, fun, opts) do
    runtime = Pulso.Runtime.current()
    Task.async_stream(collection, fn item -> Pulso.Runtime.with(runtime, fn -> fun.(item) end) end, opts)
  end
end
