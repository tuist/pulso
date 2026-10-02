Mix.install([{:nimble_parsec, "1.4.2"}])
Path.wildcard("lib/pulso/logql/ast/*.ex") |> Enum.each(&Code.require_file/1)
Code.require_file("lib/pulso/logql/parser.ex")
{:ok, snapshot} = File.read!("plans/fixtures/tuist-dashboard-queries.json") |> JSON.decode()
requests = for dashboard <- snapshot["dashboards"], request <- dashboard["requests"], request["language"] == "loki", request["purpose"] == "panel", do: {dashboard["path"], request}
failures = Enum.flat_map(requests, fn {path, request} ->
  # Representative expansion only: production variable values remain a replay task.
  expression = request["request"]["expr"] |> String.replace("$__interval", "1m")
  case Pulso.LogQL.Parser.parse(expression) do
    {:ok, _} -> []
    error -> [{path, request["pointer"], error}]
  end
end)
IO.puts("Parsed #{length(requests) - length(failures)}/#{length(requests)} Loki panel expressions with $__interval=1m")
if failures != [], do: raise(inspect(failures))
