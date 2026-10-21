Code.require_file("inventory.exs", __DIR__)
snapshot = Pulso.JSON.decode!(File.read!(Path.join(__DIR__, "grafana-rule-inventory.json")))
notifications = Pulso.JSON.decode!(File.read!(Path.join(__DIR__, "notification-inventory.json")))
{:ok, counts} = Pulso.Plans.AlertInventory.validate(snapshot, notifications)
matrix = Pulso.Plans.AlertInventory.render(snapshot["rules"])
path = Path.join(__DIR__, "coverage-matrix.md")

case System.argv() do
  ["--write-matrix"] -> File.write!(path, matrix)
  [] -> if File.read!(path) != matrix, do: raise("Checklist is stale; use --write-matrix")
  _ -> raise "Only --write-matrix is supported"
end

IO.puts(
  "Validated 115 unique entries, four recordings, source references, original digest and graph integrity: #{inspect(counts)}"
)

IO.puts("Inventory integrity only, not execution or migration acceptance.")
