integration_enabled? =
  case System.get_env("PULSO_INTEGRATION") do
    nil -> false
    "" -> false
    "0" -> false
    "false" -> false
    _ -> true
  end

exclude = if integration_enabled?, do: [], else: [:integration]

ExUnit.start(exclude: exclude)
