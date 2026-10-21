integration_enabled? =
  case System.get_env("PULSO_INTEGRATION") do
    nil -> false
    "" -> false
    "0" -> false
    "false" -> false
    _ -> true
  end

# Optional real SDK/Collector harnesses are never part of the S3 integration job.
# Opt in explicitly with --include collector_bridge, elixir_sdk, or rust_sdk.
external = [:collector_bridge, :elixir_sdk, :rust_sdk]
exclude = if integration_enabled?, do: external, else: [:integration | external]

ExUnit.start(exclude: exclude)
