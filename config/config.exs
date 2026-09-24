# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

alias Pulso.Auth.Open

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Elixir's built-in JSON module for Phoenix (avoids the Jason dependency;
# see AGENTS.md conventions).
config :phoenix, :json_library, JSON

# Explicit auth default. Environment-specific configs override; prod requires
# a runtime override to `Pulso.Auth.SharedSecret` via runtime.exs — an unset
# release still raises rather than falling back to open access.
config :pulso, Pulso.Auth, module: Open

# Configure the endpoint
config :pulso, PulsoWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [json: PulsoWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Pulso.PubSub,
  live_view: [signing_salt: "AAeCXBT1"]

config :pulso,
  generators: [timestamp_type: :utc_datetime]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
