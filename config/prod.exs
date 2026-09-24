import Config

# Do not print debug messages in production
config :logger, level: :info

# A sentinel that runtime.exs is expected to overwrite. If a release starts
# without runtime.exs having populated the real auth module, `Pulso.Auth`
# raises rather than serving requests with the Open (accept-everything)
# fallback that ships in config.exs.
config :pulso, Pulso.Auth, module: :must_configure_at_runtime

config :pulso, PulsoWeb.Endpoint,
  force_ssl: [
    rewrite_on: [:x_forwarded_proto],
    exclude: [
      # paths: ["/health"],
      hosts: ["localhost", "127.0.0.1"]
    ]
  ]

# Runtime production configuration, including reading
# of environment variables, is done on config/runtime.exs.
