import Config

# Do not print debug messages in production
config :logger, level: :info

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
