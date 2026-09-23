import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

# ## Using releases
#
# If you use `mix release`, you need to explicitly enable the server
# by passing the PHX_SERVER=true when you start it:
#
#     PHX_SERVER=true bin/pulso start
#
# Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
# script that automatically sets the env var above.
alias Pulso.Storage.S3

if System.get_env("PHX_SERVER") do
  config :pulso, PulsoWeb.Endpoint, server: true
end

config :pulso, PulsoWeb.Endpoint, http: [port: String.to_integer(System.get_env("PORT", "4000"))]

# Log storage adapter. Tests keep the in-memory adapter (see config/test.exs);
# dev and prod use the S3 adapter against any S3-compatible endpoint. RustFS
# runs locally via docker-compose.yml — the dev defaults below match its
# out-of-the-box credentials. Prod requires the env vars to be set explicitly.
case config_env() do
  :dev ->
    config :pulso, Pulso.Storage, adapter: S3

    config :pulso, S3,
      bucket: System.get_env("PULSO_S3_BUCKET", "pulso"),
      endpoint: System.get_env("PULSO_S3_ENDPOINT", "http://localhost:9000"),
      region: System.get_env("PULSO_S3_REGION", "us-east-1"),
      access_key_id: System.get_env("PULSO_S3_ACCESS_KEY_ID", "rustfsadmin"),
      secret_access_key: System.get_env("PULSO_S3_SECRET_ACCESS_KEY", "rustfsadmin"),
      allow_http: System.get_env("PULSO_S3_ALLOW_HTTP", "true") in ["1", "true", "yes"]

  :prod ->
    require_env = fn name ->
      case System.get_env(name) do
        value when is_binary(value) and value != "" ->
          value

        _ ->
          raise """
          environment variable #{name} is missing or empty.
          Pulso.Storage.S3 requires bucket/region/credentials in prod.
          """
      end
    end

    config :pulso, Pulso.Storage, adapter: S3

    config :pulso, S3,
      bucket: require_env.("PULSO_S3_BUCKET"),
      endpoint: System.get_env("PULSO_S3_ENDPOINT"),
      region: require_env.("PULSO_S3_REGION"),
      access_key_id: require_env.("PULSO_S3_ACCESS_KEY_ID"),
      secret_access_key: require_env.("PULSO_S3_SECRET_ACCESS_KEY"),
      allow_http: System.get_env("PULSO_S3_ALLOW_HTTP", "false") in ["1", "true", "yes"]

  :test ->
    :noop
end

if config_env() == :prod do
  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST") || "example.com"

  config :pulso, PulsoWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      # Enable IPv6 and bind on all interfaces.
      # Set it to  {0, 0, 0, 0, 0, 0, 0, 1} for local network only access.
      # See the documentation on https://bandit.hexdocs.pm/Bandit.html#t:options/0
      # for details about using IPv6 vs IPv4 and loopback vs public addresses.
      ip: {0, 0, 0, 0, 0, 0, 0, 0}
    ],
    secret_key_base: secret_key_base

  config :pulso, :dns_cluster_query, System.get_env("DNS_CLUSTER_QUERY")

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :pulso, PulsoWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://plug.hexdocs.pm/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :pulso, PulsoWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.
end
