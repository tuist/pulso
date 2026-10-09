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
alias Pulso.Alerting.Principal
alias Pulso.Alerting.Targets
alias Pulso.Auth.SharedSecret
alias Pulso.Storage.S3

# Environment variables are read through Pulso.Runtime so tests can evaluate this
# file against a process-owned environment map instead of mutating the OS
# environment. Without an installed runtime (every normal boot and release), this
# is System.get_env/2.
env_var = fn name, default ->
  if Code.ensure_loaded?(Pulso.Runtime), do: Pulso.Runtime.env(name, default), else: System.get_env(name, default)
end

# Alerting uses separate principal credentials; tenant-shared tokens never grant writes.
alerting_principals =
  case JSON.decode(env_var.("PULSO_ALERTING_PRINCIPALS_JSON", "[]")) do
    {:ok, values} when is_list(values) and length(values) <= 256 ->
      Enum.map(values, fn
        %{"tenant" => tenant, "id" => id, "type" => type, "token_hash" => hash, "capabilities" => caps} = value ->
          if Map.keys(value) -- ~w(tenant id type token_hash capabilities) == [] and
               Principal.valid_id?(tenant) and Principal.valid_id?(id) and
               type in ["human", "agent", "service"] and is_binary(hash) and
               Regex.match?(~r/\A[0-9a-f]{64}\z/, hash) and is_list(caps) and
               Enum.all?(
                 caps,
                 &(&1 in ~w(alert:read alert:rules:write alert:audit:read alert:preview alert:evaluate alert:import))
               ) do
            %{tenant: tenant, id: id, type: type, token_hash: hash, capabilities: caps}
          else
            raise "PULSO_ALERTING_PRINCIPALS_JSON contains an invalid principal"
          end

        _ ->
          raise "PULSO_ALERTING_PRINCIPALS_JSON contains an invalid principal"
      end)

    _ ->
      raise "PULSO_ALERTING_PRINCIPALS_JSON must be a JSON array of at most 256 principals"
  end

for field <- [:id, :token_hash] do
  keys =
    Enum.map(alerting_principals, fn principal ->
      if field == :id, do: {principal.tenant, principal.id}, else: principal.token_hash
    end)

  if length(Enum.uniq(keys)) != length(keys),
    do: raise("PULSO_ALERTING_PRINCIPALS_JSON has duplicate identities or credentials")
end

alerting_poll_interval_ms =
  case Integer.parse(env_var.("PULSO_ALERTING_POLL_INTERVAL_MS", "5000")) do
    {value, ""} when value in 1000..60_000 -> value
    _ -> raise "PULSO_ALERTING_POLL_INTERVAL_MS must be an integer in 1000..60000"
  end

alerting_targets =
  case JSON.decode(env_var.("PULSO_ALERTING_NOTIFICATION_TARGETS_JSON", "[]")) do
    {:ok, values} when is_list(values) and length(values) <= 256 ->
      Enum.map(values, fn value ->
        case Targets.validate(value) do
          {:ok, target} -> target
          _ -> raise "PULSO_ALERTING_NOTIFICATION_TARGETS_JSON contains an invalid target"
        end
      end)

    _ ->
      raise "PULSO_ALERTING_NOTIFICATION_TARGETS_JSON must be an array of at most 256 targets"
  end

identities = Enum.map(alerting_targets, &{&1["tenant"], &1["id"]})
if length(Enum.uniq(identities)) != length(identities), do: raise("Duplicate alert notification target IDs")

config :pulso, Pulso.Alerting,
  principals: alerting_principals,
  notification_targets: alerting_targets,
  notifications_enabled: env_var.("PULSO_ALERTING_NOTIFICATIONS_ENABLED", "false") in ["1", "true", "yes"],
  evaluation_enabled: env_var.("PULSO_ALERTING_EVALUATION_ENABLED", "false") in ["1", "true", "yes"],
  poll_interval_ms: alerting_poll_interval_ms

if env_var.("PHX_SERVER", nil) do
  config :pulso, PulsoWeb.Endpoint, server: true
end

# Opt in only after all writers understand compaction retirement metadata.
metrics_compaction_enabled = env_var.("PULSO_METRICS_COMPACTION_ENABLED", "false") in ["1", "true", "yes"]

# Optional node-local unkeyed coalescing; disabled unless explicitly enabled.
ingest_flush_interval_ms =
  case Integer.parse(env_var.("PULSO_INGEST_FLUSH_INTERVAL_MS", "0")) do
    {value, ""} when value in 0..1000 -> value
    _ -> raise "PULSO_INGEST_FLUSH_INTERVAL_MS must be an integer in 0..1000"
  end

# Optional per-request ingest budgets. Only supplied environment values
# override config/config.exs; invalid values fail at startup, not on traffic.
parse_ingest_limit = fn name, raw ->
  case Integer.parse(raw) do
    {value, ""} when value in 1..2_147_483_647 -> value
    _ -> raise "#{name} must be an integer in 1..2147483647"
  end
end

ingest_limits =
  for key <- [
        :max_records,
        :max_attributes,
        :max_key_bytes,
        :max_value_bytes,
        :max_attribute_bytes,
        :max_depth,
        :max_nodes
      ],
      name = "PULSO_INGEST_" <> String.upcase(Atom.to_string(key)),
      raw = env_var.(name, nil),
      raw != nil do
    {key, parse_ingest_limit.(name, raw)}
  end

# Event-time retention (docs/retention.md). Unlimited retention stays the
# default: both durations are 0 and the background worker is off. Invalid values
# fail at startup instead of falling back to unlimited or immediate deletion.
parse_retention_integer = fn name, default, range ->
  case Integer.parse(env_var.(name, Integer.to_string(default))) do
    {value, ""} -> if value in range, do: value, else: raise("#{name} must be an integer in #{inspect(range)}")
    _ -> raise "#{name} must be an integer in #{inspect(range)}"
  end
end

retention_mode =
  case env_var.("PULSO_RETENTION_MODE", "observe") do
    mode when mode in ["observe", "enforce", "paused"] -> mode
    _ -> raise "PULSO_RETENTION_MODE must be one of observe, enforce, paused"
  end

retention_config =
  [
    retention_enabled: env_var.("PULSO_RETENTION_ENABLED", "false") in ["1", "true", "yes"],
    logs_retention_days: parse_retention_integer.("PULSO_LOGS_RETENTION_DAYS", 0, 0..3650),
    metrics_retention_days: parse_retention_integer.("PULSO_METRICS_RETENTION_DAYS", 0, 0..3650),
    retention_mode: retention_mode,
    # Reader grace before deletion; never zero, so a committed expiry cannot race in-flight reads.
    retention_delete_grace_ms:
      parse_retention_integer.("PULSO_RETENTION_DELETE_GRACE_MS", 3_600_000, 60_000..2_592_000_000),
    retention_interval_ms: parse_retention_integer.("PULSO_RETENTION_INTERVAL_MS", 30_000, 1_000..3_600_000),
    retention_delete_limit: parse_retention_integer.("PULSO_RETENTION_DELETE_LIMIT", 512, 1..512),
    retention_timeout_ms: parse_retention_integer.("PULSO_RETENTION_TIMEOUT_MS", 30_000, 1_000..600_000),
    retention_migration_timeout_ms:
      parse_retention_integer.("PULSO_RETENTION_MIGRATION_TIMEOUT_MS", 600_000, 30_000..3_600_000),
    retention_future_skew_ms: parse_retention_integer.("PULSO_RETENTION_FUTURE_SKEW_MS", 600_000, 0..86_400_000)
  ] ++
    case env_var.("PULSO_RETENTION_SWEEP_HORIZON_DAYS", nil) do
      # Unset keeps the per-root default of 2 * persisted days + 2.
      nil -> []
      _ -> [retention_sweep_horizon_days: parse_retention_integer.("PULSO_RETENTION_SWEEP_HORIZON_DAYS", 0, 1..7302)]
    end

config :pulso, Pulso.IngestLimits, ingest_limits
config :pulso, PulsoWeb.Endpoint, http: [port: String.to_integer(env_var.("PORT", "4000"))]

# Browser origins allowed to call POST /mcp, comma-separated
# (e.g. "https://atlas.example.com"). Requests without an Origin header are
# always accepted; any other origin is rejected with 403.
config :pulso, PulsoWeb.MCPController,
  allowed_origins:
    "PULSO_MCP_ALLOWED_ORIGINS"
    |> env_var.("")
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)

# Log storage adapter. Tests keep the in-memory adapter (see config/test.exs);
# dev and prod use the S3 adapter against any S3-compatible endpoint. RustFS
# runs locally via docker-compose.yml — the dev defaults below match its
# out-of-the-box credentials. Prod requires the env vars to be set explicitly.
case config_env() do
  :dev ->
    config :pulso, Pulso.Storage, adapter: S3

    config :pulso, S3,
      compaction_enabled: metrics_compaction_enabled,
      ingest_flush_interval_ms: ingest_flush_interval_ms,
      bucket: env_var.("PULSO_S3_BUCKET", "pulso"),
      # mise/utilities/dev_instance_env.sh sets PULSO_S3_ENDPOINT per worktree.
      # The fallback matches the docker-compose default host port when mise
      # is not in the loop.
      endpoint: env_var.("PULSO_S3_ENDPOINT", "http://localhost:11100"),
      region: env_var.("PULSO_S3_REGION", "us-east-1"),
      access_key_id: env_var.("PULSO_S3_ACCESS_KEY_ID", "rustfsadmin"),
      secret_access_key: env_var.("PULSO_S3_SECRET_ACCESS_KEY", "rustfsadmin"),
      allow_http: env_var.("PULSO_S3_ALLOW_HTTP", "true") in ["1", "true", "yes"]

  :prod ->
    require_env = fn name ->
      case env_var.(name, nil) do
        value when is_binary(value) and value != "" ->
          value

        _ ->
          raise """
          environment variable #{name} is missing or empty.
          Pulso.Storage.S3 requires bucket/region/credentials in prod.
          """
      end
    end

    # Tenant tokens. Expected shape: a JSON object mapping tenant name to
    # "sha256$<hex-of-sha256-of-token>". Deployments compute the hash offline
    # and store only the digest in env, never the plaintext token.
    tokens =
      case env_var.("PULSO_TENANT_TOKENS", nil) do
        blob when is_binary(blob) and blob != "" ->
          case JSON.decode(blob) do
            {:ok, map} when is_map(map) ->
              map

            {:ok, _} ->
              raise "PULSO_TENANT_TOKENS must decode to a JSON object"

            {:error, reason} ->
              raise "PULSO_TENANT_TOKENS is not valid JSON: #{inspect(reason)}"
          end

        _ ->
          raise """
          environment variable PULSO_TENANT_TOKENS is missing or empty.
          Pulso.Auth.SharedSecret requires at least one tenant token in prod.
          """
      end

    config :pulso, Pulso.Auth,
      module: SharedSecret,
      tokens: tokens

    config :pulso, Pulso.Storage, adapter: S3

    config :pulso, S3,
      compaction_enabled: metrics_compaction_enabled,
      ingest_flush_interval_ms: ingest_flush_interval_ms,
      bucket: require_env.("PULSO_S3_BUCKET"),
      endpoint: env_var.("PULSO_S3_ENDPOINT", nil),
      region: require_env.("PULSO_S3_REGION"),
      access_key_id: require_env.("PULSO_S3_ACCESS_KEY_ID"),
      secret_access_key: require_env.("PULSO_S3_SECRET_ACCESS_KEY"),
      allow_http: env_var.("PULSO_S3_ALLOW_HTTP", "false") in ["1", "true", "yes"]

  :test ->
    :noop
end

# Shared by dev and prod; merged into the S3 adapter configuration above.
if config_env() in [:dev, :prod] do
  config :pulso, S3, retention_config
end

if config_env() == :prod do
  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    env_var.("SECRET_KEY_BASE", nil) ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = env_var.("PHX_HOST", nil) || "example.com"

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

  config :pulso, :dns_cluster_query, env_var.("DNS_CLUSTER_QUERY", nil)

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
