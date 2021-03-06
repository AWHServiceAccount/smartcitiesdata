use Mix.Config

get_redix_args = fn (host, password) ->
  [host: host, password: password]
  |> Enum.filter(fn
    {_, nil} -> false
    {_, ""} -> false
    _ -> true
  end)
end
redix_args = get_redix_args.(System.get_env("REDIS_HOST"), System.get_env("REDIS_PASSWORD"))

kafka_brokers = System.get_env("KAFKA_BROKERS")
endpoint =
  kafka_brokers
  |> String.split(",")
  |> Enum.map(&String.trim/1)
  |> Enum.map(fn entry -> String.split(entry, ":") end)
  |> Enum.map(fn [host, port] -> {String.to_atom(host), String.to_integer(port)} end)

config :discovery_api, DiscoveryApiWeb.Endpoint,
  url: [
    scheme: "https",
    host: System.get_env("HOST"),
    port: 443
  ]

config :discovery_api,
  ldap_user: System.get_env("LDAP_USER"),
  ldap_pass: System.get_env("LDAP_PASS"),
  hosted_bucket: System.get_env("HOSTED_FILE_BUCKET"),
  hosted_region: System.get_env("HOSTED_FILE_REGION"),
  presign_key: System.get_env("PRESIGN_KEY")

config :discovery_api, DiscoveryApi.Repo,
  database: System.get_env("POSTGRES_DBNAME"),
  username: System.get_env("POSTGRES_USER"),
  password: System.get_env("POSTGRES_PASSWORD"),
  hostname: System.get_env("POSTGRES_HOST"),
  port: System.get_env("POSTGRES_PORT")

required_envars = ["REDIS_HOST", "PRESTO_URL", "ALLOWED_ORIGINS", "PRESIGN_KEY"]

Enum.each(required_envars, fn var ->
  if is_nil(System.get_env(var)) do
    raise ArgumentError, message: "Required environment variable #{var} is undefined"
  end
end)

allowed_origins =
  System.get_env("ALLOWED_ORIGINS")
  |> String.split(",")
  |> Enum.map(&String.trim/1)

secrets_endpoint =
  case System.get_env("SECRETS_ENDPOINT") do
    "" -> nil
    val -> val
  end

config :discovery_api,
  allowed_origins: allowed_origins,
  secrets_endpoint: secrets_endpoint

config :redix,
  args: redix_args

config :prestige, :session_opts, url: System.get_env("PRESTO_URL")

config :paddle, Paddle,
  host: System.get_env("LDAP_HOST"),
  base: System.get_env("LDAP_BASE"),
  account_subdn: System.get_env("LDAP_ACCOUNT_SUBDN")

auth_provider = (System.get_env("AUTH_PROVIDER") || "default") |> String.downcase()

config :discovery_api,
  auth_provider: auth_provider

if auth_provider == "auth0" do
  config :discovery_api, DiscoveryApi.Auth.Guardian, issuer: System.get_env("AUTH_JWT_ISSUER")

  config :discovery_api,
    jwks_endpoint: System.get_env("AUTH_JWKS_ENDPOINT"),
    user_info_endpoint: System.get_env("AUTH_USER_INFO_ENDPOINT")
else
  config :discovery_api, DiscoveryApi.Auth.Guardian, secret_key: System.get_env("GUARDIAN_KEY")
end

config :ex_aws,
  region: System.get_env("HOSTED_FILE_REGION")

config :discovery_api, :brook,
  instance: :discovery_api,
  driver: [
    module: Brook.Driver.Kafka,
    init_arg: [
      endpoints: endpoint,
      topic: "event-stream",
      group: "discovery-api-event-stream",
      config: [
        begin_offset: :earliest
      ]
    ]
  ],
  handlers: [DiscoveryApi.EventHandler],
  storage: [
    module: Brook.Storage.Redis,
    init_arg: [redix_args: redix_args, namespace: "discovery-api:view"]
  ]
