use Mix.Config
aws_region = "local"
host = "localhost"
endpoints = [{to_charlist(host), 9092}]
redix_args = [host: host]

config :discovery_api, DiscoveryApiWeb.Endpoint, url: [scheme: "https", host: "data.integrationtests.example.com", port: 443]

config :discovery_api,
  allowed_origins: ["integrationtests.example.com", "localhost:9001"],
  divo: "test/integration/docker-compose.yaml",
  divo_wait: [dwell: 2000, max_tries: 35],
  ldap_user: [cn: "admin"],
  ldap_pass: "admin",
  hosted_bucket: "kdp-cloud-storage",
  hosted_region: aws_region

config :redix,
  args: redix_args

config :phoenix,
  serve_endpoints: true,
  persistent: true

config :ex_json_schema,
       :remote_schema_resolver,
       fn url -> URLResolver.resolve_url(url) end

config :prestige, :session_opts, url: "http://#{host}:8080"

config :paddle, Paddle,
  host: host,
  base: "dc=example,dc=org",
  timeout: 3000

config :ex_aws, :s3,
  scheme: "http://",
  region: aws_region,
  host: %{
    "local" => "localhost"
  },
  port: 9000

config :discovery_api, ecto_repos: [DiscoveryApi.Repo]

config :discovery_api, DiscoveryApi.Repo,
  database: "discovery_api_test",
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox,
  port: "5456"

config :discovery_api, :brook,
  instance: :discovery_api,
  driver: [
    module: Brook.Driver.Kafka,
    init_arg: [
      endpoints: endpoints,
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

config :discovery_api,
  user_visualization_limit: 4
