use Mix.Config

config :andi, AndiWeb.Endpoint,
  http: [port: 4002],
  server: true, #start server while running tests
  live_view: [
    signing_salt: "CHANGEME?"
  ]

# Print only warnings and errors during test
config :logger, level: :warn

config :andi, :brook,
  instance: :andi,
  driver: [
    module: Brook.Driver.Default,
    init_arg: []
  ],
  handlers: [Andi.EventHandler],
  storage: [
    module: Brook.Storage.Ets,
    init_arg: []
  ]
