import Config

config :flezha,
  testnet_enabled: false

config :logger,
  level: :info

config :logger, LoggerFileBackend,
  path: "logs/prod/flezha.log",
  level: :info,
  rotate: %{max_bytes: 52_428_800, keep: 10}
