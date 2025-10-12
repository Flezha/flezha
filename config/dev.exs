import Config

config :flezha,
  testnet_enabled: true

config :logger, :file_log,
  level: :debug

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true
