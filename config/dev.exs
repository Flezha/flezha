import Config

config :flezha,
  testnet_enabled: true,
  recovery_enabled: false

config :logger, :file_log, level: :debug

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true
