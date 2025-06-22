import Config

config :flezha,
  testnet_enabled: false

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true
