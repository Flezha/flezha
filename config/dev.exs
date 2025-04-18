import Config

config :aflecha,
  testnet_enabled: true

config :kafka_ex,
  brokers: [
    {"kafka", 9092}
  ],
  disable_default_worker: true
