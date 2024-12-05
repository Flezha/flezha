import Config

config :aflecha,
  # Total number of factories registered in the system by all computation enginges.
  factories_expected_count: 3,
  testnet_enabled: true

config :logger,
  level: :warning
