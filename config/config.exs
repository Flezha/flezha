import Config

config :logger,
  handle_sasl_reports: true,
  handle_otp_reports: true

config :aflecha,
  factories_expected_count: nil

import_config "#{Mix.env()}.exs"
