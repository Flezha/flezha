import Config

# config :logger,
#   handle_sasl_reports: true,
#   handle_otp_reports: true

config :flecha_core,
  factories_expected_count: nil

import_config "#{Mix.env()}.exs"
