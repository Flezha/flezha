import Config

# config :logger,
#   handle_sasl_reports: true,
#   handle_otp_reports: true

config :flecha_core,
  external_processes: [
    "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target"
  ],
  controller_init_hook: fn() -> :ok end


import_config "#{Mix.env()}.exs"
