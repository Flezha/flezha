import Config

# config :logger,
#   handle_sasl_reports: true,
#   handle_otp_reports: true

config :flecha_core,
  external_processes: [
    "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target"
  ],
  controller_init_hook: fn -> :ok end,
  socket_files_path: System.get_env("socket_files_path", "app_data/sockets/")

import_config "#{Mix.env()}.exs"
