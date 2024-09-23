import Config

# config :logger,
#   handle_sasl_reports: true,
#   handle_otp_reports: true

config :flecha_core,
  external_processes: [
    "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target"
  ],
  controller_init_hook: fn -> :ok end,
  socket_files_path: System.get_env("FLECHA_SOCKET_FILES_PATH", "app_data/sockets/"),
  io_add_timeout: System.get_env("FLECHA_IO_ADD_TIMEOUT", "1000") |> String.to_integer(),
  io_payload_add_timeout:
    System.get_env("FLECHA_IO_PAYLOAD_ADD_TIMEOUT", "1000") |> String.to_integer()

import_config "#{Mix.env()}.exs"
