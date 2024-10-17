import Config

external_processes =
  if config_env() in [:test, :dev] do
    {:ok, current_path} = :file.get_cwd()

    [
      "cargo run --manifest-path=#{current_path}/test/apps/rust_app/Cargo.toml --target-dir=./target"
    ]
  else
    System.get_env("FLECHA_EXTERNAL_PROCESSES", "") |> String.split(",", trim: true)
  end

config :flecha_core,
  socket_files_path: System.get_env("FLECHA_SOCKET_FILES_PATH", "app_data/sockets/"),
  io_add_timeout: System.get_env("FLECHA_IO_ADD_TIMEOUT", "1000") |> String.to_integer(),
  io_payload_add_timeout:
    System.get_env("FLECHA_IO_PAYLOAD_ADD_TIMEOUT", "1000") |> String.to_integer(),
  external_processes: external_processes,
  static_nodes: System.get_env("FLECHA_STATIC_NODES", "") |> String.split(",", trim: true),
  reconnect_interval: System.get_env("FLECHA_RECONNECT_INTERVAL", "0") |> String.to_integer(),
  max_reconnects: System.get_env("FLECHA_MAX_RECONNECTS", "0") |> String.to_integer()
