import Config

config :flecha_core,
  external_processes: [
    "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target"
  ]
