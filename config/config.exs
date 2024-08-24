import Config

config :flecha_core,
  computation_engines: [
    {"default", "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target"}
  ]
