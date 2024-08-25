import Config

config :flecha_core,
  computation_engines: %{
    "default" => %{
      command: "cargo run --manifest-path=sdk/flecha_rust_sdk/Cargo.toml --target-dir=./target",
      computation_templates: %{}
    }
  }
