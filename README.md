# Flecha

## Description
Distributed stateful computations. Provdes a static and dynamic creation of computations.

## Example usage
Starting the node and specifying the external processes to be used by the node mynode.
```bash
RELEASE_NODE=mynode FLECHA_EXTERNAL_PROCESSES="cargo run --manifest-path=/home/anuar/Projects/personal/flecha_core/test/apps/rust_app/Cargo.toml --target-dir=./target" flecha_core/bin/flecha_core start_iex
```

Adding a computation to the node.
```bash
flecha_core/bin/add_computation comp_id increment_state -v '{\"value\": 500}' -n "mynode"
```