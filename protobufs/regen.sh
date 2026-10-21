#!/usr/bin/env bash
# Regenerates the prost module flz_core compiles in from runner_commands.proto.
# Needs protoc on PATH. The output is committed: a source build never runs this.
#
#   protobufs/regen.sh [--out DIR]   (default: flz_core/src/generated)
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
out="$root/crates/flz_core/src/generated"
case "${1:-}" in
  "") ;;
  --out) out=${2:?--out needs a directory} ;;
  *) echo "usage: $0 [--out DIR]" >&2; exit 2 ;;
esac

mkdir -p "$out"
cargo run --quiet --locked --manifest-path "$root/protobufs/prost_gen/Cargo.toml" \
  --target-dir "$root/target/prost_gen" -- "$out"
