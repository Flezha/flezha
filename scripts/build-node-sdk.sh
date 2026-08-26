#!/usr/bin/env bash
# Build the Node SDK and its test app: the native addon, the dual-format
# package, and the test app's dependency link.
#
# `--release` builds the addon in release, which is what the bench profile
# wants; the default is a debug addon, which is what the e2e wants (it builds in
# seconds and the runner is not what a conformance run measures).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/node"
app="$root/test/apps/node_runner"

profile=debug
cargo_args=()
if [[ "${1:-}" == "--release" ]]; then
  profile=release
  cargo_args+=(--release)
fi

echo "==> building flz_core_ffi ($profile) with the napi feature"
cargo build -p flz_core_ffi --features napi "${cargo_args[@]}"

echo "==> installing @flezha/sdk dependencies"
cd "$sdk"
npm install --no-audit --no-fund --silent

echo "==> installing the $profile addon"
if [[ "$profile" == "release" ]]; then
  node scripts/install-native.mjs
else
  node scripts/install-native.mjs --debug
fi

echo "==> building the package"
node scripts/build.mjs

echo "==> linking the test app"
cd "$app"
npm install --no-audit --no-fund --silent

echo "==> node SDK ready"
