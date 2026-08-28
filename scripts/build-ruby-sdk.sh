#!/usr/bin/env bash
# Build the Ruby SDK and its test app: the native veneer, and the extension the
# gem loads.
#
# `--release` builds the veneer in release, which is what the bench profile
# wants; the default is a debug veneer, which is what the e2e wants (it builds
# in seconds and the runner is not what a conformance run measures).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/ruby"

profile=debug
cargo_args=()
if [[ "${1:-}" == "--release" ]]; then
  profile=release
  cargo_args+=(--release)
fi

echo "==> building flz_core_ffi ($profile) with the ruby feature"
cargo build -p flz_core_ffi --features ruby "${cargo_args[@]}"

# `require "flz_core_ffi"` looks for a file of that name and calls
# `Init_flz_core_ffi`, which the veneer exports. The cdylib is that file under
# another name — no second artifact, exactly as the `.node` addon is.
dlext="$(ruby -e 'print RbConfig::CONFIG["DLEXT"]')"
soext="$(ruby -e 'print RbConfig::CONFIG["SOEXT"] || "so"')"
echo "==> installing the $profile veneer as flz_core_ffi.$dlext"
mkdir -p "$sdk/lib/flezha"
cp "$root/target/$profile/libflz_core_ffi.$soext" "$sdk/lib/flezha/flz_core_ffi.$dlext"

# The gem embeds a compiled `flz_core_ffi`, so the notices for what that
# library links have to travel inside it — a user who installs the platform gem
# never sees this repository.
echo "==> staging the licence and third-party notices"
cp "$root/LICENSES/Apache-2.0.txt" "$sdk/LICENSE"
cp "$root/THIRD_PARTY_NOTICES.md" "$sdk/THIRD_PARTY_NOTICES.md"

echo "==> installing gem dependencies"
(cd "$sdk" && gem install --conservative --no-document msgpack >/dev/null)

echo "==> ruby SDK ready"
