#!/usr/bin/env bash
# Build the PHP SDK and its test app: the native library the package loads
# through ext-ffi, and the Composer autoloader the entry script requires.
#
# `--release` builds the library in release, which is what the bench profile
# wants; the default is a debug library, which is what the e2e wants (it builds
# in seconds and the runner is not what a conformance run measures).
#
# No PHP extension is compiled here, and that is the point of D2: `ext-ffi`
# needs ONE library per platform, where a native extension would need one per
# PHP minor version × thread-safety × platform.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/php"

profile=debug
cargo_args=()
if [[ "${1:-}" == "--release" ]]; then
  profile=release
  cargo_args+=(--release)
fi

echo "==> building flz_core_ffi ($profile)"
cargo build -p flz_core_ffi "${cargo_args[@]}"

# The platform directory the package selects at runtime, in the naming every
# SDK's distribution already uses.
case "$(uname -s)" in
  Linux)  os=linux; ext=so ;;
  Darwin) os=macos; ext=dylib ;;
  *) echo "unsupported platform: $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  arch=x64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
esac

echo "==> installing the $profile library for $os-$arch"
mkdir -p "$sdk/lib/$os-$arch"
cp "$root/_build/.target/$profile/libflz_core_ffi.$ext" "$sdk/lib/$os-$arch/"

echo "==> installing composer dependencies"
if command -v composer >/dev/null 2>&1; then
  (cd "$sdk" && composer install --no-interaction --no-progress)
elif docker image inspect flezha-php-zts >/dev/null 2>&1; then
  # CI's own line (.github/workflows/php-sdk.yml): composer lives in the image.
  # As the invoking user, or vendor/ comes out root-owned on the host.
  echo "==> no composer on PATH; running it in the flezha-php-zts image"
  docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$root:$root" -w "$sdk" \
    flezha-php-zts composer install --no-interaction --no-progress
else
  echo "neither composer nor the flezha-php-zts image is available: install composer," \
    "or build the image from docker/dev/Dockerfile.php (target zts)" >&2
  exit 1
fi

echo "==> php SDK ready"
