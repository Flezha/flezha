#!/usr/bin/env bash
# Build the Go SDK's test app: the core's static library, linked by cgo into
# one executable with no core library to load at run time.
#
# `--release` builds the core in release, which is what the bench profile
# wants; the default is a debug core, which is what the e2e wants.
#
# The library goes into a directory of its own, holding only the `.a`: the
# workspace target directory also holds the crate's shared library, and a
# linker given both prefers the shared one. `CGO_LDFLAGS` names that
# directory; the system libraries the core needs are in the SDK's own cgo
# directives. A C toolchain is required, as for any cgo build.
#
# Prints the `CGO_LDFLAGS` line last, so `go test ./...` in sdk/go can reuse it; that
# suite also runs `_build/.target/debug/flz-fake-controller`, built here.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/go"

profile=debug
cargo_args=()
if [[ "${1:-}" == "--release" ]]; then
  profile=release
  cargo_args+=(--release)
fi

if ! command -v go >/dev/null 2>&1; then
  echo "no \`go\` on PATH: install Go (the floor is the \`go\` line of sdk/go/go.mod)" >&2
  exit 1
fi

echo "==> building flz_core_ffi ($profile)"
cargo build -p flz_core_ffi "${cargo_args[@]}"
# The SDK's own suite drives its runner tests against this controller stand-in.
cargo build -p flz_core_ffi --features fake-controller --bin flz-fake-controller

lib_dir="$root/_build/.target/$profile/go"
mkdir -p "$lib_dir"
cp "$root/_build/.target/$profile/libflz_core_ffi.a" "$lib_dir/"
export CGO_LDFLAGS="-L$lib_dir"

echo "==> building the Go test app ($profile core)"
(cd "$sdk" && go build -o "$root/_build/.target/$profile/go_runner" ./test_app)

echo "==> go SDK ready: _build/.target/$profile/go_runner"
echo "CGO_LDFLAGS=$CGO_LDFLAGS"
