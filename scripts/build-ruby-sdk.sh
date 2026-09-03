#!/usr/bin/env bash
# Build the Ruby SDK and its test app: the native veneer, and the extension the
# gem loads.
#
# `--release` builds the veneer in release, which is what the bench profile
# wants; the default is a debug veneer, which is what the e2e wants (it builds
# in seconds and the runner is not what a conformance run measures).
#
# `--run <command>` runs that command behind the same two header guards instead
# of building, so any cargo invocation that enables a `ruby` feature can reach
# them:
#
#   scripts/build-ruby-sdk.sh --run cargo test -p flz_core_ffi --features ruby-embed
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/ruby"

profile=debug
cargo_args=()
run_cmd=()
case "${1:-}" in
  "") ;;
  --release)
    profile=release
    cargo_args+=(--release)
    ;;
  --run)
    shift
    [[ $# -gt 0 ]] || { echo "--run takes the command to run" >&2; exit 2; }
    run_cmd=("$@")
    ;;
  *)
    echo "usage: build-ruby-sdk.sh [--release] | --run <command> [args...]" >&2
    exit 2
    ;;
esac

# The two ways this build dies are both "a header is missing", and they want
# opposite remedies. First: a Ruby with no development headers, which rb-sys
# reports as `'ruby.h' file not found` — no include path fixes that, so name it
# here rather than let it read as the stdarg.h case below.
hdrdir="$(ruby -e 'print RbConfig::CONFIG["rubyhdrdir"].to_s' 2>/dev/null || true)"
if [[ ! -f "$hdrdir/ruby.h" ]]; then
  echo "the ruby on PATH ($(command -v ruby || echo none)) has no development headers" >&2
  echo "  no ruby.h under '${hdrdir:-<none>}' — install ruby-dev, or put the SDK's" >&2
  echo "  rbenv ruby on PATH. This is NOT the bindgen include-path problem." >&2
  exit 1
fi

# Second: rb-sys runs bindgen over ruby.h, and bindgen needs clang's BUILTIN
# headers: with libclang alone the build dies inside cargo with `'stdarg.h' file
# not found`. gcc's builtin include directory carries the same headers.
#
# This guard cannot move into `flz_core_ffi/build.rs`: rb-sys is a dependency,
# so its build script runs first and fails before ours exists. A bare
# `cargo build/test -p flz_core_ffi --features ruby*` on a box with no clang
# builtin headers is therefore unprotected: run it as `build-ruby-sdk.sh --run
# cargo …` instead, or install libclang-dev, which is what CI does.
if [[ -z "${BINDGEN_EXTRA_CLANG_ARGS:-}" ]] \
  && ! compgen -G "/usr/lib/llvm-*/lib/clang/*/include/stdarg.h" >/dev/null; then
  gcc_include="$(gcc -print-file-name=include 2>/dev/null || true)"
  if [[ -f "$gcc_include/stdarg.h" ]]; then
    export BINDGEN_EXTRA_CLANG_ARGS="-I$gcc_include"
    echo "==> no clang builtin headers; bindgen reads gcc's ($gcc_include)"
  else
    echo "no builtin stdarg.h for bindgen: install libclang-dev" >&2
    exit 1
  fi
fi

if (( ${#run_cmd[@]} )); then
  exec "${run_cmd[@]}"
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
