#!/usr/bin/env bash
#
# Assert that one flz_core_ffi veneer feature is ADDITIVE: it adds exactly the
# symbols its host runtime needs to find the library, removes none, and leaves
# the JNI exports alone when it is enabled beside `jni`.
#
# Usage:
#   scripts/check-feature-symbols.sh <artifact> <feature>
#   scripts/check-feature-symbols.sh _build/.target/debug/libflz_core_ffi.so napi
#
# <artifact> is the cdylib each build writes; it is an argument rather than a
# constant so the check can be driven against a staged copy. <feature> is the
# veneer feature under test, and the symbols it is allowed to add are declared
# below — a veneer that starts exporting something else is a finding, not a
# detail, because the Java SDK ships a binary built from this same crate.
#
# Exit codes: 0 clean, 1 a finding, 2 the check could not run (wrong arity, or
# a feature with no declared symbol set).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  echo "usage: $0 <artifact> <feature>" >&2
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi
artifact=$1
feature=$2

# What each veneer is allowed to add, as one space-separated line. The host
# runtime finds the library through exactly one entry point, so the list is one
# or two names and never grows by accident.
case "$feature" in
  napi) expected="napi_register_module_v1 " ;;
  ruby) expected="Init_flz_core_ffi ruby_abi_version " ;;
  *)
    echo "$0: no declared symbol set for feature '$feature'" >&2
    exit 2
    ;;
esac

cd "$root"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

syms() { nm -D --defined-only "$1" | awk '{print $3}' | grep -v '^$' | sort; }

cargo build -p flz_core_ffi
syms "$artifact" > "$work/default"
cargo build -p flz_core_ffi --features "$feature"
syms "$artifact" > "$work/feature"
cargo build -p flz_core_ffi --features "jni,$feature"
syms "$artifact" > "$work/both"

removed=$(comm -23 "$work/default" "$work/feature")
if [ -n "$removed" ]; then
  echo "the $feature feature removed exported symbols:"
  echo "$removed"
  exit 1
fi

added=$(comm -13 "$work/default" "$work/feature" | tr '\n' ' ')
if [ "$added" != "$expected" ]; then
  echo "the $feature feature added more than the module registration: $added"
  exit 1
fi

if ! grep -q Java_com_flezha_sdk_native_1_FlzCore_poll "$work/both"; then
  echo "the JNI exports are missing when both features are on"
  exit 1
fi
