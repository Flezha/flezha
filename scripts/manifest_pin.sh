#!/usr/bin/env bash
#
# Print the exact version a manifest pins for one dependency.
#
#   scripts/manifest_pin.sh sdk/python/pyproject.toml pytest   # "pytest==<v>"
#   scripts/manifest_pin.sh sdk/ruby/flezha.gemspec rubocop    # "rubocop", "<v>"
#   scripts/manifest_pin.sh .github/toolchains.json node       # "node": "<v>"
#
# Every CI step that installs a tool at the version its manifest names calls
# this, so the anchored pattern exists once and one pin cannot resolve to two
# versions in two jobs. An entry that is not one exact version — absent, a
# range (`>=`, `~>`), a comma list, or named twice — exits non-zero with
# nothing on stdout, and the install that substitutes it fails instead of
# resolving whatever is newest.
#
# A toolchain manifest is read with the same reader, from the composite actions
# under .github/actions/, so the compiler CI installs is named in one place.
# Its values are what a toolchain calls itself, which is not always a number:
# `stable` and `nightly-2026-05-07` are exact names for a Rust toolchain the
# way `3.11` is for an interpreter, so the accepted shape starts at a letter or
# a digit. A range and a list are still refused, because neither starts with
# either — a leading `>`, `~`, `^` or `=` fails the match, and a comma list
# fails on the separator.
#
# The reader is deliberately jq-free: it runs before any toolchain is installed
# and on whatever the runner image happens to carry.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <manifest> <name>" >&2
  exit 2
fi
manifest=$1
name=$2

case "$manifest" in
  *.toml) pattern="\"${name}==\\K[^\"]+" ;;
  *.gemspec) pattern="\"${name}\", \"\\K[^\"]+" ;;
  *.json) pattern="\"${name}\"\\s*:\\s*\"\\K[^\"]+" ;;
  *)
    echo "$0: $manifest: not a manifest this reader knows (pyproject.toml, a gemspec or a JSON manifest)" >&2
    exit 2
    ;;
esac

# One version, nothing but version characters: a range, a second entry or no
# entry at all fails the match.
version=$(grep -oP -- "$pattern" "$manifest" || true)
if ! [[ "$version" =~ ^[0-9A-Za-z][0-9A-Za-z.+-]*$ ]]; then
  echo "$0: $manifest pins no exact version for $name (found: ${version:-nothing})" >&2
  exit 1
fi
printf '%s\n' "$version"
