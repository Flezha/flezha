#!/usr/bin/env bash
# Format every language; with `--check`, report what is unformatted and write
# nothing. scripts/format-check.sh is the gate (this script's `--check` plus
# the licensing checks) and .github/workflows/format.yml runs this same script,
# so the list of formatters lives here and nowhere else.
#
# Every formatter is attempted and every verdict is reported. A tool that is
# not installed takes its own language out of the report and nothing else; the
# summary lists unavailable tools apart from unformatted trees, and so does the
# exit status (2 = a tool was missing, 1 = something is unformatted). See
# scripts/gate_lib.sh for why that distinction is the point of this script.
#
# Rust, Elixir and Java use the toolchain on PATH. Each veneer's formatter is
# that SDK's pinned development dependency: `npm ci` in sdk/node, the dev
# group of sdk/python/pyproject.toml into sdk/python/.venv, the gemspec's
# development dependency (`gem install`), `composer install` in sdk/php. A PHP
# interpreter that lives in a container is reached through FLEZHA_PHP, e.g.
#   FLEZHA_PHP="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work flezha-php php"
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
source scripts/gate_lib.sh

# What each formatter does when it finds drift, as opposed to failing for its
# own reasons. prettier, ruff and rubocop document 1 for the first and 2 for the
# second; php-cs-fixer's status is a bit field where exactly 8 is "needs fixing"
# and every other bit set is an error. rustfmt, mix and mvn return 1 either way,
# so the verdict each prints for drift, and for nothing else, decides. In write
# mode a formatter has nothing to find: no rule is declared, and gate_run then
# reads every non-zero status as the tool failing.
declare -A drift=()
if [[ "${1:-}" == "--check" ]]; then
  rust=(-- --check) elixir=(--check-formatted) java=(spotless:check)
  prettier=(--check) ruff=(--check) rubocop=() phpcs=(--dry-run)
  drift=([rust]='says:Diff in '
    [elixir]='says:format failed due to --check-formatted'
    [java]='says:files had format violations'
    [prettier]=status:1 [ruff]=status:1 [rubocop]=status:1 [phpcs]=status:8)
else
  rust=() elixir=() java=(spotless:apply)
  prettier=(--write) ruff=() rubocop=(-a) phpcs=()
fi

prettier_bin=sdk/node/node_modules/.bin/prettier
phpcs_bin=sdk/php/vendor/bin/php-cs-fixer
ruff_bin=sdk/python/.venv/bin/ruff
[[ -x "$ruff_bin" ]] || ruff_bin=ruff
# The formatter and the linter are the same binary over the same two trees, so
# the trees and the install hint are named once and used by both calls.
ruff_need="$ruff_bin":"install the dev group of sdk/python/pyproject.toml"
ruff_paths=(sdk/python test/apps/python_runner)
read -ra php <<< "${FLEZHA_PHP:-php}"

# The nightly is pinned to the one .github/workflows/format.yml installs.
# rustfmt.toml's `imports_granularity`, `group_imports` and `ignore` are
# nightly-only options, and two nightlies are two style opinions; bump this
# with the workflow and rustfmt.toml together, as rust-toolchain.toml says.
gate_run --finding "${drift[rust]:-}" "cargo fmt" cargo:"install Rust via rustup" -- \
  cargo +nightly-2026-05-07 fmt --all "${rust[@]}"
gate_run --finding "${drift[elixir]:-}" "mix format" mix:"install Elixir" -- \
  mix format "${elixir[@]}"
gate_run --finding "${drift[java]:-}" "mvn spotless" mvn:"install Maven and a JDK" -- \
  mvn -q -f sdk/java/pom.xml -DskipTests "${java[@]}"
gate_run --finding "${drift[prettier]:-}" prettier \
  "$prettier_bin":"run npm ci in sdk/node" -- \
  "$prettier_bin" "${prettier[@]}" \
  'sdk/node/{src,test,scripts,types-tests}/**/*.{ts,mts,cts,js,mjs,cjs}' \
  'test/apps/node_runner/*.mjs'
gate_run --finding "${drift[ruff]:-}" "ruff format" "$ruff_need" -- \
  "$ruff_bin" format "${ruff[@]}" "${ruff_paths[@]}"
# The linter, in both modes and never with `--fix`: a format pass has to stay
# reproducible by re-running the formatters, so this reports and rewrites
# nothing. What it enforces is settled in ruff.toml — the rules refused there
# carry their reason — and the enabled set is the pin's own default, so raising
# ruff==… in sdk/python/pyproject.toml settles whatever the new version reports.
gate_run --finding status:1 "ruff check" "$ruff_need" -- \
  "$ruff_bin" check "${ruff_paths[@]}"
gate_run --finding "${drift[rubocop]:-}" rubocop \
  rubocop:"install the development dependency in sdk/ruby/flezha.gemspec" -- \
  rubocop "${rubocop[@]}" sdk/ruby test/apps/ruby_runner
gate_run --finding "${drift[phpcs]:-}" php-cs-fixer \
  "${php[0]}":"install PHP or set FLEZHA_PHP" \
  "$phpcs_bin":"run composer install in sdk/php" -- \
  "${php[@]}" "$phpcs_bin" fix "${phpcs[@]}" --config .php-cs-fixer.dist.php

gate_report "UNFORMATTED — run scripts/format-apply.sh:"
