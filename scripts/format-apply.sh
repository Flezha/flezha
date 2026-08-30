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

if [[ "${1:-}" == "--check" ]]; then
  rust=(-- --check) elixir=(--check-formatted) java=(spotless:check)
  prettier=(--check) ruff=(--check) rubocop=() phpcs=(--dry-run)
  verdict="UNFORMATTED — run scripts/format-apply.sh:"
else
  rust=() elixir=() java=(spotless:apply)
  prettier=(--write) ruff=() rubocop=(-a) phpcs=()
  verdict="FAILED — the formatter itself errored:"
fi

prettier_bin=sdk/node/node_modules/.bin/prettier
phpcs_bin=sdk/php/vendor/bin/php-cs-fixer
ruff_bin=sdk/python/.venv/bin/ruff
[[ -x "$ruff_bin" ]] || ruff_bin=ruff
read -ra php <<< "${FLEZHA_PHP:-php}"

# The nightly is pinned to the one .github/workflows/format.yml installs.
# rustfmt.toml's `imports_granularity`, `group_imports` and `ignore` are
# nightly-only options, and two nightlies are two style opinions; bump this
# with the workflow and rustfmt.toml together, as rust-toolchain.toml says.
gate_run "cargo fmt" cargo:"install Rust via rustup" -- \
  cargo +nightly-2026-05-07 fmt --all "${rust[@]}"
gate_run "mix format" mix:"install Elixir" -- \
  mix format "${elixir[@]}"
gate_run "mvn spotless" mvn:"install Maven and a JDK" -- \
  mvn -q -f sdk/java/pom.xml -DskipTests "${java[@]}"
gate_run prettier "$prettier_bin":"run npm ci in sdk/node" -- \
  "$prettier_bin" "${prettier[@]}" \
  'sdk/node/{src,test,scripts,types-tests}/**/*.{ts,mts,cts,js,mjs,cjs}' \
  'test/apps/node_runner/*.mjs'
# `ruff format` is gated; `ruff check`, the linter, deliberately is not. It
# reports 77 errors on this tree (55 auto-fixable) and clearing them is a
# Python SDK source change with judgement in it — the DTZ ambient-clock family
# it flags is a rule the SDK guards on purpose, and RUF022/RUF023 reorder
# `__all__` and `__slots__`. Adopting it is its own change, tracked as KI-6 in
# docs/known_issues.md; it is not a line to uncomment here.
gate_run "ruff format" "$ruff_bin":"install the dev group of sdk/python/pyproject.toml" -- \
  "$ruff_bin" format "${ruff[@]}" sdk/python test/apps/python_runner
gate_run rubocop rubocop:"install the development dependency in sdk/ruby/flezha.gemspec" -- \
  rubocop "${rubocop[@]}" sdk/ruby test/apps/ruby_runner
gate_run php-cs-fixer "${php[0]}":"install PHP or set FLEZHA_PHP" \
  "$phpcs_bin":"run composer install in sdk/php" -- \
  "${php[@]}" "$phpcs_bin" fix "${phpcs[@]}" --config .php-cs-fixer.dist.php

gate_report "$verdict"
