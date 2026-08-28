#!/usr/bin/env bash
# Format every language; with `--check`, report what is unformatted and write
# nothing. scripts/format-check.sh is the gate (this script's `--check` plus
# the licensing checks); CI's format.yml runs the matching steps.
#
# Rust, Elixir and Java use the toolchain on PATH. Each veneer's formatter is
# that SDK's pinned development dependency: `npm ci` in sdk/node, the dev
# group of sdk/python/pyproject.toml into sdk/python/.venv, the gemspec's
# development dependency (`gem install`), `composer install` in sdk/php. A PHP
# interpreter that lives in a container is reached through FLEZHA_PHP, e.g.
#   FLEZHA_PHP="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work flezha-php php"
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--check" ]]; then
  rust=(-- --check) elixir=(--check-formatted) java=(spotless:check)
  prettier=(--check) ruff=(--check) rubocop=() phpcs=(--dry-run)
else
  rust=() elixir=() java=(spotless:apply)
  prettier=(--write) ruff=() rubocop=(-a) phpcs=()
fi

ruff_bin=sdk/python/.venv/bin/ruff
[[ -x "$ruff_bin" ]] || ruff_bin=ruff
read -ra php <<< "${FLEZHA_PHP:-php}"

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing $1 — $2" >&2; exit 1; }
}
need sdk/node/node_modules/.bin/prettier "run npm ci in sdk/node"
need "$ruff_bin" "install the dev group of sdk/python/pyproject.toml"
need rubocop "install the development dependency in sdk/ruby/flezha.gemspec"
need "${php[0]}" "install PHP or set FLEZHA_PHP"
need sdk/php/vendor/bin/php-cs-fixer "run composer install in sdk/php"

run() {
  echo "==> $1"
  shift
  "$@"
}

run "cargo fmt" cargo +nightly fmt --all "${rust[@]}"
run "mix format" mix format "${elixir[@]}"
run "mvn spotless" mvn -q -f sdk/java/pom.xml -DskipTests "${java[@]}"
run prettier sdk/node/node_modules/.bin/prettier "${prettier[@]}" \
  'sdk/node/{src,test,scripts,types-tests}/**/*.{ts,mts,cts,js,mjs,cjs}' \
  'test/apps/node_runner/*.mjs'
run "ruff format" "$ruff_bin" format "${ruff[@]}" sdk/python test/apps/python_runner
run rubocop rubocop "${rubocop[@]}" sdk/ruby test/apps/ruby_runner
run php-cs-fixer "${php[@]}" sdk/php/vendor/bin/php-cs-fixer fix "${phpcs[@]}" --config .php-cs-fixer.dist.php
