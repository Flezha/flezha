#!/usr/bin/env bash
# The campaign's clean-run bar (wip/RULES.md), one line per command.
#
#   wip/tools/clean-run.sh
#
# Runs every gate of the bar in order: the core compile, the core suite split by
# top-level test directory, the :system tier under the Elixir runtime, the
# multi-node tier and the admin e2e, the four plugin packages, the Rust
# workspace, each SDK's own suites
# and linters, then wip/tools/verify.sh and the known-bugs check. Each command
# runs under `timeout 600` and prints PASS, FAIL or NOISY with its duration:
# NOISY is green with a warning, an uncaptured [error]/[warning] line or a stray
# stderr line in its log. Logs land in $LOGS/<gate>.log (stderr also in
# <gate>.err) under the directory printed first. Exits non-zero unless every
# gate passed clean.
#
# Only our own code is held to that: third-party Elixir dependencies compile in
# a `*-deps-compile-*` step of their own, whose warnings stay in its log and
# fail nothing. Path dependencies (the core, the Elixir SDK and its test app)
# are ours: that step skips them and the compile gate after it builds them.
#
# The commands are the repository's own CI lines: elixir.yml and nightly-e2e.yml
# for the core, each sdk/<x>/scripts/ci.sh and workflow for the SDKs. An SDK's
# ci.sh fetches its core from a release; here the core is this checkout, so each
# SDK's native library comes from scripts/build-<x>-sdk.sh (as the e2e tier
# builds it), its tests resolve the contract set from the enclosing tree, and
# the smoke tier runs the fake controller built from this tree.
# test/system/plugin_package_release_test.exs is left out: it runs the prod
# release in _build/prod/rel, which nothing here builds or runs.
set -uo pipefail

ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$ROOT" || exit 1
LOGS=$ROOT/wip/tools/.cache/clean-run/$(date +%Y%m%d-%H%M%S)
mkdir -p "$LOGS"
echo "logs: $LOGS"
failed=0

# Compiler and runtime warnings of every language here, and uncaptured logs.
NOISE='warning:|\[warning\]|\[error\]|\[critical\]|npm warn|warnings summary|deprecated:|there were issues'
# Progress lines tools write to stderr by design; anything else there is stray.
BENIGN='^\s*(Compiling|Finished|Running|Doc-tests|Checking|Blocking|Updating|Locking|Adding|Downloaded|Downloading|Fresh|Building)\b|^\s*$'
# trybuild's verdict on each UI case: a result, written to stderr.
BENIGN+='|^test \S+ \.\.\. ok$'
# php-cs-fixer's banner and progress bar, written to stderr on every run.
BENIGN+='|^PHP CS Fixer [0-9.]+ .* by |^PHP runtime: [0-9.]+$|^Loaded config \S+ from ".*"\.$|^\s*[0-9]+/[0-9]+ \[[▓░]+\]\s+[0-9]+%$'
# KB-854: core notices the hosts cannot capture yet; goes when the S-3 core notice hook lands.
BENIGN+='|^\[flezha\] (LOCAL MODE: state is held in memory and NOTHING is durable — no write-ahead log, '\
'no snapshots, no dead-letter queue\. Everything this run computes is gone when the process exits\. '\
'Durability, plugins, placement and migration, the dead-letter queue, observability export, authz and '\
'multi-runner scale all need a controller\.'\
'|tasklet \S+ attempt [0-9]+/[0-9]+ failed \(retrying in [0-9]+ ms\): .+'\
'|control socket absent at /\S+ \(from \$FLEZHA_SOCKET_FILES_PATH\); walked /\S+\. '\
'Fix: start the controller, or wait for it to finish booting'\
'|Connection attempt [0-9]+ failed: the runner could not reach a controller: '\
'control socket /\S+: No such file or directory \(os error 2\)'\
'|Control channel attempt [0-9]+ in [0-9.]+m?s\.\.\.|Connected after [0-9]+ attempts'\
'|the controller refused the connect: error: the runner proved no namespace: it presented no runner '\
'credential, or one this controller does not know \(runner\.namespace_not_proven\))$'\
'|^hint: flz token issue runner@<namespace> <namespace> runner --ttl never$'
# pyright's notice that a newer release exists: it says nothing about the code.
UPDATE_NOTICE='^WARNING: there is a new pyright version available \(v[0-9.]+ -> v[0-9.]+\)\.$'

gate() { # [--in dir] [--stderr-ok] [--noise-ok] name command...
  local dir=$ROOT stderr_ok=0 noise_ok=0
  while :; do
    case $1 in
      --in) dir=$2; shift 2 ;;
      --stderr-ok) stderr_ok=1; shift ;;
      --noise-ok) noise_ok=1 stderr_ok=1; shift ;;
      *) break ;;
    esac
  done
  local name=$1 log=$LOGS/$1.log err=$LOGS/$1.err start=$SECONDS status verdict
  shift
  : >"$log"
  (cd "$dir" && timeout --kill-after=30 600 "$@") >>"$log" 2> >(tee -a "$log" >"$err")
  status=$?
  wait
  if [ $status -eq 124 ] || [ $status -eq 137 ]; then
    verdict="FAIL $name (timed out)"
  elif [ $status -ne 0 ]; then
    verdict="FAIL $name"
  elif { [ $noise_ok -eq 0 ] && grep -vE "$UPDATE_NOTICE" "$log" | grep -qiE "$NOISE"; } ||
    { [ $stderr_ok -eq 0 ] && grep -qvE "$BENIGN" "$err"; }; then
    verdict="NOISY $name"
  else
    verdict="PASS $name"
  fi
  [ "${verdict%% *}" = PASS ] || failed=1
  echo "$verdict ($((SECONDS - start))s)" | tee -a "$LOGS/summary.txt"
}

FAKE=$ROOT/target/debug/flz-fake-controller
smoke() { # name dir runner...: the smoke tier every SDK's ci.sh ends with
  local name=$1 dir=$2
  shift 2
  gate --in "$dir" "$name" "$FAKE" --factory do_nothing --relay echo-1 --channel input \
    --input alpha --input beta --timeout-ms 60000 -- "$@"
}

deps_compile() { # name dir env...: third-party deps only, warnings logged
  local name=$1 dir=$2 env
  shift 2
  for env in "$@"; do
    gate --in "$dir" --noise-ok "$name-deps-compile-$env" env MIX_ENV="$env" mix deps.compile --skip-local-deps
  done
}

# Core compile: dev's elixirc_paths carry test/fixtures and test/helpers too.
deps_compile core "$ROOT" dev test
gate core-compile mix compile --warnings-as-errors
gate core-compile-test env MIX_ENV=test mix compile --warnings-as-errors

# What elixir.yml prepares before `mix test`: the rust_runner PortPilot boots and
# the QUIC development certificates.
gate --stderr-ok core-prep-runner cargo build --quiet --manifest-path=sdk/rust/test_app/Cargo.toml --target-dir=./target
gate --stderr-ok core-prep-certs env MIX_ENV=test mix run --no-start -e 'Flezha.Transport.CertGenerator.ensure_certs()'

# Core suite, one command per large test/flezha directory and one for the rest.
small=()
for d in "$ROOT"/test/flezha/*/; do
  rel=${d#"$ROOT"/}
  if [ "$(find "$d" -name '*_test.exs' | wc -l)" -ge 15 ]; then
    gate "core-suite-$(basename "$d")" mix test --warnings-as-errors "${rel%/}"
  else
    small+=("${rel%/}")
  fi
done
gate core-suite-flezha-rest mix test --warnings-as-errors "${small[@]}" test/flezha/*_test.exs
gate core-suite-rest mix test --warnings-as-errors test/*_test.exs test/sdk_conformance test/mix test/load test/system

# :system tier under the Elixir runtime, as nightly-e2e.yml prepares and runs it.
gate --stderr-ok e2e-prep-elixir-deps bash -c 'cd test/apps/elixir_runner && mix deps.get'
deps_compile e2e-prep-elixir "$ROOT/test/apps/elixir_runner" dev
gate --stderr-ok e2e-prep-elixir-compile bash -c 'cd test/apps/elixir_runner && mix compile'
mapfile -t system < <(cd "$ROOT" && ls test/system/*_test.exs | grep -v plugin_package_release_test)
for ((i = 0; i < ${#system[@]}; i += 6)); do
  gate "e2e-elixir-$((i / 6 + 1))" env FLEZHA_E2E_RUNTIME=elixir \
    mix test --warnings-as-errors --include system "${system[@]:i:6}"
done
gate e2e-admin mix test --warnings-as-errors --include system test/system/sdk_admin_e2e_test.exs
gate e2e-multi-node mix test --warnings-as-errors --only multi_node

# The plugin packages build against this checkout (`path: "../.."`); their
# broker-backed integration tags stay excluded, as each test_helper sets.
# Their own mix.lock must pin every git dependency they share with the core at
# the core's commit, or they test the core against a different library.
gate plugin-locks bash -c '
  bad=0
  while IFS= read -r line; do
    dep=${line%%:*}
    for l in plugins/*/mix.lock; do
      got=$(grep -F "$dep:" "$l") || continue
      [ "$got" = "$line" ] || { echo "$l: ${dep//[\" ]/} is not locked as in mix.lock"; bad=1; }
    done
  done < <(grep ":git," mix.lock)
  exit $bad'
for p in "$ROOT"/plugins/*/; do
  n=$(basename "$p" _plugin)
  n=${n#flezha_}
  gate --in "$p" --stderr-ok "plugin-$n-deps" mix deps.get
  deps_compile "plugin-$n" "$p" dev test
  gate --in "$p" "plugin-$n-compile" mix compile --warnings-as-errors
  gate --in "$p" "plugin-$n-test" mix test --warnings-as-errors
  gate --in "$p" "plugin-$n-format" mix format --check-formatted
done

# Rust: the workspace (sdk/rust and its crates are members), with the feature
# flags elixir.yml and sdk/rust/scripts/ci.sh test them under.
gate rust-build cargo build --quiet --workspace
gate rust-test-core cargo test --quiet -p flz_core -p flz_transport -p flz_nif -p flz
# Quiet through the environment, so the cargo trybuild runs inside the UI tests is
# quiet too: it copies the workspace's whole [patch], whose flz_core_ffi entry
# only the Elixir NIF uses, and would warn that it went unused. NO_COLOR keeps
# trybuild's per-case results plain.
gate rust-test-runner env CARGO_TERM_QUIET=true NO_COLOR=1 cargo test -p flezha_runner --features bench-internals,testing --lib --tests
gate rust-test-macros cargo test --quiet -p flezha-macros
gate rust-test-app cargo test --quiet -p rust_runner
gate rust-clippy cargo clippy --quiet --workspace -- -D warnings
gate rust-clippy-runner cargo clippy --quiet -p flezha_runner --features bench-internals,testing --lib --tests -- -D warnings
for m in sdk/rust/Cargo.toml sdk/rust/macros/Cargo.toml sdk/rust/test_app/Cargo.toml; do
  gate "rust-fmt-$(basename "$(dirname "$m")")" cargo +nightly-2026-05-07 fmt --manifest-path "$m" -- --check
done
gate --stderr-ok sdk-fake-controller cargo build --quiet -p flz_core_ffi --features fake-controller --bin flz-fake-controller
smoke rust-smoke "$ROOT/sdk/rust" "$ROOT/target/debug/rust_runner" smoke

# Java: sdk/java/scripts/ci.sh and the workflow's format job.
J=$ROOT/sdk/java
gate --stderr-ok sdk-java-native scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
gate --in "$J" sdk-java-install mvn -B -P native-verify install
gate --in "$J" sdk-java-test-app mvn -B -q -DskipTests -f test_app/pom.xml package
gate --in "$J" sdk-java-starter mvn -B -q -f examples/starter/pom.xml package
gate --in "$J" sdk-java-spotless mvn -B -q -DskipTests spotless:check
smoke sdk-java-smoke "$J" java -jar test_app/target/java_runner.jar smoke

# Node: sdk/node/scripts/ci.sh and the workflow's format job.
N=$ROOT/sdk/node
gate --stderr-ok sdk-node-build scripts/build-node-sdk.sh
gate --in "$N" sdk-node-typecheck npm run typecheck
gate --in "$N" sdk-node-test npm test
gate --in "$N" sdk-node-prettier npx prettier --check "src/**/*.ts" "test/**/*.mjs" "scripts/**/*.mjs"
gate --in "$N" sdk-node-jscpd npx jscpd src
smoke sdk-node-smoke "$N" node test_app/index.mjs smoke

# Python: sdk/python/scripts/ci.sh and the workflow's types job.
P=$ROOT/sdk/python
gate --stderr-ok sdk-python-build scripts/build-python-sdk.sh
gate --in "$P" --stderr-ok sdk-python-install bash -c '.venv/bin/python -m pip install --quiet --disable-pip-version-check --upgrade "pip>=25.1" && .venv/bin/python -m pip install --quiet --disable-pip-version-check --group dev'
gate --in "$P" sdk-python-ruff .venv/bin/ruff check .
gate --in "$P" sdk-python-ruff-format .venv/bin/ruff format --check .
gate --in "$P" sdk-python-pytest env PYTHONPATH=src:tests PATH="$P/.venv/bin:$PATH" .venv/bin/python -m pytest tests examples/starter -q
gate --in "$P" sdk-python-pyright env PYTHONPATH=src .venv/bin/pyright --pythonpath .venv/bin/python src/flezha tests
gate --in "$P" sdk-python-pyright-wrong-edge bash -c '! PYTHONPATH=src .venv/bin/pyright --pythonpath .venv/bin/python --project tests/typing_fixtures tests/typing_fixtures/wrong_edge.py >/dev/null'
gate --in "$P" sdk-python-pyright-wrong-emit bash -c '! PYTHONPATH=src .venv/bin/pyright --pythonpath .venv/bin/python --project tests/typing_fixtures tests/typing_fixtures/wrong_emit.py >/dev/null'
gate --in "$P" sdk-python-pyright-well-typed env PYTHONPATH=src .venv/bin/pyright --pythonpath .venv/bin/python tests/typing_fixtures/well_typed.py
smoke sdk-python-smoke "$P" .venv/bin/python test_app/runner.py smoke

# Ruby: sdk/ruby/scripts/ci.sh and the workflow's format job.
# Under the Ruby its CI runs (3.3/3.4): the gemspec pins minitest 5.x, which
# refuses Ruby 4, and the native library links the Ruby that builds it.
export RBENV_VERSION=3.4.10
R=$ROOT/sdk/ruby
gate --in "$R" --stderr-ok sdk-ruby-gems bash -c 'ruby -e '\''Gem::Specification.load("flezha.gemspec").dependencies.each { |d| puts "#{d.name}\t#{d.requirement}" }'\'' | while IFS=$'\''\t'\'' read -r n r; do gem install --conservative --no-document "$n" -v "$r" || exit 1; done'
gate --stderr-ok sdk-ruby-build scripts/build-ruby-sdk.sh
gate --in "$R" sdk-ruby-test rake test
gate --in "$R" sdk-ruby-rubocop rubocop
smoke sdk-ruby-smoke "$R" ruby test_app/runner.rb smoke
unset RBENV_VERSION

# PHP: sdk/php/scripts/ci.sh and the workflow's format job.
H=$ROOT/sdk/php
gate --stderr-ok sdk-php-build scripts/build-php-sdk.sh
gate --in "$H" sdk-php-lint bash -c "find src tests examples test_app -name '*.php' -not -path '*/vendor/*' -print0 | xargs -0 -n1 php -l >/dev/null"
gate --in "$H" sdk-php-phpunit vendor/bin/phpunit
gate --in "$H" sdk-php-cs-fixer vendor/bin/php-cs-fixer fix --dry-run --diff --config .php-cs-fixer.dist.php
smoke sdk-php-smoke "$H" php test_app/runner.php smoke

# Elixir: sdk/elixir/scripts/ci.sh (it has no workflow of its own).
E=$ROOT/sdk/elixir
gate --in "$E" --stderr-ok sdk-elixir-deps mix deps.get
deps_compile sdk-elixir "$E" dev test
deps_compile sdk-elixir-test-app "$E/test_app" dev
gate --in "$E" sdk-elixir-compile mix compile --warnings-as-errors
gate --in "$E" sdk-elixir-test mix test
gate --in "$E/test_app" sdk-elixir-test-app mix compile --warnings-as-errors
smoke sdk-elixir-smoke "$E" sh -c 'cd test_app && FLEZHA_TRANSPORT=uds exec mix run --no-halt --no-deps-check -- smoke'

gate gates wip/tools/verify.sh
gate ledgers python3 docs/known-bugs.py check

exit $failed
