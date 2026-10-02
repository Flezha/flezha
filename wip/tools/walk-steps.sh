# The capture helpers every guide walk shares; sourced by a walk that has set
# OUT (where its output goes), `page` (the page being walked) and `n` (the next
# step's number, reset to 0 per page).

step() { # name, command (run by bash, shown as typed)
  local file
  file=$(printf '%s/%s/%02d-%s.txt' "$OUT" "$page" "$n" "$1")
  n=$((n + 1))
  mkdir -p "$OUT/$page"
  { echo "\$ $2"; bash -c "$2" </dev/null 2>&1; } >"$file"
  echo "  $(basename "$file")"
}
note() { echo "$*" >>"$OUT/$page/notes.txt"; }

# Waits a bounded time for what a step reads next, so the capture is not a race.
until_ok() { # seconds, command
  local deadline=$((SECONDS + $1))
  while [ $SECONDS -lt "$deadline" ]; do bash -c "$2" </dev/null >/dev/null 2>&1 && return 0; sleep 1; done
  note "gave up after $1 s waiting for: $2"
}

# A guide example's runner, built and run on this machine from the checkout the
# way its README's "from a checkout" section says, so Parts 1 and 2 walk the
# tracked code. `example_build <sdk>` and `example_run <sdk> <name> [args...]`
# print the command to run in the example's folder; <name> is the jar or binary
# (echo, tradedesk). They need the SDK's artifacts built first: the native
# library, the wheel, the gem or the installed jars.
example_build() {
  case "$1" in
    java) echo "mvn -q clean package" ;; # clean: an up-to-date jar keeps the native library it was built with
    rust) echo "mkdir -p .cargo && printf '%s\n' '[patch.crates-io]' 'flezha_runner = { path = \"$ROOT/sdk/rust\" }' '' '$(core_patch)' >.cargo/config.toml && cargo build --release --offline --quiet" ;;
    node) echo "npm install --no-save --install-links --silent ../.. && mkdir -p node_modules/@flezha/sdk/native && cp ../../native/flz_core_ffi.node node_modules/@flezha/sdk/native/" ;;
    python) echo "python3 -m venv .venv && .venv/bin/pip install --quiet --force-reinstall ../../dist/flezha-0.1.0-*.whl && { [ ! -f requirements.txt ] || .venv/bin/pip install --quiet -r requirements.txt; }" ;;
    ruby) echo "gem install --local --silent ../../pkg/flezha-0.1.0-x86_64-linux.gem && bundle install --quiet" ;;
    php) echo "composer update --no-interaction --quiet" ;; # with php_checkout's COMPOSER_HOME; update: install keeps a stale copy of the SDK
    elixir) echo "mix deps.get && mix compile" ;;
  esac
}
example_run() {
  local sdk=$1 name=$2
  shift 2
  case "$sdk" in
    java) echo "java -jar target/$name.jar $*" ;;
    rust) echo "target/release/$name $*" ;;
    node) echo "node runner.mjs $*" ;;
    python) echo ".venv/bin/python runner.py $*" ;;
    ruby) echo "bundle exec ruby runner.rb $*" ;;
    php) echo "php runner.php $*" ;;
    elixir) echo "mix run --no-halt" ;; # the default namespace: mix run takes an argument as a file
  esac
}
# The cargo patch that points the core, which a package names by its git tag, at
# this checkout.
core_patch() {
  printf '[patch."https://github.com/flezha/flezha"]\nflz_core_ffi = { path = "%s/flz_core_ffi" }\nflz_core = { path = "%s/flz_core" }\nflz_transport = { path = "%s/flz_transport" }\n' \
    "$ROOT" "$ROOT" "$ROOT"
}
# Builds the example in <dir> with `example_build`, its output in $OUT/build.log.
build_example() {
  (cd "$1" && bash -c "$(example_build "$SDK")") >"$OUT/build.log" 2>&1 ||
    { echo "walk: $1 did not build, see $OUT/build.log" >&2; exit 1; }
}
# The PHP SDK is not on Packagist: the README's path repository, in a
# COMPOSER_HOME of the walks' own so the user's global configuration is untouched.
php_checkout() {
  export COMPOSER_HOME=$ROOT/wip/tools/.cache/composer-php
  composer config --global repositories.flezha-sdk \
    "{\"type\": \"path\", \"url\": \"$ROOT/sdk/php\", \"options\": {\"symlink\": false, \"versions\": {\"flezha/sdk\": \"0.1.0\"}}}"
}
