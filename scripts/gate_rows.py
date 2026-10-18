"""The gate list: every check this repository runs, one row per gate.

`scripts/naming_gate.py` is the runner and the only reader of `GATES`; a gate is
added here and nowhere else.
"""

import glob
import os
import re
import tempfile

# Compiler and runtime warnings of every language here, and uncaptured logs.
NOISE = re.compile(
    r"warning:|\[warning\]|\[error\]|\[critical\]|npm warn|warnings summary|deprecated:"
    r"|there were issues", re.I)
# Progress lines tools write to stderr by design; anything else there is stray.
BENIGN = re.compile(
    r"^\s*(Compiling|Finished|Running|Doc-tests|Checking|Blocking|Updating|Locking|Adding"
    r"|Downloaded|Downloading|Fresh|Building)\b|^\s*$"
    # trybuild's verdict on each UI case: a result, written to stderr.
    r"|^test \S+ \.\.\. ok$"
    # php-cs-fixer's banner and progress bar, written to stderr on every run.
    r'|^PHP CS Fixer [0-9.]+ .* by |^PHP runtime: [0-9.]+$|^Loaded config \S+ from ".*"\.$'
    r"|^\s*[0-9]+/[0-9]+ \[[▓░]+\]\s+[0-9]+%$"
    # KB-854: core notices the hosts cannot capture yet; goes when the S-3 core notice hook lands.
    r"|^\[flezha\] (LOCAL MODE: state is held in memory and NOTHING is durable — no write-ahead log, "
    r"no snapshots, no dead-letter queue\. Everything this run computes is gone when the process exits\. "
    r"Durability, plugins, placement and migration, the dead-letter queue, observability export, authz and "
    r"multi-runner scale all need a controller\."
    r"|tasklet \S+ attempt [0-9]+/[0-9]+ failed \(retrying in [0-9]+ ms\): .+"
    r"|control socket absent at /\S+ \(from \$FLEZHA_SOCKET_FILES_PATH\); walked /\S+\. "
    r"Fix: start the controller, or wait for it to finish booting"
    r"|Connection attempt [0-9]+ failed: the runner could not reach a controller: "
    r"control socket /\S+: No such file or directory \(os error 2\)"
    r"|Control channel attempt [0-9]+ in [0-9.]+m?s\.\.\.|Connected after [0-9]+ attempts"
    r"|the controller refused the connect: error: the runner proved no namespace: it presented no runner "
    r"credential, or one this controller does not know \(runner\.namespace_not_proven\)"
    r"|the controller refused the connect: error: refused by the test \(runner\.namespace_not_proven\)"
    r"|RunnerShutdownAck timeout for (dyn-1|abi_probe))$"
    r"|^hint: flz token issue runner@<namespace> <namespace> runner --ttl never$"
    # The per-module timings and the conformance table `TimingFormatter` prints.
    r"|^\[(timing|conformance)\] |^  [0-9.]+ ms  [0-9]+ tests  \S+$"
    r"|^  [a-z0-9_]+ +(pass|fail|deferred|untested)$")
# pyright's notice that a newer release exists: it says nothing about the code.
UPDATE_NOTICE = re.compile(
    r"^WARNING: there is a new pyright version available \(v[0-9.]+ -> v[0-9.]+\)\.$")


def noisy(row, out, err):
    """The line that makes a green `quiet` row a finding, or None: a warning or
    an uncaptured log line anywhere in its output, or a stderr line that is not
    a known progress line."""
    if not row.get("quiet"):
        return None
    for line in out.splitlines():
        if NOISE.search(line) and not UPDATE_NOTICE.search(line):
            return line
    if not row.get("stderr_ok"):
        return next((line for line in err.splitlines() if not BENIGN.search(line)), None)
    return None


# ---------------------------------------------------------------------------
# The gate registry. One row per gate; `--run` is the one reader.
#
# A gate is declared here and nowhere else: CI runs `naming_gate.py --run @<group>`
# rather than re-spelling one step per gate, so adding a gate is a row and a
# workflow that forgets to call it is not a thing that can happen. A row is
# `id`, `group` and `argv`. `tools` names what the gate needs on PATH together
# with how to install it; an absent tool takes that gate out of the report and
# nothing else. A gate answers on this contract unless it declares `finding`
# (`status:<codes>`, `status:any` or `says:<regex>`), which is how a tool whose
# status alone cannot tell "found something" from "did not complete" says which
# it was. A row may also declare `cwd` (relative to the root), `env` (values may
# name `$VAR`s of the caller's environment), `timeout` in seconds, `quiet` (a
# green run is still a finding when its output is noisy, `noisy` above) and
# `stderr_ok` (under `quiet`, stderr is not judged). A row declaring `groups`
# instead of `argv` stands for the rows that spec selects.
GATES = (
    {"id": "overflow-naming", "group": "naming",
     "argv": ["scripts/check-overflow-naming.sh"]},
    {"id": "local-mode-naming", "group": "naming",
     "argv": ["scripts/check-local-mode-naming.sh"]},
    {"id": "module-naming", "group": "naming",
     "argv": ["scripts/check-module-naming.sh"]},
    {"id": "layer-direction", "group": "naming",
     "argv": ["scripts/check-layer-direction.sh"]},
    {"id": "mimic-global", "group": "naming",
     "argv": ["scripts/check-mimic-global.sh"]},
    {"id": "docs-citations", "group": "naming",
     "argv": ["scripts/check-docs-citations.sh"]},
    {"id": "test-fixture", "group": "naming",
     "argv": ["scripts/check-test-fixture.sh"]},
    {"id": "live-env-in-tests", "group": "naming",
     "argv": ["scripts/check-live-env-in-tests.sh"]},
    {"id": "ci-toolchain", "group": "naming",
     "argv": ["scripts/check-ci-toolchain.sh"]},
    {"id": "vm-args", "group": "naming",
     "argv": ["scripts/check-vm-args.sh"]},
    # The queue's order, and the scenarios a MODIFIED block would delete.
    # `/openspec/changes` is in .gitignore, so a CI checkout carries none of it
    # and both rows read an empty queue and say so; where they bite is a
    # developer's tree and the `--run @naming` made before an archive.
    {"id": "openspec-order", "group": "naming",
     "argv": ["scripts/check-openspec-order.sh"]},
    {"id": "openspec-scenarios", "group": "naming",
     "argv": ["scripts/check-openspec-scenarios.sh"]},
    {"id": "build-artifacts-tracked", "group": "naming",
     "argv": ["scripts/check-build-artifacts.sh", "tracked"]},
    {"id": "naming-gate-self-test", "group": "naming",
     "argv": ["python3", "scripts/naming_gate.py", "--self-test"]},
    {"id": "layer-direction-self-test", "group": "naming",
     "argv": ["scripts/check-layer-direction.sh", "--self-test"]},
    {"id": "openspec-scenarios-self-test", "group": "naming",
     "argv": ["scripts/check-openspec-scenarios.sh", "--self-test"]},
    {"id": "ceilings-self-test", "group": "naming",
     "argv": ["python3", "scripts/check_ceilings.py", "--self-test"]},
    # File 800, function 50 and the compile cycles, against a baseline that may
    # only shrink. Its own group: the cycle count needs `mix`.
    {"id": "ceilings", "group": "ceilings", "tools": ["mix:install Elixir"],
     "argv": ["python3", "scripts/check_ceilings.py"]},
    # The publication-readiness record's SHAPE. Its content arm is the row
    # below, in its own group, because every blocking entry is open until the
    # day of the release and a gate that lands red stops being read.
    {"id": "publish-readiness", "group": "naming",
     "argv": ["scripts/check-publish-readiness.sh"]},
    {"id": "feature-maturity", "group": "naming",
     "argv": ["scripts/check-feature-maturity.sh"]},
    # Content: every blocking entry settled. Selected only by `@release`, which
    # the tag-triggered release job runs before it builds any artefact - after
    # an artefact is on a registry it is too late by definition.
    {"id": "publish-readiness-release", "group": "release",
     "argv": ["scripts/check-publish-readiness.sh", "--release"]},
    # The immutable-tag guard of scripts/publish-image.sh, against a stub docker.
    {"id": "publish-image-test", "group": "release",
     "argv": ["scripts/publish-image-test.sh"]},
    # Every SDK repository's CI, run locally against a release built from this
    # tree. Run by hand before a release: it builds, and it needs docker.
    {"id": "sdk-ci-rehearsal", "group": "rehearsal",
     "tools": ["docker:install Docker", "act:brew install act"],
     "argv": ["scripts/rehearse-sdk-ci.sh"]},
    # Its `size` arm reads the packed repository, so it needs the whole history
    # and runs in a job that checks out with fetch-depth 0.
    {"id": "build-artifacts-size", "group": "history",
     "argv": ["scripts/check-build-artifacts.sh", "size"]},
    # The source-shape gate. `format-apply.sh --check` answers on this contract
    # itself, so its status is read as one of ours instead of being flattened.
    {"id": "formatters", "group": "format",
     "argv": ["scripts/format-apply.sh", "--check"]},
    # `reuse lint-file` over git-listed paths: a full `reuse lint` walks the
    # tree and chokes on the stale test sockets under app_data/, which git never
    # lists. xargs collapses every status from 1 to 125 into 123, which alone
    # would make a reuse crash and a file with no licence header the same
    # number, so the wrapper keeps reuse's own 1 and turns every other failure
    # into 255 — which xargs reports as 124.
    {"id": "reuse-lint", "group": "format", "finding": "status:123",
     "tools": ["reuse:pipx install reuse"],
     "argv": ["bash", "-c",
              "git ls-files -co --exclude-standard -z | "
              "xargs -0 sh -c 'reuse lint-file \"$@\" || exit $(($? == 1 ? 1 : 255))' reuse-lint"]},
    {"id": "third-party-notices", "group": "format", "finding": "status:1",
     "tools": ["cargo-about:cargo install cargo-about"],
     "argv": ["bash", "-c",
              'g="${TMPDIR:-/tmp}/third_party_notices.md"; '
              # A generator that cannot run is not a stale notices file: keep
              # its status out of diff's, whose 1 is the only finding here.
              'cargo about generate about.hbs -o "$g" || exit 255; '
              'diff -u THIRD_PARTY_NOTICES.md "$g"']},
)

# `scripts/check-feature-symbols.sh` is deliberately not a row. It is not a gate
# over the tree: it builds an artefact three ways and compares the exported
# symbols, so it takes arguments, needs a compiler, and answers about one build
# rather than about the repository. Two CI workflows call it directly.


# ---------------------------------------------------------------------------
# The bar: what the clean run selects (`@clean`), and the groups each workflow selects.
# Generated where the tree decides the list: one core suite per large test
# directory, `:system` shards of six per runtime manifest, one block per plugin.

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNTIMES = sorted(os.path.basename(os.path.dirname(path))[:-len("_runner")]
                  for path in glob.glob(f"{ROOT}/test/apps/*_runner/conformance.json"))
FAKE = f"{ROOT}/target/debug/flz-fake-controller"
# The runtimes the bar runs the `:system` tier under: the nightly's schedule and elixir.
E2E_RUNTIMES = ("elixir", "rust", "java")


def _files(pattern):
    return sorted(os.path.relpath(path, ROOT) for path in glob.glob(f"{ROOT}/{pattern}"))


def _bar(row_id, group, *argv, **opts):
    """A row of the bar: a plain tool, so any failing status is what it found,
    and its output must be quiet unless it says otherwise."""
    return {"id": row_id, "group": group, "argv": list(argv), "quiet": True,
            "finding": "status:any", **opts}


def _mix_test(row_id, group, *args, env=None, **opts):
    """A core `mix test` row, recording per-test timings beside the usual output."""
    timing = os.path.join(tempfile.gettempdir(), f"flezha_timing_{row_id}.tsv")
    return _bar(row_id, group, "mix", "test", "--warnings-as-errors",
                "--formatter", "ExUnit.CLIFormatter",
                "--formatter", "FlezhaTest.Helper.TimingFormatter", *args,
                env={**(env or {}), "TIMING_OUT": timing}, **opts)


def _deps_compile(name, group, cwd, *envs):
    """Third-party dependencies alone: their warnings stay in the log and fail
    nothing. Path dependencies are ours and build in the compile row after."""
    return [_bar(f"{name}-deps-compile-{env}", group, "mix", "deps.compile", "--skip-local-deps",
                 cwd=cwd, env={"MIX_ENV": env}, quiet=False) for env in envs]


def _build(row_id, group, runtime, **opts):
    """A runner built by its manifest's test build, through the one build entry."""
    return _bar(row_id, group, "scripts/build-runner.sh", runtime, "test", stderr_ok=True, **opts)


def _smoke(row_id, group, cwd, *runner, **opts):
    """The smoke tier every SDK's ci.sh ends with, against the fake controller."""
    return _bar(row_id, group, FAKE, "--factory", "do_nothing", "--relay", "echo-1",
                "--channel", "input", "--input", "alpha", "--input", "beta",
                "--timeout-ms", "60000", "--", *runner, cwd=cwd, **opts)


def _e2e_smoke(runtime, env=None):
    """The shared smoke tier under one SDK's runtime, against the controller."""
    return _mix_test(f"sdk-{runtime}-e2e-smoke", f"sdk-{runtime}", "--include", "sdk_smoke",
                     "test/system/sdk_smoke_e2e_test.exs",
                     env={**(env or {}), "FLEZHA_E2E_RUNTIME": runtime})


def _core():
    # On a cold `_build` the first quiet row would also compile, and judge, the
    # third-party dependencies, so a selection names `@deps` ahead of it.
    rows = [{"id": "core-deps", "group": "core", "groups": "@deps"},
            *_deps_compile("core", "deps", None, "dev", "test"),
            _bar("core-compile", "core", "mix", "compile", "--warnings-as-errors"),
            _bar("core-compile-test", "core", "mix", "compile", "--warnings-as-errors",
                 env={"MIX_ENV": "test"}),
            # The WebSocket testnet bundle the testnet cases need (`mix test.setup`).
            _bar("core-testnet", "core", "mix", "test.setup", stderr_ok=True),
            _build("core-prep-runner", "core", "rust"),
            _bar("core-prep-certs", "core", "mix", "run", "--no-start", "-e",
                 "Flezha.Transport.CertGenerator.ensure_certs()", env={"MIX_ENV": "test"},
                 stderr_ok=True)]
    small = []
    for directory in _files("test/flezha/*/"):
        if len(glob.glob(f"{ROOT}/{directory}/**/*_test.exs", recursive=True)) >= 15:
            rows.append(_mix_test(f"core-suite-{os.path.basename(directory)}", "core", directory))
        else:
            small.append(directory)
    rows.append(_mix_test("core-suite-flezha-rest", "core", *small,
                          *_files("test/flezha/*_test.exs")))
    rows.append(_mix_test("core-suite-rest", "core", *_files("test/*_test.exs"),
                          "test/sdk_conformance", "test/mix", "test/load", "test/system"))
    return rows


# Builds the prod release itself and drives no runner: one row, not one per runtime.
PACKAGE_RELEASE = "test/system/plugin_package_release_test.exs"


def _e2e(runtime):
    """The `:system` tier under one runtime, in shards of six files."""
    group = f"e2e-{runtime}"
    app = "test/apps/elixir_runner"
    rows = [_bar("e2e-prep-elixir-deps", group, "mix", "deps.get", cwd=app, stderr_ok=True),
            *_deps_compile("e2e-prep-elixir", group, app, "dev")] if runtime == "elixir" else []
    rows.append(_build(f"e2e-prep-{runtime}", group, runtime))
    system = [path for path in _files("test/system/*_test.exs") if path != PACKAGE_RELEASE]
    for start in range(0, len(system), 6):
        rows.append(_mix_test(f"e2e-{runtime}-{start // 6 + 1}", group, "--include", "system",
                              *system[start:start + 6], env={"FLEZHA_E2E_RUNTIME": runtime}))
    return rows


# A plugin's mix.lock must pin every git and hex dependency it shares with the
# core exactly as mix.lock does, or it tests the core against another library.
PLUGIN_LOCKS = r"""
  bad=0
  while IFS= read -r line; do
    dep=${line%%:*}
    for l in plugins/*/mix.lock; do
      got=$(grep -F "$dep:" "$l") || continue
      [ "$got" = "$line" ] || { echo "$l: ${dep//[\" ]/} is not locked as in mix.lock"; bad=1; }
    done
  done < <(grep -E ":(git|hex)," mix.lock)
  exit $bad"""


def _plugins():
    """The plugin packages build against this checkout; their broker-backed
    integration tags stay excluded, as each test_helper sets."""
    rows = [_bar("plugin-locks", "plugins", "bash", "-c", PLUGIN_LOCKS)]
    for path in _files("plugins/*/"):
        name = os.path.basename(path).removesuffix("_plugin").removeprefix("flezha_")
        rows += [_bar(f"plugin-{name}-deps", "plugins", "mix", "deps.get", cwd=path,
                      stderr_ok=True),
                 *_deps_compile(f"plugin-{name}", "plugins", path, "dev", "test"),
                 _bar(f"plugin-{name}-compile", "plugins", "mix", "compile",
                      "--warnings-as-errors", cwd=path),
                 _bar(f"plugin-{name}-test", "plugins", "mix", "test", "--warnings-as-errors",
                      cwd=path),
                 _bar(f"plugin-{name}-format", "plugins", "mix", "format", "--check-formatted",
                      cwd=path)]
    return rows


RUBY = {"RBENV_VERSION": "3.4.10"}


def _rust():
    """The workspace (sdk/rust and its crates are members), under the feature
    flags CI tests each crate with."""
    ffi = ["cargo", "test", "--quiet", "-p", "flz_core_ffi", "--features"]
    return [
        _bar("rust-build", "rust", "cargo", "build", "--quiet", "--workspace"),
        _bar("rust-test-core", "rust", "cargo", "test", "--quiet",
             "-p", "flz_core", "-p", "flz_transport", "-p", "flz_nif", "-p", "flz"),
        # Quiet through the environment, so the cargo trybuild runs inside the UI
        # tests is quiet too: it copies the workspace's whole [patch], whose
        # flz_core_ffi entry only the Elixir NIF uses, and would warn that it went
        # unused. NO_COLOR keeps trybuild's per-case results plain.
        _bar("rust-test-runner", "rust", "cargo", "test", "-p", "flezha_runner",
             "--features", "bench-internals,testing", "--lib", "--tests",
             env={"CARGO_TERM_QUIET": "true", "NO_COLOR": "1"}),
        _bar("rust-test-macros", "rust", "cargo", "test", "--quiet", "-p", "flezha-macros"),
        _bar("rust-test-app", "rust", "cargo", "test", "--quiet", "-p", "rust_runner"),
        # The ABI scenario through each veneer, and the crate with none of them.
        _bar("rust-test-ffi-jni", "rust", *ffi, "jni", "--test", "abi_scenario", "veneer_jni"),
        _bar("rust-test-ffi-napi", "rust", *ffi, "napi"),
        # The embedded VM records the stack frame it boots from: one thread. Run
        # through the Ruby build, which points bindgen at builtin headers.
        _bar("rust-test-ffi-ruby", "rust", "scripts/build-ruby-sdk.sh", "--run", *ffi,
             "ruby-embed", "--", "--test-threads=1", env=RUBY),
        _bar("rust-test-ffi-python", "rust", *ffi, "python"),
        _bar("rust-build-ffi", "rust", "cargo", "build", "--quiet", "-p", "flz_core_ffi"),
        # Every target under the runner's test features, so the criterion
        # benches no other row builds compile too.
        _bar("rust-clippy", "rust", "cargo", "clippy", "--quiet", "--workspace", "--all-targets",
             "--features", "flezha_runner/bench-internals,flezha_runner/testing",
             "--", "-D", "warnings"),
        _bar("sdk-fake-controller", "rust", "cargo", "build", "--quiet", "-p", "flz_core_ffi",
             "--features", "fake-controller", "--bin", "flz-fake-controller", stderr_ok=True),
        _smoke("rust-smoke", "rust", "sdk/rust", f"{ROOT}/target/debug/rust_runner", "smoke"),
    ]


def _sdk_java():
    """sdk/java/scripts/ci.sh and the workflow's format job."""
    g, d = "sdk-java", "sdk/java"
    return [_build("sdk-java-build", g, "java"),
            _bar("sdk-java-install", g, "mvn", "-B", "-P", "native-verify", "install", cwd=d),
            _bar("sdk-java-starter", g, "mvn", "-B", "-q", "-f", "examples/starter/pom.xml",
                 "package", cwd=d),
            _bar("sdk-java-spotless", g, "mvn", "-B", "-q", "-DskipTests", "spotless:check",
                 cwd=d),
            _smoke("sdk-java-smoke", g, d, "java", "-jar", "test_app/target/java_runner.jar",
                   "smoke"),
            _e2e_smoke("java")]


def _sdk_node():
    """sdk/node/scripts/ci.sh and the workflow's format job."""
    g, d = "sdk-node", "sdk/node"
    return [_build("sdk-node-build", g, "node"),
            _bar("sdk-node-typecheck", g, "npm", "run", "typecheck", cwd=d),
            _bar("sdk-node-test", g, "npm", "test", cwd=d),
            _bar("sdk-node-prettier", g, "npx", "prettier", "--check", "src/**/*.ts",
                 "test/**/*.mjs", "scripts/**/*.mjs", cwd=d),
            _bar("sdk-node-jscpd", g, "npx", "jscpd", "src", cwd=d),
            _smoke("sdk-node-smoke", g, d, "node", "test_app/index.mjs", "smoke"),
            _e2e_smoke("node")]


def _sdk_python():
    """sdk/python/scripts/ci.sh and the workflow's types job."""
    g, d = "sdk-python", "sdk/python"
    pip = ".venv/bin/python -m pip install --quiet --disable-pip-version-check"
    return [_build("sdk-python-build", g, "python"),
            _bar("sdk-python-install", g, "bash", "-c",
                 f'{pip} --upgrade "pip>=25.1" && {pip} --group dev', cwd=d, stderr_ok=True),
            _bar("sdk-python-ruff", g, ".venv/bin/ruff", "check", ".", cwd=d),
            _bar("sdk-python-ruff-format", g, ".venv/bin/ruff", "format", "--check", ".", cwd=d),
            # pyright on PATH, or tests/test_typing.py skips itself.
            _bar("sdk-python-pytest", g, ".venv/bin/python", "-m", "pytest", "tests",
                 "examples/starter", "-q", cwd=d,
                 env={"PYTHONPATH": "src:tests", "PATH": f"{ROOT}/{d}/.venv/bin:$PATH"}),
            _bar("sdk-python-pyright", g, ".venv/bin/pyright", "--pythonpath", ".venv/bin/python",
                 "src/flezha", "tests", cwd=d, env={"PYTHONPATH": "src"}),
            _smoke("sdk-python-smoke", g, d, ".venv/bin/python", "test_app/runner.py", "smoke"),
            _e2e_smoke("python")]


# Gem install: every dependency the gemspec names, at its requirement.
RUBY_GEMS = ("ruby -e 'Gem::Specification.load(\"flezha.gemspec\").dependencies.each "
             "{ |d| puts \"#{d.name}\\t#{d.requirement}\" }' | while IFS=$'\\t' read -r n r; "
             "do gem install --conservative --no-document \"$n\" -v \"$r\" || exit 1; done")


def _sdk_ruby():
    """sdk/ruby/scripts/ci.sh and the workflow's format job, under the Ruby its
    CI runs (3.3/3.4): the gemspec pins minitest 5.x, which refuses Ruby 4, and
    the native library links the Ruby that builds it."""
    g, d = "sdk-ruby", "sdk/ruby"
    return [_bar("sdk-ruby-gems", g, "bash", "-c", RUBY_GEMS, cwd=d, env=RUBY, stderr_ok=True),
            _build("sdk-ruby-build", g, "ruby", env=RUBY),
            _bar("sdk-ruby-test", g, "rake", "test", cwd=d, env=RUBY),
            _bar("sdk-ruby-rubocop", g, "rubocop", cwd=d, env=RUBY),
            _smoke("sdk-ruby-smoke", g, d, "ruby", "test_app/runner.rb", "smoke", env=RUBY),
            _e2e_smoke("ruby", RUBY)]


# The threaded executor on a thread-safe PHP with Swoole, the build most
# distributions do not ship: the image is built once, then the floor is
# confirmed and the package's suite runs on it, its threaded tests included.
PHP_ZTS = r"""
  set -euo pipefail
  docker image inspect flezha-php-zts >/dev/null 2>&1 ||
    docker build -f docker/dev/Dockerfile.php --target zts -t flezha-php-zts .
  docker run --rm flezha-php-zts php -r '
    assert(PHP_ZTS, "not a thread-safe build");
    assert(class_exists("Swoole\\Thread"), "no Swoole\\Thread");
    assert(ini_get("ffi.enable") === "1", "ffi.enable must be true inside a thread");
    printf("php %s zts, swoole %s, %d cpus\n", PHP_VERSION, swoole_version(), swoole_cpu_num());
  '
  docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -v "$PWD:$PWD" -w "$PWD/sdk/php" \
    flezha-php-zts vendor/bin/phpunit"""


def _sdk_php():
    """sdk/php/scripts/ci.sh, the workflow's format job and its threaded arm."""
    g, d = "sdk-php", "sdk/php"
    lint = ("find src tests examples test_app -name '*.php' -not -path '*/vendor/*' -print0 | "
            "xargs -0 -n1 php -l >/dev/null")
    return [_build("sdk-php-build", g, "php"),
            _bar("sdk-php-lint", g, "bash", "-c", lint, cwd=d),
            _bar("sdk-php-phpunit", g, "vendor/bin/phpunit", cwd=d),
            _bar("sdk-php-cs-fixer", g, "vendor/bin/php-cs-fixer", "fix", "--dry-run", "--diff",
                 "--config", ".php-cs-fixer.dist.php", cwd=d),
            _bar("sdk-php-threaded", g, "bash", "-c", PHP_ZTS, stderr_ok=True, timeout=1800,
                 tools=["docker:install Docker"]),
            _smoke("sdk-php-smoke", g, d, "php", "test_app/runner.php", "smoke"),
            _e2e_smoke("php")]


def _sdk_elixir():
    """sdk/elixir/scripts/ci.sh (it has no workflow of its own)."""
    g, d = "sdk-elixir", "sdk/elixir"
    return [_bar("sdk-elixir-deps", g, "mix", "deps.get", cwd=d, stderr_ok=True),
            *_deps_compile("sdk-elixir", g, d, "dev", "test"),
            *_deps_compile("sdk-elixir-test-app", g, f"{d}/test_app", "dev"),
            _bar("sdk-elixir-compile", g, "mix", "compile", "--warnings-as-errors", cwd=d),
            _bar("sdk-elixir-test", g, "mix", "test", cwd=d),
            _bar("sdk-elixir-format", g, "mix", "format", "--check-formatted", cwd=d),
            _bar("sdk-elixir-test-app", g, "mix", "compile", "--warnings-as-errors",
                 cwd=f"{d}/test_app"),
            _smoke("sdk-elixir-smoke", g, d, "sh", "-c",
                   "cd test_app && FLEZHA_TRANSPORT=uds "
                   "exec mix run --no-halt --no-deps-check -- smoke")]


# Tag=budget in bytes: 300 MiB for the prod image, 450 MiB for runtime-dev (the
# prod budget plus Node.js and the built testnet bundle).
IMAGE_SIZE = r"""
  fail=0
  table="## Image sizes"$'\n\n'"| Tag | Size | Limit |"$'\n'"|-----|------|-------|"
  for spec in flezha/flezha:latest=314572800 flezha/flezha:latest-dev=471859200; do
    tag=${spec%=*} limit=${spec#*=}
    size=$(docker image inspect "$tag" --format '{{.Size}}') || exit 255
    table+=$'\n'"| \`$tag\` | $((size >> 20)) MiB | $((limit >> 20)) MiB |"
    [ "$size" -le "$limit" ] || { echo "$tag is $((size >> 20)) MiB, over its budget"; fail=1; }
  done
  echo "$table"
  [ -z "${GITHUB_STEP_SUMMARY:-}" ] || echo "$table" >>"$GITHUB_STEP_SUMMARY"
  exit $fail"""


GATES += (
    *_core(),
    *(row for runtime in RUNTIMES for row in _e2e(runtime)),
    {"id": "e2e", "group": "e2e", "groups": ",".join(f"@e2e-{rt}" for rt in E2E_RUNTIMES)},
    _mix_test("e2e-admin", "e2e", "--include", "system", "test/system/sdk_admin_e2e_test.exs"),
    _mix_test("e2e-multi-node", "e2e", "--only", "multi_node"),
    # The one scenario that places two runtimes in a run (rust and java).
    _mix_test("e2e-interop", "e2e-java", "--include", "system", "--include", "interop",
              "test/system/cross_sdk_placement_e2e_test.exs", env={"FLEZHA_E2E_RUNTIME": "java"}),
    _mix_test("e2e-plugin-package-release", "e2e-rust", "--include", "system", PACKAGE_RELEASE,
              env={"FLEZHA_E2E_RUNTIME": "rust"}, timeout=1800),
    *_plugins(), *_rust(), *_sdk_java(), *_sdk_node(), *_sdk_python(), *_sdk_ruby(),
    *_sdk_php(), *_sdk_elixir(),
    _bar("ledgers", "ledgers", "python3", "docs/known-bugs.py", "check"),
    # The committed prost output is what protobufs/regen.sh writes. A
    # generator that cannot run is not a stale file: only diff's 1 is a finding.
    _bar("proto-fresh", "contract", "bash", "-c",
         'd=$(mktemp -d) && trap \'rm -rf "$d"\' EXIT && '
         '{ protobufs/regen.sh --out "$d" || exit 255; } && '
         'diff -u flz_core/src/generated/runner_commands.rs "$d/runner_commands.rs"',
         finding="status:1", tools=["protoc:apt install protobuf-compiler"]),
    # The controller images' size budgets (docker/prod/README.md), measured on
    # images built before the row runs; the table also goes to a CI step summary.
    _bar("image-size", "docker", "bash", "-c", IMAGE_SIZE),
    # The prost generator is its own workspace, outside the whole-workspace check.
    _bar("prost-gen-fmt", "format", "bash", "-c",
         'n=$(scripts/manifest_pin.sh .github/toolchains.json rust_nightly) || exit 255; '
         'cargo +"$n" fmt --manifest-path protobufs/prost_gen/Cargo.toml -- --check',
         finding="says:Diff in "),
    # What an item is committed on: the specs, the naming gates, a warnings-as-
    # errors compile and the AI kit's freshness (a docs page edited without
    # `mix docs` leaves its kit copy stale).
    _bar("openspec-validate", "item", "openspec", "validate", "--specs", "--strict",
         stderr_ok=True),
    {"id": "item", "group": "item", "groups": "@naming,@deps,core-compile"},
    _mix_test("ai-kit", "item", "test/mix/tasks/flezha_ai_kit_test.exs",
              "test/flezha/mcp/knowledge_test.exs"),
    {"id": "clean", "group": "clean",
     "groups": "@core,@e2e,@plugins,@rust,@sdk-java,@sdk-node,@sdk-python,@sdk-ruby,@sdk-php,"
               "@sdk-elixir,@naming,@ceilings,@format,@history,@ledgers,@contract"},
)
