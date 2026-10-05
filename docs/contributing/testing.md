# Running the tests

The tests need a built checkout: [Setting up a checkout](setting_up.md) gets
you there.

## The controller

```sh
$ mix test
```

That runs the controller's unit and integration tests and the static gate of
the cross-SDK suite, and starts no runner. Slower slices of the suite, called
tiers, are left out of a plain run and are included by their tag.

## One SDK at a time

One end-to-end suite serves every SDK. `FLEZHA_E2E_RUNTIME` picks the SDK a run
uses, `rust` by default:

```sh
$ FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke   # the two-minute subset
$ FLEZHA_E2E_RUNTIME=java mix test --include system      # the whole suite
```

Each SDK's test app, `test/apps/<sdk>_runner`, carries a `conformance.json`
that names how to build it, how to launch it and which capabilities it does not
carry. A capability the manifest leaves out is skipped by name, and everything
else must pass. When the test app is not built yet, the run stops and names the
command that builds it.

[`docs/sdk/conformance.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/conformance.md)
has the whole contract: every tier and what runs it, what the Ruby and PHP
runs need on your machine, and what a new SDK owes.

## The Rust crates

```sh
$ cargo test -p flz_core -p flz_transport -p flz_nif -p flz
$ cargo test -p flezha_runner --features bench-internals,testing --lib --tests
```

Without the `testing` feature, cargo skips some of the runner's tests without
saying so.

## The Java SDK

```sh
$ mvn -f sdk/java/pom.xml -P coverage,native-verify verify
```

It needs the native library from
[Setting up a checkout](setting_up.md#the-native-library-for-java). The
`native-verify` profile checks that it is there and names the command that
builds it when it is not.

On a machine where `/tmp` is mounted `noexec`, set `FLEZHA_NATIVE_LIB_DIR` to
a writable directory the native library can be loaded from.

## CI on your machine

`scripts/ci-local.sh` runs the same commands, in the same order, as the
GitHub Actions workflows it mirrors, so a failure shows up locally instead of
after a remote run:

```sh
$ scripts/ci-local.sh               # every section
$ scripts/ci-local.sh elixir        # mix compile, the Rust tests, mix test
$ scripts/ci-local.sh java          # the Java smoke tier
```

What each section runs is listed in the script's own header. It builds what it
needs, and uses its own `target-ci-local/` directory so leftovers in `target/`
cannot get in the way. It ends with `ALL CI CHECKS PASSED`, or exits non-zero
on the first section that fails.

## The Docker stacks

Two smoke tests run against the development containers, so they need Docker
and are not part of `mix test`:

```sh
$ mix test.connection_healing
```

The first restarts the controller and the runner in turn and checks that they
find each other again. The second restarts the runner and checks that the
plugins enabled on its relay are still there.

The stacks start through `scripts/harness-boot.sh`, which checks the mounted
directories, fetches the Elixir dependencies when `mix.lock` changed, and waits
for the controller's socket. On a host where the image's file ownership does
not apply, such as one with user namespace remapping, start them with
`FLEZHA_CONTAINER_USER=0:0`: the script then fixes the ownership it needs and
drops root before it starts the service.

## Benchmarks

```sh
$ scripts/bench.sh            # Rust over UDS, the defaults
$ scripts/bench.sh --help
```

[`docs/operations/benchmarking.md`](https://github.com/flezha/flezha/blob/main/docs/operations/benchmarking.md)
covers every flag, the recorded floors, the CI gates and what to do when one
fails.

## Writing a test

A test waits on a condition, never on the clock.
[How a test waits](code_standards.md#how-a-test-waits) has the rules and the
one helper to use.
