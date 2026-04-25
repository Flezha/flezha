# agent-packaging Specification

## Purpose

The agent-packaging capability defines what the Flezha agent ships as: the prod runtime image base, layer composition, supported architectures, exposed binaries, and the explicitly excluded surface (no Node.js, no testnet, no full Elixir installation, no extra arches). It also covers the dev image contract (host-arch only, full dev toolchain bundled) and the build-context discipline (Java SDK natives and fat jars are committed artefacts; CI never cross-compiles them).

## Requirements


### Requirement: Prod runtime image SHALL be based on `debian:bookworm-slim`

The final stage of `docker/prod/Dockerfile.agent` SHALL use `debian:bookworm-slim` (or a pinned digest of it) as its base image. The runtime image SHALL NOT contain a system-installed Elixir, Erlang, or build toolchain. Instead, the agent SHALL run from the Mix release's bundled ERTS (`include_erts: true` in `mix.exs`).

The runtime image SHALL apt-install only the following packages: `libssl3`, `libstdc++6`, `libtinfo6`, `locales`, `ca-certificates`, `tini`. After installation, the apt cache SHALL be cleaned (`rm -rf /var/lib/apt/lists/*`).

The image SHALL NOT contain Node.js, npm, TypeScript, the testnet TS source, the Java SDK, the Rust SDK source, the test compute engines, or any `flz_jni` artefacts.

#### Scenario: Built prod image is below 300MB

- **GIVEN** a clean checkout of `main`
- **WHEN** `VERSION=test make docker-build` is run
- **THEN** `docker image inspect alfetahe/flezha:test --format '{{.Size}}'` returns a value less than `314572800` (300 MiB)

#### Scenario: Runtime image has no system Elixir

- **GIVEN** a built prod image `alfetahe/flezha:test`
- **WHEN** the operator runs `docker run --rm alfetahe/flezha:test which elixir`
- **THEN** the container exits non-zero with empty stdout (no system `elixir` binary on `$PATH`); the assembled release at `/flezha/bin/flzvm` SHALL still start, verified by `docker run --rm alfetahe/flezha:test bin/flzvm version` printing the release version

#### Scenario: Runtime image has no Node.js or testnet sources

- **GIVEN** a built prod image
- **WHEN** the operator runs `docker run --rm alfetahe/flezha:test sh -c 'which node; ls test/apps/testnet 2>&1; true'`
- **THEN** stdout contains no path for `node` and contains "No such file or directory" for the testnet path

### Requirement: Prod image SHALL be buildable for `linux/amd64` and `linux/arm64` only

`make docker-build-multiarch` SHALL produce manifests for exactly `linux/amd64` and `linux/arm64`. The Dockerfile SHALL NOT contain branches for any other Linux architecture. The protoc-download arch-detection block SHALL accept exactly `x86_64` and `aarch64` machine types and SHALL exit non-zero with `Unsupported architecture: <arch>` for anything else.

#### Scenario: Multi-arch buildx succeeds for amd64 + arm64

- **GIVEN** Docker buildx is available with the `docker-container` driver
- **WHEN** `VERSION=test docker buildx build --platform linux/amd64,linux/arm64 -f docker/prod/Dockerfile.agent --output=type=cacheonly .` is run
- **THEN** the build completes successfully for both platforms with no "unsupported architecture" errors

#### Scenario: Build on a non-supported arch fails fast

- **GIVEN** a hypothetical builder reporting `uname -m` as `riscv64`
- **WHEN** the protoc-install layer runs
- **THEN** the layer exits with status 1 and stderr `Unsupported architecture: riscv64`

### Requirement: Prod release SHALL exclude `:wx` and `:observer` extra applications

`mix.exs`'s `application/0` SHALL list `:wx` and `:observer` only when `Mix.env() != :prod`. The prod `extra_applications` list SHALL contain `[:logger, :runtime_tools, :inets, :crypto, :ssl, :telemetry]` and SHALL NOT contain `:wx` or `:observer`. Dev/test envs MAY still include them so `:observer.start()` works during local debugging.

#### Scenario: Prod release does not bundle wx or observer

- **GIVEN** a prod release built via `MIX_ENV=prod mix release`
- **WHEN** the operator inspects `_build/prod/rel/flezha/lib/`
- **THEN** there SHALL be no directories named `wx-*` or `observer-*` (those OTP apps SHALL NOT be packaged into the release)

#### Scenario: Dev release still ships observer

- **GIVEN** a dev-env compile via `mix compile`
- **WHEN** `iex -S mix` is started
- **THEN** evaluating `Code.ensure_loaded?(:observer)` returns `true`

### Requirement: Cargo dependency declarations SHALL match actual source usage

Every `[dependencies]` entry in any crate's `Cargo.toml` SHALL correspond to at least one `use <crate>::` or `extern crate <crate>` reference in that crate's `src/`. Dependencies declared but unused SHALL be removed. Specifically, `byteorder` SHALL be removed from `flz_nif/Cargo.toml`, `sdk/rust/Cargo.toml`, and `test/apps/rust_compute_engine/Cargo.toml` (it remains in `flz_transport/Cargo.toml` where `src/service/payloader.rs` uses it).

#### Scenario: byteorder is removed from non-using crates

- **GIVEN** the cleaned-up Cargo.toml files
- **WHEN** `grep '^byteorder' flz_nif/Cargo.toml sdk/rust/Cargo.toml test/apps/rust_compute_engine/Cargo.toml` is run
- **THEN** stdout is empty

#### Scenario: Workspace still builds after cleanup

- **GIVEN** the cleaned-up Cargo.toml files
- **WHEN** `cargo build --workspace` is run
- **THEN** the build completes with exit 0 and no errors

### Requirement: `Flezha.Testnet` and the testnet TS app SHALL be test-only

The Elixir module `Flezha.Testnet` SHALL be moved out of `lib/` into a path compiled only in the test environment (`test/support/testnet.ex`). `mix.exs`'s `elixirc_paths(:test)` SHALL be updated to include `test/support`. The runtime gate `:testnet_enabled` SHALL remain (defaulting `false` in prod).

The TypeScript source under `test/apps/testnet/` SHALL NOT be copied into the prod Docker image (enforced by `.dockerignore`).

#### Scenario: Prod compile does not include Flezha.Testnet

- **GIVEN** `MIX_ENV=prod mix compile`
- **WHEN** the prod beam files are inspected
- **THEN** there SHALL be no `Elixir.Flezha.Testnet.beam` under `_build/prod/lib/flezha/ebin/`

#### Scenario: Test compile still includes Flezha.Testnet

- **GIVEN** `MIX_ENV=test mix compile`
- **WHEN** the test beam files are inspected
- **THEN** `_build/test/lib/flezha/ebin/Elixir.Flezha.Testnet.beam` SHALL exist

#### Scenario: Prod image excludes testnet sources

- **GIVEN** a built prod Docker image
- **WHEN** the operator runs `docker run --rm <image> ls /flezha/test/apps/testnet 2>&1`
- **THEN** the output indicates the path does not exist (the entire `test/` tree SHALL be absent from the runtime image)

### Requirement: Dev Docker image SHALL bundle the full toolchain

`docker/dev/Dockerfile.agent` SHALL ship as `alfetahe/flezha:latest-dev`
and SHALL include Elixir+OTP, Rust, `protobuf-compiler`, Node.js, npm,
and a global TypeScript install. The container SHALL be runnable against
bind-mounted source via `docker compose -f docker/dev/docker-compose.yml
up` with no further per-host setup.

The image SHALL NOT pre-build `test/apps/testnet` at image-build time
(the bind mount overlays `/flezha` at runtime, so any pre-built artefacts
under that path are wasted). The build is left to a one-time
`docker exec ... npm install && npm run build` after first start.

#### Scenario: Dev image ships the dev toolchain

- **GIVEN** a built dev image `alfetahe/flezha:latest-dev`
- **WHEN** the operator runs `docker run --rm alfetahe/flezha:latest-dev sh -c 'which elixir; which cargo; which node; which npm; which tsc; which protoc'`
- **THEN** every binary resolves to a path on `$PATH`

#### Scenario: Dev image is built and pushed under the dev tag

- **GIVEN** a clean checkout
- **WHEN** the operator runs `docker build -f docker/dev/Dockerfile.agent -t alfetahe/flezha:latest-dev .`
- **THEN** the build completes with exit 0 and the resulting tag is `alfetahe/flezha:latest-dev` (NOT `:latest`, which is reserved for the prod image)

### Requirement: `.dockerignore` SHALL exclude all non-source artefacts from the build context

The repo's `.dockerignore` SHALL exclude at minimum: `**/target`, `_build`, `deps`, `priv/native/*.so`, `**/node_modules`, `.git`, `.github`, `.vscode`, `.elixir_ls`, `cover`, `*.dump`, `erl_crash.dump`, `docs`, `test/apps/testnet/dist`, `test/apps/testnet/node_modules`, and (for the prod image) the entire `test/` tree.

#### Scenario: Prod image's COPY does not pull in build artefacts

- **GIVEN** a working tree with populated `target/`, `_build/`, `deps/`, and `test/apps/testnet/node_modules/`
- **WHEN** `make docker-build` is run with BuildKit enabled
- **THEN** the build context size reported by Docker SHALL be less than 50MB (well under what including `target/` alone would produce — typically 1GB+)

### Requirement: Java native binaries and fat jars SHALL be committed to git, not built by CI

The five native binaries `libflz_transport.{so,dylib,dll}` for `linux-x64`, `linux-arm64`, `macos-x64`, `macos-arm64`, `windows-x64` SHALL be committed under `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/`. The Java SDK fat jar SHALL be committed at `sdk/java/flezha-sdk/dist/flezha-sdk.jar`. The test compute-engine fat jar SHALL be committed at `test/apps/java_compute_engine/dist/java_compute_engine.jar`.

CI SHALL NOT cross-compile any of the five native binaries. CI SHALL NOT package either fat jar. Operators SHALL refresh these artefacts locally with `scripts/build-native.sh --targets all` and `scripts/build-java-jars.sh`, then `git add` and commit them.

The `flezha-sdk-native` Maven build SHALL provide a `native-verify` profile that asserts all five native binaries exist on disk without invoking any build step. CI SHALL use `-P native-verify` (never `-P native-build`).

#### Scenario: java-sdk.yml does not invoke cargo-zigbuild

- **GIVEN** the merged `.github/workflows/java-sdk.yml`
- **WHEN** the file is grepped for `cargo-zigbuild`, `cargo zigbuild`, or `ziglang`
- **THEN** there SHALL be no matches; the workflow SHALL NOT install or invoke Zig

#### Scenario: java-sdk.yml uses the native-verify profile

- **GIVEN** the `java-unit-tests` job in `.github/workflows/java-sdk.yml`
- **WHEN** the Maven verify step is inspected
- **THEN** the command SHALL include `-P coverage,native-verify` (and SHALL NOT include `-P native-build`)

#### Scenario: native-verify fails when a binary is missing

- **GIVEN** a working tree where `sdk/java/flezha-sdk-native/src/main/resources/native/macos-arm64/libflz_transport.dylib` has been deleted
- **WHEN** `mvn -f sdk/java/pom.xml -P native-verify verify` is run
- **THEN** the build SHALL fail in the `prepare-package` phase with a message naming the missing file

### Requirement: `mix test.java_sdk` alias SHALL not invoke Maven

The `test.java_sdk` alias in `mix.exs` SHALL be exactly `["test --only java_sdk"]`. It SHALL NOT shell out to `scripts/build-native.sh`, `mvn`, or any other build tool. The Java compute-engine jar consumed by tests under `:java_sdk` SHALL be the committed jar at `test/apps/java_compute_engine/dist/java_compute_engine.jar`.

The default `ExUnit.start/1` invocation in `test/test_helper.exs` SHALL exclude `:java_sdk` so `mix test` does not trigger the integration matrix; only `mix test.java_sdk` (or an explicit `mix test --only java_sdk`) SHALL run those cases.

#### Scenario: mix test does not run java_sdk cases

- **GIVEN** a clean checkout
- **WHEN** `mix test` is run
- **THEN** no test tagged `:java_sdk` SHALL execute (the run SHALL succeed without requiring the committed test compute-engine jar to exist)

#### Scenario: mix test.java_sdk runs only the matrix

- **GIVEN** the committed test compute-engine jar at `test/apps/java_compute_engine/dist/java_compute_engine.jar`
- **WHEN** `mix test.java_sdk` is run
- **THEN** only `:java_sdk`-tagged tests SHALL execute and the alias SHALL NOT shell out to `mvn` or `scripts/build-native.sh`

### Requirement: `elixir.yml` SHALL be Elixir+Rust only

`.github/workflows/elixir.yml` SHALL NOT install Java or invoke Maven. It SHALL NOT run `cargo build --workspace` as a separate pre-step before `mix test`. Its path filters SHALL NOT include `test/apps/java_compute_engine/**` (those changes belong to `java-sdk.yml`). The workflow SHALL run only `mix deps.get`, `mix compile --warnings-as-errors`, and `mix test` (which uses the default-excluded `[:system, :java_sdk]` tag set).

#### Scenario: elixir.yml does not set up Java

- **GIVEN** the merged `.github/workflows/elixir.yml`
- **WHEN** the file is grepped for `setup-java` or `actions/setup-java`
- **THEN** there SHALL be no matches

#### Scenario: elixir.yml does not run cargo workspace build

- **GIVEN** the merged `.github/workflows/elixir.yml`
- **WHEN** the file is grepped for `cargo build --workspace` or `mvn`
- **THEN** there SHALL be no matches

### Requirement: CI workflows SHALL use a single, consistent OTP/Elixir version pair

Every workflow under `.github/workflows/` that sets up Erlang/Elixir SHALL use the same `(otp, elixir)` pair. The pair SHALL match the version used by the agent's prod Docker build. If the prod build pins OTP 27 + Elixir 1.17, every CI workflow SHALL pin OTP 27 + Elixir 1.17 — no workflow SHALL silently use a different pair.

#### Scenario: All workflows pin matching versions

- **GIVEN** the merged set of CI workflows
- **WHEN** every `setup-beam` step is inspected
- **THEN** every step's `otp-version` and `elixir-version` SHALL match the pair used in `docker/prod/Dockerfile.agent`

### Requirement: A CI gate SHALL prevent prod-image size regressions

A workflow (either `.github/workflows/elixir.yml` or a dedicated `docker.yml`) SHALL build the prod image on every PR that touches `docker/prod/**`, `mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, or `flz_nif/**`, and SHALL fail the PR if the image size exceeds a configured threshold. The initial threshold SHALL be 300 MB. The workflow SHALL emit the measured size to the GitHub step summary regardless of pass/fail.

#### Scenario: PR that bloats the image fails the gate

- **GIVEN** a PR that adds a 200MB binary asset under `priv/`
- **WHEN** the docker-size CI step runs
- **THEN** the step exits non-zero with stderr like `prod image size 480MB exceeds threshold 300MB`

#### Scenario: PR that doesn't touch packaging skips the gate

- **GIVEN** a PR whose diff is exclusively under `lib/flezha/processor/**`
- **WHEN** CI runs
- **THEN** the docker-size gate is not triggered (the workflow's path filter excludes the change)
