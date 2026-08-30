# controller-packaging Specification

## Purpose

The controller-packaging capability defines what the Flezha agent ships as. Three Docker tags compose the published surface:

| Tag                                | Dockerfile                          | Purpose                                                            |
|------------------------------------|-------------------------------------|--------------------------------------------------------------------|
| `alfetahe/flezha:latest`           | `docker/prod/Dockerfile.controller`      | Prod runtime — Mix release + graceful-restart entrypoint           |
| `alfetahe/flezha:latest-dev`       | `docker/dev/Dockerfile.runtime`     | Runtime + Node.js + pre-built testnet WS server                    |
| `alfetahe/flezha:latest-toolchain` | `docker/dev/Dockerfile.toolchain`   | Dev toolchain (Elixir+Rust+Node+protoc, source-mounted)            |

The capability defines the prod runtime image base, layer composition, supported architectures, exposed binaries, and the explicitly excluded surface (no Node.js, no testnet, no full Elixir installation, no extra arches). It also covers the runtime-dev image contract (prod runtime + Node + pre-built testnet, for downstream consumers), the toolchain image contract (host-arch only, full dev toolchain bundled, for hacking on Flezha itself), and the build-context discipline (Java SDK natives and fat jars are committed artefacts; CI never cross-compiles them).
## Requirements
### Requirement: Prod runtime image SHALL be based on `debian:bookworm-slim`

The final stage of `docker/prod/Dockerfile.controller` SHALL use `debian:bookworm-slim` (or a pinned digest of it) as its base image. The runtime image SHALL NOT contain a system-installed Elixir, Erlang, or build toolchain. Instead, the agent SHALL run from the Mix release's bundled ERTS (`include_erts: true` in `mix.exs`).

The runtime image SHALL apt-install only the following packages: `libssl3`, `libstdc++6`, `libtinfo6`, `locales`, `ca-certificates`, `tini`. After installation, the apt cache SHALL be cleaned (`rm -rf /var/lib/apt/lists/*`).

The image SHALL NOT contain Node.js, npm, TypeScript, the testnet TS source, the Java SDK, the Rust SDK source, the test compute engines, or any `flz_core_ffi` artefacts.

#### Scenario: Built prod image is below 300MB

- **GIVEN** a clean checkout of `main`
- **WHEN** `VERSION=test make docker-build` is run
- **THEN** `docker image inspect alfetahe/flezha:test --format '{{.Size}}'` returns a value less than `314572800` (300 MiB)

#### Scenario: Runtime image has no system Elixir

- **GIVEN** a built prod image `alfetahe/flezha:test`
- **WHEN** the runner runs `docker run --rm alfetahe/flezha:test which elixir`
- **THEN** the container exits non-zero with empty stdout (no system `elixir` binary on `$PATH`); the assembled release at `/flezha/bin/flzvm` SHALL still start, verified by `docker run --rm alfetahe/flezha:test bin/flzvm version` printing the release version

#### Scenario: Runtime image has no Node.js or testnet sources

- **GIVEN** a built prod image
- **WHEN** the runner runs `docker run --rm alfetahe/flezha:test sh -c 'which node; ls test/apps/testnet 2>&1; true'`
- **THEN** stdout contains no path for `node` and contains "No such file or directory" for the testnet path

### Requirement: Prod image SHALL be buildable for `linux/amd64` and `linux/arm64` only

`make docker-build-multiarch` SHALL produce manifests for exactly `linux/amd64` and `linux/arm64`. The Dockerfile SHALL NOT contain branches for any other Linux architecture. The protoc-download arch-detection block SHALL accept exactly `x86_64` and `aarch64` machine types and SHALL exit non-zero with `Unsupported architecture: <arch>` for anything else.

#### Scenario: Multi-arch buildx succeeds for amd64 + arm64

- **GIVEN** Docker buildx is available with the `docker-container` driver
- **WHEN** `VERSION=test docker buildx build --platform linux/amd64,linux/arm64 -f docker/prod/Dockerfile.controller --output=type=cacheonly .` is run
- **THEN** the build completes successfully for both platforms with no "unsupported architecture" errors

#### Scenario: Build on a non-supported arch fails fast

- **GIVEN** a hypothetical builder reporting `uname -m` as `riscv64`
- **WHEN** the protoc-install layer runs
- **THEN** the layer exits with status 1 and stderr `Unsupported architecture: riscv64`

### Requirement: Prod release SHALL exclude `:wx` and `:observer` extra applications

`mix.exs`'s `application/0` SHALL list `:wx` and `:observer` only when `Mix.env() != :prod`. The prod `extra_applications` list SHALL contain `[:logger, :runtime_tools, :inets, :crypto, :ssl, :telemetry]` and SHALL NOT contain `:wx` or `:observer`. Dev/test envs MAY still include them so `:observer.start()` works during local debugging.

#### Scenario: Prod release does not bundle wx or observer

- **GIVEN** a prod release built via `MIX_ENV=prod mix release`
- **WHEN** the runner inspects `_build/prod/rel/flezha/lib/`
- **THEN** there SHALL be no directories named `wx-*` or `observer-*` (those OTP apps SHALL NOT be packaged into the release)

#### Scenario: Dev release still ships observer

- **GIVEN** a dev-env compile via `mix compile`
- **WHEN** `iex -S mix` is started
- **THEN** evaluating `Code.ensure_loaded?(:observer)` returns `true`

### Requirement: Cargo dependency declarations SHALL match actual source usage

Every `[dependencies]` entry in any crate's `Cargo.toml` SHALL correspond to at least one `use <crate>::` or `extern crate <crate>` reference in that crate's `src/`. Dependencies declared but unused SHALL be removed. Specifically, `byteorder` SHALL be removed from `flz_nif/Cargo.toml`, `sdk/rust/Cargo.toml`, and `test/apps/rust_runner/Cargo.toml` (it remains in `flz_transport/Cargo.toml` where `src/service/payloader.rs` uses it).

#### Scenario: byteorder is removed from non-using crates

- **GIVEN** the cleaned-up Cargo.toml files
- **WHEN** `grep '^byteorder' flz_nif/Cargo.toml sdk/rust/Cargo.toml test/apps/rust_runner/Cargo.toml` is run
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
- **WHEN** the runner runs `docker run --rm <image> ls /flezha/test/apps/testnet 2>&1`
- **THEN** the output indicates the path does not exist (the entire `test/` tree SHALL be absent from the runtime image)

### Requirement: Toolchain Docker image SHALL bundle the full toolchain

`docker/dev/Dockerfile.toolchain` SHALL ship as `alfetahe/flezha:latest-toolchain`
and SHALL include Elixir+OTP, Rust, `protobuf-compiler`, Node.js, npm,
and a global TypeScript install. The container SHALL be runnable against
bind-mounted source via `docker compose -f docker/dev/docker-compose.yml
up` with no further per-host setup. This image is intended for hacking on
Flezha itself, not for downstream consumption.

The image SHALL NOT pre-build `test/apps/testnet` at image-build time
(the bind mount overlays `/flezha` at runtime, so any pre-built artefacts
under that path are wasted). The build is left to a one-time
`docker exec ... npm install && npm run build` after first start.

#### Scenario: Toolchain image ships the dev toolchain

- **GIVEN** a built toolchain image `alfetahe/flezha:latest-toolchain`
- **WHEN** the runner runs `docker run --rm alfetahe/flezha:latest-toolchain sh -c 'which elixir; which cargo; which node; which npm; which tsc; which protoc'`
- **THEN** every binary resolves to a path on `$PATH`

#### Scenario: Toolchain image is built and pushed under the toolchain tag

- **GIVEN** a clean checkout
- **WHEN** the runner runs `docker build -f docker/dev/Dockerfile.toolchain -t alfetahe/flezha:latest-toolchain .`
- **THEN** the build completes with exit 0 and the resulting tag is `alfetahe/flezha:latest-toolchain` (NOT `:latest`, which is the prod runtime, nor `:latest-dev`, which is the runtime + Node + testnet image)

### Requirement: Runtime-dev Docker image SHALL bundle Node.js and a pre-built testnet WS server

`docker/dev/Dockerfile.runtime` SHALL ship as `alfetahe/flezha:latest-dev`.
It SHALL `FROM alfetahe/flezha:latest` (the prod runtime), then layer:

1. Node.js + npm via apt.
2. The testnet TypeScript app at `test/apps/testnet/`, COPYed into `/flezha/testnet` and built at image-build time (`npm install --omit=dev && npm run build`) so consumers do NOT rebuild it on container start.
3. No additional ENTRYPOINT or CMD — both are inherited from `:latest`. The shared agent entrypoint launches the testnet WS server iff `FLEZHA_TESTNET_WS_PORT` is set in the environment.

The image's purpose is to give downstream consumers (e.g. `flezha_market`) a single image they can pull as `:latest-dev` to get the prod runtime plus the testnet driver, without having to maintain their own wrapper Dockerfile.

The image inherits the prod image's HEALTHCHECK (control-socket readiness, see the prod-image entrypoint requirement below).

#### Scenario: Runtime-dev image has Node.js and the pre-built testnet

- **GIVEN** a built runtime-dev image `alfetahe/flezha:latest-dev`
- **WHEN** the runner runs `docker run --rm alfetahe/flezha:latest-dev sh -c 'which node; ls /flezha/testnet/dist/index.js'`
- **THEN** `node` resolves to a path on `$PATH` and `/flezha/testnet/dist/index.js` exists

#### Scenario: Testnet auto-starts when FLEZHA_TESTNET_WS_PORT is set

- **GIVEN** the runtime-dev image
- **WHEN** the runner runs `docker run --rm -e FLEZHA_TESTNET_WS_PORT=4500 alfetahe/flezha:latest-dev` and waits for startup
- **THEN** the entrypoint launches `node /flezha/testnet/dist/index.js` listening on port 4500 in addition to the agent

#### Scenario: Runtime-dev image inherits prod size budget plus a testnet allowance

- **GIVEN** a built runtime-dev image
- **WHEN** the runner runs `docker image inspect alfetahe/flezha:latest-dev --format '{{.Size}}'`
- **THEN** the value SHALL be less than `471859200` (450 MiB) — prod's 300 MiB budget plus an allowance for Node.js and the built testnet bundle

### Requirement: Prod image entrypoint SHALL provide graceful-restart lifecycle

`docker/prod/Dockerfile.controller`'s runtime stage SHALL replace the direct `CMD ["bin/flzvm", "start"]` with `CMD ["/usr/local/bin/agent-entrypoint.sh"]`. Tini SHALL remain the ENTRYPOINT. The entrypoint script SHALL:

1. Background `bin/flzvm start`.
2. If `FLEZHA_TESTNET_WS_PORT` is set and `/flezha/testnet/dist/index.js` exists, also background the testnet WS server (no-op in `:latest`; consumed by `:latest-dev` via inheritance).
3. If `FLEZHA_SNAPSHOT_ENABLED` is unset or truthy AND a snapshot file exists at `${FLEZHA_SNAPSHOT_DIR:-/flezha/app_data/snapshots}/latest.flz`, optionally wait up to `${FLEZHA_RESTORE_WAIT_TIMEOUT_S:-60}` seconds for an SDK socket matching `${FLEZHA_RESTORE_WAIT_SOCKET_GLOB}` (empty default = no wait), then run `flz cluster restore` against the snapshot file with `--warn-if-older-than ${FLEZHA_SNAPSHOT_STALENESS_THRESHOLD_S:-86400}s --peer-timeout-ms ${FLEZHA_SNAPSHOT_PEER_TIMEOUT_MS:-5000}`.
4. Install a SIGTERM/SIGINT trap whose behaviour depends on `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` (default `true`):
   - **Enabled** (default): the trap SHALL `exec /flezha/bin/flz controller stop --timeout-ms "${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS:-60000}"`. The CLI's RPC drives `Flezha.Shutdown.Coordinator` inside the BEAM (quiesce → drain via HotSwap → finalise). `snapshot-on-stop.sh` is NOT invoked on this branch — HotSwap migration replaces snapshot-driven restart-survival for steady-state Operatives.
   - **Disabled** (`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` set to `false`/`0`/`no`/`off` case-insensitive): the trap SHALL run `/usr/local/bin/snapshot-on-stop.sh` then `kill -TERM` the agent — today's behaviour. This branch exists as the kill-switch fall-through and is removed once `auto-restore-on-boot` lands and `cluster-snapshot-restore` is deleted.
5. `wait` on the agent PID.

`/usr/local/bin/snapshot-on-stop.sh` SHALL remain a separate script invoked both by the (disabled-branch) SIGTERM trap and (in k8s with snapshot-mode) by `lifecycle.preStop.exec.command`. It atomically writes `${FLEZHA_SNAPSHOT_DIR}/latest.flz.tmp` then renames to `latest.flz`. It SHALL no-op (exit 0) when `FLEZHA_SNAPSHOT_ENABLED` is set to any of `false`, `0`, `no`, `off` (case-insensitive).

The image SHALL declare a HEALTHCHECK that tests for the existence of `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock`.

#### Scenario: Cold start with no snapshot file

- **GIVEN** an empty `/flezha/app_data/snapshots` volume
- **WHEN** the container starts
- **THEN** the entrypoint logs `graceful-restart: no snapshot file at <path>, cold start`, the agent starts normally, and `flz cluster restore` is NOT invoked

#### Scenario: Warm start restores from existing snapshot

- **GIVEN** `/flezha/app_data/snapshots/latest.flz` written by a prior graceful stop with snapshot mode
- **WHEN** the container starts
- **THEN** the entrypoint waits for SDK readiness (if `FLEZHA_RESTORE_WAIT_SOCKET_GLOB` is set) then logs `graceful-restart: restoring from <path>` and runs `flz cluster restore`; the CLI exits 0 and the agent serves restored operatives

#### Scenario: SIGTERM with graceful shutdown enabled drives the BEAM coordinator

- **GIVEN** a running container with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=true` (default), one Operative locally hosted, and a peer container reachable
- **WHEN** the orchestrator sends SIGTERM (e.g. `docker stop`)
- **THEN** the entrypoint trap execs `/flezha/bin/flz controller stop --timeout-ms <timeout>`; the CLI streams drain progress; the BEAM exits cleanly within `stop_grace_period`; the Operative is alive on the peer; `snapshot-on-stop.sh` is NOT invoked

#### Scenario: SIGTERM with graceful shutdown disabled writes a snapshot before exit

- **GIVEN** a running container with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` and `FLEZHA_SNAPSHOT_ENABLED=true`
- **WHEN** the orchestrator sends SIGTERM
- **THEN** the entrypoint logs `graceful-restart: SIGTERM received` and invokes `snapshot-on-stop.sh`, which atomically writes `latest.flz.tmp` then renames to `latest.flz`; the file is preserved across the container teardown — equivalent to today's behaviour

#### Scenario: FLEZHA_SNAPSHOT_ENABLED=false disables both restore and snapshot

- **GIVEN** `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false` and `FLEZHA_SNAPSHOT_ENABLED=false`
- **WHEN** the container starts and is later sent SIGTERM
- **THEN** the entrypoint logs `restore disabled` on start (skipping any restore) and `snapshot disabled` on stop (leaving any existing `latest.flz` untouched)

#### Scenario: Healthcheck reports unhealthy until the control socket appears

- **GIVEN** a container starting up
- **WHEN** the BEAM has not yet opened its control socket
- **THEN** the HEALTHCHECK reports unhealthy; once the socket exists at `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock`, the healthcheck reports healthy

### Requirement: docker-compose controller services SHALL declare an explicit stop signal and grace period

Every controller service in `docker/prod/docker-compose.yml`, `docker/dev/docker-compose.yml`, `docker/dev/docker-compose.healing.yml`, and `docker/dev/docker-compose.testing.yml` SHALL declare:

- `stop_signal: SIGTERM` — explicit; matches docker's default but documents intent.
- `stop_grace_period: 75s` — long enough for `Flezha.Shutdown.Coordinator`'s 60 s drain budget plus 15 s headroom for finalise and BEAM exit.

Operators changing `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` MUST update `stop_grace_period` to `(timeout_ms / 1000) + 15s` in lockstep. The pairing is documented in `docs/operations/graceful_shutdown.md`.

#### Scenario: `docker compose stop` lets the BEAM drain without SIGKILL

- **GIVEN** a `docker compose up` of `docker/prod/docker-compose.yml` with a controller hosting one Operative and a peer service running
- **WHEN** the operator runs `docker compose stop`
- **THEN** docker sends SIGTERM (per `stop_signal`); the controller drains within 60 s; the 75 s `stop_grace_period` elapses without docker escalating to SIGKILL; the controller exits cleanly; the Operative is running on the peer

### Requirement: K8s DaemonSet SHALL declare a preStop hook and a tuned termination grace period

`kubernetes/flezha-daemonset.yaml`'s controller container SHALL declare:

- `lifecycle.preStop.exec.command: ["/flezha/bin/flz", "controller", "stop", "--timeout-ms", "$(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)"]` — the **primary** path for k8s pod termination; runs synchronously before k8s issues SIGTERM.
- `terminationGracePeriodSeconds: 75` — `(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000) + 15` headroom; default value matches the configmap default of `60000`.

`kubernetes/flezha-configmap.yaml` SHALL declare `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` (default `60000`). The DaemonSet SHALL `envFrom` the configmap so the BEAM and the preStop command read the same value. Operators tuning the value MUST also bump `terminationGracePeriodSeconds` to maintain the 15 s headroom.

#### Scenario: K8s pod deletion runs preStop, then SIGTERM, exits within grace

- **GIVEN** a controller pod hosting one Operative with a peer pod available
- **WHEN** the operator runs `kubectl delete pod <pod>`
- **THEN** k8s runs `flz controller stop --timeout-ms 60000` via preStop (which drives `Flezha.Shutdown.Coordinator` to drain the Operative to the peer), then sends SIGTERM (the SignalHandler observes it as a second-signal during shutdown and ignores), then waits up to 75 s; the pod exits cleanly with the Operative running on the peer

#### Scenario: Operator-tuned timeout takes effect via configmap

- **GIVEN** an operator updates `kubernetes/flezha-configmap.yaml` to set `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=120000` AND updates the DaemonSet's `terminationGracePeriodSeconds` to `135`
- **WHEN** the DaemonSet is reapplied
- **THEN** preStop resolves to `flz controller stop --timeout-ms 120000` and pods get a 135 s grace window — the BEAM's coordinator and k8s's wall-clock budget agree

### Requirement: `.dockerignore` SHALL exclude all non-source artefacts from the build context

The repo's `.dockerignore` SHALL exclude at minimum: `**/target`, `_build`, `deps`, `priv/native/*.so`, `**/node_modules`, `.git`, `.github`, `.vscode`, `.elixir_ls`, `cover`, `*.dump`, `erl_crash.dump`, `docs`, `test/apps/testnet/dist`, `test/apps/testnet/node_modules`, `sdk/java/flezha-sdk-native/src/main/resources/native`, `sdk/java/flezha-sdk/dist`, `test/apps/java_runner/dist`, and (for the prod image) the entire `test/` tree.

The three Java artefact paths are no longer tracked in git, so they are present
only on a machine that has built them; excluding them keeps a developer's build
context the same size as a clean checkout's.

#### Scenario: Prod image's COPY does not pull in build artefacts

- **GIVEN** a working tree with populated `target/`, `_build/`, `deps/`, and `test/apps/testnet/node_modules/`
- **WHEN** `make docker-build` is run with BuildKit enabled
- **THEN** the build context size reported by Docker SHALL be less than 50MB (well under what including `target/` alone would produce — typically 1GB+)

#### Scenario: A built Java tree does not enlarge the build context

- **GIVEN** a working tree where the five native binaries and both fat jars have been built and are present on disk but untracked
- **WHEN** `make docker-build` is run with BuildKit enabled
- **THEN** the build context size SHALL still be less than 50MB

### Requirement: `elixir.yml` SHALL be Elixir+Rust only

`.github/workflows/elixir.yml` SHALL NOT install Java or invoke Maven. It SHALL NOT run `cargo build --workspace` as a separate pre-step before `mix test`. Its path filters SHALL NOT include `test/apps/java_runner/**` (those changes belong to `java-sdk.yml`). The workflow SHALL run only `mix deps.get`, `mix compile --warnings-as-errors`, and `mix test` (which uses the default-excluded tag set, `:system` among it, and therefore spawns no runner; the SDK conformance gate is an untagged module and runs inside it).

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
- **THEN** every step's `otp-version` and `elixir-version` SHALL match the pair used in `docker/prod/Dockerfile.controller`

### Requirement: A CI gate SHALL prevent prod-image size regressions

A workflow (either `.github/workflows/elixir.yml` or a dedicated `docker.yml`) SHALL build the prod image on every PR that touches `docker/prod/**`, `mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, or `flz_nif/**`, and SHALL fail the PR if the image size exceeds a configured threshold. The initial threshold SHALL be 300 MB. The workflow SHALL emit the measured size to the GitHub step summary regardless of pass/fail.

#### Scenario: PR that bloats the image fails the gate

- **GIVEN** a PR that adds a 200MB binary asset under `priv/`
- **WHEN** the docker-size CI step runs
- **THEN** the step exits non-zero with stderr like `prod image size 480MB exceeds threshold 300MB`

#### Scenario: PR that doesn't touch packaging skips the gate

- **GIVEN** a PR whose diff is exclusively under `lib/flezha/operative/**`
- **WHEN** CI runs
- **THEN** the docker-size gate is not triggered (the workflow's path filter excludes the change)

### Requirement: Dev harness containers SHALL run unprivileged

`docker/dev/Dockerfile.toolchain` SHALL create every directory that a
harness compose file mounts a named volume onto — the in-repo build,
deps and cargo-target paths, the container-local `MIX_BUILD_PATH` /
`MIX_DEPS_PATH` paths under `/home/flezha`, and the socket-data mount
path — owned by the `flezha` user at image-build time, so Docker
initialises a freshly-provisioned named volume with that ownership.

Given that image, a harness compose service that boots a controller or a
runner against bind-mounted source SHALL run as the unprivileged uid by
default and SHALL NOT perform an unconditional recursive `chown` at boot. A service SHALL be able to opt back into running as root for
hosts where the mount ownership cannot be satisfied by the image (for
example userns-remap); on that path it SHALL recurse only into
directories whose top-level owner is wrong.

A harness service that starts as root and drops privileges SHALL drop
them with a mechanism that replaces the process image, so the workload
retains PID 1 and receives the container stop signal directly. It SHALL
NOT wrap the workload in `su`, which terminates its child shortly after
SIGTERM and so truncates the controller's graceful drain.

When a mount path is not writable by the effective uid, the service
SHALL fail at boot with a message naming the remedy, rather than
surfacing the failure as a permission error inside a downstream build
tool or as a handshake timeout.

#### Scenario: Fresh named volume is writable without root

- **GIVEN** a toolchain image built from `docker/dev/Dockerfile.toolchain`
- **WHEN** a freshly-provisioned named volume is mounted at a harness build or deps path and the container runs as the unprivileged uid
- **THEN** that path is writable, and no recursive `chown` runs at boot

#### Scenario: The workload owns PID 1 and the stop signal

- **GIVEN** a harness controller service, started either unprivileged or as root
- **WHEN** the container is stopped within its declared stop grace period
- **THEN** the controller process is PID 1 and receives SIGTERM directly
- **AND** no privilege-drop wrapper terminates it before it exits on its own terms

Whether the application's drain then completes is the shutdown capability's
concern, not packaging's; packaging guarantees only that the signal arrives
and that nothing between the container and the workload cuts the drain short.

#### Scenario: Root fallback stays available and bounded

- **GIVEN** a host on which the image-supplied mount ownership does not apply
- **WHEN** the harness service is configured to start as root
- **THEN** it chowns only the mount paths whose top-level owner is wrong, drops privileges without `su`, and the workload still owns PID 1

#### Scenario: Unwritable mount fails with an actionable message

- **GIVEN** a harness service running as the unprivileged uid against an image that predates the mountpoint ownership contract
- **WHEN** the service starts against an empty named volume
- **THEN** it exits non-zero with a message naming both the image rebuild and the run-as-root override

### Requirement: Harness dependency fetch SHALL be guarded by the lock file

A harness compose service that fetches Elixir dependencies at boot SHALL
run the fetch only when the resolved `mix.lock` differs from a hash
stamped in the deps volume, and SHALL write the stamp only after a
successful fetch. An unchanged lock file SHALL therefore require no
package-registry access, so a harness controller can boot with no
network once its deps volume is populated.

#### Scenario: Unchanged lock file skips the fetch

- **GIVEN** a harness controller whose deps volume was populated by a previous boot
- **WHEN** the controller restarts with an unchanged `mix.lock`
- **THEN** no dependency resolution runs and the boot log shows no registry access

#### Scenario: Changed lock file re-fetches and re-stamps

- **GIVEN** a populated deps volume whose stamp does not match the current `mix.lock`
- **WHEN** the controller boots
- **THEN** dependencies are fetched and the stamp is updated only after the fetch succeeds

#### Scenario: Interrupted fetch retries on the next boot

- **GIVEN** a dependency fetch that did not complete
- **WHEN** the controller boots again
- **THEN** no stamp was written, and the fetch runs again

