# controller-packaging Specification

## Purpose

What the controller ships as and how it is deployed. The production image is a Debian slim,
multi-architecture Mix release with a graceful-restart entrypoint, sized VM arguments, the
applications its dependencies need, pinned downloads, no cookie file, and a size gate, beside dev
and toolchain images and unprivileged harness containers. The Kubernetes reference manifests declare
resource limits, probes, labels, portable storage, the controller's plugins and a restricted
security profile, limit who reaches the distribution ports, and are held by a test that reads their
volumes and variables.

## Requirements

### Requirement: Prod runtime image SHALL be based on `debian:bookworm-slim`

The final stage of `deploy/docker/prod/Dockerfile.controller` SHALL use `debian:bookworm-slim` (or a pinned digest of it) as its base image. The runtime image SHALL NOT contain a system-installed Elixir, Erlang, or build toolchain. Instead, the agent SHALL run from the Mix release's bundled ERTS (`include_erts: true` in `mix.exs`).

The runtime image SHALL apt-install only the following packages: `libssl3`, `libstdc++6`, `libtinfo6`, `locales`, `ca-certificates`, `tini`. After installation, the apt cache SHALL be cleaned (`rm -rf /var/lib/apt/lists/*`).

The image SHALL NOT contain Node.js, npm, TypeScript, the testnet TS source, the Java SDK, the Rust SDK source, the test compute engines, or any `flz_core_ffi` artefacts.

#### Scenario: Built prod image is below 300MB

- **GIVEN** a clean checkout of `main`
- **WHEN** `VERSION=test make docker-build` is run
- **THEN** `docker image inspect flezha/flezha:test --format '{{.Size}}'` returns a value less than `314572800` (300 MiB)

#### Scenario: Runtime image has no system Elixir

- **GIVEN** a built prod image `flezha/flezha:test`
- **WHEN** the runner runs `docker run --rm flezha/flezha:test which elixir`
- **THEN** the container exits non-zero with empty stdout (no system `elixir` binary on `$PATH`); the assembled release at `/flezha/bin/flzvm` SHALL still start, verified by `docker run --rm flezha/flezha:test bin/flzvm version` printing the release version

#### Scenario: Runtime image has no Node.js or testnet sources

- **GIVEN** a built prod image
- **WHEN** the runner runs `docker run --rm flezha/flezha:test sh -c 'which node; ls test/apps/testnet 2>&1; true'`
- **THEN** stdout contains no path for `node` and contains "No such file or directory" for the testnet path

### Requirement: Prod image SHALL be buildable for `linux/amd64` and `linux/arm64` only

The controller image SHALL be published for exactly `linux/amd64` and `linux/arm64`,
one architecture per publishing host. `make docker-publish` runs
`scripts/publish-image.sh`, which builds the HOST architecture natively — no QEMU,
no `buildx --platform` — pushes the per-arch tag `flezha/flezha:<family>-<arch>`,
and then rebuilds the `<family>` manifest list from whichever of the `amd64` and
`arm64` per-arch tags exist on Docker Hub, so publishing one architecture never
overwrites the other. The script SHALL refuse any other `uname -m` before it builds.
`.github/workflows/docker.yml` builds the images on pull requests for the size gate
and SHALL NOT push, keeping the script the single publisher of the manifest list.

The Dockerfile SHALL NOT contain branches for any other Linux architecture, and its Rust stage SHALL NOT install `protoc`: the prost output is committed.

#### Scenario: A publish assembles the manifest from the per-arch tags

- **GIVEN** `flezha/flezha:latest-amd64` is already published and the user is on an arm64 host
- **WHEN** `make docker-publish` runs
- **THEN** `scripts/publish-image.sh` builds and pushes `flezha/flezha:latest-arm64` natively, then rebuilds the `flezha/flezha:latest` manifest list from exactly the `amd64` and `arm64` per-arch tags that exist, leaving the amd64 variant intact

#### Scenario: Publishing from an unsupported host arch fails before the build

- **GIVEN** a publishing host whose `uname -m` reports `riscv64`
- **WHEN** `scripts/publish-image.sh --prod` is run
- **THEN** it exits 1 with `ERROR: unsupported architecture riscv64` on stderr and no image is built

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

### Requirement: `Flezha.Testnet` and the testnet TS app SHALL be test-only

The Elixir module `Flezha.Testnet` SHALL live in a path `mix.exs` compiles only outside `:prod` — `test/helpers/testnet.ex`, reached by the `elixirc_paths(:test)` entry `test/helpers` — and SHALL NOT be reachable from `elixirc_paths(:prod)`. The runtime gate `:testnet_enabled` SHALL remain (defaulting `false` in prod).

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

`deploy/docker/dev/Dockerfile.toolchain` SHALL ship as `flezha/flezha:latest-toolchain`
and SHALL include Elixir+OTP, Rust, `protobuf-compiler`, Node.js, npm,
and a global TypeScript install. The container SHALL be runnable against
bind-mounted source via `docker compose -f deploy/docker/dev/docker-compose.yml
up` with no further per-host setup. This image is intended for hacking on
Flezha itself, not for downstream consumption.

The image SHALL NOT pre-build `test/apps/testnet` at image-build time
(the bind mount overlays `/flezha` at runtime, so any pre-built artefacts
under that path are wasted). The build is left to a one-time
`docker exec ... npm install && npm run build` after first start.

#### Scenario: Toolchain image ships the dev toolchain

- **GIVEN** a built toolchain image `flezha/flezha:latest-toolchain`
- **WHEN** the runner runs `docker run --rm flezha/flezha:latest-toolchain sh -c 'which elixir; which cargo; which node; which npm; which tsc; which protoc'`
- **THEN** every binary resolves to a path on `$PATH`

#### Scenario: Toolchain image is built and pushed under the toolchain tag

- **GIVEN** a clean checkout
- **WHEN** the runner runs `docker build -f deploy/docker/dev/Dockerfile.toolchain -t flezha/flezha:latest-toolchain .`
- **THEN** the build completes with exit 0 and the resulting tag is `flezha/flezha:latest-toolchain` (NOT `:latest`, which is the prod runtime, nor `:latest-dev`, which is the runtime + Node + testnet image)

### Requirement: Runtime-dev Docker image SHALL bundle Node.js and a pre-built testnet WS server

`deploy/docker/dev/Dockerfile.runtime` SHALL ship as `flezha/flezha:latest-dev`.
It SHALL `FROM flezha/flezha:latest` (the prod runtime), then layer:

1. Node.js + npm via apt.
2. The testnet TypeScript app at `test/apps/testnet/`, COPYed into `/flezha/testnet` and built at image-build time (`npm ci --omit=dev && npm run build`) so consumers do NOT rebuild it on container start.
3. No additional ENTRYPOINT or CMD — both are inherited from `:latest`. The shared agent entrypoint launches the testnet WS server iff `FLEZHA_TESTNET_WS_PORT` is set in the environment.

The image's purpose is to give downstream consumers (e.g. `flezha_market`) a single image they can pull as `:latest-dev` to get the prod runtime plus the testnet driver, without having to maintain their own wrapper Dockerfile.

The image inherits the prod image's HEALTHCHECK (control-socket readiness, see the prod-image entrypoint requirement below).

#### Scenario: Runtime-dev image has Node.js and the pre-built testnet

- **GIVEN** a built runtime-dev image `flezha/flezha:latest-dev`
- **WHEN** the runner runs `docker run --rm flezha/flezha:latest-dev sh -c 'which node; ls /flezha/testnet/dist/index.js'`
- **THEN** `node` resolves to a path on `$PATH` and `/flezha/testnet/dist/index.js` exists

#### Scenario: Testnet auto-starts when FLEZHA_TESTNET_WS_PORT is set

- **GIVEN** the runtime-dev image
- **WHEN** the runner runs `docker run --rm -e FLEZHA_TESTNET_WS_PORT=4500 flezha/flezha:latest-dev` and waits for startup
- **THEN** the entrypoint launches `node /flezha/testnet/dist/index.js` listening on port 4500 in addition to the agent

#### Scenario: Runtime-dev image inherits prod size budget plus a testnet allowance

- **GIVEN** a built runtime-dev image
- **WHEN** the runner runs `docker image inspect flezha/flezha:latest-dev --format '{{.Size}}'`
- **THEN** the value SHALL be less than `471859200` (450 MiB) — prod's 300 MiB budget plus an allowance for Node.js and the built testnet bundle

### Requirement: Prod image entrypoint SHALL provide graceful-restart lifecycle

`deploy/docker/prod/Dockerfile.controller`'s runtime stage SHALL replace the direct `CMD ["bin/flzvm", "start"]` with `CMD ["/usr/local/bin/agent-entrypoint.sh"]`. Tini SHALL remain the ENTRYPOINT. The entrypoint script SHALL:

1. Remove a stale `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock` left by a previous container on a reused volume, then background `bin/flzvm start`. Only the controller's own control socket is removed; SDK-owned sockets are held open by external processes and MUST NOT be touched.
2. If `FLEZHA_TESTNET_WS_PORT` is set and `/flezha/testnet/dist/index.js` exists, also background the testnet WS server (no-op in `:latest`; consumed by `:latest-dev` via inheritance).
3. Wait up to `${FLEZHA_RESTORE_WAIT_TIMEOUT_S:-60}` seconds for the BEAM to bind `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock` — the same readiness signal the HEALTHCHECK keys on. It SHALL exit 1, naming the cause, if the agent process exits before the socket appears or if the socket has not appeared when the budget elapses.
4. Install a SIGTERM/SIGINT trap whose behaviour depends on `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` (default `true`):
   - **Enabled** (default): the trap SHALL `exec /flezha/bin/flz controller stop --timeout-ms "${FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS:-60000}"`. The CLI's RPC drives `Flezha.Shutdown.Coordinator` inside the BEAM (quiesce → drain to peers → finalise). When `FLEZHA_SHUTDOWN_TOKEN_FILE` names a readable file, the call SHALL present its content as `FLEZHA_TOKEN`, so an enforcing controller accepts the admin verb.
   - **Disabled** (`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` set to `false`/`0`/`no`/`off` case-insensitive): the trap SHALL `kill -TERM` the agent directly, bypassing the coordinator. This is the kill-switch fall-through, and it matches what the same variable means inside the BEAM (`controller-graceful-shutdown`: `SignalHandler` installs no OS handlers, so kernel's `erl_signal_handler` stays the owner).
5. `wait` on the agent PID.

Restart survival SHALL come from ProcessHub's durable registry and the per-Cell WAL (`cluster-boot-recovery`, `durability-cell-state`). The entrypoint SHALL NOT write or read a cluster-wide snapshot file, and the image SHALL NOT ship a pre-stop snapshot hook.

The image SHALL declare a HEALTHCHECK that tests for the existence of `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock`.

#### Scenario: SIGTERM with graceful shutdown enabled drives the BEAM coordinator

- **GIVEN** a running container with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=true` (default), one Relay locally hosted, and a peer container reachable
- **WHEN** the orchestrator sends SIGTERM (e.g. `docker stop`)
- **THEN** the entrypoint trap execs `/flezha/bin/flz controller stop --timeout-ms <timeout>`; the CLI streams drain progress; the BEAM exits cleanly within `stop_grace_period`; the Relay is alive on the peer

#### Scenario: SIGTERM with graceful shutdown disabled terminates the agent directly

- **GIVEN** a running container with `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`
- **WHEN** the orchestrator sends SIGTERM
- **THEN** the entrypoint logs `graceful-restart: SIGTERM received` and sends SIGTERM to the agent PID without invoking `flz controller stop`; no snapshot file is written and none is looked for

#### Scenario: Control socket does not appear within the budget

- **GIVEN** a container whose BEAM refuses to boot — for example with `FLEZHA_DURABILITY_BACKEND` unset — and `FLEZHA_RESTORE_WAIT_TIMEOUT_S=20`
- **WHEN** the container starts
- **THEN** the entrypoint exits non-zero within the budget, naming the missing control socket or the agent's early exit, rather than waiting indefinitely

#### Scenario: Healthcheck reports unhealthy until the control socket appears

- **GIVEN** a container starting up
- **WHEN** the BEAM has not yet opened its control socket
- **THEN** the HEALTHCHECK reports unhealthy; once the socket exists at `${FLEZHA_SOCKET_FILES_PATH}/runner_control_channel.sock`, the healthcheck reports healthy

#### Scenario: An enforcing controller stops gracefully on SIGTERM

- **GIVEN** a container with authorization enforced and claimed, and `FLEZHA_SHUTDOWN_TOKEN_FILE` naming a file that holds an `admin@_system` token
- **WHEN** the orchestrator sends SIGTERM
- **THEN** the trap's `flz controller stop` is accepted and the BEAM drains and exits before the grace period, rather than being refused and killed

### Requirement: docker-compose controller services SHALL declare an explicit stop signal and grace period

Every controller service in `deploy/docker/prod/docker-compose.yml`, `deploy/docker/dev/docker-compose.yml`, `deploy/docker/dev/docker-compose.healing.yml`, and `deploy/docker/dev/docker-compose.testing.yml` SHALL declare:

- `stop_signal: SIGTERM` — explicit; matches docker's default but documents intent.
- `stop_grace_period: 75s` — long enough for `Flezha.Shutdown.Coordinator`'s 60 s drain budget plus 15 s headroom for finalise and BEAM exit.

Users changing `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` MUST update `stop_grace_period` to `(timeout_ms / 1000) + 15s` in lockstep. The pairing is documented in `docs/public/topics/deploying/deploying_a_controller.md`.

#### Scenario: `docker compose stop` lets the BEAM drain without SIGKILL

- **GIVEN** a `docker compose up` of `deploy/docker/prod/docker-compose.yml` with a controller hosting one Relay and a peer service running
- **WHEN** the user runs `docker compose stop`
- **THEN** docker sends SIGTERM (per `stop_signal`); the controller drains within 60 s; the 75 s `stop_grace_period` elapses without docker escalating to SIGKILL; the controller exits cleanly; the Relay is running on the peer

### Requirement: K8s DaemonSet SHALL declare a preStop hook and a tuned termination grace period

`deploy/kubernetes/flezha-daemonset.yaml`'s controller container SHALL declare:

- `lifecycle.preStop.exec.command: ["/flezha/bin/flz", "controller", "stop", "--timeout-ms", "$(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS)"]` — the **primary** path for k8s pod termination; runs synchronously before k8s issues SIGTERM.
- `terminationGracePeriodSeconds: 75` — `(FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS / 1000) + 15` headroom; default value matches the configmap default of `60000`.

`deploy/kubernetes/flezha-configmap.yaml` SHALL declare `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` (default `60000`). The DaemonSet SHALL `envFrom` the configmap so the BEAM and the preStop command read the same value. Users tuning the value MUST also bump `terminationGracePeriodSeconds` to maintain the 15 s headroom.

#### Scenario: K8s pod deletion runs preStop, then SIGTERM, exits within grace

- **GIVEN** a controller pod hosting one Relay with a peer pod available
- **WHEN** the user runs `kubectl delete pod <pod>`
- **THEN** k8s runs `flz controller stop --timeout-ms 60000` via preStop (which drives `Flezha.Shutdown.Coordinator` to drain the Relay to the peer), then sends SIGTERM (the SignalHandler observes it as a second-signal during shutdown and ignores), then waits up to 75 s; the pod exits cleanly with the Relay running on the peer

#### Scenario: User-tuned timeout takes effect via configmap

- **GIVEN** a user updates `deploy/kubernetes/flezha-configmap.yaml` to set `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS=120000` AND updates the DaemonSet's `terminationGracePeriodSeconds` to `135`
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

### Requirement: A CI gate SHALL prevent prod-image size regressions

A workflow (either `.github/workflows/elixir.yml` or a dedicated `docker.yml`) SHALL build the prod image on every PR that touches `deploy/docker/prod/**`, `mix.exs`, `mix.lock`, `Cargo.toml`, `Cargo.lock`, or `crates/flz_nif/**`, and SHALL fail the PR if the image size exceeds a configured threshold. The initial threshold SHALL be 300 MB. The workflow SHALL emit the measured size to the GitHub step summary regardless of pass/fail.

#### Scenario: PR that bloats the image fails the gate

- **GIVEN** a PR that adds a 200MB binary asset under `priv/`
- **WHEN** the docker-size CI step runs
- **THEN** the step exits non-zero with stderr like `prod image size 480MB exceeds threshold 300MB`

#### Scenario: PR that doesn't touch packaging skips the gate

- **GIVEN** a PR whose diff is exclusively under `lib/flezha/relay/**`
- **WHEN** CI runs
- **THEN** the docker-size gate is not triggered (the workflow's path filter excludes the change)

### Requirement: Dev harness containers SHALL run unprivileged

`deploy/docker/dev/Dockerfile.toolchain` SHALL create every directory that a
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

- **GIVEN** a toolchain image built from `deploy/docker/dev/Dockerfile.toolchain`
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

### Requirement: The Release SHALL Ship VM Arguments Sized for the Controller, and CLI Arguments Sized for the CLI's Node

The release SHALL ship a `vm.args` tuned for a long-running controller and a `remote.vm.args` sized for the short-lived node `flzvm rpc` and `flzvm remote` start, which no `flz` verb starts, and each flag SHALL carry its reason where it is set.

The two files are separately addressable by the launcher: start commands read
`RELEASE_VM_ARGS` and `rpc`/`remote` read `RELEASE_REMOTE_VM_ARGS`. A short-lived node and a long-running controller are two
different jobs and they get two different budgets.

A short-lived node the release starts SHALL run with the Erlang break handler
disabled, so that SIGINT terminates it rather than answering with the `BREAK:` menu
and leaving the node alive.

The controller node SHALL run with the break handler disabled too, so that Ctrl-C
on a controller started in attached mode ends it. The default handler pauses the
whole VM behind the `BREAK:` menu, so the prompt returns while the node keeps its
name and control socket and answers no `flz` verb. SIGINT is a hard stop with no
drain; SIGTERM is the graceful one.

#### Scenario: A booted controller reports the shipped values

- **WHEN** the release is started and asked for its own `system_info`
- **THEN** every value the shipped `vm.args` sets SHALL be the value reported

#### Scenario: A remote shell's node is smaller than the controller's

- **WHEN** `flzvm remote` starts its node
- **THEN** that node SHALL run with the reduced scheduler budget `remote.vm.args` sets

#### Scenario: A shipped throughput flag names its evidence

- **WHEN** a flag is shipped for throughput rather than for capacity, clustering or clock semantics
- **THEN** the benchmarking documentation SHALL record the measured series that chose it, including the arm it was measured against

#### Scenario: A remote shell's node ends on SIGINT

- **WHEN** SIGINT reaches the node `flzvm remote` started
- **THEN** the node SHALL terminate without printing the `BREAK:` menu, and the shell SHALL exit

#### Scenario: An attached controller ends on SIGINT

- **WHEN** SIGINT reaches a controller started with `flz controller start` in attached mode
- **THEN** the node SHALL terminate without printing the `BREAK:` menu, and a following `flz controller start` SHALL boot

### Requirement: The Prod Image SHALL Honour `ERL_FLAGS` From the Container Environment

The prod image SHALL start the controller with any `ERL_FLAGS` present in the container environment, and the deployment documentation SHALL show the one-line form for a pulled image and the mounted-file form for a read-only root filesystem.

A user who pulls the image cannot edit the args file inside it, and under
the shipped DaemonSet cannot write to the root filesystem at all. Passing a
variable is the only route that costs neither a rebuild nor a volume, so it is
the route the documentation leads with.

#### Scenario: A pulled image is retuned with one variable

- **WHEN** the image is run with `ERL_FLAGS` set in the container environment
- **THEN** the controller SHALL start with those flags applied over the shipped defaults, with no rebuild and no mounted file

#### Scenario: A read-only root filesystem replaces the file instead

- **GIVEN** a deployment whose container root filesystem is read only
- **WHEN** an args file is mounted and `RELEASE_VM_ARGS` names it
- **THEN** the controller SHALL start with that file's flags

#### Scenario: The Kubernetes ConfigMap shows the seam

- **WHEN** a user reads the shipped ConfigMap
- **THEN** it SHALL carry `ERL_FLAGS` as a commented key naming the documentation section, so the override is visible in the manifest a user already edits

### Requirement: Release SHALL ship every application a shipped dependency needs

Every release `mix.exs` defines SHALL ship each Mix dependency of every application it ships, including a dependency a library declares `runtime: false`. Mix leaves such a dependency out of the library's `.app` file, so `mix release`, which follows only `applications` and `included_applications`, cannot reach it, while `mix test` loads it from the code path. The root `mix.exs` SHALL declare any such dependency itself.

The default `mix test` suite SHALL check this for every release `mix.exs` defines. It SHALL compute the release's closure with the function `mix release` itself uses, compare it with each shipped application's dependencies as `Mix.Project.deps_tree/1` records them, and fail naming each dependency the closure leaves out and the shipped application that needs it. It SHALL fail rather than pass when the closure does not contain `:process_hub` or the tree does not carry process_hub's `runtime: false` edge to `:elector`. The check reads the test environment's build, so a dependency declared `only: :prod` is outside it.

#### Scenario: A library's runtime-false dependency ships because the root declares it

- **GIVEN** process_hub declares elector `runtime: false`, and the root `mix.exs` declares elector
- **WHEN** the default `mix test` suite runs `test/flezha/deployment/release_closure_test.exs`
- **THEN** the release closure SHALL contain `:elector` and the test SHALL pass

#### Scenario: A dependency the release leaves out fails the default suite

- **GIVEN** a root `mix.exs` that does not declare elector
- **WHEN** the default `mix test` suite runs
- **THEN** the release closure test SHALL fail with `release flezha leaves out [elector: :process_hub]`

#### Scenario: A build tool the root declares runtime-false is not required

- **GIVEN** the root declares credo `only: [:dev, :test], runtime: false`
- **WHEN** the check runs in the test environment, where credo, bunt and file_system are compiled
- **THEN** none of them SHALL be required in the release, because no shipped application depends on them

#### Scenario: The check cannot pass without looking

- **WHEN** the computed closure does not contain `:process_hub`
- **THEN** the test SHALL fail on the `{:process_hub, :elector}` edge it requires, not pass with nothing compared

### Requirement: The prod image entrypoint SHALL sync a declared plugin list before starting the controller

`deploy/docker/prod/controller-entrypoint.sh` SHALL run `/flezha/bin/flz plugin sync` before it
starts `bin/flzvm` whenever `FLEZHA_PLUGINS` is set in the container's environment, even to
an empty value, and SHALL exit non-zero, naming the failure and without starting the
controller, when the sync fails. When the variable is unset, the entrypoint SHALL start the
controller without syncing, and the plugin root SHALL be whatever the volume or the image
holds. The image itself SHALL NOT change with the plugin set.

#### Scenario: A declared list is fetched before start

- **GIVEN** the prod image run with `FLEZHA_PLUGINS` naming the example package and a named
  volume on `/flezha/storage/app/plugins`
- **WHEN** the container starts
- **THEN** the entrypoint SHALL log the sync, the archive SHALL be in the volume before
  `bin/flzvm start` runs, and `flz plugin list --installed` SHALL show the package loaded

#### Scenario: A failed sync keeps the controller down

- **GIVEN** `FLEZHA_PLUGINS` naming an unreachable URL
- **WHEN** the container starts
- **THEN** the entrypoint SHALL exit non-zero naming `plugin.fetch_failed` and SHALL NOT
  start the controller

#### Scenario: A derived image needs no sync

- **GIVEN** an image built `FROM` the prod image that copies the example archive into
  `/flezha/storage/app/plugins/`, run with no volume on that path and no `FLEZHA_PLUGINS`
- **WHEN** the container starts
- **THEN** the controller SHALL load the package

### Requirement: The release SHALL ship its consolidated protocols with their debug info

The release step SHALL copy the build's consolidated protocol beams
(`Mix.Project.consolidation_path/0`) over `<release>/releases/<vsn>/consolidated/` after
`:assemble`, so that every consolidated protocol in the release keeps its `Dbgi` chunk,
which boot-time re-consolidation reads (`plugin-installation`). Every other beam of the
release SHALL stay stripped.

#### Scenario: The released protocols carry debug info

- **WHEN** `mix release` has run
- **THEN** every beam under `releases/<vsn>/consolidated/` SHALL carry a `Dbgi` chunk, and
  `lib/flezha-<vsn>/ebin/Elixir.Flezha.Application.beam` SHALL NOT

### Requirement: A plugin-builder stage SHALL share the controller build's toolchain

`deploy/docker/prod/Dockerfile.controller` SHALL define a `plugin-builder` stage whose base image
is the same `hexpm/elixir` tag as the stage that builds the release, written once in an
`ARG` both stages read. The stage SHALL hold Hex, rebar and every tool compiling `flezha`
as a dependency needs (`git`, `cmake`, `build-essential`, `protobuf-compiler`), SHALL set
`RUSTLER_SKIP_COMPILATION=true` and `MIX_ENV=prod`, and SHALL NOT be part of the runtime
image. Its OTP release and Elixir version SHALL therefore equal the runtime image's, so a
package it builds loads in that image.

#### Scenario: A package built in the builder loads in the image

- **WHEN** the example plugin is packaged with `mix flezha.plugin.package` inside an image
  built with `docker build --target plugin-builder`, and the archive is installed into a
  container of the runtime image built from the same Dockerfile
- **THEN** the controller SHALL load the package, and its `built_with.otp` SHALL equal the
  runtime's OTP release

#### Scenario: One tag, two stages

- **WHEN** the `hexpm/elixir` tag is changed in the Dockerfile
- **THEN** it SHALL be changed in one `ARG` line, and both the release stage and the
  `plugin-builder` stage SHALL build from the new tag

### Requirement: The image SHALL be built on a runtime with no open advisory for a shipped application

The controller image SHALL be built on an Erlang/OTP patch release for which no published
Erlang/OTP security advisory with an available fix affects an application the release ships.
The toolchain image and `.github/toolchains.json` SHALL name the same OTP and Elixir versions as
the controller image. The publishing-readiness document SHALL describe the advisory check to run
before a release: `mix hex.audit` for the controller and each plugin, `cargo audit`, a query of
every lock file in the repository and its SDK submodules against the OSV database, and the
Erlang/OTP advisories for the image's OTP version.

#### Scenario: The shipped applications are at patched versions

- **WHEN** the controller image is built and its `lib/` directory is listed
- **THEN** it SHALL contain `ssl-11.6.0.7` or later, `public_key-1.20.3.6` or later,
  `asn1-5.4.3.1` or later, `inets-9.6.2.4` or later, `stdlib-7.3.0.2` or later and
  `erts-16.4.0.6` or later

#### Scenario: The toolchain pins agree

- **WHEN** `scripts/check-ci-toolchain.sh` runs
- **THEN** it SHALL pass with the controller image, the toolchain image and
  `.github/toolchains.json` naming Elixir 1.20.5 on OTP 28

### Requirement: The image SHALL NOT ship a cookie file

The controller image SHALL NOT contain `releases/COOKIE`, as the release archive already does not.

#### Scenario: No cookie file in the image

- **WHEN** the controller image is built
- **THEN** `/flezha/releases/COOKIE` SHALL NOT exist in it, and a controller started from it with
  `RELEASE_COOKIE` set SHALL cluster as before

### Requirement: Build-time downloads SHALL be pinned

Every tool the image build downloads outside a package manager SHALL be fetched at a named
version and checked against a recorded SHA-256 digest before use; a mismatch SHALL fail the
build.

#### Scenario: A tampered rebar3 fails the build

- **WHEN** the image build fetches `rebar3` and its digest differs from the recorded one
- **THEN** the build SHALL fail at that step

### Requirement: Reference workloads declare resource requests and limits
Every container in the reference manifests (`deploy/kubernetes/flezha-daemonset.yaml` controller container and `deploy/kubernetes/service-template.yaml` runner container) SHALL declare CPU and memory `requests` and `limits` with conservative, documented defaults that a user can tune. No reference workload container SHALL ship without resource bounds.

#### Scenario: Controller container has resource bounds
- **WHEN** `deploy/kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the `flezha` container declares `resources.requests` and `resources.limits` for both `cpu` and `memory`, with values documented as tunable defaults in `deploy/kubernetes/README.md`

#### Scenario: Runner template carries the same baseline
- **WHEN** the `service-template.yaml` reference is used
- **THEN** its runner container also declares CPU and memory requests and limits

### Requirement: Controller container declares liveness, readiness, and startup probes

The controller container SHALL declare a `startupProbe`, a `livenessProbe`, and a `readinessProbe` so Kubernetes restarts a wedged controller and gates rollouts and traffic on the right fact. The probes SHALL use a controller-provided signal (an HTTP endpoint the controller itself serves, a `flz` command exec, or a TCP check on a controller port) and SHALL NOT assume any external dependency.

Liveness and readiness are different questions and SHALL NOT share one endpoint. The `livenessProbe` and the `startupProbe` SHALL read the health endpoint, which answers success throughout a planned recovery so the kubelet does not restart a controller mid-replay. The `readinessProbe` SHALL read the readiness endpoint, which answers failure for exactly that same window, so a pod that has not finished coming up is not routed to.

A readiness probe SHALL NOT report a controller ready on the grounds that nothing is yet running on it. A controller in its first tick — no namespace hub created, no relay bound, nothing restored — SHALL read as not ready.

#### Scenario: Probes present and self-contained

- **WHEN** `deploy/kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the `flezha` container declares `startupProbe`, `livenessProbe`, and `readinessProbe`, each using a self-contained check served by the controller itself, with no probe depending on a service outside the pod

#### Scenario: Liveness and readiness read different endpoints

- **WHEN** `deploy/kubernetes/flezha-daemonset.yaml` is inspected
- **THEN** the `livenessProbe` and `startupProbe` target the health endpoint and the `readinessProbe` targets the readiness endpoint

#### Scenario: A recovering controller is live but not ready

- **GIVEN** a controller whose namespace hubs are replaying their durable registry
- **WHEN** the kubelet runs both probes
- **THEN** the liveness probe SHALL succeed, so the pod is not restarted, and the readiness probe SHALL fail, so the pod is removed from readiness-gated endpoints until the replay completes

#### Scenario: A freshly started controller is not ready

- **GIVEN** a controller pod in its first seconds, before any namespace hub exists
- **WHEN** the kubelet runs the readiness probe
- **THEN** it SHALL fail, and the pod SHALL NOT be added to readiness-gated endpoints

#### Scenario: Failing health restarts the pod

- **WHEN** the controller becomes unhealthy past the liveness threshold
- **THEN** the kubelet restarts the container, and while unready the pod is removed from readiness-gated endpoints

### Requirement: Reference manifests meet the Pod Security Standards restricted profile
The controller (and runner template) Pod and container `securityContext` SHALL satisfy the *restricted* Pod Security Standard: `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `seccompProfile.type: RuntimeDefault`, and `readOnlyRootFilesystem: true` with explicit writable `emptyDir`/volume mounts for any path the process must write. Where a root-running init container (e.g. `fix-permissions`) is required only because a volume does not honor `fsGroup`, the manifest SHALL document that switching to an `fsGroup`-honoring volume removes the need for the root init container under the restricted profile.

#### Scenario: Restricted securityContext fields are set
- **WHEN** `deploy/kubernetes/flezha-daemonset.yaml` is applied
- **THEN** the controller container sets `runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`, `seccompProfile.type: RuntimeDefault`, and `readOnlyRootFilesystem: true`, with writable mounts declared for every path the controller writes

#### Scenario: Restricted profile admits the namespace
- **WHEN** the `flezha` namespace is labelled `pod-security.kubernetes.io/enforce: restricted`
- **THEN** the reference DaemonSet and runner template are admitted without warnings, OR the README documents the exact reason and the `fsGroup`-volume alternative that makes them compliant

### Requirement: Reference image is pinned and the pull secret is optional
The reference manifests SHALL reference the controller image by a **pinned version tag** (not `:latest`). Because the published image is public, a stock `kubectl apply -f deploy/kubernetes/` SHALL succeed **without** creating any registry secret: `imagePullSecrets` SHALL be optional (absent by default or clearly commented), and the README SHALL document how to add one for a private registry.

#### Scenario: Stock apply needs no pull secret
- **WHEN** a user runs `kubectl apply -f deploy/kubernetes/` against a cluster that can reach Docker Hub
- **THEN** the controller image pulls successfully without any `imagePullSecrets` being created first

#### Scenario: Image tag is immutable
- **WHEN** the reference DaemonSet is inspected
- **THEN** the controller `image` uses a pinned version tag rather than `:latest`

### Requirement: Reference manifests carry the recommended Kubernetes labels
All reference manifests SHALL set the recommended `app.kubernetes.io/*` labels (`name`, `instance`, `component`, `part-of`, `managed-by`, and `version`) so standard tooling and selectors work, in addition to any existing selector labels.

#### Scenario: Recommended labels present
- **WHEN** any reference manifest is applied
- **THEN** its resources carry `app.kubernetes.io/name`, `app.kubernetes.io/component`, `app.kubernetes.io/part-of`, and `app.kubernetes.io/managed-by` labels (with `instance`/`version` where applicable)

### Requirement: Persistent storage is portable and parameterized
The reference manifests SHALL NOT hardcode environment-specific assumptions for persistent state. The controller's persistent recovery path MAY default to a node-local volume (correct for a DaemonSet), but the path and volume type SHALL be parameterized and `deploy/kubernetes/README.md` SHALL document the portable choice (a CSI/`local` PersistentVolume for cloud clusters) versus `hostPath` (bare-metal/single-node), including the trade-offs.

#### Scenario: Storage is documented and tunable, not assumed
- **WHEN** a cloud user reviews the reference manifests
- **THEN** the persistent-state volume path/type is parameterized (not a fixed host directory baked into the only supported path), and the README explains the CSI/local-PV option alongside `hostPath` with their trade-offs

### Requirement: Reference manifests are correct and environment-agnostic
The reference manifests SHALL be free of YAML/scheduling defects and SHALL NOT embed any environment-specific identifiers (node hostnames, single-cluster IPs, or architecture pins beyond the multi-arch image). The `service-template.yaml` `podAntiAffinity` SHALL use a valid `ops` key in its `matchExpressions`.

#### Scenario: podAntiAffinity expression is valid
- **WHEN** `deploy/kubernetes/service-template.yaml` is applied
- **THEN** its `podAntiAffinity` `matchExpressions` entry uses `operator: In` (not the invalid `runner: In`), and the manifest is accepted by the API server

#### Scenario: No environment-specific identifiers
- **WHEN** any reference manifest is reviewed
- **THEN** it contains no node hostnames, no hardcoded cluster IPs, and no architecture-specific node selectors (architecture is resolved by the multi-arch image)

### Requirement: The reference manifests SHALL declare the controller's plugins

`deploy/kubernetes/flezha-configmap.yaml` SHALL set `FLEZHA_PLUGIN_ROOT` to a directory on the
persistent recovery volume (`/var/lib/flezha/plugins`), because the controller container's
root filesystem is read-only and the loader expands packages under the plugin root at every
boot. It SHALL carry `FLEZHA_PLUGINS` as a commented example of a declared list, with a
pinned `#sha256=` entry. The DaemonSet SHALL add no init container for plugins: the image
entrypoint syncs the declared list before it starts the controller (`controller-packaging`).
`deploy/kubernetes/README.md` SHALL state that a change to the list takes effect with
`kubectl rollout restart daemonset/flezha-daemonset`, that the declared list is the only
truth on such a node (install and uninstall are refused there), and how to supply a
package from a URL the cluster can reach.

#### Scenario: A declared plugin loads in the reference deployment

- **GIVEN** a throwaway single-node cluster running the reference manifests with the locally
  built image and `FLEZHA_PLUGINS` naming the example package
- **WHEN** the pod becomes ready
- **THEN** `kubectl exec` of `flz plugin list --installed` SHALL show the package loaded, and
  its archive SHALL be under `/var/lib/flezha/plugins`

#### Scenario: Changing the list is a rollout

- **GIVEN** that deployment
- **WHEN** `FLEZHA_PLUGINS` is emptied in the ConfigMap and the DaemonSet is restarted with
  `kubectl rollout restart`
- **THEN** the new pod SHALL list no package, and the archive SHALL be gone from the plugin
  root

#### Scenario: Install is refused on a node whose list is declared

- **GIVEN** that deployment with `FLEZHA_PLUGINS` set
- **WHEN** `kubectl exec` runs `flz plugin install` of any archive
- **THEN** it SHALL be refused with `plugin.list_declared`

### Requirement: The reference manifests SHALL limit who reaches the distribution ports

The reference manifests SHALL include a NetworkPolicy for the controller pods that admits traffic
to the EPMD port and the distribution port only from pods labelled as Flezha controllers, admits
traffic to the `http_api` port from pods labelled as the console, and admits traffic to the health
port from any source. The DaemonSet SHALL set `FLEZHA_DIST_PORT` and declare that port and the
EPMD port on the controller container. The console's Kubernetes example SHALL reach the
controllers through the `http_api` port with a client certificate and a token, and SHALL NOT be
given the cookie.

#### Scenario: The manifest declares the policy and the port

- **WHEN** the reference manifests are read
- **THEN** a NetworkPolicy SHALL select the controller pods and allow ports 4369 and the
  `FLEZHA_DIST_PORT` value only from the controller label, and the `http_api` port from the
  console label, and the DaemonSet SHALL set `FLEZHA_DIST_PORT` and declare it as a container port

#### Scenario: An unrelated pod is blocked on a cluster that enforces policies

- **GIVEN** the reference manifests applied to a throwaway k3s cluster with policy enforcement
- **WHEN** a pod without the controller or console label connects to a controller's port 4369 or
  its distribution port
- **THEN** the connection SHALL fail, while the controllers still form a cluster and the health
  probes still pass

#### Scenario: The console pod cannot reach distribution

- **GIVEN** the same cluster with the console example applied
- **WHEN** a console pod connects to a controller's port 4369 or its distribution port
- **THEN** the connection SHALL fail, while its HTTPS calls to the `http_api` port succeed

### Requirement: The reference init container SHALL NOT widen file modes

The init container that prepares the controller's host paths SHALL set ownership and SHALL NOT
make any file or directory under them readable by other users. It SHALL NOT change the mode of
files the controller has already written.

#### Scenario: Keys stay private across a restart

- **GIVEN** a controller pod has written key files with mode `0600` under its certificate host
  path
- **WHEN** the pod restarts and the init container runs again
- **THEN** the key files SHALL still have mode `0600`, and no directory the init container touched
  SHALL be readable by other users

### Requirement: A k8s manifest sanity test SHALL prevent volume/env-var drift

The repository SHALL include a unit test that loads `deploy/kubernetes/flezha-daemonset.yaml` and `deploy/kubernetes/flezha-configmap.yaml` and asserts the volumes and env vars this change relies on are present and well-formed, so that future deployment-manifest edits cannot silently break the dev/k8s parity.

#### Scenario: Required volumes and env vars are present
- **GIVEN** the k8s manifests at `deploy/kubernetes/flezha-daemonset.yaml` and `deploy/kubernetes/flezha-configmap.yaml`
- **WHEN** the manifest sanity test runs
- **THEN** the test SHALL assert: a `flezha-sockets` volume exists in the daemonset and is mounted at `/flezha/sockets`; the daemonset's container env contains `FLEZHA_SOCKET_FILES_PATH=/flezha/sockets`; the configmap defines `FLEZHA_RUNNER_REMOVAL_GRACE_MS` with a positive integer (in milliseconds)
- **AND** the test SHALL fail with a clear message naming the missing field if any assertion fails
