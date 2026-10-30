# build-artifact-distribution Specification

## Purpose
TBD - created by archiving change unbundle-committed-build-artifacts. Update Purpose after archive.
## Requirements
### Requirement: The repository SHALL NOT track build artefacts

No compiled artefact SHALL be tracked in git. This covers at minimum
`sdk/java/flezha-sdk-native/src/main/resources/native/**`,
`sdk/java/flezha-sdk/dist/**`, `test/apps/java_runner/dist/**`, and any
`*.jar`, `*.so`, `*.dylib`, `*.dll`, `*.class` outside a fixture whose bytes are
the subject of a test.

A gate script SHALL fail when any tracked path matches those globs, and SHALL
name the offending path and the `.gitignore` entry that should cover it. The
gate SHALL run in the quality workflow on every pull request.

#### Scenario: The gate refuses a newly committed jar

- **GIVEN** a working tree where `sdk/java/flezha-sdk/dist/flezha-sdk.jar` has been `git add`ed
- **WHEN** the artefact gate runs
- **THEN** it SHALL exit non-zero naming that path

#### Scenario: The gate passes on a tree that builds its artefacts

- **GIVEN** a working tree where the five native binaries and both jars exist on disk but are untracked
- **WHEN** the artefact gate runs
- **THEN** it SHALL exit zero

#### Scenario: A test fixture whose bytes are under test is exempt

- **GIVEN** a tracked binary fixture that an existing test reads and asserts against
- **WHEN** the artefact gate runs
- **THEN** it SHALL exit zero, the exemption being declared by path in the gate rather than by extension

### Requirement: Local development SHALL require only the host triple

`scripts/build-native.sh` with no `--targets` argument SHALL build the host
triple alone, using the platform's default linker and requiring neither zig nor
`cargo-zigbuild`.

The Maven `native-verify` profile SHALL assert that the host triple's binary is
present under `flezha-sdk-native/src/main/resources/native/<os>-<arch>/` and
SHALL NOT assert the presence of any other triple. When it is absent the build
SHALL fail naming both the missing path and the command that produces it.

`mvn verify`, `mix test.java_sdk`, and the SDK conformance suite SHALL all pass
against a host-triple-only tree, with no network access.

#### Scenario: Host-only tree passes verification

- **GIVEN** a tree where only `native/linux-x64/libflz_core_ffi.so` exists and the other four triples are absent
- **WHEN** `mvn -f sdk/java/pom.xml -P native-verify verify` is run on a linux-x64 host
- **THEN** the build SHALL succeed

#### Scenario: Missing host binary names its build command

- **GIVEN** a tree with no binary under `native/<host-os>-<host-arch>/`
- **WHEN** `mvn -f sdk/java/pom.xml -P native-verify verify` is run
- **THEN** the build SHALL fail in the `prepare-package` phase with a message naming the missing path and `scripts/build-native.sh`

#### Scenario: The Java suites run offline

- **GIVEN** a tree whose host triple has been built and with no network route available
- **WHEN** `mix test.java_sdk` is run
- **THEN** it SHALL pass, no step having attempted a download

### Requirement: A release SHALL publish every supported triple

A tag matching the release pattern SHALL trigger a CI workflow that cross-builds
`flz_core_ffi` for all five supported triples with the `jni` feature and again
with the `napi` feature, builds the `abi3` wheels and the free-threaded wheels
for the five triples, builds a precompiled Ruby gem for each of the five
triples carrying one library per supported Ruby minor, builds the fake
controller binary for the five triples, assembles the SDK and test-runner fat
jars, stages the fixture archive, stages the controller archives, and attaches
all of them and a SHA-256 manifest covering all of them to the GitHub Release
for that tag. The `napi` binaries SHALL be staged under the per-platform package
layout the Node SDK's `package.json` declares. Staging SHALL refuse a set missing
any of them, naming what is missing.

The workflow SHALL attach nothing to the release until every build and check it
runs has passed. A tag whose version carries a `-` SHALL make a pre-release.

Pull-request CI SHALL NOT cross-compile any non-host triple. The five-triple
matrix SHALL run on release tags only.

#### Scenario: A release tag publishes the full matrix

- **WHEN** a release tag is pushed and its workflow completes
- **THEN** the GitHub Release SHALL carry five `jni` `libflz_core_ffi.*` assets, five `napi` addons, five `abi3` wheels, five free-threaded wheels per supported free-threaded interpreter, five precompiled Ruby gems, five fake controller binaries, `flezha-sdk.jar`, `java_runner.jar`, the fixture archive, the three controller archives, and a checksum manifest covering all of them

#### Scenario: A failing job attaches nothing

- **WHEN** a release tag is pushed and any build, smoke test or load check of its workflow fails
- **THEN** the GitHub Release SHALL carry no asset from that run

#### Scenario: Pull-request CI does not cross-compile

- **GIVEN** the merged workflows under `.github/workflows/`
- **WHEN** their pull-request-triggered jobs are inspected
- **THEN** none SHALL install or invoke `zig`, `cargo-zigbuild`, or `ziglang`

#### Scenario: An incomplete release refuses to stage

- **GIVEN** a build output missing a free-threaded wheel, a Ruby platform gem or a controller archive
- **WHEN** `scripts/build-dist.sh --stage-release <dir>` runs
- **THEN** it SHALL fail naming the missing gem, the wheel count of that tier or the missing archive, and SHALL write no manifest

### Requirement: Portable packaging SHALL fetch and verify published assets

The Maven `native-build` profile SHALL, for each of the five triples absent from
`flezha-sdk-native/src/main/resources/native/`, download that asset from the
GitHub Release matching the pinned SDK version and verify it against the
published SHA-256 manifest before staging it. A binary already present on disk
SHALL be left untouched, so a locally cross-built tree is never overridden.

A checksum mismatch SHALL fail the build. When no release matches the pinned
version, the build SHALL fail naming the local cross-build command rather than
falling back to a different version.

#### Scenario: Absent triples are fetched and verified

- **GIVEN** a tree containing only the host triple and a pinned version with a published release
- **WHEN** `mvn -P native-build package` is run
- **THEN** the four absent binaries SHALL be downloaded, checksum-verified, and staged, and the host binary SHALL be left as it was

#### Scenario: A corrupted asset fails the build

- **GIVEN** a downloaded asset whose SHA-256 does not match the manifest
- **WHEN** `mvn -P native-build package` is run
- **THEN** the build SHALL fail naming the mismatching asset, and SHALL NOT stage it

#### Scenario: No matching release refuses rather than substitutes

- **GIVEN** a pinned version with no corresponding published release
- **WHEN** `mvn -P native-build package` is run
- **THEN** the build SHALL fail naming `scripts/build-native.sh --targets all`, and SHALL NOT fetch a different version's assets

### Requirement: Repository history SHALL be purged of build artefacts and stay bounded

The history SHALL be rewritten with `git filter-repo` to remove every blob under
`sdk/java/flezha-sdk-native/**/native/`, `sdk/java/flezha-sdk/dist/`,
`test/apps/java_runner/dist/`, `test/apps/java_executor/`, and
`test/apps/java_compute_engine/`.

The rewrite SHALL be verified by measurement: the tip tree after the rewrite
SHALL be byte-identical to the tip tree before it, no blob over 5 MB SHALL
remain on any of those paths across all refs, and a fresh clone SHALL be under
150 MB. A tag SHALL record the pre-rewrite tip on the remote before the
force-push.

A CI check SHALL fail when the packed repository size exceeds 150 MB, so a
regression is caught at the commit that causes it.

#### Scenario: The rewrite changes no source

- **GIVEN** the tip commit before the rewrite and the tip commit after it
- **WHEN** their tree hashes are compared
- **THEN** they SHALL be identical

#### Scenario: No artefact blobs survive

- **WHEN** `git rev-list --objects --all` is filtered to the five purged path prefixes
- **THEN** it SHALL report no blob larger than 5 MB

#### Scenario: A fresh clone stays under the size ceiling

- **WHEN** the rewritten remote is cloned
- **THEN** the resulting `.git` SHALL be smaller than 150 MB

#### Scenario: The size ceiling is enforced going forward

- **GIVEN** a pull request whose commits push the packed size over 150 MB
- **WHEN** the quality workflow runs
- **THEN** the size check SHALL fail naming the paths responsible

### Requirement: A veneer SHALL be built from outside through a wrapper crate on a git dependency

An SDK that builds its native binary from source SHALL do so through a wrapper
crate in its own tree: a `cdylib` package whose one dependency is
`flz_core_ffi` from this repository at a git tag with that veneer's feature,
and which adds no veneer code. No SDK build SHALL name a path outside its own
tree. Inside this tree the build scripts SHALL keep building the feature from
the workspace and staging the binary where the SDK's loader looks first.
Switching the dependency to a registry SHALL be a change to that one line.

#### Scenario: The wrapper exports the veneer

- **WHEN** the wrapper for a feature is built
- **THEN** its exported symbols SHALL include every entry point `abi_surface.txt` lists and the feature's module-init export

#### Scenario: No build walks to the workspace

- **WHEN** an SDK's build scripts, package manifests and extension configuration are searched for a path that leaves the SDK's tree
- **THEN** none SHALL be found

#### Scenario: An in-tree build needs no network

- **WHEN** `scripts/build-node-sdk.sh` runs in this tree
- **THEN** it SHALL build from the workspace and fetch nothing

### Requirement: The Rust SDK depends on the core by git tag and the workspace patches it back

The Rust SDK, its macros crate and the reference runner SHALL be three crates
in the Rust SDK's repository, with no cargo workspace of their own, whose
dependencies on `flz_core` and `flz_transport` are git dependencies on this
repository at a tag. This repository's root workspace SHALL list the three as
members by their paths inside the `sdk/rust` submodule, and SHALL carry a
`[patch]` mapping that git source to the path crates, so a build in this tree
fetches nothing and builds the submodule against the tree. Every command that
names the reference runner's manifest SHALL name its real path, because cargo
refuses a manifest reached through the `test/apps/rust_runner` symlink.

#### Scenario: The Rust tree builds alone
- **WHEN** each crate's tests run in the Rust SDK repository with no core tree beside it
- **THEN** they SHALL fetch the pinned tag and pass

#### Scenario: This tree builds the submodule against itself
- **WHEN** `cargo build --workspace` runs here with the network unavailable
- **THEN** it SHALL succeed, resolving `flz_core` and `flz_transport` to the path crates through the patch

#### Scenario: The suite still boots the reference runner
- **WHEN** plain `mix test` runs here
- **THEN** it SHALL launch two reference runners from `test/apps/rust_runner` through the symlink and they SHALL connect within the boot window

### Requirement: A release SHALL carry a controller archive per platform, run on that platform first

A release SHALL build the controller release, with `bin/flz`, `bin/flzvm` and its own Erlang
runtime, natively on Linux x86_64, macOS arm64 and macOS x86_64, and publish each as
`flezha-<version>-<platform>.tar.gz`, where `<version>` is the Mix project version and
`<platform>` is `linux-x86_64`, `macos-arm64` or `macos-x86_64`. Each archive SHALL leave out
the release's `tmp` and `storage` directories.

Before it is published, each archive SHALL be unpacked and smoke-tested on a runner of its own
platform by `scripts/release-archive.sh smoke`: the controller starts as a daemon and becomes ready,
`flz runner check` reaches its control socket, a runner connects and `flz runner list` shows
it, and `flz controller stop` exits 0. The smoke test SHALL leave nothing behind outside its
temporary directory.

`make release-archive` SHALL build and pack the archive for the host by the same steps, so the
archive and its smoke test can be produced and run locally.

#### Scenario: Each platform's archive has run on that platform
- **WHEN** a release tag is pushed and its workflow completes
- **THEN** the release carries `flezha-<version>-linux-x86_64.tar.gz`, `flezha-<version>-macos-arm64.tar.gz` and `flezha-<version>-macos-x86_64.tar.gz`, and the run shows each one's smoke test passing on a runner of its platform

#### Scenario: An archive that cannot serve a runner is not published
- **WHEN** the unpacked controller of one platform does not become ready, does not answer `flz runner check`, or shows no connected runner
- **THEN** that smoke test fails and the release carries no asset from the run

#### Scenario: The archive is built and checked locally
- **WHEN** a developer runs `make release-archive` and then `scripts/release-archive.sh smoke` on the archive it wrote
- **THEN** the archive for the host's platform is in `_build/.dist/controller/` and the smoke test passes, as in CI

### Requirement: The macOS SDK libraries SHALL be loaded on a Mac before a release

The two cross-built macOS `libflz_core_ffi.dylib` assets SHALL each be loaded on a macOS
runner of their own chip, with one exported symbol looked up, before the release is
published.

#### Scenario: A dylib that cannot load stops the release
- **WHEN** the staged macOS arm64 or x86_64 `libflz_core_ffi.dylib` fails to load, or lacks the symbol, on a runner of its chip
- **THEN** the load check fails and the release carries no asset from the run

### Requirement: Only `main`, pull requests, schedules and release tags SHALL start CI

No workflow SHALL start on a push to a branch other than `main`. Pushes to `main`, pull
requests, schedules, manual dispatches and `v*` tags remain the only triggers.

#### Scenario: A push to dev starts nothing
- **GIVEN** the workflows under `.github/workflows/`
- **WHEN** their `on:` blocks are inspected
- **THEN** every `push` trigger names `branches: [main]`, `tags: ["v*"]` or both, and none names another branch

