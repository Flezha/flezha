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

A tag matching the release pattern SHALL trigger a CI job that cross-builds
`flz_core_ffi` (feature `jni`) for all five supported triples, assembles the SDK
and test-runner fat jars, and attaches the five binaries, both jars, and a
SHA-256 manifest to the GitHub Release for that tag.

Pull-request CI SHALL NOT cross-compile any non-host triple. The five-triple
matrix SHALL run on release tags only.

#### Scenario: A release tag publishes the full matrix

- **WHEN** a release tag is pushed and its workflow completes
- **THEN** the GitHub Release SHALL carry five `libflz_core_ffi.*` assets, `flezha-sdk.jar`, `java_runner.jar`, and a checksum manifest covering all seven

#### Scenario: Pull-request CI does not cross-compile

- **GIVEN** the merged `.github/workflows/java-sdk.yml`
- **WHEN** the pull-request-triggered jobs are inspected
- **THEN** none SHALL install or invoke `zig`, `cargo-zigbuild`, or `ziglang`

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

#### Scenario: A fresh clone is bounded

- **WHEN** the rewritten remote is cloned
- **THEN** the resulting `.git` SHALL be smaller than 150 MB

#### Scenario: The size ceiling is enforced going forward

- **GIVEN** a pull request whose commits push the packed size over 150 MB
- **WHEN** the quality workflow runs
- **THEN** the size check SHALL fail naming the paths responsible

