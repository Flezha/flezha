# java-sdk-packaging Specification

## Purpose

Defines how the Java SDK is structured, built, and distributed: Maven module layout, native binary packaging and runtime loading, version pinning to `flz_core_ffi`, and CI publication of build artifacts.
## Requirements
### Requirement: Maven Multi-Module Layout

The Java SDK SHALL live under `sdk/java/` as a Maven multi-module project with the following modules: `flezha-sdk-api`, `flezha-sdk-runtime`, `flezha-sdk-native`, `flezha-sdk-testing`, `flezha-sdk-examples`, and `flezha-sdk` (aggregator / fat jar). Each module's `pom.xml` MUST declare `java.version = 21` via Maven toolchains or the `maven-compiler-plugin` `<release>21</release>` setting.

#### Scenario: Fresh checkout builds with a single Maven command
- **WHEN** a developer runs `mvn -f sdk/java/pom.xml install` from a fresh checkout with Java 21 and a host-triple `flz_core_ffi` native build available
- **THEN** all six modules SHALL compile and their unit tests SHALL pass

#### Scenario: Module targeting wrong Java version is rejected
- **WHEN** any module's `pom.xml` is changed to target Java < 21
- **THEN** the Maven build SHALL fail (either at configuration time via enforcer, or at compile time on Java-21 language features used by the SDK)

### Requirement: Native Library Packaging

The published `flezha-sdk` fat jar SHALL bundle prebuilt `flz_core_ffi`
binaries (built with the `jni` feature, so one binary carries both the C
ABI and the JNI exports) for at minimum the five supported target triples,
under resource paths `native/<os>-<arch>/libflz_core_ffi.<ext>`
(linux-x64, linux-arm64, macos-x64, macos-arm64, windows-x64;
`.so`/`.dylib`/`.dll` per platform). The Java SDK links no native surface
besides `flz_core_ffi`, whose `jni_glue.rs` is its JNI binding.

None of the five SHALL be tracked in git. The Maven `native-build` profile
SHALL obtain each triple absent from
`flezha-sdk-native/src/main/resources/native/` by downloading and
checksum-verifying the published release asset for the pinned version, and
SHALL leave any binary already present on disk untouched, so a set cross-built
in the core's tree (`scripts/build-native.sh --targets all`, which
`scripts/build-dist.sh` drives) is packaged as it is.

The `native-verify` profile SHALL assert the **host triple only** and SHALL NOT
invoke any build or download step. Every test path — `mvn verify`,
`mix test.java_sdk`, the SDK conformance suite — SHALL use `native-verify` and
SHALL therefore pass on a host-triple-only tree with no network access.

#### Scenario: Fat jar contains the five core binaries
- **WHEN** `mvn -P native-build package` completes successfully
- **THEN** `unzip -l flezha-sdk/target/flezha-sdk-*.jar` in the Java SDK's tree SHALL list the five `native/<os>-<arch>/libflz_core_ffi.*` paths, and no other native library

#### Scenario: Missing native target fails the packaging step
- **WHEN** `mvn -P native-build` runs and a triple can be neither found on disk nor fetched for the pinned version
- **THEN** the build SHALL fail naming the pinned tag and the core tree's cross-build command

#### Scenario: native-verify asserts only the host triple
- **GIVEN** a tree containing `native/<host-os>-<host-arch>/libflz_core_ffi.*` and none of the other four
- **WHEN** `mvn -P native-verify verify` is run in the Java SDK's tree
- **THEN** the build SHALL succeed, and SHALL NOT download or build anything

#### Scenario: No native binary is tracked in git
- **WHEN** `git ls-files flezha-sdk-native/src/main/resources/native/` is run in the Java SDK's repository
- **THEN** it SHALL report no files

### Requirement: Runtime Native Library Loading

On SDK initialization the Java SDK SHALL (a) detect the current
`<os>-<arch>` from `os.name` and `os.arch`, (b) extract the matching
`flz_core_ffi` binary to a writable temp directory whose path includes the
content hash, (c) call `System.load` with the absolute path, and (d) fail
fast with an `UnsatisfiedLinkError` wrapping a descriptive message if
extraction or load fails. The SDK MUST honor `FLEZHA_NATIVE_LIB_DIR` as an
override for hardened containers. After loading, context creation SHALL
perform the ABI's schema-version negotiation; a mismatch SHALL fail
startup with an error naming both versions.

#### Scenario: Library extracted and loaded on first SDK init
- **WHEN** `Flezha.createRunner(...)` is first called on a Linux x86_64 host
- **THEN** `libflz_core_ffi.so` SHALL be extracted under `$TMPDIR/flezha-<version>-<hash>/`, `System.load` SHALL succeed, and context creation SHALL negotiate the schema version

#### Scenario: Concurrent JVMs do not corrupt the extracted library
- **WHEN** two JVM processes start simultaneously and both extract the same native library
- **THEN** both SHALL `System.load` successfully — the extraction path includes a content hash and writes are temp-then-rename

#### Scenario: Unsupported OS/arch fails fast with a clear error
- **WHEN** the SDK starts on a target with no bundled binary
- **THEN** it SHALL throw an `UnsatisfiedLinkError` naming the detected `<os>-<arch>` and the supported set

#### Scenario: Schema mismatch refuses startup loudly
- **WHEN** the extracted core speaks a different `SCHEMA_VERSION` than the Java shim was built against
- **THEN** startup SHALL fail before any controller connection with an error naming both versions

### Requirement: Version Pinning Between Java SDK and flz_core_ffi

The Java SDK Maven version SHALL be identical, per release, to the bundled
`flz_core_ffi` crate version. Version drift detection is the ABI's schema
negotiation: context creation SHALL fail hard on an incompatible
activation schema (replacing the old warn-and-continue `flezha_version()`
check, whose silent-continue posture is exactly how runtime skew hid).

#### Scenario: Matching versions start silently
- **WHEN** the bundled core's schema version equals the shim's build-time constant
- **THEN** the SDK SHALL start without version-related log entries

#### Scenario: Mismatched schema refuses instead of warning
- **WHEN** the bundled core's schema version differs from the shim's
- **THEN** context creation SHALL fail with an error naming both versions — startup SHALL NOT continue

### Requirement: CI Build and Publish Artifacts

The Java SDK's own repository SHALL run, on every pull request, its three tiers
against a core release (the modules' suites with the spec-coverage gate, local
mode, and the smoke tier against the fake controller) and its format check.
This repository's Java SDK steps relay SHALL run, on pull requests, the JNI surface
assertion and the integration-test matrix from the `java-sdk-testing` spec
against the pinned submodule. Pull-request jobs SHALL NOT cross-build any
non-host triple and SHALL NOT install `zig` or `cargo-zigbuild`.

A separate release job in this repository, triggered by a version tag and not by
pull requests, SHALL cross-build all five triples, assemble both fat jars, and
publish the five binaries, both jars, and a SHA-256 manifest as GitHub Release
assets for that tag. Maven-registry publication remains out of scope.

#### Scenario: The Java repository proves the SDK on every pull request
- **WHEN** a pull request is opened in the Java SDK's repository
- **THEN** its steps relay SHALL run `scripts/ci.sh` against the pinned core release and SHALL fail when any tier fails

#### Scenario: A release tag publishes the portable matrix
- **WHEN** a version tag is pushed
- **THEN** the release job SHALL publish five `libflz_core_ffi.*` assets, `flezha-sdk.jar`, `java_runner.jar`, and a SHA-256 manifest to that tag's GitHub Release

#### Scenario: Pull-request jobs never cross-compile
- **GIVEN** the merged `.github/workflows/java-sdk.yml`
- **WHEN** its pull-request-triggered jobs are grepped for `cargo-zigbuild`, `cargo zigbuild`, or `ziglang`
- **THEN** there SHALL be no matches

### Requirement: Java SDK artifact identifiers use canonical vocabulary

Java SDK Maven artifact IDs and module names SHALL use canonical vocabulary (`flezha-sdk-api`, `flezha-sdk-runtime`, `flezha-sdk-native`).

#### Scenario: Built artifacts use canonical names
- **WHEN** `mvn -f sdk/java verify` produces SDK jars
- **THEN** the produced artifact filenames SHALL be the canonical module names (`flezha-sdk-api`, `flezha-sdk-runtime`, `flezha-sdk-native`)

#### Scenario: Manifest metadata reflects canonical role names
- **WHEN** a Java SDK jar's `META-INF/MANIFEST.MF` is inspected
- **THEN** any role-tag entries SHALL use `Runner`; free-text descriptions SHALL use `Relay`

### Requirement: A documented build command SHALL succeed on a repeated run

A build command this repository documents or invokes SHALL succeed when run
twice in the same checkout without an intervening clean. A command that works
only against an empty `target/` fails for every caller after the first, and the
first green run is what hides it.

Packaging SHALL NOT consume its own previous output. Where a module produces no
artifact of its own, it SHALL NOT inherit and re-process the artifact a previous
run left in place.

#### Scenario: The conformance build command runs twice

- **GIVEN** a checkout whose Java `target/` directory is not empty
- **WHEN** the test-app build command is run twice with no clean between
- **THEN** both runs SHALL succeed

#### Scenario: The fat jar carries one copy of its notices

- **WHEN** the assembled fat jar is listed
- **THEN** `META-INF/THIRD_PARTY_NOTICES.md` SHALL appear exactly once, and the five `native/<os>-<arch>/libflz_core_ffi.*` entries SHALL be the only native libraries present

### Requirement: The Java tree builds and verifies from its own files

The Java SDK SHALL build, verify and package from its own repository with no
path outside it: the spec-coverage script bound to `verify`, the licence,
notice and third-party files the shade plugin attaches, and the native
profile's inputs SHALL all live in the Java tree. The `native-build` profile
SHALL obtain absent triples by downloading and verifying the release assets
for the pinned core tag, and SHALL refuse naming the core tree's cross-build
command when no release matches; cross-building is the core tree's path and
is reachable only through the submodule there.

#### Scenario: A checkout of the Java tree alone verifies
- **WHEN** `mvn verify` runs in the Java repository with the host triple staged and no core tree beside it
- **THEN** it SHALL succeed, and the spec-coverage gate SHALL have run

#### Scenario: No release for the pin refuses
- **WHEN** `mvn -P native-build package` runs in the Java repository and no release matches the pinned tag
- **THEN** it SHALL fail naming the tag and the core tree's cross-build command

### Requirement: The JNI surface is a fixture both sides assert

`test/fixtures/jni_surface.txt` SHALL list every `static native` method of
`FlzCore.java` paired with the ABI entry point it reaches, and SHALL be the one
declared table of the Java surface. The Rust test SHALL assert `FlzCore.java`'s
declarations and `jni_glue.rs`'s exports against it, and a Java test SHALL
assert the class's declarations against the same fixture, so the Java
repository holds an assertion of its own.

#### Scenario: A native method added without the fixture is red on both sides
- **WHEN** a `static native` declaration is added to `FlzCore.java` and not to the fixture
- **THEN** the Java test SHALL fail naming the method, and the Rust test SHALL fail if `jni_glue.rs` gained the export

