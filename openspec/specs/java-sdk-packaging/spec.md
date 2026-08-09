# java-sdk-packaging Specification

## Purpose

Defines how the Java SDK is structured, built, and distributed: Maven module layout, native binary packaging and runtime loading, version pinning to `flz_core_ffi`, and CI publication of build artifacts.

## Requirements

### Requirement: Maven Multi-Module Layout

The Java SDK SHALL live under `sdk/java/` as a Maven multi-module project with the following modules: `flezha-sdk-api`, `flezha-sdk-protobuf`, `flezha-sdk-runtime`, `flezha-sdk-native`, `flezha-sdk` (aggregator / fat jar), and `flezha-sdk-integration-tests`. Each module's `pom.xml` MUST declare `java.version = 21` via Maven toolchains or the `maven-compiler-plugin` `<release>21</release>` setting.

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
`.so`/`.dylib`/`.dll` per platform). The cross-build SHALL be driven by
`cargo-zigbuild` from the existing Maven `native-build` profile
(`scripts/build-native.sh`), and `scripts/build-dist.sh` SHALL stage all
five before the jar is assembled. The transport JNI crate (`flz_jni`) and
its binaries are deleted: the Java SDK links no native surface besides
`flz_core_ffi`.

#### Scenario: Fat jar contains the five core binaries
- **WHEN** `mvn -P native-build package` completes successfully
- **THEN** `unzip -l sdk/java/flezha-sdk/target/flezha-sdk-*.jar` SHALL list the five `native/<os>-<arch>/libflz_core_ffi.*` paths, and no `libflz_jni.*` path

#### Scenario: Missing native target fails the packaging step
- **WHEN** `mvn -P native-build package` (or `-P native-verify`) runs but any of the five binaries is missing from `flezha-sdk-native/src/main/resources/native/`
- **THEN** the build SHALL fail with an enforcer error naming the refresh command

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

CI SHALL include a Java SDK workflow that (a) cross-builds `flz_core_ffi` (feature `jni`) for the five target triples, (b) assembles the fat jar, (c) runs Java unit tests, (d) runs the integration-test matrix from the `java-sdk-testing` spec, and (e) publishes the built jar as a CI artifact for download. Maven-registry publication is out of scope for this change.

#### Scenario: Pull request CI produces a downloadable jar
- **WHEN** a pull request is opened that affects `sdk/java/**`, `flz_core/**`, `flz_core_ffi/**`, or `flz_transport/**`
- **THEN** CI SHALL attach the built `flezha-sdk-<version>.jar` as a workflow artifact on the PR

#### Scenario: Java-only changes reuse cached native binaries
- **WHEN** a pull request touches only files under `sdk/java/` (excluding `sdk/java/flezha-sdk-native/src/main/resources/native/`) and none of the native crates
- **THEN** CI SHALL reuse cached native binaries from the most recent successful main-branch build instead of cross-rebuilding them

### Requirement: Java SDK artifact identifiers use canonical vocabulary

Java SDK Maven artifact IDs and module names SHALL use canonical vocabulary. Modules previously named with `processor`, `operator`, or `compute-engine` entities SHALL use `operative` and `runner` entities respectively. Existing module names that do not carry those terms (e.g., `flezha-sdk-api`, `flezha-sdk-runtime`, `flezha-sdk-native`) keep their names.

#### Scenario: Built artifacts use canonical names
- **WHEN** `mvn -f sdk/java verify` produces SDK jars
- **THEN** the produced artifact filenames MUST NOT contain the substrings `processor`, `compute-engine`, or `operator`; canonical module names (`flezha-sdk-api`, `flezha-sdk-runtime`, `flezha-sdk-native`) appear unchanged

#### Scenario: Manifest metadata reflects canonical role names
- **WHEN** a Java SDK jar's `META-INF/MANIFEST.MF` is inspected
- **THEN** any role-tag entries SHALL use `Runner` (not `ComputeEngine` or `Operator`); free-text descriptions SHALL use `Operative` rather than `Processor`
