# java-sdk-packaging Specification

## Purpose

Defines how the Java SDK is structured, built, and distributed: Maven module layout, native binary packaging and runtime loading, version pinning to `flz_transport`, protobuf code generation, and CI publication of build artifacts.

## Requirements

### Requirement: Maven Multi-Module Layout

The Java SDK SHALL live under `sdk/java/` as a Maven multi-module project with the following modules: `flezha-sdk-api`, `flezha-sdk-protobuf`, `flezha-sdk-runtime`, `flezha-sdk-native`, `flezha-sdk` (aggregator / fat jar), and `flezha-sdk-integration-tests`. Each module's `pom.xml` MUST declare `java.version = 21` via Maven toolchains or the `maven-compiler-plugin` `<release>21</release>` setting.

#### Scenario: Fresh checkout builds with a single Maven command
- **WHEN** a developer runs `mvn -f sdk/java/pom.xml install` from a fresh checkout with Java 21 and a host-triple `flz_transport` native build available
- **THEN** all six modules SHALL compile and their unit tests SHALL pass

#### Scenario: Module targeting wrong Java version is rejected
- **WHEN** any module's `pom.xml` is changed to target Java < 21
- **THEN** the Maven build SHALL fail (either at configuration time via enforcer, or at compile time on Java-21 language features used by the SDK)

### Requirement: Native Library Packaging

The published `flezha-sdk` fat jar SHALL bundle prebuilt `flz_transport` native binaries for **at minimum** the following target triples, under resource path `native/<os>-<arch>/libflz_transport.<ext>`:

| Target | Resource path |
|---|---|
| linux x86_64 | `native/linux-x64/libflz_transport.so` |
| linux aarch64 | `native/linux-arm64/libflz_transport.so` |
| macOS x86_64 | `native/macos-x64/libflz_transport.dylib` |
| macOS aarch64 | `native/macos-arm64/libflz_transport.dylib` |
| Windows x86_64 | `native/windows-x64/libflz_transport.dll` |

The cross-build SHALL be driven by `cargo-zigbuild` invoked from a Maven profile (`native-build`) that runs before the jar is assembled.

#### Scenario: Fat jar contains all five binaries
- **WHEN** `mvn -P native-build package` completes successfully
- **THEN** `unzip -l sdk/java/flezha-sdk/target/flezha-sdk-*.jar` SHALL list exactly those five `libflz_transport.*` paths

#### Scenario: Missing native target fails the packaging step
- **WHEN** `mvn -P native-build package` runs but one of the five binaries is missing from `flezha-sdk-native/src/main/resources/native/`
- **THEN** the build SHALL fail with an error naming the missing target triple

### Requirement: Runtime Native Library Loading

On SDK initialization the Java SDK SHALL (a) detect the current `<os>-<arch>` from `os.name` and `os.arch`, (b) extract the matching native binary to a writable temp directory whose path includes the content hash of the binary, (c) call `System.load` with the absolute path, and (d) fail fast with an `UnsatisfiedLinkError` wrapping a descriptive message if the extraction or load fails. The SDK MUST honor the `FLEZHA_NATIVE_LIB_DIR` environment variable as an override for the extraction directory to support hardened containers with noexec `/tmp`.

#### Scenario: Library extracted and loaded on first SDK init
- **WHEN** `Flezha.newInstance(...)` is called on a Linux x86_64 host for the first time in the JVM
- **THEN** `libflz_transport.so` SHALL be extracted under `$TMPDIR/flezha-<version>-<hash>/` and `System.load` SHALL succeed

#### Scenario: Concurrent JVMs do not corrupt the extracted library
- **WHEN** two JVM processes start simultaneously and both extract the same native library
- **THEN** both SHALL successfully `System.load` without races or partial-file errors, because the extraction target path includes a content hash and writes use atomic temp-then-rename

#### Scenario: Unsupported OS/arch fails fast with a clear error
- **WHEN** the SDK starts on a target for which no binary is bundled (e.g., `os.arch = riscv64`)
- **THEN** the SDK SHALL throw an `UnsatisfiedLinkError` whose message names the detected `<os>-<arch>` and the supported set

#### Scenario: FLEZHA_NATIVE_LIB_DIR override is honored
- **WHEN** `FLEZHA_NATIVE_LIB_DIR=/opt/flezha/native` is set and writable
- **THEN** the SDK SHALL extract to that directory instead of `$TMPDIR`

### Requirement: Version Pinning Between Java SDK and flz_transport

The Java SDK Maven version and the bundled `flz_transport` version SHALL be identical per release. On startup the SDK MUST call `flezha_version()` via JNI and compare against a build-time constant; on mismatch the SDK SHALL log a WARNING and continue, naming both versions in the log message.

#### Scenario: Matching versions startup silently
- **WHEN** the bundled native library's `flezha_version()` equals the SDK's build-time version constant
- **THEN** the SDK SHALL start without emitting any version-related log entries

#### Scenario: Mismatched versions emit warning
- **WHEN** `flezha_version()` returns a version that differs from the SDK's build-time constant
- **THEN** the SDK SHALL log at WARNING level with both version strings, and SHALL continue startup normally

### Requirement: Protobuf Code Generation

The `flezha-sdk-protobuf` module SHALL generate Java classes from the existing `flz_transport/protobufs/*.proto` files at build time using `protobuf-maven-plugin`. No generated Java sources SHALL be committed to Git; the module's `src/main/java` directory MUST remain empty.

#### Scenario: Build regenerates protobuf classes
- **WHEN** `mvn -f sdk/java/flezha-sdk-protobuf/pom.xml generate-sources` is run
- **THEN** Java classes for `CommandRequest`, `CommandResponse`, `AssignExecutorId`, `ConnectSocket`, `AddRelay`, `RemRelay`, `PartitionState`, `MigrationSerialization`, `MigrationDeserialization`, `ExecutorDisconnect` SHALL be produced under `target/generated-sources/protobuf/`

#### Scenario: Proto changes are picked up without manual regeneration
- **WHEN** a field is added to a `.proto` file in `flz_transport/protobufs/` and the Java SDK is rebuilt
- **THEN** the regenerated Java class SHALL expose the new field without manual codegen steps

### Requirement: CI Build and Publish Artifacts

CI SHALL include a Java SDK workflow that (a) cross-builds `flz_transport` for the five target triples, (b) assembles the fat jar, (c) runs Java unit tests, (d) runs the integration-test matrix from the `java-sdk-testing` spec, and (e) publishes the built jar as a CI artifact for download. Maven-registry publication is out of scope for this change.

#### Scenario: Pull request CI produces a downloadable jar
- **WHEN** a pull request is opened that affects `sdk/java/**` or `flz_transport/**`
- **THEN** CI SHALL attach the built `flezha-sdk-<version>.jar` as a workflow artifact on the PR

#### Scenario: Java-only changes reuse cached native binaries
- **WHEN** a pull request touches only files under `sdk/java/` (excluding `sdk/java/flezha-sdk-native/src/main/resources/native/`) and not `flz_transport/`
- **THEN** CI SHALL reuse cached native binaries from the most recent successful main-branch build instead of cross-rebuilding them
