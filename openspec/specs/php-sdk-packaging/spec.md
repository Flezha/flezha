# php-sdk-packaging Specification

## Purpose
TBD - created by archiving change add-php-sdk. Update Purpose after archive.
## Requirements
### Requirement: The SDK ships as one Composer package with a declared PHP floor
The PHP SDK SHALL be distributed as a single Composer package named `flezha/sdk`, requiring `php >= 8.4` and `ext-ffi` and nothing else.

The 8.4 floor is the supported PHP line, not the lowest the code could parse: the authoring surface needs only 8.1 — enums and `never` — and `ext-ffi` itself is stable from 8.0, but 8.4 is the line this project builds, tests and benchmarks the SDK on. The floor SHALL be declared in `require` rather than pinned through `config.platform`, so the manifest states what is supported instead of freezing what resolves. `ext-swoole` SHALL be the only `suggest` entry, declared at the minimum version providing `Swoole\Thread`, and the threaded executor MAY declare a higher PHP floor than the package as a whole, checked when it is selected rather than at install.

#### Scenario: A minimal install works
- **WHEN** a project runs `composer require flezha/sdk` on PHP 8.4 with `ext-ffi` enabled
- **THEN** installation SHALL pull no further packages, and a runner SHALL start

#### Scenario: The threaded executor checks its own requirements
- **WHEN** `SwooleExecutor` is selected on a build that does not satisfy its PHP or extension floor
- **THEN** the SDK SHALL raise an error naming what is missing, and installation of the package itself SHALL never have been affected by it

#### Scenario: An unsupported PHP version is refused at install
- **WHEN** installation is attempted on PHP below 8.4
- **THEN** Composer SHALL refuse it on the declared platform constraint

### Requirement: One runner per process; the SDK spawns none
The PHP SDK SHALL start exactly one runner per process and SHALL NOT fork, spawn, supervise, or recycle processes.

Process multiplicity SHALL be the orchestrator's concern — Kubernetes replicas, systemd template units, or a supervisor's own process management — so the SDK stays agnostic to which one is in use.

#### Scenario: The entry script starts one runner
- **WHEN** the runner entry point is invoked with a namespace
- **THEN** exactly one process, one `FlzContext` and one controller connection SHALL result

#### Scenario: More processes means more runners
- **WHEN** an orchestrator starts several runner processes for one namespace on one host
- **THEN** each SHALL register as its own runner, and the controller SHALL spread operatives across them by its existing placement

#### Scenario: The SDK exposes no worker-count knob
- **WHEN** the SDK's command-line surface is inspected
- **THEN** it SHALL offer no flag that spawns additional processes

### Requirement: Native binaries ride the package, provisionally
The package SHALL carry prebuilt `libflz_core_ffi` binaries for the same five target triples the Java distribution already ships, selected at runtime by detected platform, and `scripts/build-dist.sh` SHALL produce them from the existing cross-build.

This bundling is explicitly provisional while flezha is private; the package SHALL document that platform-specific distribution is the intended end state, as recorded in `TODO.md`.

#### Scenario: The right binary is selected at start
- **WHEN** a runner starts on a supported platform
- **THEN** the SDK SHALL load the matching bundled binary without user configuration

#### Scenario: An unsupported platform fails clearly
- **WHEN** a runner starts on a platform with no bundled binary
- **THEN** it SHALL raise an error naming the detected platform and the override environment variable, rather than a dynamic-loader failure

#### Scenario: An external binary can override the bundle
- **WHEN** the native-library directory environment variable names a directory containing a compatible binary
- **THEN** the SDK SHALL load that binary in preference to the bundled one

### Requirement: Schema version is negotiated, never assumed
The package SHALL carry the `SCHEMA_VERSION` it was built against and pass it to `flz_core_context_new`, so a shim paired with a mismatched core is refused before any controller connection.

A version bump in `flz_core/src/schema.rs` SHALL therefore require the PHP fixtures and constant to move in the same commit, exactly as the Rust and Java fixture suites do.

#### Scenario: A mismatched pair refuses to start
- **WHEN** the package's schema version differs from the loaded core's
- **THEN** context creation SHALL fail with an error naming both versions

#### Scenario: CI catches an unsynchronised bump
- **WHEN** the core's schema changes without the PHP fixtures being updated
- **THEN** the PHP fixture suite SHALL fail

