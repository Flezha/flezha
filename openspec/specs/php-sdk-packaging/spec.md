# php-sdk-packaging Specification

## Purpose
TBD - created by archiving change add-php-sdk. Update Purpose after archive.
## Requirements
### Requirement: The SDK ships as one Composer package with a declared PHP floor
The PHP SDK SHALL be distributed as a single Composer package named `flezha/sdk`, requiring `php >= 8.4`, `ext-ffi`, `ext-mbstring` and `composer-plugin-api`, and nothing else.

The 8.4 floor is the supported PHP line, not the lowest the code could parse: the authoring surface needs only 8.1 — enums and `never` — and `ext-ffi` itself is stable from 8.0, but 8.4 is the line this project builds, tests and benchmarks the SDK on. The floor SHALL be declared in `require` rather than pinned through `config.platform`, so the manifest states what is supported instead of freezing what resolves. `composer-plugin-api` is the one requirement that is not a runtime need: it is what makes the package a Composer plugin, which is how it obtains its native binary, and like `php` and the extensions it is a platform package that pulls nothing. `ext-swoole` SHALL be the only `suggest` entry, declared at the minimum version providing `Swoole\Thread`, and the threaded executor MAY declare a higher PHP floor than the package as a whole, checked when it is selected rather than at install.

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

### Requirement: Schema version is negotiated, never assumed
The package SHALL carry the `SCHEMA_VERSION` it was built against and pass it to `flz_core_context_new`, so a shim paired with a mismatched core is refused before any controller connection.

A version bump in `flz_core/src/schema.rs` SHALL therefore require the PHP fixtures and constant to move in the same commit, exactly as the Rust and Java fixture suites do.

#### Scenario: A mismatched pair refuses to start
- **WHEN** the package's schema version differs from the loaded core's
- **THEN** context creation SHALL fail with an error naming both versions

#### Scenario: CI catches an unsynchronised bump
- **WHEN** the core's schema changes without the PHP fixtures being updated
- **THEN** the PHP fixture suite SHALL fail

### Requirement: The package obtains the host's native binary at install
The package SHALL make a `libflz_core_ffi` binary available for the host it is installed on, for the same five target triples the Java distribution ships, selected at runtime by detected platform. It SHALL obtain exactly the host's binary rather than carrying every triple, from the release matching the package's own version, and `scripts/build-dist.sh` SHALL produce the artefacts it obtains. A binary obtained at install time SHALL be verified against the checksum manifest published beside it and SHALL be refused on a mismatch. The package SHALL NOT depend on build output being present in version control.

This obtain-one shape is the split that bundling every triple is recorded as owing; it arrives first here because a Composer package is published from a version-control tag and so cannot carry untracked build output at all.

#### Scenario: The right binary is selected at start
- **WHEN** a runner starts on a supported platform
- **THEN** the SDK SHALL load the matching binary without user configuration

#### Scenario: An unsupported platform fails clearly
- **WHEN** a runner starts on a platform with no available binary
- **THEN** it SHALL raise an error naming the detected platform, the location searched and the override environment variable, rather than a dynamic-loader failure

#### Scenario: An external binary can override the bundle
- **WHEN** the native-library directory environment variable names a directory containing a compatible binary
- **THEN** the SDK SHALL load that binary in preference to any other, on any platform

#### Scenario: A populated checkout touches no network
- **WHEN** the package's platform directory already holds a compatible binary
- **THEN** it SHALL be used and no download SHALL be attempted

#### Scenario: An obtained binary is verified before use
- **WHEN** a binary is obtained at install time
- **THEN** its checksum SHALL be compared against the published manifest for the package's own version, and a mismatch SHALL refuse the install, leaving no binary behind

#### Scenario: A package published from a tag still installs
- **WHEN** the package is installed from a registry that serves it from a version-control tag, carrying no build output
- **THEN** the installation SHALL still yield a loadable binary for the host, or SHALL fail naming what to do by hand

#### Scenario: Disabled install scripts do not break the install
- **WHEN** Composer installs the package with scripts disabled
- **THEN** the binary SHALL still be obtained

#### Scenario: An install that skipped the fetch names how to finish it
- **WHEN** the package was installed without its binary being obtained, and a runner starts
- **THEN** the error SHALL name the asset and manifest to download, the directory to place the binary in, the override environment variable, and the Composer command that obtains it

#### Scenario: A platform with no release and a failed fetch refuse differently
- **WHEN** installation meets a platform no release publishes a binary for, or a fetch of a published binary that fails
- **THEN** the first SHALL direct the operator to build the binary for that platform and the second to retry or fetch by hand, in messages that do not share wording

