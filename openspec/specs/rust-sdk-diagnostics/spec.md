# rust-sdk-diagnostics Specification

## Purpose

The Rust SDK's diagnostics surface for applications: the exported `EnvFilter` directive list covering every tracing target the SDK emits on (`flezha_runner::diag::DEFAULT_DIRECTIVES`) and the opt-in subscriber installer (`init_default_tracing`). The set of diagnostic targets is SDK knowledge; exporting it closes the enumerate-or-silently-discard trap in application filters, and the SDK-side coverage test turns target drift into a red build in the repo where the drift happens.

## Requirements

### Requirement: Exported default tracing directives cover every SDK diagnostic target

The Rust SDK SHALL export a public constant `flezha_runner::diag::DEFAULT_DIRECTIVES` in `tracing_subscriber::EnvFilter` directive syntax. The directive list SHALL pass (at `warn` level and above) every tracing target the SDK's diagnostics are emitted on: the explicit `flezha::*` target family and the module-path targets of every crate the SDK's untargeted events inherit (`flz_core`, `flz_transport`, `flezha_runner`). An SDK-side test SHALL emit one probe event per diagnostic target family through an `EnvFilter` built from the constant and SHALL fail if any probe is discarded, so a new diagnostic target added without updating the constant fails the SDK build rather than silently disappearing downstream.

#### Scenario: Every diagnostic target family passes the exported directives

- **WHEN** one `warn`-level probe event is emitted per SDK diagnostic target family under an `EnvFilter` built from `DEFAULT_DIRECTIVES`
- **THEN** every probe SHALL be observed by the subscriber; none is discarded

#### Scenario: A new target family without a directive is a red build

- **WHEN** a diagnostic target family is added to the SDK without extending `DEFAULT_DIRECTIVES`
- **THEN** the SDK's directive-coverage test SHALL fail, naming the discarded target

### Requirement: Opt-in default subscriber installation

The Rust SDK SHALL export `flezha_runner::diag::init_default_tracing()`, which installs a `tracing_subscriber::fmt` subscriber whose filter is taken from the `RUST_LOG` environment variable when set and from `DEFAULT_DIRECTIVES` otherwise. The SDK SHALL NOT install any global subscriber implicitly — the helper runs only when the application calls it. A second call SHALL be a no-op that logs a warning rather than panicking, so two components defensively initializing tracing cannot take the process down.

#### Scenario: RUST_LOG overrides the exported defaults

- **WHEN** an application calls `init_default_tracing()` with `RUST_LOG=flezha=debug` set
- **THEN** the installed filter SHALL be built from `RUST_LOG`, not from `DEFAULT_DIRECTIVES`

#### Scenario: No implicit installation

- **WHEN** an application creates and runs a runner without calling `init_default_tracing()`
- **THEN** the SDK SHALL NOT have installed a global tracing subscriber

#### Scenario: Double initialization is survivable

- **WHEN** `init_default_tracing()` is called after a global subscriber is already installed
- **THEN** the call SHALL return without panicking and SHALL emit a single warning
