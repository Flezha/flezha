# ruby-sdk-packaging Specification

## Purpose
TBD - created by archiving change ruby-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Ruby veneer is a feature of `flz_core_ffi`, not a separate crate

The Ruby binding SHALL be built from `flz_core_ffi` under a `ruby` cargo feature, beside the existing `jni` feature, as a one-to-one veneer over the same C entry points in the same crate. A second crate, a second ABI surface, or a Ruby-only reimplementation of an entry point SHALL be a defect.

#### Scenario: One crate exports both veneers
- **WHEN** `flz_core_ffi` is built with `--features ruby` and with `--features jni`
- **THEN** both SHALL produce the same C ABI plus their language veneer, from the same source crate

#### Scenario: The veneer adds no entry point of its own
- **WHEN** the Ruby veneer's exported functions are compared against the C entry points
- **THEN** each SHALL map one-to-one onto a C entry point, with no Ruby-only kernel path

### Requirement: Precompiled platform gems install without a Rust toolchain

The SDK SHALL publish a precompiled native gem per supported platform, each embedding the built native library, so that installation on a supported platform requires no Rust toolchain, no cargo invocation and no network fetch of Rust dependencies. A source gem SHALL remain available as the fallback and SHALL state its toolchain requirement. Cross-built gems SHALL be produced by CI on every change to the SDK or the core, and the gem CI installs SHALL be the gem the end-to-end arm runs.

#### Scenario: Installation on a supported platform needs no Rust
- **WHEN** the gem is installed on a supported platform in an image with no Rust toolchain
- **THEN** installation SHALL succeed and the runner SHALL start

#### Scenario: The published gem is the tested gem
- **WHEN** the end-to-end Ruby arm runs in CI
- **THEN** it SHALL run against the cross-built gem produced by the same steps relay, not against an ad-hoc local build

### Requirement: Schema version is negotiated before any controller connection

Context creation SHALL negotiate the activation schema version between the gem's veneer and the core and SHALL fail with an error naming both versions when they are incompatible, before any controller connection is attempted.

#### Scenario: A stale gem against a newer core fails at boot
- **WHEN** a gem built against schema version N loads a core speaking an incompatible N+1
- **THEN** startup SHALL fail naming both versions, and no connection SHALL be attempted

### Requirement: YJIT is enabled by default and reported at startup

The runner entry point SHALL enable YJIT when the running Ruby supports it and the user has not already decided, SHALL allow the user to override that decision, and SHALL report the resulting JIT state in the runner's startup line so a measurement can never be ambiguous about it.

#### Scenario: Default-on and reported
- **WHEN** the runner starts on a Ruby with YJIT available and no user override
- **THEN** YJIT SHALL be enabled and the startup line SHALL say so

#### Scenario: The user's decision wins
- **WHEN** the user has explicitly disabled YJIT
- **THEN** the SDK SHALL NOT enable it, and the startup line SHALL report it disabled

### Requirement: `Process.warmup` runs after declaration and before serving

The runner entry point SHALL call `Process.warmup` after class declaration completes and before the first `poll`, and the SDK documentation SHALL state that ordering together with the rule that the core must not be started before a fork.

#### Scenario: Warmup precedes the first poll
- **WHEN** the entry point runs
- **THEN** `Process.warmup` SHALL be called after the last class declaration and before any activation is polled

#### Scenario: The prefork ordering is documented in one place
- **WHEN** the SDK page's startup section is read
- **THEN** it SHALL give the order declare → warm up → fork → start, and SHALL state that starting the core before a fork is unsupported

### Requirement: The reference runner image documents jemalloc

The reference runner image SHALL use jemalloc and the SDK documentation SHALL name heap fragmentation under long-lived workers as the reason. jemalloc SHALL NOT be a hard runtime requirement of the gem.

#### Scenario: The image ships it and the gem does not require it
- **WHEN** the reference image is built and, separately, the gem is installed on a host without jemalloc
- **THEN** the image SHALL run with jemalloc preloaded, and the gem SHALL install and run without it

### Requirement: The gem is licensed and marked as SDK-side

The gem SHALL carry the SDK-side licence the other SDKs carry, SHALL be recorded in the repository's licensing manifest, and SHALL ship the third-party notices for the native code it embeds.

#### Scenario: Licensing is declared, not implied
- **WHEN** the licensing manifest and the built gem are inspected
- **THEN** the gem SHALL be recorded SDK-side, and the notices for the embedded native library SHALL travel inside the gem

### Requirement: The source gem builds from the wrapper crate at the pinned core tag

The gem's `extconf.rb` SHALL build the wrapper crate in the gem's `ext/`
directory, whose one dependency is `flz_core_ffi` at the pinned core tag with
the `ruby` feature, after trying the precompiled library first. It SHALL name
no path outside the gem. When neither the precompiled library nor `cargo` and
`git` are available it SHALL refuse naming both and the precompiled gem as
the alternative. The precompiled platform gems the packaging requirement
already mandates SHALL be produced by the core release job for every
supported triple and Ruby minor.

#### Scenario: A source install with a toolchain builds
- **GIVEN** cargo and git on the host and no precompiled library
- **WHEN** the source gem installs
- **THEN** `extconf.rb` SHALL build the wrapper at the pinned tag and the runner SHALL start

#### Scenario: A source install without a toolchain refuses usefully
- **GIVEN** no cargo on the host and no precompiled library
- **WHEN** the source gem installs
- **THEN** it SHALL refuse naming cargo, git and the precompiled gem for the platform

