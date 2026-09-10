# node-sdk-packaging Specification

## Purpose
TBD - created by archiving change node-sdk. Update Purpose after archive.
## Requirements
### Requirement: The Node Veneer Is a Feature on the Existing ABI Crate

The Node binding SHALL be built from the existing C ABI crate under its own feature, beside the
Java shim, and SHALL be a one-to-one veneer over the same entry points. It SHALL NOT introduce a
second crate, a second copy of the ABI, or a kernel capability the C ABI does not expose.

Enabling the feature SHALL NOT alter the crate's existing artifacts or the binaries other SDKs ship.

#### Scenario: The veneer adds no ABI surface
- **WHEN** the Node veneer's entry points are compared with the C ABI's
- **THEN** each SHALL correspond to an existing entry point, and no new kernel capability SHALL be
  reachable through it

#### Scenario: The feature is additive
- **WHEN** the crate is built with and without the Node feature
- **THEN** the existing artifacts SHALL be unchanged and the Java shim SHALL be unaffected

### Requirement: Schema Version Is Negotiated Before Any Controller Connection

The binding SHALL pass its compiled-against activation schema version at context creation, and a
mismatch SHALL fail with an error naming both versions before any controller connection is
attempted.

#### Scenario: A skewed binary refuses to start
- **WHEN** the JavaScript package and the native binary were built against different schema versions
- **THEN** startup SHALL fail naming both versions, and no connection SHALL be attempted

### Requirement: Payloads Cross the Boundary Without an Intermediate Copy

User payload bytes SHALL be exposed to JavaScript without an intermediate allocation, and every
buffer the core hands out SHALL be freed through the ABI's free entry point before control returns,
so that no native buffer escapes into the JavaScript heap unmanaged.

#### Scenario: No native buffer escapes
- **WHEN** activations are dispatched and completed under sustained load
- **THEN** native memory SHALL stay bounded and every core-allocated buffer SHALL have been freed

### Requirement: Errors and Panics Map Onto Host Errors

Core errors SHALL surface as host errors preserving the engine's detail, and a panic crossing the
boundary SHALL become a host error rather than terminating the process.

#### Scenario: A core error carries its detail
- **WHEN** an entry point fails
- **THEN** the thrown error SHALL carry the core's recorded reason

### Requirement: Prebuilt Binaries Ship as Per-Platform Optional Dependencies

The package SHALL distribute native binaries as separate per-platform packages resolved through
optional dependencies, with a source build from a checkout as the fallback, so that installing the
SDK requires neither a Rust toolchain nor downloading binaries for platforms the user does not run.
Every per-platform package the SDK declares SHALL be produced by the build and published at the same
version as the parent package, and SHALL be published before it, so the parent never declares a
version that cannot yet resolve. The set of declared packages and the set the loader resolves SHALL derive from
one table, so a platform cannot be added to one and omitted from the other. When no platform
package resolved and no fallback is available, the SDK SHALL refuse at load with a message naming
the detected platform, the package it looked for, and how to build the binary locally.

Bundling every supported triple into one artifact is refused: it is the distribution shape already
recorded as owed a split, and this package has an established convention for doing it correctly from
the start.

#### Scenario: One platform's binary is installed
- **WHEN** the package is installed on a supported platform
- **THEN** only that platform's binary SHALL be fetched, and no toolchain SHALL be required

#### Scenario: An unsupported platform is told what a source build needs
- **WHEN** the package is installed on a platform no per-platform package is published for
- **THEN** the SDK SHALL name that platform as unpublished and state the source build's
  requirements, rather than naming a package that does not exist

#### Scenario: Every declared platform package is produced
- **WHEN** the build's published artefacts are enumerated
- **THEN** every package named in the SDK's optional dependencies SHALL be among them, at the parent's version

#### Scenario: A resolution failure is not silent
- **WHEN** the SDK is loaded on an installation where no platform package resolved and no fallback binary is present
- **THEN** it SHALL raise an error naming the detected platform, the expected package and the local build command, rather than surfacing a dynamic-loader error

#### Scenario: The shipped file list matches the documented fallback
- **WHEN** the package's file list is compared against the fallback the documentation describes
- **THEN** the fallback SHALL be present in the published package, or the documentation SHALL state that it is available only in a source checkout

### Requirement: One Binary Serves Every Supported Host Version

The binding SHALL target the stable ABI its host runtime provides, so that one binary per platform
serves every supported host version with no per-version rebuild. The supported host version window
SHALL be documented, and the package SHALL be consumable from both module systems.

#### Scenario: A host upgrade needs no rebuild
- **WHEN** the host runtime is upgraded within the supported window
- **THEN** the installed binary SHALL continue to load

#### Scenario: Both module systems import the package
- **WHEN** the package is imported from an ECMAScript module and required from a CommonJS module
- **THEN** both SHALL resolve and expose the same surface

