# go-sdk-packaging Specification

## Purpose
How the Go SDK reaches the core: the plain C ABI over cgo, linked statically into one executable, with the standard library only, built and tested from its own tree.
## Requirements
### Requirement: The Go binding is the plain C ABI over cgo
The Go SDK SHALL call the `flz_core_ffi` C entry points through cgo, with no Rust glue feature, no new Rust crate and no Go implementation of the protocol. The entry points it declares SHALL be asserted against `test/fixtures/abi_surface.txt` by a test in the SDK's own suite, so a symbol the core removes or renames fails that test.

#### Scenario: The declared surface matches the fixture
- **WHEN** the Go SDK's declared C entry points are compared with `abi_surface.txt`
- **THEN** every declared entry SHALL be in the fixture, and a declared entry the fixture lacks SHALL fail the test naming it

### Requirement: The Go runner links the core statically into one executable
The Go SDK SHALL link the crate's `staticlib`, `libflz_core_ffi.a`, with the library directory supplied by the build through `CGO_LDFLAGS`, and SHALL name in its cgo directives the system libraries the core needs, taken from the Rust toolchain's report for that target. A built Go runner SHALL NOT need `libflz_core_ffi` at run time.

#### Scenario: The runner needs no core library at run time
- **WHEN** the Go test app is built by `scripts/build-go-sdk.sh` and its dynamic dependencies are listed
- **THEN** no `libflz_core_ffi` SHALL be among them, and the runner SHALL start from a directory holding only itself

#### Scenario: A missing library names the setting
- **WHEN** the SDK is built without a directory holding `libflz_core_ffi.a`
- **THEN** the SDK guide's build section SHALL name `CGO_LDFLAGS` and the build script that sets it, and the link error SHALL name `flz_core_ffi`

### Requirement: The Go SDK depends on the standard library only
The Go SDK's module SHALL require no module beyond the Go standard library, and its `go.mod` SHALL declare the toolchain floor: the older of the two Go releases the Go project supports on the day the change is applied.

#### Scenario: The module requires nothing
- **WHEN** `go.mod` is read
- **THEN** it SHALL carry a `go` directive at the floor and no `require` line

### Requirement: The Go SDK is developed and tested from its own tree
The SDK SHALL live in its own repository, `flezha/flezha_go_sdk`, pinned as a submodule at `sdk/go` with `test/apps/go_runner` a symlink to its test app, and SHALL carry the three tiers an SDK repository runs without a controller, resolving the contract set through `$FLEZHA_CONTRACT_DIR`, then `contract/`, then the tree the module sits in, and failing rather than skipping when none resolves.

#### Scenario: The suite runs in the SDK tree without a controller
- **WHEN** `go test ./...` runs in `sdk/go` with the core library built
- **THEN** the unit, fixture and fake-controller tiers SHALL run and no controller SHALL be started

#### Scenario: A missing contract set fails
- **WHEN** the suite runs with no contract set reachable
- **THEN** the fixture tests SHALL fail naming the three places searched

