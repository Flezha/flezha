# sdk-core-abi Specification

## Purpose
TBD - created by archiving change extract-sdk-poll-core. Update Purpose after archive.
## Requirements
### Requirement: C ABI surface and lifecycle

`flz_core_ffi` SHALL expose the core over a C ABI: context create/destroy, configure,
start, `poll` (blocking, with timeout), `complete`, shutdown, `last_error`, and a log
callback. Ownership follows the existing FFI convention: whoever allocates frees;
buffers returned by the core are freed via a core-provided free function; the context
pointer is opaque.

#### Scenario: Shutdown wakes blocked pollers
- **WHEN** host workers are blocked in `poll` and shutdown is requested
- **THEN** every blocked `poll` SHALL return promptly with a distinguishable "shutting down" result, and no activation SHALL be lost (unpolled work follows the existing durability rules)

#### Scenario: Poll honors its timeout
- **WHEN** `poll` is called with a 100 ms timeout and no work arrives
- **THEN** it SHALL return an empty/timeout result within a bounded margin of 100 ms rather than blocking indefinitely

### Requirement: Versioned activation schema with boot-time refusal

The Activation/ActivationResult schema SHALL carry a version. Core and host shim SHALL
negotiate at context creation and SHALL refuse to start on an incompatible version with
an error naming both versions. Silent runtime skew is forbidden. Every activation kind
SHALL be pinned by a cross-language byte fixture shared between core tests and each
shim's tests, extending the existing fixture discipline (`flusher.rs` ↔ `wire_test.exs`).

#### Scenario: Version mismatch fails at boot
- **WHEN** a shim built against schema version N connects to a core speaking N+1 incompatibly
- **THEN** context creation SHALL fail with an error naming N and N+1, and no connection to the controller SHALL be attempted

### Requirement: Thread-safety contract

The core SHALL accept `poll` from any host thread and SHALL accept `complete` from any
host thread, including a different thread than the one that polled the activation. The
core SHALL NOT require thread affinity from the host.

#### Scenario: Complete from a different thread
- **WHEN** worker A polls an activation and hands it to worker B, which completes it
- **THEN** the completion SHALL be accepted and the entity released, identically to same-thread completion

### Requirement: The C ABI cannot lag the crate API

The Rust SDK SHALL consume `flz_core`'s Rust API (the same crate the C ABI wraps), and
CI SHALL drive at least one full runner scenario through `flz_core_ffi` itself. A
capability reachable through the crate API but not through the C ABI is a defect of
this capability, not a backlog item.

#### Scenario: FFI parity is tested, not assumed
- **WHEN** the SDK test suite runs in CI
- **THEN** it SHALL include a scenario that connects, processes input activations, fires a timer, and shuts down entirely through the C ABI, failing if any required entry point is missing

### Requirement: The declaration surface covers the full host layer

`flz_core_declare_class` SHALL accept the complete host-class declaration:
entity mode (single/static with keys, dynamic with TTL and cap), clock
kind, correction window and beyond-window policy, delivery tier
(effectively-once, checkpoint ack-granularity), inter-procedure channels
with edge ids and keying markers, per-channel retry and overflow
policies, the record-header lang/format bytes, persistence, and workflow
lifecycle facts (loop mode, rollback mode, retry, wake-after,
compensable procs). Timer caps stay host-layer policy — in both SDKs the
cap is enforced where the registration API lives, not in the core. A declaration
the core cannot honor SHALL be refused at declaration time with an error
naming the field — never accepted and degraded silently.

#### Scenario: A full declaration round-trips into controller metadata
- **WHEN** a shim declares a checkpoint-ack, correction-windowed, dynamic-mode class through the ABI and the controller sends `AddOperative`
- **THEN** the `AddOperativeResponse` metadata SHALL carry the same declarations the typed Rust path would produce for an identical class

#### Scenario: An unsupported declaration is refused loudly
- **WHEN** a shim declares a field combination the host layer cannot serve
- **THEN** `flz_core_declare_class` SHALL return an error naming the offending field, and the class SHALL NOT be registered

### Requirement: Widened host capabilities are reachable through the ABI

Every capability the widened host layer supports SHALL be exercisable
through `flz_core_ffi` alone — the five command activation kinds, the
prepare crossing, entity modes, tiers, timers, inter-procedure routing,
and workflow lifecycles. The CI scenario suite SHALL grow to drive at least:
a restore fold, a live state read, a keyed dynamic route via the prepare
crossing, an effectively-once completion with offsets, and a workflow
run-sugar lifecycle — all through the C entry points, failing if any
required surface is missing.

#### Scenario: A restore fold crosses the ABI
- **WHEN** the scenario suite ships a persisted change log for an ABI-declared entity and the controller path delivers `RestoreInitState`
- **THEN** a `RestoreFold` activation SHALL be pollable through `flz_core_poll`, its completion SHALL unblock the entity, and the entity's next input activation SHALL observe the folded state's effects

#### Scenario: The Java shim is a consumer, not a fork
- **WHEN** the Java SDK's JNI bridge is compared against the ABI's exported symbol set
- **THEN** it SHALL call only `flz_core_ffi` entry points — any Java-reachable kernel capability that bypasses the ABI is a defect of this capability
