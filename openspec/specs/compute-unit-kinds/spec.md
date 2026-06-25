# compute-unit-kinds Specification

## Purpose

The compute-unit-kinds capability lets the controller host more than one kind of compute unit on its generic relay control plane without forking that plane or scattering `if kind == …` special cases. Every relay carries a `kind` (an atom, default `:relay`, frozen for the relay's lifetime), and each kind declares a sparse, default-`false` capability descriptor in `Flezha.Relay.Kind` that the control plane consults to derive kind-divergent behaviour. The capability owns the kind field on `Flezha.Relay.Builder`, the `Flezha.Relay.Kind` source of truth (`capabilities/1`, `capable?/2`, `known?/1`), the unknown-kind rejection at start (`Flezha.Service.Validator.valid_kind/1`), and the one live capability gate today — per-partition state introspection, which fails with `{:error, {:unsupported_for_kind, :persistent_state}}` for a kind lacking `persistent_state?` instead of querying a compute unit that has no state. The default `:relay` kind is fully capable, so existing behaviour is preserved exactly; new capability flags are additive and do not affect kinds that do not opt in.

## Requirements
### Requirement: A relay declares a kind

Every relay SHALL carry a `kind` (an atom). The kind SHALL be set when the relay is started and SHALL be frozen for the relay's lifetime — it cannot change without a stop + start cycle. When a kind is not specified at start, it SHALL default to `:relay`. In this change, `:relay` is the only valid kind; starting a relay with any other kind SHALL fail with a clear error naming the unknown kind, and no relay process SHALL be created.

#### Scenario: Default kind is `:relay`
- **WHEN** a relay is started without an explicit kind
- **THEN** its kind SHALL be `:relay`

#### Scenario: Kind is frozen for the relay's lifetime
- **WHEN** a relay is running with a given kind
- **THEN** there SHALL be no operation that mutates the kind of a running relay; changing kind requires stopping and re-starting the relay

#### Scenario: Unknown kind is rejected at start
- **WHEN** a relay is started with a kind that is not recognized
- **THEN** the start SHALL fail with an error that names the unknown kind, and no relay process SHALL be created

### Requirement: Each kind declares a sparse, default-false capability descriptor

There SHALL be a single source of truth mapping a kind to a **sparse** capability descriptor — a map that declares only the capabilities the kind *has* (as `true`). The control plane SHALL read every capability with a default of `false`, so any capability a kind does not declare SHALL be treated as absent. Capabilities SHALL attach to the kind, not to user-authored compute-unit code: choosing a kind SHALL be sufficient, and no SDK author SHALL be required to enumerate capability flags. This change defines exactly one capability, `persistent_state?` (the kind has per-partition durable state: write-ahead log, snapshots, restore, time-travel, and per-partition state introspection). The `:relay` kind SHALL declare `persistent_state?` `true`.

#### Scenario: `:relay` declares persistent state
- **WHEN** the capability descriptor for kind `:relay` is requested
- **THEN** reading `persistent_state?` SHALL yield `true`

#### Scenario: Undeclared capabilities read as false
- **WHEN** the control plane reads a capability that a kind's descriptor does not declare
- **THEN** the value SHALL be `false`, and the descriptor SHALL NOT be required to list every defined capability

### Requirement: The capability model is extensible without affecting existing kinds

Introducing a new capability flag SHALL be additive: it SHALL require only defining the flag, adding the gate that reads it with a `false` default, and opting in the kinds that need it. A kind that does not opt into a newly-introduced capability SHALL be unaffected and SHALL continue to read that capability as `false`. No change to a new capability SHALL force edits to unrelated kinds or to user-authored compute-unit code.

#### Scenario: A new flag defaults off for kinds that do not opt in
- **WHEN** a new capability flag is added to the model and a kind's descriptor does not declare it
- **THEN** that kind SHALL read the new capability as `false`, and its existing behavior SHALL be unchanged

### Requirement: State introspection is gated by `persistent_state?` with a structured error

A request for a relay's per-partition state SHALL succeed only when the relay's kind reads `persistent_state?` as `true`. When the capability is absent, the request SHALL fail with the structured error `{:error, {:unsupported_for_kind, :persistent_state}}` rather than querying the compute unit or returning an empty or misleading result. The error SHALL be distinguishable from "relay not found" and from a transient query failure, and SHALL surface through the CLI as a clear, non-zero outcome.

#### Scenario: State query succeeds for a stateful kind
- **WHEN** the per-partition state of a relay whose kind has `persistent_state? = true` is requested
- **THEN** the request SHALL return the serialized state exactly as it does today

#### Scenario: State query is rejected for a stateless kind
- **WHEN** the per-partition state of a relay whose kind has `persistent_state? = false` is requested
- **THEN** the request SHALL return `{:error, {:unsupported_for_kind, :persistent_state}}` and SHALL NOT contact the compute unit for state

### Requirement: The default `:relay` kind preserves existing behavior exactly

Introducing the kind dimension and the capability model SHALL NOT change any observable behavior of a relay started without an explicit kind. The `:relay` kind being fully capable, the state-introspection gate SHALL pass, and every other kind-divergent path (partition-count quota, restore-on-boot, HotSwap state transfer, output-stall watchdog, convergence health) SHALL behave identically to the pre-change control plane — these paths self-gate on the presence of partitions/state and are not modified by this change. There SHALL be no change to the wire protocol, the SDK, the CLI commands, the on-disk layout, configuration, or telemetry.

#### Scenario: Existing relays are unaffected
- **WHEN** a relay is started, run, migrated, queried, and stopped without specifying a kind
- **THEN** its behavior SHALL be indistinguishable from the behavior before this change, including partition-quota enforcement, restore-on-boot, HotSwap migration, self-healing, and state queries

#### Scenario: No external surface changes
- **WHEN** this change is applied
- **THEN** the `AddRelay` wire message, the Rust and Java SDKs, the CLI commands, the on-disk state layout, the runtime configuration keys, and the emitted telemetry SHALL be unchanged

