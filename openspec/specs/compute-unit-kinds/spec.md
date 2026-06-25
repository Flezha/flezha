# compute-unit-kinds Specification

## Purpose

The compute-unit-kinds capability lets the controller host more than one kind of compute unit on its generic relay control plane without forking that plane or scattering `if kind == …` special cases. Every relay carries a `kind` (an atom, default `:relay`, frozen for the relay's lifetime), and each kind declares a sparse, default-`false` capability descriptor in `Flezha.Relay.Kind` that the control plane consults to derive kind-divergent behaviour. The capability owns the kind field on `Flezha.Relay.Builder`, the `Flezha.Relay.Kind` source of truth (`capabilities/1`, `capable?/2`, `known?/1`), the unknown-kind rejection at start (`Flezha.Service.Validator.valid_kind/1`), and the one live capability gate today — per-partition state introspection, which fails with `{:error, {:unsupported_for_kind, :persistent_state}}` for a kind lacking `persistent_state?` instead of querying a compute unit that has no state. The default `:relay` kind is fully capable, so existing behaviour is preserved exactly; new capability flags are additive and do not affect kinds that do not opt in.

## Requirements
### Requirement: A relay declares a kind

Every relay SHALL carry a `kind` (an atom). The kind SHALL be set when the relay is started and SHALL be frozen for the relay's lifetime — it cannot change without a stop + start cycle. When a kind is not specified at start, it SHALL default to `:relay`. The recognized kinds are `:relay` and `:task`; starting a relay with any other kind SHALL fail with a clear error naming the unknown kind, and no relay process SHALL be created.

#### Scenario: Default kind is `:relay`
- **WHEN** a relay is started without an explicit kind
- **THEN** its kind SHALL be `:relay`

#### Scenario: Task is a recognized kind
- **WHEN** a relay is started with kind `:task`
- **THEN** the start SHALL be accepted and the relay's kind SHALL be `:task`

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

Introducing the task kind SHALL NOT change any observable behavior of a relay started without an explicit kind. The `:relay` kind being fully capable, the state-introspection gate SHALL pass, and every other kind-divergent path (partition-count quota, restore-on-boot, HotSwap state transfer, output-stall watchdog, convergence health) SHALL behave identically to the pre-change control plane — these paths self-gate on the presence of partitions/state. The `AddRelay` wire message MAY now carry an optional `kind` field; when the field is empty or absent it SHALL be interpreted as `:relay`, so existing relays and pre-change SDK builds are unaffected. The on-disk state layout, runtime configuration keys, and emitted telemetry for a `:relay` relay SHALL be unchanged, and the relay dispatch hot path SHALL be unchanged save a single per-frame match over the sealed unit registry.

#### Scenario: Existing relays are unaffected
- **WHEN** a relay is started, run, migrated, queried, and stopped without specifying a kind
- **THEN** its behavior SHALL be indistinguishable from the behavior before this change, including partition-quota enforcement, restore-on-boot, HotSwap migration, self-healing, and state queries

#### Scenario: Default kind is wire-backward-compatible
- **WHEN** a relay is started without an explicit kind, or a pre-change SDK omits the `kind` field
- **THEN** the `AddRelay` `kind` field SHALL be empty and SHALL be interpreted as `:relay`, and the relay SHALL behave exactly as before this change

### Requirement: A relay's kind is carried to the SDK on `AddRelay`

The controller SHALL transport a relay's `kind` to the SDK on the `AddRelay` wire message via an optional `kind` field populated from the relay's `:kind`. The field SHALL be backward-compatible: an empty or absent value SHALL be interpreted by the SDK as `:relay`. This is the single, kind-agnostic mechanism by which any kind (today `:relay` and `:task`) reaches the SDK; no per-kind wire message SHALL be required.

#### Scenario: Task kind travels on the wire
- **WHEN** the controller starts a relay whose kind is `:task` and issues `AddRelay`
- **THEN** the `AddRelay` `kind` field SHALL carry the task kind, and the SDK SHALL resolve it to spawn a task instance

#### Scenario: Absent kind field resolves to relay
- **WHEN** the SDK receives an `AddRelay` with an empty or absent `kind` field
- **THEN** it SHALL resolve the kind to `:relay`

