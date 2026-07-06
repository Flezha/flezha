# compute-unit-kinds Specification

## Purpose

The compute-unit-kinds capability defines the sparse, default-`false` **capability descriptor** model that lets the controller derive kind-divergent behaviour without forking the control plane or scattering `if kind == …` special cases. Each operative kind declares a sparse capability descriptor in `Flezha.Operative.Kind` that the control plane consults (`capabilities/1`, `capable?/2`); the control plane reads every capability with a default of `false`. The capability owns the one live capability gate today — per-segment state introspection, which fails with `{:error, {:unsupported_for_kind, :persistent_state}}` for a kind lacking `persistent_state?` instead of querying an operative that has no state. The kind discriminator itself (every operative carrying a `kind`, the recognized `:unbounded`/`:bounded` values, and the wire transport of the kind on `AddOperative`) is owned by the `operative-model` capability. New capability flags are additive and do not affect kinds that do not opt in.

## Requirements
### Requirement: Each kind declares a sparse, default-false capability descriptor

There SHALL be a single source of truth mapping a kind to a **sparse** capability descriptor — a map that declares only the capabilities the kind *has* (as `true`). The control plane SHALL read every capability with a default of `false`, so any capability a kind does not declare SHALL be treated as absent. Capabilities SHALL attach to the kind, not to user-authored operative code: choosing a kind SHALL be sufficient, and no SDK author SHALL be required to enumerate capability flags. This change defines exactly one capability, `persistent_state?` (the kind has per-segment durable state: write-ahead log, snapshots, restore, time-travel, and per-segment state introspection). The `:unbounded` kind SHALL declare `persistent_state?` `true`.

#### Scenario: `:unbounded` declares persistent state
- **WHEN** the capability descriptor for kind `:unbounded` is requested
- **THEN** reading `persistent_state?` SHALL yield `true`

#### Scenario: Undeclared capabilities read as false
- **WHEN** the control plane reads a capability that a kind's descriptor does not declare
- **THEN** the value SHALL be `false`, and the descriptor SHALL NOT be required to list every defined capability

### Requirement: The capability model is extensible without affecting existing kinds

Introducing a new capability flag SHALL be additive: it SHALL require only defining the flag, adding the gate that reads it with a `false` default, and opting in the kinds that need it. A kind that does not opt into a newly-introduced capability SHALL be unaffected and SHALL continue to read that capability as `false`. No change to a new capability SHALL force edits to unrelated kinds or to user-authored operative code.

#### Scenario: A new flag defaults off for kinds that do not opt in
- **WHEN** a new capability flag is added to the model and a kind's descriptor does not declare it
- **THEN** that kind SHALL read the new capability as `false`, and its existing behavior SHALL be unchanged

### Requirement: State introspection is gated by `persistent_state?` with a structured error

A request for an operative's per-segment state SHALL succeed only when the operative's kind reads `persistent_state?` as `true`. When the capability is absent, the request SHALL fail with the structured error `{:error, {:unsupported_for_kind, :persistent_state}}` rather than querying the operative or returning an empty or misleading result. The error SHALL be distinguishable from "operative not found" and from a transient query failure, and SHALL surface through the CLI as a clear, non-zero outcome.

#### Scenario: State query succeeds for a stateful kind
- **WHEN** the per-segment state of an operative whose kind has `persistent_state? = true` is requested
- **THEN** the request SHALL return the serialized state exactly as it does today

#### Scenario: State query is rejected for a stateless kind
- **WHEN** the per-segment state of an operative whose kind has `persistent_state? = false` is requested
- **THEN** the request SHALL return `{:error, {:unsupported_for_kind, :persistent_state}}` and SHALL NOT contact the operative for state
