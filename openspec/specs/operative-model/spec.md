# operative-model Specification

## Purpose

The operative-model capability is the canonical vocabulary and kind model for Flezha's compute workloads. It defines the **Operative** umbrella (the generic top-level unit of work a Runner runs at a Controller's direction), its two kinds today — **Unbounded** (runs-forever) and **Bounded** (runs-to-completion) — and the per-Operative concepts of **Procedure** (a typed stage) and **Entity** (the per-key, state-bearing slice). It mandates the no-synonyms rule (no `relay`, `shard`, `partition`, or `task` survives in any shipped surface) and the sparse, default-`false` capability descriptor model that lets new kinds and capabilities be added additively without touching existing kinds. Capabilities attach to the kind, not to user computation code; `persistent_state?` gates per-Entity state introspection. The unified `flz operative` CLI verb tree manages every kind's lifecycle.
## Requirements
### Requirement: Canonical operative vocabulary

The system SHALL use exactly these terms for compute workloads, and code, modules, protobuf, wire payload-ids, CLI, config keys, env vars, documentation, and on-disk path components SHALL NOT introduce or retain the synonyms `relay`, `shard`, `partition`, `task`, or `continuous`:

- **Operative** — the generic top-level unit of work that a Runner runs at a Controller's direction; the thing operators start and stop. Every Operative has a `kind`.
- **Unbounded Operative** — the operative kind that runs with no inherent end, folding an ongoing stream of input until it is stopped. This is the workload formerly called "Relay".
- **Bounded Operative** — the operative kind that runs to completion and finishes. This is the workload formerly called "Task".
- **Procedure** — a typed stage within an Operative that processes inputs from a Channel. Formerly "Shard".
- **Entity** — the per-key, state-bearing slice of a Procedure; durable state lives at the Entity level. Formerly "Partition".
- **Controller**, **Runner**, and **Channel** are unchanged. "Operator" remains reserved for ProcessHub-internal ring-node jargon and is never used for an Operative.

#### Scenario: Documentation and glossary use operative vocabulary

- **WHEN** any spec, doc under `docs/`, `docs/glossary.md`, or `CLAUDE.md` references the top-level unit of work, its stage, or its per-key slice
- **THEN** it SHALL use "Operative" (with kind "Unbounded" or "Bounded"), "Procedure", and "Entity" respectively, and SHALL NOT use "Relay", "Shard", "Partition", "Task", or "Continuous"

#### Scenario: No synonym survives in shipped surfaces

- **WHEN** the controller, SDKs, protobuf, CLI, config, and env-var surfaces are inspected after this change
- **THEN** no identifier, message name, field name, wire payload-id string, CLI verb, config key, or env var SHALL contain `relay`, `shard`, `partition`, `task`, or `continuous` as the name of a workload concept

### Requirement: An operative declares a kind

Every Operative SHALL carry a `kind` atom. The kind SHALL be set when the operative is started and SHALL be frozen for its lifetime — it cannot change without a stop + start cycle. When a kind is not specified at start, it SHALL default to `:unbounded`. The recognized kinds are `:unbounded` and `:bounded`; starting an operative with any other kind SHALL fail with a clear error naming the unknown kind, and no operative process SHALL be created. On the wire, the accepted set of `kind` string literals SHALL be exactly `""` (empty ⇒ the `:unbounded` default), `"unbounded"`, and `"bounded"`; the SDK SHALL reject any other literal with an error naming the unknown kind.

#### Scenario: Default kind is `:unbounded`

- **WHEN** an operative is started without an explicit kind
- **THEN** its kind SHALL be `:unbounded`

#### Scenario: Bounded is a recognized kind

- **WHEN** an operative is started with kind `:bounded`
- **THEN** the start SHALL be accepted and the operative's kind SHALL be `:bounded`

#### Scenario: Kind is frozen for the operative's lifetime

- **WHEN** an operative is running with a given kind
- **THEN** there SHALL be no procedure that mutates the kind of a running operative; changing kind requires stopping and re-starting it

#### Scenario: Unknown kind is rejected at start

- **WHEN** an operative is started with a kind that is not recognized (any wire literal other than `""`, `"unbounded"`, or `"bounded"`)
- **THEN** the start SHALL fail with an error that names the unknown kind, and no operative process SHALL be created

### Requirement: Each kind declares a sparse, default-false capability descriptor

There SHALL be a single source of truth mapping a kind to a **sparse** capability descriptor — a map declaring only the capabilities the kind *has* (as `true`). The control plane SHALL read every capability with a default of `false`. Capabilities SHALL attach to the kind, not to user-authored computation code.

The previously-coarse `persistent_state?` capability SHALL be **split** so that it means exactly "has durable per-Entity WAL state + restore", and the unbounded-only behaviors that previously rode on it SHALL each have their own default-`false` flag:

- `persistent_state?` — durable per-Entity WAL + snapshot + restore. Resolved per kind+class: `:unbounded` declares it, and `:bounded` declares it **by default** unless its class opts out of persistence (see `bounded-operative-durability`).
- `time_travel?` — point-in-time queries and retention beyond the latest snapshot (gates `flz state at`). Declared by `:unbounded` only.
- `dynamic_entities?` — lazy per-key Entity creation and destructive idle eviction with cold-start-fresh. Declared by `:unbounded` only.
- `output_stall_watch?` — the per-Entity output-staleness watchdog that forces a rebind. Declared by `:unbounded` only.

#### Scenario: Unbounded declares the full persistence surface

- **WHEN** the capability descriptor for kind `:unbounded` is requested
- **THEN** `persistent_state?`, `time_travel?`, `dynamic_entities?`, and `output_stall_watch?` SHALL all read `true`

#### Scenario: A durable bounded operative has durable state without unbounded-only behaviors

- **WHEN** a `:bounded` class that has not opted out of persistence is read
- **THEN** `persistent_state?` SHALL read `true` while `time_travel?`, `dynamic_entities?`, and `output_stall_watch?` SHALL each read `false`

#### Scenario: An opted-out bounded operative declares no durable state

- **WHEN** a `:bounded` class that opted out of persistence is read
- **THEN** `persistent_state?` SHALL read `false`

#### Scenario: Undeclared capabilities read as false

- **WHEN** the control plane reads a capability that a kind's descriptor does not declare
- **THEN** the value SHALL be `false`, and the descriptor SHALL NOT be required to list every defined capability

### Requirement: The capability model is extensible without affecting existing kinds

Introducing a new operative kind OR a new capability flag SHALL be additive: it SHALL require only declaring the new kind/flag and opting in the kinds that need it. A kind that does not opt into a newly-introduced capability SHALL be unaffected and SHALL continue to read that capability as `false`. Adding a third (or later) kind SHALL NOT force edits to the existing `:unbounded` or `:bounded` kinds, to user-authored computation code, or to the routing boundary beyond registering the new kind.

#### Scenario: A new flag defaults off for kinds that do not opt in

- **WHEN** a new capability flag is added and a kind's descriptor does not declare it
- **THEN** that kind SHALL read the new capability as `false`, and its existing behavior SHALL be unchanged

#### Scenario: A future kind is additive

- **WHEN** a new operative kind is registered with its own capability descriptor
- **THEN** the existing kinds SHALL be unaffected, and the new kind SHALL be selectable at start without changing the existing kinds' behavior

### Requirement: Entity-state introspection is gated by `persistent_state?`

A request for an operative's per-Entity state SHALL succeed only when the operative's kind+class reads `persistent_state?` as `true` (true for `:unbounded`, and for a `:bounded` class that has not opted out of persistence). When the capability is absent, the request SHALL fail with the structured error `{:error, {:unsupported_for_kind, :persistent_state}}` rather than querying a computation that has no durable state. Point-in-time `flz state at` queries SHALL additionally require `time_travel?`, so a durable bounded operative (which does not declare `time_travel?`) SHALL expose current Entity state but SHALL reject time-travel queries.

#### Scenario: Entity-state query succeeds for a durable operative

- **WHEN** the per-Entity state of an operative whose kind+class declares `persistent_state? = true` is requested
- **THEN** the request SHALL return the serialized current state

#### Scenario: Entity-state query is rejected for a stateless operative

- **WHEN** the per-Entity state of an operative whose kind+class reads `persistent_state? = false` is requested
- **THEN** the request SHALL return `{:error, {:unsupported_for_kind, :persistent_state}}` and SHALL NOT contact the computation

#### Scenario: Time-travel is rejected for a kind lacking `time_travel?`

- **WHEN** a `flz state at` point-in-time query targets a durable bounded operative (which does not declare `time_travel?`)
- **THEN** the query SHALL be rejected as unsupported, while a current-state query SHALL still succeed

### Requirement: Unified `flz operative` CLI verb

The CLI SHALL expose a single `flz operative` verb tree for lifecycle management of every operative kind: `flz operative start --kind <unbounded|bounded>`, `flz operative stop`, `flz operative list`, and `flz operative info`. `--kind` SHALL default to `unbounded`. There SHALL be no `flz relay` verb.

#### Scenario: Start an unbounded operative by default

- **WHEN** the operator runs `flz operative start <class>` without `--kind`
- **THEN** an unbounded operative SHALL be started

#### Scenario: Start a bounded operative

- **WHEN** the operator runs `flz operative start <class> --kind bounded`
- **THEN** a bounded operative SHALL be started

#### Scenario: The legacy verb is gone

- **WHEN** the operator runs `flz relay start`
- **THEN** the CLI SHALL reject it as an unknown command

