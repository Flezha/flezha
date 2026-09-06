# convergence-recovery-gate Specification

## Purpose

The convergence-recovery-gate capability defines a single, authoritative query API for hub recovery state within the convergence subsystem. It isolates the "is this namespace ready for healing?" decision behind `Flezha.Convergence.RecoveryGate`, keeping `Probe` and `Loop` free of inline recovery-state logic and ensuring that a hub stuck in `:recovery_timeout` does not permanently block healing for its namespace.
## Requirements
### Requirement: `RecoveryGate` is the single query API for hub recovery state

`Flezha.Convergence.RecoveryGate` SHALL be a pure query module (no GenServer, no ETS ownership) that reads `Flezha.Convergence.Snapshot`'s existing ETS table. It SHALL expose three functions:

- `ready_namespaces(intent)` — given an `Intent` struct, returns a `MapSet` of namespace identifiers whose associated hubs are in a state where healing is permitted (`:normal | :skipped | :timeout`). For `:timeout` hubs it SHALL emit one `Logger.warning` per namespace per call.
- `await_recovery_complete(hub_ids, timeout)` — blocks the caller until every hub in `hub_ids` reaches `:normal | :skipped`, or `timeout` ms elapses. Returns `:ok` or `{:error, {:recovery_timeout, [hub_ids]}}`. This function is moved from `Flezha.Convergence.Probe`.
- `observe_state(hub_id, state)` — thin delegation to `Flezha.Convergence.Snapshot.observe_recovery/2`, provided so callers that currently call `Snapshot` for this procedure have a consistent recovery-gate import.

No other module other than `RecoveryGate` SHALL compute "is this namespace ready for healing?" from recovery state. `Flezha.Convergence.Loop` and `Flezha.Convergence.Probe` SHALL delegate all recovery-gate queries to `RecoveryGate`.

#### Scenario: Normal hub is included in ready set

- **WHEN** `RecoveryGate.ready_namespaces/1` is called and hub `ns1` has recovery state `:normal`
- **THEN** the returned `MapSet` contains `"ns1"`

#### Scenario: Recovering hub is excluded from ready set

- **WHEN** `RecoveryGate.ready_namespaces/1` is called and hub `ns1` has recovery state `:recovering`
- **THEN** the returned `MapSet` does not contain `"ns1"`
- **AND** no heal ops are issued for operatives in that namespace on the current tick

#### Scenario: Timed-out hub is included with a warning

- **WHEN** `RecoveryGate.ready_namespaces/1` is called and hub `ns1` has recovery state `:timeout`
- **THEN** the returned `MapSet` contains `"ns1"`
- **AND** a `Logger.warning` is emitted identifying `ns1` as timed-out

#### Scenario: `await_recovery_complete` resolves when all hubs normalise

- **WHEN** `RecoveryGate.await_recovery_complete([:hub_a, :hub_b], 30_000)` is called
- **AND** `:hub_a` reaches `:normal` after 200 ms and `:hub_b` after 500 ms
- **THEN** the function returns `:ok` after ≈500 ms

#### Scenario: `await_recovery_complete` times out when a hub is stuck

- **WHEN** `RecoveryGate.await_recovery_complete([:hub_a], 500)` is called
- **AND** `:hub_a` remains in `:recovering` for the full 500 ms
- **THEN** the function returns `{:error, {:recovery_timeout, [:hub_a]}}`

### Requirement: `RecoveryGate` SHALL own the "is any hub recovering" predicate

`Flezha.Convergence.RecoveryGate` SHALL expose the predicate that answers whether any hub is in ProcessHub's recovery state machine, and SHALL be the only module that decides it. The predicate SHALL be public — `any_recovering?/0,1` over the recovery-state cache, and a companion `recovery_summary/0` that names the state a reader should report — so the health endpoint, the convergence tick summary and the boot readiness table read one fact rather than each carrying its own copy of the member list.

The member list that defines "recovering" SHALL be written in exactly one place. No other module SHALL restate it.

This predicate SHALL remain distinct from `ready_namespaces/1`. `ready_namespaces/1` answers whether the convergence loop may heal a namespace and deliberately admits a `:timeout` hub; `any_recovering?/0,1` answers whether recovery is still in progress and counts a `:timeout` hub as recovering. A hub that timed out is healable and is not recovered.

#### Scenario: One member list, one module

- **WHEN** the tree is searched for the recovery-state member list that defines "recovering"
- **THEN** it SHALL appear once, inside `RecoveryGate`, and no other module SHALL carry a private predicate over the same states

#### Scenario: A timed-out hub is recovering but healable

- **GIVEN** a hub whose recovery has reached `:timeout`
- **WHEN** `ready_namespaces/1` and `any_recovering?/0` are each called
- **THEN** `ready_namespaces/1` SHALL include the namespace and `any_recovering?/0` SHALL answer true

