# operative-migration-consent Specification

## Purpose

The operative-migration-consent capability makes Overwatch a participant in ProcessHub's migration-consent protocol so a cluster redistribution (node join/leave, convergence rebind, graceful-shutdown drain) can no longer tear an Overwatch out mid-critical-work. Overwatch answers `:defer` while a late-input correction is pending, while a durable workflow instance is between commit barriers, or while an effectively-once instance has unflushed outbox items — and signals `ProcessHub.Service.Migration.migration_ready/2` at its existing safe barriers (suspension, correction completion) so deferred moves resume promptly. `Flezha.Service.HubManager` enables the protocol via `consent_settings` on the HotSwap strategy, with the deferral ceiling derived from the graceful-shutdown budget and each knob env-overridable. A deferred operative that exhausts its deferral window is force-migrated through the existing best-effort handover and lands on the existing recovery paths (RestoreInitState/baseline pull, task-journal fold, the workflow ack timeout → compensation → re-placement) with no new wire message.
## Requirements
### Requirement: Overwatch SHALL participate in ProcessHub's migration-consent protocol

`Flezha.Operative.Overwatch` SHALL `use ProcessHub.Strategy.Migration.MigrationConsent` and override `migration_consent/1`. The callback SHALL answer over the Overwatch's own GenServer state (plus at most one bounded-time pending-outbox query) and SHALL return:

- `:defer` while a late-input correction is pending (`corrections != %{}`),
- `:defer` while the instance journals its progress stage by stage and is awake (a persistent workflow that is not sleeping: `durable_workflow and not suspended`), because a pass in flight is not a state document a handover can ship,
- `:defer` while an effectively-once instance has pending (unflushed) outbox items in any of its live WalWriters,
- `:ready` otherwise, including for a sleeping instance of either kind, and for an awake stream whether or not its class declares `sleep_when_idle`, which migrates in place through the handover.

A failure of the pending-outbox query SHALL be treated as `:ready` (fail-open: worst case equals pre-consent behaviour). While the Overwatch is synchronously executing a correction/replay transaction it cannot answer the consent query; the protocol's no-reply-within-`consent_timeout` path SHALL be relied upon to defer it — no additional in-flight marker is introduced.

#### Scenario: Mid-correction operative defers instead of migrating

- **GIVEN** an Overwatch with a correction request coalescing in `corrections` (debounce window open)
- **WHEN** a cluster redistribution asks it for migration consent
- **THEN** `migration_consent/1` returns `:defer` and the child is parked in the hub's deferred list instead of migrating

#### Scenario: Unsuspended durable workflow defers; suspended one consents

- **GIVEN** two durable workflow Overwatches, one mid-iteration (bound to a runner, not suspended) and one suspended at a committed iteration boundary
- **WHEN** both are asked for migration consent
- **THEN** the unsuspended one returns `:defer` and the suspended one returns `:ready`

#### Scenario: A sleeping stream consents

- **GIVEN** a stream Overwatch whose class declares `sleep_when_idle` and which is suspended
- **WHEN** it is asked for migration consent
- **THEN** it returns `:ready`, and the next bind on the destination node wakes it from its durable state

#### Scenario: Effectively-once instance with pending outbox defers

- **GIVEN** an effectively-once Overwatch whose cell WalWriter reports pending outbox items not yet flushed downstream
- **WHEN** it is asked for migration consent
- **THEN** it returns `:defer`
- **AND** after the outbox drains, the next consent re-query (retry tick) receives `:ready` and the child migrates

#### Scenario: Idle stream operative migrates immediately

- **GIVEN** a stream Overwatch with no correction pending and no pending outbox, awake, whether or not its class can sleep
- **WHEN** it is asked for migration consent
- **THEN** it returns `:ready` and migrates through the regular HotSwap handover path with no deferral

### Requirement: Overwatch SHALL signal early migration readiness at its existing safe barriers

`ProcessHub.Service.Migration.migration_ready/2` SHALL be called from exactly two places in `Overwatch`: (a) `enter_suspended/2`, when an instance of either kind went to sleep, and (b) correction completion when `corrections` becomes empty. Both calls SHALL tolerate the `{:error, :not_deferred}` no-op result (no migration was waiting). No new completion-notification seam SHALL be added for the WAL/outbox condition; it relies on the consent re-query performed on every deferred-list retry tick.

#### Scenario: Deferred workflow operative migrates promptly on suspension

- **GIVEN** a durable workflow Overwatch parked in the deferred list because it was mid-iteration
- **WHEN** the instance parks (`IterPark`) and `enter_suspended/2` runs
- **THEN** `migration_ready/2` is called and the child migrates on the immediate retry tick rather than waiting a full `retry_interval`

#### Scenario: migration_ready outside a migration is a harmless no-op

- **GIVEN** an Overwatch suspending while no redistribution is in progress
- **WHEN** `enter_suspended/2` calls `migration_ready/2`
- **THEN** the call returns `{:error, :not_deferred}` and suspension proceeds unaffected

### Requirement: HubManager SHALL enable consent with settings derived from the shutdown budget

The `HotSwap` migration strategy in `Flezha.Service.HubManager` SHALL carry `consent_settings` with defaults `consent_timeout: 5_000`, `retry_interval: 2_000`, and `max_defer_time` equal to the resolved `:graceful_shutdown_timeout_ms` (60 000 default). Each value SHALL be overridable via runtime config/env: `FLEZHA_MIGRATION_CONSENT_TIMEOUT_MS`, `FLEZHA_MIGRATION_RETRY_INTERVAL_MS`, `FLEZHA_MIGRATION_MAX_DEFER_MS`.

#### Scenario: Defaults bound deferral by the shutdown window

- **GIVEN** a controller booted with no migration-consent env overrides and `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS` unset
- **WHEN** the namespace hub starts
- **THEN** its migration strategy carries `consent_settings` with `consent_timeout` 5 000 ms, `retry_interval` 2 000 ms and `max_defer_time` 60 000 ms

#### Scenario: Env override changes the deferral ceiling

- **GIVEN** `FLEZHA_MIGRATION_MAX_DEFER_MS=10000`
- **WHEN** the namespace hub starts
- **THEN** `max_defer_time` is 10 000 ms while the other consent settings keep their defaults

### Requirement: Forced migration SHALL land on the existing recovery paths with a consistent SDK protocol

A deferred Overwatch that reaches `max_defer_time` (or the drain deadline) SHALL be force-migrated through the existing best-effort HotSwap handover. After a forced migration: a stream instance SHALL resync via the existing `RestoreInitState`/baseline-pull path; a durable workflow instance SHALL resume from its task-journal fold (committed tasks skipped, the uncommitted task re-run); an SDK driver blocked on a `TaskCommit`/`IterCommit` ack whose controller-side Overwatch has left SHALL hit the existing ≤30 s ack timeout → terminal error → compensation, after which convergence re-places the instance. No new wire message SHALL be introduced.

#### Scenario: Forced migration of a busy workflow operative resumes from the journal

- **GIVEN** a durable workflow Overwatch under continuous input that never suspends, deferred until `max_defer_time` expires
- **WHEN** the hub force-migrates it to a peer node
- **THEN** upstream logs the forced-migration warning, the target node rebinds the instance, its task-journal fold skips every committed task, and the interrupted iteration re-runs without protocol wedge — the runner-side driver either re-attaches or fails its pending ack within the existing 30 s bound and recovers via compensation + re-placement

