# Workflow entity retry (`workflow-entity-retry`)

- **Ladder (SDK, Rust)**: `run_entity_retrying` in `workflow/driver.rs` wraps the
  pool task — only the FAILING entity re-runs its batch (siblings stay buffered
  at the barrier), fresh sink per attempt (a failed attempt's staged output is
  dropped, never double-emitted). Backoff REUSES `dispatcher::retry_backoff`
  (the stream DLQ ladder's helper: `base × 2^(n-1)` ±50% jitter, capped
  `RETRY_BACKOFF_MAX_MS` = 5 s) and the wait is cancellation-aware
  (`sleep_cancellable`, ≤200 ms poll → a stop/`max_duration` ends it at once).
  Retries count into `entity::park::RETRIES_TOTAL` (same `data_heartbeat`
  metric as stream).
- **Policy**: `WorkflowOperative::retry_attempts()` (default 3 TOTAL attempts) +
  `retry_backoff()` (default 500 ms base) → resolved onto the SHARED
  `channel::RetryPolicy` in `WorkflowSpec.retry`; `0` collapses to one attempt
  (fail-fast); `attempts > 1` with a zero backoff is rejected at registration
  (`BuildWorkflowError::ZeroRetryBackoff`).
- **Classification**: `WorkflowError { message, non_retryable }` (was a tuple
  struct); `WorkflowError::non_retryable/1`, `From<String>/<&str>` stay
  retryable. A procedure body classifies with the EXISTING
  `ProcedureError::non_retryable` — `run_collecting` carries `is_non_retryable()`
  through to the ladder. A panic stays retryable (author never classified it).
- **Boundaries**: retry is strictly PRE-commit (a journaled proc is never
  retried); a crash mid-ladder resumes as a normal uncommitted-proc re-run
  (attempt counter resets). `run`-sugar has no entities → terminal on first Err.
  A stage error while the cancel token is set now reports `cancelled` (was
  `error`) — `run_stage_parallel`'s caller folds it through `abnormal_status`.
- **Tests**: `workflow_driver_scenarios` (recover / exhaust+compensate /
  non-retryable / attempts=0 / stop-interrupts-backoff / run-sugar counted
  once), `retry_policy_is_resolved_and_validated`, e2e "entity retry (rust/uds)"
  in `test/system/workflow_crash_resume_e2e_test.exs` (fixture `retry_workflow`,
  failures armed via `FLEZHA_WORKFLOW_RETRY_FAILS`). GOTCHA: an e2e/unit scenario
  must never busy-wait on the main test thread — it is the fake controller and
  must stay in `drain` to ack `ProcCommit`, else the driver's 30 s ack timeout
  fires (cancel from a helper thread instead).
- **Docs**: `docs/resilience/workflow_operative.md` "Entity retry: the rung
  before the abort" (incl. the workflow-vs-stream escape-ladder split),
  `docs/resilience/data_durability.md` DLQ "Workflow equivalence".
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `retryAttempts()` / `retryBackoff()` ladder.

