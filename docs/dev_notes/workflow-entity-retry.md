# Workflow entity retry (`workflow-entity-retry`)

- **Ladder (now in `flz_core`)**: the per-entity retry ladder is step-barrier
  state — `workflow/step.rs` `fail_or_retry` / `nearest_due` /
  `abandon_backoffs` — only the FAILING entity re-runs its batch (siblings
  stay buffered at the barrier), fresh sink per attempt (a failed attempt's
  staged output is dropped, never double-emitted). Backoff REUSES the shared
  `flz_core::retry::note_failed_attempt` (the stream DLQ ladder's helper:
  `base × 2^(n-1)` ±50% jitter, capped 5 s); a pending backoff is a due-time
  the step loop waits on, and a cooperative stop cuts it short via
  `abandon_backoffs`. Retries count into `flz_core::park::RETRIES_TOTAL`
  (same `data_heartbeat` metric as stream).
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
  `error`) — `drive_chain`/`run_iteration` fold it through `abnormal_status`.
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

