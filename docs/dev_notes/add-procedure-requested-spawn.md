# Procedure-requested spawn (`add-procedure-requested-spawn`)

- **`ctx.spawn_workflow` / `ctx.spawnWorkflow`** — spawn-or-deliver a workflow
  instance from inside any procedure, in the requester's own namespace. Free id
  ⇒ spawn with the payload as the first input; taken id ⇒ deliver
  (`IfRunning::Deliver`) or `already_running` (`Error`).
- **The namespace is NOT on the wire.** `SpawnWorkflowRequest{instance_id,
  factory_id, payload, if_running}` (tag 34) + `SpawnWorkflowResponse{outcome,
  error}` (tag 35). The controller takes the namespace from the ControlChannel
  the frame arrived on — cross-namespace spawning is impossible by
  construction, not by validation.
- **GOTCHA — the first-frame guarantee is `:seed_input`, not a follow-up send.**
  The payload rides the Builder's `opts[:seed_input]` and is ingested by
  `Overwatch.seed_input/2` INSIDE the bind `handle_continue`, so no other
  mailbox message (including a concurrent requester's delivery) can be ingested
  ahead of it. A start-then-send would lose that race. `fresh_inbox?/1` skips
  the seed when a durable instance's inbox already holds entries — that is a
  restart, not a spawn, and the cspec (which carries the seed) is replayed on
  controller boot.
- **`OperativeManager.start_workflow/5`** is the ONE place the registry's
  duplicate-id refusal (`"operative id already in use"` → `:duplicate`) is
  recognised; `Schedule.Job` and `spawn_or_deliver/5` both go through it.
  `spawn_or_deliver/5` = existence check (`exist?/2`) → factory check
  (`FleetManager.runners_for_factory`) → `start_workflow` → race-loser falls
  back to `send_io_confirming` (the existing confirming ingest).
- **GOTCHA — resolution must NOT run on the ControlChannel process.** It starts
  an operative, which drives `AddOperative` and waits for the response on that
  same control stream. `handle_spawn_workflow/3` runs in a `Task` and sends
  `{:spawn_workflow_result, ...}` back so the reply frame is still written by the
  channel process (same reason as the workflow-completed teardown).
- **Failure isolation**: the SDK call returns a handle; refusals never reach
  `run`. Rust drops an unobserved handle into ONE process-wide reaper thread
  (`spawn.rs`); Java logs from the reply callback unless `await`/`tryResult`
  claimed the handle first. Java additionally always logs the pre-resolved
  `Unavailable` (no control plane) — documented asymmetry.
- **`shutdown::AckResult` is now `Result<String, String>`** (was
  `Result<(), String>`): the Ok payload carries the reply's outcome, empty for
  bare acks. Java's `CompletableFuture<String>` is `CoreSpawnRequester`'s
  wrapper over the blocking `FlzCore.spawnWorkflow` ABI (the old Java
  `AckRegistry` is gone). One rendezvous, no second registry.
- **`runner::coordinator_try_send`** reports whether the message reached a
  coordinator; `coordinator_send` delegates to it. A stale trigger (closed
  runner) now resolves a spawn as `Unavailable` instead of waiting out 30 s.
- **Replay issues nothing** — `ProcedureCtx::during_replay` (Rust, set from
  `EntityPayload.replay`) / `ProcedureCtx.replaying()` (Java, served from the
  core's `WorkItem.Input.replay`) ⇒ `SkippedOnReplay`.
- **Tests**: `test/flezha/service/operative_manager_spawn_test.exs` (ETS
  `insert_new` stands in for the registry's atomic claim, so the concurrent
  burst exercises the real race), `control_channel_spawn_workflow_test.exs`,
  Rust `procedure::tests::spawn`, Java `SpawnHandleTest` + `CoreSpawnRequesterTest`.
  GOTCHA: the Rust coordinator trigger is process-global — every test that
  installs one takes `runner::env_test_guard()`.
- **Docs**: `docs/sdk/procedure_requested_spawn.md`, `docs/sdk/protocol_spec.md`,
  `docs/resilience/workflow_operative.md`. Examples:
  `sdk/rust/examples/spawn_workflow_worker.rs`, Java `E08_SpawnWorkflowWorker`.

