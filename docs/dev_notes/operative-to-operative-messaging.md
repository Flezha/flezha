# Operative-to-operative messaging (`operative-to-operative-messaging`)

- **The route already existed; only a VERDICT was missing.**
  `OperativeManager.send_io/4` → `HubManager.get_pid` → `cast {:io_in,..}` →
  `Overwatch.ingest/3` → `:on_input` chain → `journal_input/2` has been there
  for over a year with ZERO production callers. This change adds the plugin
  that exposes it as configuration plus a confirming entry point a retry loop
  can read. No new control message, no proto change, no new dedup store.
- **`Overwatch` `handle_call({:ingest, msg, md})`** — now TWO clauses
  (`init_error` / normal); the suspended case moved into the shared
  `Ingest.confirming_ingest/3`, which the blocking `:io_in` path also uses.
  Verdicts: `:ok | {:ok, :absorbed} | {:ok, :suspended} | {:error, :not_ready
  | :not_found | :refused}`. `{:error, :not_found}` is minted by
  `OperativeManager.send_io_confirming/5`, not by the Overwatch.
- **`journal_input/2` now answers `:refused`** (was an indistinguishable `nil`)
  when the durable-inbox append FAILS — absorbed-duplicate stays `nil`. So
  `ingest/3` returns `nil | :refused | {msg, md}`, and both legacy `:io_in`
  clauses match on the 2-tuple instead of `nil` (behaviour-identical: neither
  was ever forwarded).
- **`forward_input/2` was EXTRACTED from the existing `{:io_in,..}` call
  clause** and is now shared with the confirming path — there is exactly ONE
  channel-forward + `[:flezha, :stream, :drop]` telemetry site. It also gained
  a `channel: nil` clause (the old code would have exited on a call to nil).
- **GOTCHA — an unbound stream is NOT uniformly an error.** `confirm_forward/2`
  maps `{:error, :no_stream}` to `:ok` for a durable target (effectively-once
  or durable workflow: it is already journaled, the bind delivers it) and to
  `{:error, :not_ready}` otherwise. Returning `not_ready` for a durable target
  would make the sender retry a payload the receiver already has, which for an
  effectively-once target appends at a NEW offset = a genuine duplicate.
- **The cast keeps its silent `init_error` drop** — deliberately. A sender that
  needs the payload held uses the confirming path.
- **Provider = `Flezha.Plugin.Provider.OperativeForward`** (`:io_binding`,
  `:on_output` + `:on_input`) + `ForwardSup` (THIRD `WorkerSup` family, beside
  `Kafka.ProducerSup` / `Postgres.SinkSup`; key = `{plugin_id, target}`) +
  `ForwardWorker`. Wired in `application.ex` next to the other two.
- **GOTCHA — the worker must NOT deliver from its own `handle_call`.** It
  spawns a monitored process per in-flight item (`make_ref()` tag + separate
  monitor ref; demonitor+flush on result). If it blocked in its own mailbox,
  A.Overwatch→workerA→B.Overwatch→workerB→A.Overwatch is a FOUR-WAY deadlock
  the moment both peers have a message in flight. `operative_forward_test`'s
  cycle case reproduces exactly that (each stand-in peer echoes through its own
  forward plugin from inside its ingest handler) rather than asserting around it.
- **One payload in flight at a time** — order-preserving; a retryable verdict
  re-arms the same queue head. `:transient` restart so `Table.do_unsubscribe`'s
  `safe_kill(owned_pid)` (exit `:shutdown`) STOPS the worker instead of having
  the DynamicSupervisor restart it. (The Postgres sink worker is still
  `:permanent` and does restart; Kafka's producer worker has since moved
  to `:transient` too, so the asymmetry is Postgres-only.)
- **Source identity reaches `init_provider/1` via ONE generic seam**:
  `Table.with_host_identity/2` merges `%{source_namespace:,
  source_operative_id:}` LAST (same anti-spoof rule as `bulk_activate`'s
  `activation_env`). Needed for the self-target / cross-namespace refusals.
- **Refusals at enable time** (= inside `init_provider/1`, which runs in the
  `PluginManager.enable_plugin` call): missing `:target_operative_id`,
  target == self, target resolving in a DIFFERENT running namespace. A target
  resolving NOWHERE is ACCEPTED — it may not be started yet and the worker
  retries. Longer cycles (A→B→A) are deliberately allowed.
- **`build/1` is OVERRIDDEN to reject unknown string keys** — the generic
  `ProviderBehaviour` `build/1` silently drops any key that is not a struct
  field, which would let `delivery_mode: "exactly_once"` read as accepted. The
  guarantee is DERIVED, never configured (outbox ⇒ effectively-once; no outbox
  ⇒ at-least-once), so there must be no ack-mode knob.
- **`ProviderBehaviour.retry_backoff_ms/3`** — the Postgres sink's inline
  `min(base * 2^(n-1), max)` extracted; both sinks share it now.
- **RESTART LIMITATION (verified, documented, NOT fixed)**: in-flight queued
  items do NOT survive a controller restart. `ProcJournal.flush_entry/3` casts
  `{:io_out,..}` THEN appends `outbox_flushed`; `WalWriter.dispatch_outbox_
  entries/2` casts THEN advances+persists `outbox_flushed_seq`. Neither cursor
  is gated on a sink ack, so once the marker lands the item is no longer owed.
  Same posture as the Kafka/Postgres buffers. Already pinned by the existing
  `proc_journal_test` "pending_entries == [] after flush" assertion — no new test.
- **Telemetry** `[:flezha, :plugin, :operative_forward, :delivered|:retry|
  :queue_depth|:saturated]` (Kafka's `[:flezha, :plugin, <provider>, _]` shape)
  → `TelemetryBridge` source `:flezha_operative_forward`.
- **NOT this change**: synchronous request/reply — that is the shipped
  `operative-message-handlers` (`:on_message` + `MessageForward`), which reaches
  the SDK factory's `handle_message`. This reaches the INPUT STREAM. Both docs
  cross-link.
- **Tests**: `test/flezha/operative/overwatch_confirming_ingest_test.exs` (all
  six verdicts + the cast's unchanged silent drop; drives `handle_call/3` on a
  struct like `overwatch_suspension_test`, with a spawned stand-in StreamChannel
  because the test process cannot be its own channel),
  `test/flezha/plugin/provider/operative_forward_test.exs` (refusals, cycle,
  reuse, saturation, retry-until-delivered, identity stamps, disable-stops-worker,
  telemetry). `Mimic.copy(Flezha.State.ProcInbox)` added to `test_helper.exs`.
- **Docs**: `docs/operative_forward_plugin.md` (new),
  `docs/resilience/data_durability.md` "Operative to operative",
  `docs/glossary.md`, `docs/plugin_lifecycle.md`,
  `docs/operative_message_handlers.md`. `sdk_protocol_spec.md` needs NO change —
  both ends are controller-side and the payload reaches the target's runner on
  the ordinary `:io_in` frame.

