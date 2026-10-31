# Timers: implementation notes

For SDK implementers and contributors. How to use it is on
[Timers](../../public/topics/core/timers.md).

## Durability: timers are folded state

Timers are records in the cell's own change log, rebuilt by the ordinary restore fold;
[Durability](../../public/topics/core/timers.md#durability) on the Timers page says what a
crash, a replay and the effectively-once tier do to them. On the wire the ops ride two ways,
both stored verbatim by the controller: outside the effectively-once tier as a
`format_id = 3` record under the ordinary `cell_state_change` payload-id (body: postcard
`Vec<TimerOp>`); inside the tier as `EffectRecordBody.timers`.

## Where the code lives

| Piece | Rust | Java | Elixir |
| --- | --- | --- | --- |
| Pending set, `TimerOp`, `TimerPolicy`, `TimerSession`, `TimerCounters`, arming (`apply_ops` / `rearm_restored`) | `crates/flz_core/src/timer.rs` (re-exported by `sdk/rust/src/cell/timer.rs`, which adds `timer_policy_for::<S>()`) | shared core; `flezha-sdk-runtime/.../state/TimerOp.java` is the op an item records | shared core; `lib/flezha/sdk/ctx.ex` records the item's ops |
| A hosted item's ops judged at settle (`TimerPolicy::judge`: cap and clock, against the live set) and committed | `crates/flz_core/src/host/activation.rs`; Rust's own tasklets judge at the call | shared core | shared core |
| Both triggers: event-time due generation at batch assembly, processing-time on the deferred-wake list | `crates/flz_core/src/poll_core/` | shared core | shared core |
| The surface and the hook | `sdk/rust/src/tasklet/{ctx,mod}.rs` | `flezha-sdk-api/.../tasklet/{Ctx,Tasklet}.java`, `.../state/TimerKind.java` | `lib/flezha/sdk/{ctx,tasklet}.ex` |
| The invocation (`invoke_once`) and the restore fold | `sdk/rust/src/relay/dispatcher.rs`, `sdk/rust/src/cell/restore.rs` | `flezha-sdk-runtime/.../core/ActivationDispatcher.java` (`RestoreFold`) | `lib/flezha/sdk/dispatch.ex` |
| Record formats (`FORMAT_ID_TIMER_OPS`, `EffectRecordBody.timers`) | `crates/flz_core/src/{header,schema}.rs` | shared core | shared core |
| Controller: `Flezha.State.Wire` decodes the effect record's `timers` field; nothing else changes | `lib/flezha/state/wire.ex` | — | — |

Tests: `sdk/rust/tests/timer_{durability,firing,lifecycle,surface}.rs`,
`crates/flz_core/src/host/timer_settle_tests.rs`, Java `ActivationDispatcherTest`,
Elixir `sdk/elixir/test/flezha/sdk/ctx_test.exs`,
and `test/system/cell_timers_e2e_test.exs`, which drives a session-timeout
fixture on every runtime, including a re-seat from disk that proves the
pending timer survived it, and a `timer_cap` fixture whose refused items park
and whose restored Cell reads as the live one. `test/system/steps_timers_e2e_test.exs` drives
the steps relay rules on every runtime, and `steps_crash_resume_e2e_test.exs`
the two crash placements around a firing's commit.
