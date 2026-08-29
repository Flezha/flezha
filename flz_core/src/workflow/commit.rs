//! The workflow driver's durability barriers: `ProcCommit` at a proc boundary
//! and `IterCommit` at an iteration boundary, each a "journal this, then tell
//! me it is durable" round trip over the reference-id-keyed reply path. The
//! invariant: a child procedure never runs ahead of its producer's committed
//! record, and a barrier is journaled once by its identity, so a re-send is
//! acked from the existing entry.

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use super::compensation::{EntityRun, StagedOutput};
use super::driver::{ChainCtx, Value, CANCEL_POLL_CHUNK};
use crate::channel::RetryPolicy;
use crate::entity::EntitySlot;
use crate::operative::topology::ProcedureDescriptor;

/// Upper bound on one wait for the controller's `ProcCommitResponse` /
/// `IterCommitResponse`. A wait that expires re-sends the same frame under the
/// same `reference_id` (the controller's journal is idempotent by barrier
/// identity) up to the class's `barrier_retry_attempts`, and only the exhausted
/// ladder terminates the run abnormally — the same posture as any mid-run
/// channel loss (the un-acked proc/iteration re-runs on resume,
/// at-least-once). Overridable with `FLEZHA_WORKFLOW_ACK_TIMEOUT_MS`.
const ACK_TIMEOUT_DEFAULT_MS: u64 = 30_000;
const ENV_ACK_TIMEOUT_MS: &str = "FLEZHA_WORKFLOW_ACK_TIMEOUT_MS";

/// Ship `ProcCommit` for a completed stage and block until the controller's
/// durable `ProcCommitResponse` (workflow-proc-journal D3). The handoff batch is
/// serialized through the procedure's output-channel codec (validated present
/// at registration; a terminal or plugin-bound output has no codec and ships
/// an empty handoff), and the stage's staged plugin output rides along as the
/// procedure's outbox (`workflow-transactional-outbox`) — journaled with the
/// commit in one durable write, flushed by the controller only afterwards.
/// Skipped for opted-out classes and transport-less runs (unit tests /
/// standalone, where no coordinator is installed — mirroring the DLQ park's
/// sink gate); those never stage, so nothing is stranded. A failed or timed-out
/// ack is a terminal run error; the un-acked proc re-runs on resume, and its
/// staged output — never journaled — is simply dropped (at-least-once).
pub(super) fn commit_proc_boundary(
    ctx: &ChainCtx,
    stage_idx: usize,
    iter: u32,
    proc: &ProcedureDescriptor,
    entities: &[EntityRun],
    slots: &[Arc<EntitySlot>],
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    let handoff = encode_handoff(ctx, proc.output_channel_id, batch)
        .map_err(|e| format!("proc {stage_idx} {e}"))?;

    let entities_ran: Vec<Vec<u8>> = entities
        .iter()
        .map(|seg_run| slots[seg_run.entity_index].entity_identity.as_ref().clone())
        .collect();

    ship_and_await(
        ctx,
        stage_idx,
        "commit",
        true,
        crate::pb::command_request::Command::ProcCommit(crate::pb::ProcCommit {
            operative_id: ctx.instance_id.to_string(),
            proc_idx: stage_idx as u32,
            iter,
            entities_ran,
            handoff,
            outbox: entities
                .iter()
                .flat_map(|seg_run| outbox_items(&slots[seg_run.entity_index], &seg_run.staged))
                .collect(),
            compensating: false,
        }),
    )
}

/// Ship `IterCommit` for a finished loop iteration and block until the
/// controller's durable `IterCommitResponse` (`workflow-durable-loop` D1): the
/// journal records `iter_committed(iter, consumed_offset)` with the iteration's
/// terminal handoff, the instance's entities are snapshotted at the boundary,
/// the iteration's staged outbox is flushed, and the consumed inbox prefix is
/// released. The next iteration starts only on the successful reply, so a crash
/// re-runs at most the whole in-flight iteration — never a committed one.
pub(super) fn commit_iteration(
    ctx: &ChainCtx,
    iter: u32,
    consumed_offset: u64,
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    // The terminal handoff re-seeds iteration N+1 of a handoff-chained loop
    // after a crash; an inbox-driven loop re-seeds from its inbox instead and
    // ships whatever the terminal procedure released (empty when it has no
    // codec'd output channel, and for a branching graph, which has no single
    // terminal procedure — registration refuses to chain a loop over one).
    let terminal_channel = ctx
        .dag
        .single_leaf()
        .and_then(|leaf| ctx.proc(leaf).output_channel_id);
    let handoff = encode_handoff(ctx, terminal_channel, batch)
        .map_err(|e| format!("iteration {iter} {e}"))?;

    ship_and_await(
        ctx,
        iter as usize,
        "iteration commit",
        true,
        crate::pb::command_request::Command::IterCommit(crate::pb::IterCommit {
            operative_id: ctx.instance_id.to_string(),
            iter,
            consumed_offset,
            handoff,
        }),
    )
}

/// Serialize a released batch through its producing channel's inter-proc codec.
/// Empty when the procedure has no output channel (a terminal stage) or the
/// channel carries no codec (a plugin-bound output).
fn encode_handoff(
    ctx: &ChainCtx,
    channel_id: Option<u32>,
    batch: &[Value],
) -> Result<Vec<crate::pb::ProcHandoffItem>, String> {
    let Some(channel_id) = channel_id else {
        return Ok(Vec::new());
    };
    let Some(codec) = ctx
        .topology
        .channel(channel_id)
        .and_then(|c| c.codec.as_ref())
    else {
        return Ok(Vec::new());
    };

    batch
        .iter()
        .map(|value| {
            (codec.encode)(value.as_ref())
                .map(|payload| crate::pb::ProcHandoffItem {
                    channel_id,
                    payload,
                })
                .map_err(|e| format!("handoff serialization failed on channel {channel_id}: {e}"))
        })
        .collect()
}

/// One entity's staged plugin output as outbox entries, tagged with the
/// producing entity's wire identity and the entity-local emit order
/// (`seq_in_proc`) — the controller mints the flush key from that tuple.
pub(super) fn outbox_items(
    slot: &Arc<EntitySlot>,
    staged: &[StagedOutput],
) -> Vec<crate::pb::ProcOutboxItem> {
    staged
        .iter()
        .enumerate()
        .map(
            |(seq_in_proc, (channel, payload))| crate::pb::ProcOutboxItem {
                entity_id: slot.entity_identity.as_ref().clone(),
                seq_in_proc: seq_in_proc as u32,
                channel: channel.clone(),
                payload: payload.clone(),
            },
        )
        .collect()
}

/// Emit one durability handshake — `ProcCommit` (forward proc boundary or
/// compensation undo record) or `IterCommit` (iteration boundary) — and block
/// until the controller's durable ack. All three are "journal this, then tell me
/// it is durable" round-trips over the same reference-id-keyed reply path.
///
/// A wait that expires is not yet a failure: the same frame is re-sent under
/// the same `reference_id` — the controller journals a barrier once by its
/// identity and acks a re-send from the existing entry — after the class's
/// retry backoff, up to its `barrier_retry_attempts`; a transient control
/// channel stall therefore delays the chain instead of ending it. The waiter
/// stays registered across the ladder, so a late ack to an earlier send
/// continues the chain too.
///
/// `abort_on_cancel` aborts the wait early on a cooperative cancel, which the
/// caller maps to the `cancelled` terminal status. The compensation undo record
/// passes `false`: rollback IS the response to that cancel, and abandoning the
/// record because the token is set would drop the undo output it carries — it
/// waits out the full ladder instead.
pub(super) fn ship_and_await(
    ctx: &ChainCtx,
    idx: usize,
    what: &str,
    abort_on_cancel: bool,
    command: crate::pb::command_request::Command,
) -> Result<(), String> {
    let reference_id = format!(
        "proc-commit-{}-{}-{}",
        ctx.instance_id,
        idx,
        crate::util::nonce_suffix()
    );
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());
    let attempts = ctx.barrier_retry_attempts.max(1);
    let policy = RetryPolicy {
        max_attempts: attempts,
        backoff_base_ms: ctx.retry.backoff_base_ms,
    };
    let timeout = ack_timeout();
    let label = format!("workflow {} {what} {idx}", ctx.instance_id);

    for attempt in 1..=attempts {
        crate::runner::emit_control_request(reference_id.clone(), command.clone());
        let outcome =
            await_with_backoff(ctx, &rx, timeout, abort_on_cancel, &label, attempt, policy);
        if matches!(outcome, AckWait::TimedOut) && attempt < attempts {
            continue;
        }
        let error = match outcome {
            AckWait::Acked => return Ok(()),
            AckWait::Refused(e) => format!("{what} {idx} refused by controller: {e}"),
            AckWait::Interrupted => format!("{what} {idx} interrupted by stop"),
            AckWait::Dropped => format!("{what} {idx} waiter dropped"),
            AckWait::TimedOut => format!(
                "{what} {idx} ack timed out after {attempts} attempt(s) of {}s",
                timeout.as_secs()
            ),
        };
        crate::shutdown::drop_pending_ack(&reference_id);
        return Err(error);
    }
    unreachable!("the ladder returns from its last attempt")
}

/// One attempt's wait: the ack timeout, and — when it expires with attempts
/// left — the retry backoff spent as one more wait on the same receiver, so a
/// late ack to this send ends the ladder instead of costing a re-send.
fn await_with_backoff(
    ctx: &ChainCtx,
    rx: &mpsc::Receiver<crate::shutdown::AckResult>,
    timeout: Duration,
    abort_on_cancel: bool,
    label: &str,
    attempt: u32,
    policy: RetryPolicy,
) -> AckWait {
    let outcome = await_ack(ctx, rx, timeout, abort_on_cancel);
    if !matches!(outcome, AckWait::TimedOut) || attempt >= policy.max_attempts {
        return outcome;
    }
    let delay = crate::retry::note_failed_attempt(label, attempt, policy, &"ack timed out");
    await_ack(ctx, rx, delay, abort_on_cancel)
}

/// How one ack wait ended.
enum AckWait {
    Acked,
    Refused(String),
    Interrupted,
    Dropped,
    TimedOut,
}

/// One wait for the ack, chunked so a cooperative cancel is observed promptly.
fn await_ack(
    ctx: &ChainCtx,
    rx: &mpsc::Receiver<crate::shutdown::AckResult>,
    timeout: Duration,
    abort_on_cancel: bool,
) -> AckWait {
    let deadline = Instant::now() + timeout;
    loop {
        match rx.recv_timeout(CANCEL_POLL_CHUNK) {
            Ok(Ok(_)) => return AckWait::Acked,
            Ok(Err(e)) => return AckWait::Refused(e),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if abort_on_cancel && ctx.instance.cancel.load(Ordering::Acquire) {
                    return AckWait::Interrupted;
                }
                if Instant::now() >= deadline {
                    return AckWait::TimedOut;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return AckWait::Dropped,
        }
    }
}

fn ack_timeout() -> Duration {
    Duration::from_millis(crate::util::env_parse(
        ENV_ACK_TIMEOUT_MS,
        ACK_TIMEOUT_DEFAULT_MS,
    ))
}
