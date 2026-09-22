//! The workflow driver's durability barriers: `TaskCommit` at a task boundary
//! and `IterCommit` at an iteration boundary, each a "journal this, then tell
//! me it is durable" round trip over the reference-id-keyed reply path. The
//! invariant: a child tasklet never runs ahead of its producer's committed
//! record, and a barrier is journaled once by its identity, so a re-send is
//! acked from the existing entry.

use std::sync::atomic::Ordering;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use super::compensation::{CellRun, StagedOutput};
use super::driver::{ChainCtx, Value, CANCEL_POLL_CHUNK};
use super::WorkflowError;
use crate::cell::CellSlot;
use crate::channel::RetryPolicy;

/// One wait for a commit response (`FLEZHA_WORKFLOW_ACK_TIMEOUT_MS`); an
/// expired wait re-sends under the same `reference_id`, up to
/// [`BARRIER_RESEND_ATTEMPTS`] sends backed off on the shared default ladder.
const ACK_TIMEOUT_DEFAULT_MS: u64 = 30_000;
const ENV_ACK_TIMEOUT_MS: &str = "FLEZHA_WORKFLOW_ACK_TIMEOUT_MS";
/// Sends of one barrier before an unacked commit fails the run. A re-send is
/// the controller connection's business, not the class's, so no class sets it.
const BARRIER_RESEND_ATTEMPTS: u32 = 3;

/// Ship `TaskCommit` for a completed stage (its handoff through the output
/// channel's codec, its staged plugin output as the outbox, the completion it
/// asked for) and block for the durable ack. Skipped for a run that does not
/// journal, which never stages. A refused ack ends the run; the un-acked task
/// re-runs on resume.
pub(super) fn commit_task_boundary(
    ctx: &ChainCtx,
    stage_idx: usize,
    iter: u32,
    cells: &[CellRun],
    batch: &[Value],
    completion: Option<&[u8]>,
) -> Result<(), WorkflowError> {
    if !ctx.journaling() {
        return Ok(());
    }

    let slots = &ctx.stage_cells[stage_idx];
    let handoff = encode_handoff(ctx, ctx.task(stage_idx).output_channel_id, batch)
        .map_err(|e| WorkflowError::from(format!("task {stage_idx} {e}")))?;

    let cells_ran: Vec<Vec<u8>> = cells
        .iter()
        .map(|seg_run| slots[seg_run.cell_index].cell_identity.as_ref().clone())
        .collect();

    ship_and_await(
        ctx,
        stage_idx,
        "commit",
        true,
        crate::pb::command_request::Command::TaskCommit(crate::pb::TaskCommit {
            operative_id: ctx.instance_id.to_string(),
            task_idx: stage_idx as u32,
            iter,
            cells_ran,
            handoff,
            outbox: cells
                .iter()
                .flat_map(|seg_run| outbox_items(&slots[seg_run.cell_index], &seg_run.staged))
                .collect(),
            compensating: false,
            completes: completion.is_some(),
            result: completion.map(<[u8]>::to_vec).unwrap_or_default(),
        }),
    )
}

/// Ship `IterCommit` for a finished iteration, naming the entry channel
/// whose chain it ran (so a resumed chained loop re-enters that chain) and
/// the completion a tasklet asked for, and block for the durable ack; the
/// next iteration starts only on it, so a crash re-runs at most the
/// in-flight one.
pub(super) fn commit_iteration(
    ctx: &ChainCtx,
    iter: u32,
    consumed_offset: u64,
    channel_id: crate::channel::ChannelId,
    batch: &[Value],
    completion: Option<&[u8]>,
) -> Result<(), WorkflowError> {
    if !ctx.journaling() {
        return Ok(());
    }

    // The terminal handoff re-seeds iteration N+1 of a handoff-chained loop
    // after a crash; a message-fed loop re-seeds from its inbox instead and
    // ships whatever the chain's terminal tasklet released (empty when it has
    // no codec'd output channel, and for a branching chain, which has no
    // single terminal tasklet — registration refuses to chain a loop over one).
    let terminal_channel = ctx
        .dag
        .chain(channel_id)
        .and_then(|chain| chain.single_leaf(ctx.dag))
        .and_then(|leaf| ctx.task(leaf).output_channel_id);
    let handoff = encode_handoff(ctx, terminal_channel, batch)
        .map_err(|e| WorkflowError::from(format!("iteration {iter} {e}")))?;
    let entry_channel = ctx
        .topology
        .channel(channel_id)
        .map(|c| c.name.to_string())
        .unwrap_or_default();

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
            completes: completion.is_some(),
            result: completion.map(<[u8]>::to_vec).unwrap_or_default(),
            entry_channel,
        }),
    )
}

/// A released batch through its channel's codec; empty for a terminal or
/// plugin-bound output.
fn encode_handoff(
    ctx: &ChainCtx,
    channel_id: Option<u32>,
    batch: &[Value],
) -> Result<Vec<crate::pb::TaskHandoffItem>, WorkflowError> {
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
                .map(|payload| crate::pb::TaskHandoffItem {
                    channel_id,
                    payload,
                })
                .map_err(|e| {
                    WorkflowError::from(format!(
                        "handoff serialization failed on channel {channel_id}: {e}"
                    ))
                })
        })
        .collect()
}

/// One cell's staged plugin output as outbox entries, keyed by its wire
/// identity and emit order.
pub(super) fn outbox_items(
    slot: &Arc<CellSlot>,
    staged: &[StagedOutput],
) -> Vec<crate::pb::TaskOutboxItem> {
    staged
        .iter()
        .enumerate()
        .map(
            |(seq_in_task, (channel, payload))| crate::pb::TaskOutboxItem {
                cell_id: slot.cell_identity.as_ref().clone(),
                seq_in_task: seq_in_task as u32,
                channel: channel.clone(),
                payload: payload.clone(),
            },
        )
        .collect()
}

/// One durability handshake: emit the command and block for its ack,
/// re-sending under the same `reference_id` after each expired wait. The
/// waiter stays registered across the ladder, so a late ack continues the
/// chain. `abort_on_cancel` is `false` for an undo record: rollback IS the
/// response to that cancel.
pub(super) fn ship_and_await(
    ctx: &ChainCtx,
    idx: usize,
    what: &str,
    abort_on_cancel: bool,
    command: crate::pb::command_request::Command,
) -> Result<(), WorkflowError> {
    let reference_id = format!(
        "task-commit-{}-{}-{}",
        ctx.instance_id,
        idx,
        crate::util::nonce_suffix()
    );
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());
    let attempts = BARRIER_RESEND_ATTEMPTS;
    let policy = RetryPolicy {
        max_attempts: attempts,
        ..RetryPolicy::default()
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
        return Err(WorkflowError::from(error));
    }
    unreachable!("the ladder returns from its last attempt")
}

/// One attempt's wait, with the retry backoff spent as one more wait on the
/// same receiver so a late ack ends the ladder.
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

enum AckWait {
    Acked,
    Refused(String),
    Interrupted,
    Dropped,
    TimedOut,
}

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
