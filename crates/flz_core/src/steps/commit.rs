//! A steps relay's durability barriers as frames: `TaskCommit` at a task
//! boundary, `IterCommit` at an iteration boundary, and the undo record a
//! compensation journals. Each is sent with its ack registered as an event;
//! the controller journals a barrier once by its identity, so a re-send under
//! the same reference id is acked from the existing entry.

use std::sync::Arc;
use std::time::Duration;

use super::compensation::{CellRun, RecordSpan};
use super::instance::{FiringKey, Value};
use super::run::Run;
use super::StepsError;
use crate::channel::ChannelId;
use crate::pb::command_request::Command;

/// How long one send waits for its ack (`FLEZHA_STEPS_ACK_TIMEOUT_MS`)
/// before it goes out again.
const ACK_TIMEOUT_DEFAULT_MS: u64 = 30_000;
const ENV_ACK_TIMEOUT_MS: &str = "FLEZHA_STEPS_ACK_TIMEOUT_MS";

/// Sends of one barrier before an unacked commit fails the run. A re-send is
/// the controller connection's business, not the class's, so no class sets it.
pub(super) const BARRIER_SENDS: u32 = 3;

pub(super) fn ack_timeout() -> Duration {
    Duration::from_millis(crate::util::env_parse(
        ENV_ACK_TIMEOUT_MS,
        ACK_TIMEOUT_DEFAULT_MS,
    ))
}

/// `TaskCommit` for a completed stage: each Cell it ran with the records it
/// wrote, its handoff through the output channel's codec, the completion it
/// asked for, and on a timer-fed pass's root the firing that opened it.
pub(super) fn task_frame(
    r: &Run<'_>,
    node: usize,
    iter: u32,
    cells: &[CellRun],
    completion: Option<Vec<u8>>,
    trigger: Option<FiringKey>,
) -> Result<Command, StepsError> {
    let values: Vec<Value> = cells
        .iter()
        .flat_map(|run| run.outputs.iter().map(Arc::clone))
        .collect();
    let handoff = encode_handoff(r, r.task(node).output_channel_id, &values)
        .map_err(|e| StepsError::from(format!("task {node} {e}")))?;
    Ok(Command::TaskCommit(crate::pb::TaskCommit {
        relay_id: r.inst.instance_id.to_string(),
        task_idx: node as u32,
        iter,
        cells_ran: cells
            .iter()
            .map(|run| RecordSpan::committed_cell(&run.cell, run.record.as_ref()))
            .collect(),
        handoff,
        compensating: false,
        completes: completion.is_some(),
        result: completion.unwrap_or_default(),
        trigger: trigger.as_ref().map(FiringKey::to_wire),
    }))
}

/// `IterCommit` for a finished iteration, naming the entry channel whose
/// chain it ran (so a resumed chained loop re-enters that chain) and the
/// completion a tasklet asked for; or for a turn that failed, carrying the
/// failure the controller parks its message under.
pub(super) fn iteration_frame(
    r: &Run<'_>,
    iter: u32,
    consumed_offset: Option<u64>,
    channel_id: ChannelId,
    values: &[Value],
    completion: Option<Vec<u8>>,
    dead_letter: Option<crate::pb::DeadLetter>,
) -> Result<Command, StepsError> {
    // The terminal handoff re-seeds iteration N+1 of a handoff-chained loop
    // after a crash; a message-fed loop re-seeds from its input log instead
    // and ships whatever the chain's terminal tasklet released (empty when it
    // has no codec'd output channel, and for a branching chain, which has no
    // single terminal tasklet — registration refuses to chain a loop over one).
    let terminal_channel = r
        .chain(channel_id)
        .and_then(|chain| chain.single_leaf(&r.spec.dag))
        .and_then(|leaf| r.task(leaf).output_channel_id);
    let handoff = encode_handoff(r, terminal_channel, values)
        .map_err(|e| StepsError::from(format!("iteration {iter} {e}")))?;
    let entry_channel = r
        .inst
        .topology
        .channel(channel_id)
        .map(|c| c.name.to_string())
        .unwrap_or_default();
    Ok(Command::IterCommit(crate::pb::IterCommit {
        relay_id: r.inst.instance_id.to_string(),
        iter,
        consumed_offset,
        handoff,
        completes: completion.is_some(),
        result: completion.unwrap_or_default(),
        entry_channel,
        dead_letter,
    }))
}

/// One tasklet's undo entry: each Cell that ran, named with the record its
/// hook wrote among `written`, or bare.
pub(super) fn undo_frame(
    r: &Run<'_>,
    node: usize,
    iter: u32,
    cells_ran: Vec<Vec<u8>>,
    written: Vec<crate::pb::CommittedCell>,
) -> Command {
    let cells_ran = cells_ran
        .into_iter()
        .map(|cell_id| {
            written
                .iter()
                .find(|cell| cell.cell_id == cell_id)
                .cloned()
                .unwrap_or(crate::pb::CommittedCell {
                    cell_id,
                    ..Default::default()
                })
        })
        .collect();
    Command::TaskCommit(crate::pb::TaskCommit {
        relay_id: r.inst.instance_id.to_string(),
        task_idx: node as u32,
        iter,
        cells_ran,
        handoff: Vec::new(),
        compensating: true,
        completes: false,
        result: Vec::new(),
        trigger: None,
    })
}

/// A released batch through its channel's codec; empty for a terminal or
/// plugin-bound output.
fn encode_handoff(
    r: &Run<'_>,
    channel_id: Option<u32>,
    batch: &[Value],
) -> Result<Vec<crate::pb::TaskHandoffItem>, StepsError> {
    let Some(channel_id) = channel_id else {
        return Ok(Vec::new());
    };
    let Some(codec) = r
        .inst
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
                    StepsError::from(format!(
                        "handoff serialization failed on channel {channel_id}: {e}"
                    ))
                })
        })
        .collect()
}
