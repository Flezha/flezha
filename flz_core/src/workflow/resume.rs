//! Where an iteration starts: the frontier a message, a chained handoff or a
//! journal resume seeds, and the journal's committed work as stage records.
//! Every pass runs one chain: the one rooted at the entry channel its
//! message arrived on, or the one a chained loop's first message picked. The
//! invariant: a resumed chain sees exactly what the live one did — a
//! re-seeded batch goes through the same edge mapping, and a journaled
//! iteration is either wholly committed or wholly in flight.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam::channel::RecvTimeoutError;

use super::compensation::{CellRun, StageRecord};
use super::driver::{
    extend_mapped, ChainCtx, Completion, RunOutcome, Runnable, Value, CANCEL_POLL_CHUNK,
};
use super::instance::{CommittedTask, Inbound, InboxItem, JournalResume};
use super::spec::EntryChain;
use super::{LoopMode, WorkflowError};
use crate::channel::{ChannelId, ChannelKind};

/// Eviction hysteresis: how long an inbox-driven loop waits at a committed
/// boundary before it parks (`FLEZHA_WORKFLOW_PARK_IDLE_MS`).
const PARK_IDLE_DEFAULT_MS: u64 = 5_000;
const ENV_PARK_IDLE_MS: &str = "FLEZHA_WORKFLOW_PARK_IDLE_MS";

/// How long a rehydrated loop waits for the owed inbox tail before parking
/// regardless; a lost forward must not wedge it.
const OWED_INBOX_WAIT: Duration = Duration::from_secs(60);

/// Where an iteration starts: the chain it runs, the runnable nodes plus,
/// on a resume over a join, the contributions banked for nodes still waiting
/// on a producer.
pub(super) struct IterationStart {
    /// The entry channel whose chain this pass runs.
    pub(super) channel: ChannelId,
    pub(super) ready: Vec<Runnable>,
    /// Batches of nodes with producers still outstanding.
    pub(super) pending: HashMap<usize, Vec<Value>>,
    pub(super) waiting: HashMap<usize, usize>,
    /// On a resume, what the skipped committed tasklets asked for; they
    /// never run again to ask twice.
    pub(super) completion: Completion,
}

impl IterationStart {
    fn on_chain(channel: ChannelId) -> Self {
        Self {
            channel,
            ready: Vec::new(),
            pending: HashMap::new(),
            waiting: HashMap::new(),
            completion: Completion::default(),
        }
    }

    /// A pass from a message: every root of the message's chain, seeded
    /// through its own entry edge. A root whose edge filters the message out
    /// does not run.
    fn from_message(ctx: &ChainCtx, item: &InboxItem) -> Result<Self, WorkflowError> {
        let chain = ctx.chain(item.channel_id)?;
        let mut start = Self::on_chain(item.channel_id);
        for &root in &chain.roots {
            let batch = seed_root(ctx, item, root)?;
            if !batch.is_empty() {
                start.ready.push((root, batch));
            }
        }
        Ok(start)
    }

    /// A chained pass from the previous terminal batch, into every root of
    /// the chain the loop's first message picked.
    fn from_carry(channel: ChannelId, chain: &EntryChain, batch: Vec<Value>) -> Self {
        let mut start = Self::on_chain(channel);
        for &root in &chain.roots {
            start.ready.push((root, batch.clone()));
        }
        start
    }
}

/// One root's batch from a message: the value through the entry edge into
/// that root, the same application the live routing makes on a stream.
fn seed_root(ctx: &ChainCtx, item: &InboxItem, root: usize) -> Result<Vec<Value>, WorkflowError> {
    let target = ctx.dag.order[root];
    let edge = ctx
        .topology
        .channel(item.channel_id)
        .and_then(|channel| channel.consumers.iter().find(|e| e.tasklet == target));
    let mut batch = Vec::new();
    extend_mapped(&mut batch, edge, std::iter::once(Arc::clone(&item.value)))?;
    Ok(batch)
}

/// A mid-iteration resume of the pass on `channel`: every uncommitted node of
/// its chain re-seeds from each committed producer's journaled handoff on that
/// producer's own channel; a join with a producer still uncommitted banks it
/// and waits, counting only the producers in this chain. An uncommitted root
/// re-runs over the message the controller re-forwarded, or over the carry
/// the journal holds.
fn resumed_start(
    ctx: &ChainCtx,
    resume: &JournalResume,
    committed: &[usize],
    channel: ChannelId,
    message: Option<&InboxItem>,
) -> Result<IterationStart, WorkflowError> {
    let chain = ctx.chain(channel)?;
    let mut start = IterationStart::on_chain(channel);
    for task in &resume.committed_tasks {
        let node = task.task_idx as usize;
        if task.iter == resume.iter && committed.contains(&node) {
            start.completion.note(node, task.completion.clone());
        }
    }
    for node in 0..ctx.dag.len() {
        if !chain.contains(node) || committed.contains(&node) {
            continue;
        }
        if chain.roots.contains(&node) {
            let batch = match message {
                Some(item) => seed_root(ctx, item, node)?,
                None => seed_from_journal(ctx, &resume.handoff, None)?,
            };
            if !batch.is_empty() {
                start.ready.push((node, batch));
            }
            continue;
        }
        let parents: Vec<usize> = ctx.dag.parents[node]
            .iter()
            .copied()
            .filter(|p| chain.contains(*p))
            .collect();
        let committed_parents: Vec<usize> = parents
            .iter()
            .copied()
            .filter(|p| committed.contains(p))
            .collect();
        if committed_parents.is_empty() {
            // A node with no committed parent becomes runnable through the
            // normal accumulate flow.
            continue;
        }
        let mut batch: Vec<Value> = Vec::new();
        for parent in &committed_parents {
            let handoff = resume
                .task_handoffs
                .iter()
                .find(|edge| edge.task_idx as usize == *parent)
                .map(|edge| edge.handoff.as_slice())
                .unwrap_or_default();
            let seeded = seed_from_journal(ctx, handoff, ctx.task(*parent).output_channel_id)?;
            extend_mapped(&mut batch, ctx.edge(*parent, node), seeded.into_iter())?;
        }
        let outstanding = parents.len() - committed_parents.len();
        if outstanding == 0 {
            start.ready.push((node, batch));
        } else {
            start.pending.insert(node, batch);
            start.waiting.insert(node, outstanding);
        }
    }
    Ok(start)
}

pub(super) enum IterationSeed {
    /// Start here; `Some(offset)` when a journaled message supplied the seed.
    Frontier(IterationStart, Option<u64>),
    Done,
    /// The inbox stayed empty past the idle threshold: suspend.
    Park,
    /// A stop arrived while waiting for input; nothing to roll back.
    Cancelled,
    /// The message could not enter its chain and was parked: nothing ran,
    /// so the pass is not sealed and the next message is waited for.
    Unread,
    Failed(WorkflowError),
}

/// The frontier this iteration starts from: the uncommitted part of its
/// chain on a mid-iteration resume, else the chain the loop's shape seeds.
pub(super) fn next_iteration_seed(
    ctx: &ChainCtx,
    iter: u32,
    resume: &JournalResume,
    committed: &[usize],
    carry: Option<Vec<Value>>,
    pass_channel: Option<ChannelId>,
    cancel: &AtomicBool,
) -> IterationSeed {
    if committed.is_empty() {
        return seed_for_mode(ctx, iter, resume, carry, pass_channel, cancel);
    }
    if message_fed(ctx, iter) {
        // The pass took its message before the crash; the controller
        // re-forwards it, so the resumed pass is that message's pass again.
        return match wait_for_input(ctx, cancel) {
            Some(item) => seed_or_fail(
                resumed_start(ctx, resume, committed, item.channel_id, Some(&item)),
                journaled_offset(&item),
            ),
            None => no_input(ctx, cancel),
        };
    }
    let channel = carried_channel(ctx, resume, pass_channel);
    seed_or_fail(resumed_start(ctx, resume, committed, channel, None), None)
}

/// Whether iteration `iter` takes its seed from the inbox: every pass of a
/// `PerInput` loop, the first pass of any loop.
fn message_fed(ctx: &ChainCtx, iter: u32) -> bool {
    ctx.loop_mode == LoopMode::PerInput || iter == 0
}

/// The chain a carry-fed pass re-enters: the one the loop's first message
/// picked, known in memory or from the journaled entry channel.
fn carried_channel(
    ctx: &ChainCtx,
    resume: &JournalResume,
    pass_channel: Option<ChannelId>,
) -> ChannelId {
    pass_channel
        .or_else(|| ctx.topology.lookup_channel_id(&resume.entry_channel))
        .or_else(|| {
            ctx.topology
                .first_of_kind(ChannelKind::Entry)
                .map(|(id, _)| id)
        })
        .expect("a structured workflow declares an entry channel (validated at registration)")
}

/// The pass's seed by the loop's shape: the next message for a message-fed
/// pass, the previous terminal batch (in memory or from the journal) for a
/// chained loop past its first pass; a `Once` loop is done after its one.
fn seed_for_mode(
    ctx: &ChainCtx,
    iter: u32,
    resume: &JournalResume,
    carry: Option<Vec<Value>>,
    pass_channel: Option<ChannelId>,
    cancel: &AtomicBool,
) -> IterationSeed {
    match ctx.loop_mode {
        LoopMode::Once if iter > 0 => IterationSeed::Done,
        LoopMode::Chained if iter > 0 => {
            let batch = match carry {
                Some(batch) => batch,
                None => match seed_from_journal(ctx, &resume.handoff, None) {
                    Ok(batch) => batch,
                    Err(e) => return IterationSeed::Failed(e),
                },
            };
            if batch.is_empty() {
                return IterationSeed::Done;
            }
            let channel = carried_channel(ctx, resume, pass_channel);
            match ctx.chain(channel) {
                Ok(chain) => {
                    IterationSeed::Frontier(IterationStart::from_carry(channel, chain, batch), None)
                }
                Err(e) => IterationSeed::Failed(e),
            }
        }
        _ => match wait_for_input(ctx, cancel) {
            Some(item) => enter(ctx, &item),
            None => no_input(ctx, cancel),
        },
    }
}

/// What an empty wait means: a stop, a suspension for a journaling instance,
/// else the inbox is closed.
fn no_input(ctx: &ChainCtx, cancel: &AtomicBool) -> IterationSeed {
    if cancel.load(Ordering::Acquire) {
        IterationSeed::Cancelled
    } else if ctx.journaling() {
        IterationSeed::Park
    } else {
        IterationSeed::Done
    }
}

/// Offset 0 = unjournaled input (opted-out class): keep the recorded
/// consumed offset where it is.
fn journaled_offset(item: &InboxItem) -> Option<u64> {
    (item.offset > 0).then_some(item.offset)
}

fn seed_or_fail(
    seeded: Result<IterationStart, WorkflowError>,
    consumed: Option<u64>,
) -> IterationSeed {
    match seeded {
        Ok(start) => IterationSeed::Frontier(start, consumed),
        Err(e) => IterationSeed::Failed(e),
    }
}

/// The pass a message starts. One whose entry edge would not let it into
/// its chain rides that channel's ladder as a stream's does, on this thread:
/// a re-attempt that lets it in starts its pass, and one that never does
/// parks it under the channel, consumed without a pass, while the instance
/// keeps waiting.
fn enter(ctx: &ChainCtx, item: &InboxItem) -> IterationSeed {
    let error = match IterationStart::from_message(ctx, item) {
        Ok(start) => return IterationSeed::Frontier(start, journaled_offset(item)),
        Err(e) => e,
    };
    let Some(channel) = ctx.topology.channel(item.channel_id) else {
        return IterationSeed::Failed(error);
    };
    let mut entered = None;
    crate::retry::run_retry_park_ladder(
        &format!("entry channel `{}` into its chain", channel.name),
        crate::retry::ParkTarget::edge(channel),
        ctx.instance_id,
        journaled_offset(item),
        None,
        error.message,
        || {
            entered = Some(IterationStart::from_message(ctx, item).map_err(|e| e.message)?);
            Ok(())
        },
    );
    match entered {
        Some(start) => IterationSeed::Frontier(start, journaled_offset(item)),
        None => {
            ctx.note_parked(item.offset);
            IterationSeed::Unread
        }
    }
}

/// Block for the next message; `None` on a stop, a closed inbox, or (for a
/// journaling instance) once idle past the hysteresis window. A message the
/// entrance parked is consumed on the way, and the idle clock restarts.
fn wait_for_input(ctx: &ChainCtx, cancel: &AtomicBool) -> Option<InboxItem> {
    loop {
        match wait_for_inbound(ctx, cancel)? {
            Inbound::Message(item) => return Some(item),
            Inbound::Parked(offset) => ctx.note_parked(offset),
        }
    }
}

fn wait_for_inbound(ctx: &ChainCtx, cancel: &AtomicBool) -> Option<Inbound> {
    // The controller forwards a rehydrated loop's journaled tail after the
    // bind; while any of it is still owed the wait is the owed-tail bound,
    // not the park idle — a park here would drop the tail at the socket.
    let idle = if ctx.owed_inbox.get() > 0 {
        OWED_INBOX_WAIT
    } else {
        park_idle()
    };
    let deadline = Instant::now() + idle;
    loop {
        if cancel.load(Ordering::Acquire) {
            return None;
        }
        match ctx.inbox.recv_timeout(CANCEL_POLL_CHUNK) {
            Ok(item) => {
                ctx.owed_inbox.set(ctx.owed_inbox.get().saturating_sub(1));
                return Some(item);
            }
            Err(RecvTimeoutError::Timeout) => {
                if ctx.journaling() && Instant::now() >= deadline {
                    return None;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
        }
    }
}

fn park_idle() -> Duration {
    Duration::from_millis(crate::util::env_parse(
        ENV_PARK_IDLE_MS,
        PARK_IDLE_DEFAULT_MS,
    ))
}

/// Suspend at the committed boundary of `iter`: ship `IterPark`, carrying
/// the last message this run parked unread so the controller releases it,
/// and tear down without a completion; the controller rehydrates on the next
/// input or the wake deadline.
pub(super) fn park(ctx: &ChainCtx, iter: u32) -> RunOutcome {
    let wake_deadline_ms = ctx.wake_after.map_or(0, |after| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (now + after).as_millis() as u64
    });
    crate::runner::emit_control_request(
        format!(
            "iter-park-{}-{}-{}",
            ctx.instance_id,
            iter,
            crate::util::nonce_suffix()
        ),
        crate::pb::command_request::Command::IterPark(crate::pb::IterPark {
            operative_id: ctx.instance_id.to_string(),
            iter,
            wake_deadline_ms,
            consumed_offset: ctx.parked_offset.get(),
        }),
    );
    RunOutcome::Parked
}

/// A journaled handoff back into typed values through each item's producing
/// channel's codec; `only` restricts it to one edge.
fn seed_from_journal(
    ctx: &ChainCtx,
    handoff: &[(u32, Vec<u8>)],
    only: Option<ChannelId>,
) -> Result<Vec<Value>, WorkflowError> {
    let mut seeded: Vec<Value> = Vec::with_capacity(handoff.len());
    for (channel_id, bytes) in handoff {
        if only.is_some_and(|edge| edge != *channel_id) {
            continue;
        }
        let codec = ctx
            .topology
            .channel(*channel_id)
            .and_then(|c| c.codec.as_ref())
            .ok_or_else(|| {
                WorkflowError::from(format!(
                    "journaled handoff references channel id {channel_id} without an inter-task \
                     codec; cannot re-seed the resumed tasklet"
                ))
            })?;
        let value = (codec.decode)(bytes.clone()).map_err(|e| {
            WorkflowError::from(format!(
                "journaled handoff decode failed on channel {channel_id}: {e}"
            ))
        })?;
        seeded.push(value);
    }
    Ok(seeded)
}

/// The journal's committed work as stage records: the fully-committed
/// iterations, and the in-flight iteration's stages (skipped on resume, still
/// rolled back). Outputs are empty, as on any crash-resume.
pub(super) fn journaled_stages(
    ctx: &ChainCtx,
    resume: &JournalResume,
) -> (Vec<Vec<StageRecord>>, Vec<StageRecord>) {
    let mut by_iter = stages_by_iter(ctx, &resume.committed_tasks);
    // The resume point splits the two: everything below it is committed progress.
    let in_flight = by_iter
        .split_off(&resume.iter)
        .remove(&resume.iter)
        .unwrap_or_default();
    (by_iter.into_values().collect(), in_flight)
}

/// Journaled commits as stage records, grouped by iteration and ordered by
/// chain position; shared by the forward resume and the rewind undo.
pub(super) fn stages_by_iter(
    ctx: &ChainCtx,
    tasks: &[CommittedTask],
) -> BTreeMap<u32, Vec<StageRecord>> {
    let mut by_iter: BTreeMap<u32, Vec<StageRecord>> = BTreeMap::new();
    for task in tasks {
        if let Some(record) = stage_record(ctx, task.task_idx, task.iter, &task.cells_ran) {
            by_iter.entry(task.iter).or_default().push(record);
        }
    }
    for stages in by_iter.values_mut() {
        // Completion order within an iteration is chain order.
        stages.sort_by_key(|s| s.stage_idx);
    }
    by_iter
}

/// One journaled commit as a `StageRecord`; `None` when the tasklet is not
/// in this build's chain.
fn stage_record(
    ctx: &ChainCtx,
    task_idx: u32,
    iter: u32,
    cells_ran: &[Vec<u8>],
) -> Option<StageRecord> {
    let stage_idx = task_idx as usize;
    let slots = ctx.stage_cells.get(stage_idx)?;
    let cells = cells_ran
        .iter()
        .filter_map(|id| {
            slots
                .iter()
                .position(|slot| slot.cell_identity.as_slice() == id.as_slice())
        })
        .map(|cell_index| CellRun {
            cell_index,
            outputs: Vec::new(),
            staged: Vec::new(),
            completion: None,
        })
        .collect();
    Some(StageRecord {
        stage_idx,
        iter,
        cells,
    })
}
