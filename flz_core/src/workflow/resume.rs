//! Where an iteration starts: the frontier a fresh pass, an inbox item, a
//! chained handoff or a journal resume seeds, and the journal's committed
//! work as stage records. The invariant: a resumed chain sees exactly what the
//! live one did — a re-seeded batch goes through the same edge mapping, and a
//! journaled iteration is either wholly committed or wholly in flight.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam::channel::RecvTimeoutError;

use super::compensation::{EntityRun, StageRecord};
use super::driver::{extend_mapped, ChainCtx, RunOutcome, Runnable, Value, CANCEL_POLL_CHUNK};
use super::instance::{CommittedProc, InboxItem, JournalResume};
use super::{LoopMode, WorkflowError};
use crate::channel::{ChannelId, ChannelKind};
use crate::operative::topology::OperativeTopology;

/// Eviction hysteresis: how long an inbox-driven loop waits at a committed
/// boundary before it parks (`FLEZHA_WORKFLOW_PARK_IDLE_MS`).
const PARK_IDLE_DEFAULT_MS: u64 = 5_000;
const ENV_PARK_IDLE_MS: &str = "FLEZHA_WORKFLOW_PARK_IDLE_MS";

/// How long a rehydrated loop waits for the owed inbox tail before parking
/// regardless; a lost forward must not wedge it.
const OWED_INBOX_WAIT: Duration = Duration::from_secs(60);

/// Where an iteration starts: the runnable nodes plus, on a resume over a
/// join, the contributions banked for nodes still waiting on a producer.
#[derive(Default)]
pub(super) struct IterationStart {
    pub(super) ready: Vec<Runnable>,
    /// Batches of nodes with producers still outstanding.
    pub(super) pending: HashMap<usize, Vec<Value>>,
    pub(super) waiting: HashMap<usize, usize>,
}

impl IterationStart {
    fn fresh(batch: Vec<Value>) -> Self {
        Self {
            ready: vec![(0, batch)],
            ..Default::default()
        }
    }
}

/// A mid-iteration resume: every uncommitted node re-seeds from each
/// committed producer's journaled handoff on that producer's own channel; a
/// join with a producer still uncommitted banks it and waits.
fn resumed_start(
    ctx: &ChainCtx,
    resume: &JournalResume,
    committed: &[usize],
) -> Result<IterationStart, WorkflowError> {
    let mut start = IterationStart::default();
    for node in 0..ctx.dag.len() {
        if committed.contains(&node) {
            continue;
        }
        let parents = &ctx.dag.parents[node];
        let committed_parents: Vec<usize> = parents
            .iter()
            .copied()
            .filter(|p| committed.contains(p))
            .collect();
        if committed_parents.is_empty() {
            // The root (no parents) is never here — a mid-iteration resume
            // implies the root committed — and a deeper node with no committed
            // parent becomes runnable through the normal accumulate flow.
            continue;
        }
        let mut batch: Vec<Value> = Vec::new();
        for parent in &committed_parents {
            let handoff = resume
                .proc_handoffs
                .iter()
                .find(|edge| edge.proc_idx as usize == *parent)
                .map(|edge| edge.handoff.as_slice())
                .unwrap_or_default();
            let seeded = seed_from_journal(ctx, handoff, ctx.proc(*parent).output_channel_id)?;
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
    /// Start here; `Some(offset)` when an inbox item supplied the seed.
    Frontier(IterationStart, Option<u64>),
    Done,
    /// The inbox stayed empty past the idle threshold: suspend.
    Park,
    /// A stop arrived while waiting for input; nothing to roll back.
    Cancelled,
    Failed(WorkflowError),
}

/// The frontier this iteration starts from: the uncommitted subgraphs on a
/// mid-iteration resume, else the root seeded by the loop's shape.
pub(super) fn next_iteration_seed(
    ctx: &ChainCtx,
    args: &str,
    iter: u32,
    resume: &JournalResume,
    committed: &[usize],
    carry: Option<Vec<Value>>,
    cancel: &AtomicBool,
) -> IterationSeed {
    if !committed.is_empty() {
        return match resumed_start(ctx, resume, committed) {
            Ok(start) => IterationSeed::Frontier(start, None),
            Err(e) => IterationSeed::Failed(e),
        };
    }
    seed_for_mode(ctx, args, iter, resume, carry, cancel)
}

/// The root's seed by the loop's shape: the spawn args on the first pass,
/// the previous terminal batch (in memory or from the journal) for a chained
/// loop, the next inbox item for a per-input one.
fn seed_for_mode(
    ctx: &ChainCtx,
    args: &str,
    iter: u32,
    resume: &JournalResume,
    carry: Option<Vec<Value>>,
    cancel: &AtomicBool,
) -> IterationSeed {
    match ctx.loop_mode {
        LoopMode::Once | LoopMode::Chained if iter == 0 => {
            seed_or_fail(seed_bytes(ctx.topology, args.as_bytes().to_vec()), None)
        }
        LoopMode::Once => IterationSeed::Done,
        LoopMode::Chained => {
            let batch = match carry {
                Some(batch) => batch,
                None => match seed_from_journal(ctx, &resume.handoff, None) {
                    Ok(batch) => batch,
                    Err(e) => return IterationSeed::Failed(e),
                },
            };
            if batch.is_empty() {
                IterationSeed::Done
            } else {
                IterationSeed::Frontier(IterationStart::fresh(batch), None)
            }
        }

        LoopMode::PerInput => match wait_for_input(ctx, cancel) {
            Some(item) => seed_or_fail(
                seed_bytes(ctx.topology, item.payload),
                // Offset 0 = unjournaled input (opted-out class): keep the
                // recorded consumed offset where it is.
                (item.offset > 0).then_some(item.offset),
            ),
            None if cancel.load(Ordering::Acquire) => IterationSeed::Cancelled,
            None if ctx.journaling() => IterationSeed::Park,
            None => IterationSeed::Done,
        },
    }
}

fn seed_or_fail(seeded: Result<Vec<Value>, WorkflowError>, consumed: Option<u64>) -> IterationSeed {
    match seeded {
        Ok(batch) => IterationSeed::Frontier(IterationStart::fresh(batch), consumed),
        Err(e) => IterationSeed::Failed(e),
    }
}

/// Block for the next inbox item; `None` on a stop, a closed inbox, or (for
/// a journaling instance) once idle past the hysteresis window.
fn wait_for_input(ctx: &ChainCtx, cancel: &AtomicBool) -> Option<InboxItem> {
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

/// Suspend at the committed boundary of `iter`: ship `IterPark` and tear
/// down without a completion; the controller rehydrates on the next input or
/// the wake deadline.
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
        }),
    );
    RunOutcome::Parked
}

/// One entry payload into the entry channel's typed value.
fn seed_bytes(topology: &OperativeTopology, bytes: Vec<u8>) -> Result<Vec<Value>, WorkflowError> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .expect("structured workflow has an entry channel (validated at registration)");
    let wrap = entry
        .wrap_input
        .as_ref()
        .expect("entry channel carries a wrap_input");
    wrap(bytes)
        .map(|v| vec![v])
        .map_err(|e| WorkflowError::from(format!("seeding entry channel failed: {e}")))
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
                    "journaled handoff references channel id {channel_id} without an inter-proc \
                     codec; cannot re-seed the resumed procedure"
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
    let mut by_iter = stages_by_iter(ctx, &resume.committed_procs);
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
    procs: &[CommittedProc],
) -> BTreeMap<u32, Vec<StageRecord>> {
    let mut by_iter: BTreeMap<u32, Vec<StageRecord>> = BTreeMap::new();
    for proc in procs {
        if let Some(record) = stage_record(ctx, proc.proc_idx, proc.iter, &proc.entities_ran) {
            by_iter.entry(proc.iter).or_default().push(record);
        }
    }
    for stages in by_iter.values_mut() {
        // Completion order within an iteration is chain order.
        stages.sort_by_key(|s| s.stage_idx);
    }
    by_iter
}

/// One journaled commit as a `StageRecord`; `None` when the procedure is not
/// in this build's chain.
fn stage_record(
    ctx: &ChainCtx,
    proc_idx: u32,
    iter: u32,
    entities_ran: &[Vec<u8>],
) -> Option<StageRecord> {
    let stage_idx = proc_idx as usize;
    let slots = ctx.stage_entities.get(stage_idx)?;
    let entities = entities_ran
        .iter()
        .filter_map(|id| {
            slots
                .iter()
                .position(|slot| slot.entity_identity.as_slice() == id.as_slice())
        })
        .map(|entity_index| EntityRun {
            entity_index,
            outputs: Vec::new(),
            staged: Vec::new(),
        })
        .collect();
    Some(StageRecord {
        stage_idx,
        iter,
        entities,
    })
}
