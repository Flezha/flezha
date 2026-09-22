//! The events that advance a workflow's pass sheet, each handled on the
//! thread that brings it: a message on the data reader, a settled item on the
//! worker that ran it, a barrier's ack on the control reader, a deadline on
//! the poller that promotes it. Every event decides under the sheet lock and
//! acts after it: no frame is sent, no item admitted and no author code run
//! while the lock is held, and nothing waits.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use flz_transport::transport::FlezhaStream;

use super::commit::{ack_timeout, iteration_frame, task_frame, undo_frame, BARRIER_SENDS};
use super::compensation::{
    plan_compensation, rewind_steps, saga_steps, stages_from_intents, CellRun,
};
use super::instance::{
    AfterUndo, Attempt, Barrier, BarrierFor, Carry, CellPos, Deadline, Failure, Message, MidResume,
    Pass, PassSheet, Phase, Release, Stage, StageState, Undo, UndoStep, Value, WorkflowRun,
};
use super::resume::{carried_channel, journaled_stages, resumed_seed, seed_from_journal, Seed};
use super::spec::{EntryChain, WorkflowSpec};
use super::{LoopMode, RollbackMode, WorkflowError};
use crate::cell::{make_cell_payload, CellItem, CellSlot};
use crate::channel::{ChannelDescriptor, ChannelId, ChannelKind, ConsumerEdge, OverflowPolicy};
use crate::host::HostPayload;
use crate::operative::instance::OperativeInstance;
use crate::operative::sleep::{iter_park, park_idle, OWED_INBOX_WAIT};
use crate::operative::topology::{CellMode, TaskletDescriptor};
use crate::pb::command_request::Command;
use crate::routing::Destination;
use crate::state_sink::{COMPENSATION_DONE_PAYLOAD_ID, COMPENSATION_INTENT_PAYLOAD_ID};

/// How long a message's item waits before a full mailbox is tried again.
const ADMIT_RETRY: Duration = Duration::from_millis(5);
/// How long an undo waits for its Cell's handout to be answered.
const UNDO_GATE_RETRY: Duration = Duration::from_millis(1);
/// The restore gate's cap: a restore that never lands degrades to a
/// best-effort re-run rather than a hung operative.
const RESTORE_GATE: Duration = Duration::from_secs(30);

/// One workflow instance's run with the class facts every event reads.
pub(crate) struct Run<'a> {
    pub(crate) inst: &'a OperativeInstance,
    pub(crate) run: &'a WorkflowRun,
    pub(crate) spec: &'a WorkflowSpec,
}

impl<'a> Run<'a> {
    pub(crate) fn of(inst: &'a OperativeInstance) -> Option<Self> {
        Some(Self {
            inst,
            run: inst.workflow.as_ref()?,
            spec: inst.topology.workflow.as_ref()?,
        })
    }

    /// Whether this run journals its task boundaries — the one gate on both
    /// the `TaskCommit` handshake and output staging. A transport-less run
    /// has no journal, so its plugin output goes straight to the wire.
    pub(crate) fn journaling(&self) -> bool {
        self.inst.durable()
    }

    pub(crate) fn task(&self, node: usize) -> &TaskletDescriptor {
        self.inst
            .topology
            .tasklet(self.spec.dag.order[node])
            .expect("dag tasklet present in topology")
    }

    /// The stage a Cell runs and its place among the stage's Cells.
    pub(super) fn locate(&self, slot: &CellSlot) -> Option<(usize, CellPos)> {
        let node = self.spec.dag.node_of(slot.tasklet_id)?;
        let index = slot.cell_key.as_deref().map_or(0, |key| {
            self.inst.cell_key_to_idx[slot.tasklet_id as usize]
                .get(key)
                .copied()
                .unwrap_or(0)
        });
        Some((node, (index, slot.cell_key.clone())))
    }

    pub(crate) fn chain(&self, channel: ChannelId) -> Option<&EntryChain> {
        self.spec.dag.chain(channel)
    }

    fn weak(&self) -> Weak<OperativeInstance> {
        self.run.me.clone()
    }

    /// Decide under the sheet lock, then act on what was decided.
    fn decide(&self, f: impl FnOnce(&mut Decide<'_, '_, '_>)) {
        let mut fx = Effects::default();
        {
            let mut sheet = self.run.sheet();
            f(&mut Decide {
                r: self,
                s: &mut sheet,
                fx: &mut fx,
            });
        }
        self.act(fx);
    }
}

// ---- the events ------------------------------------------------------------

/// The bind is complete: `init_values` are in place. Arms `max_duration` and
/// opens the restore gate once every restore the bind announced has landed.
pub fn open(inst: &OperativeInstance) {
    let Some(r) = Run::of(inst) else { return };
    r.decide(|d| {
        if let Some(limit) = d.r.spec.max_duration {
            d.arm(Deadline::MaxDuration, limit);
        }
        d.s.bound = true;
        if d.s.restored >= d.s.expected {
            d.open_gate();
        } else {
            d.arm(Deadline::RestoreGate, RESTORE_GATE);
        }
    });
}

/// One Cell restore landed, with the saga markers the controller persisted.
pub fn restore_landed(inst: &OperativeInstance, cell_id: &[u8], intent: bool, done: bool) {
    let Some(r) = Run::of(inst) else { return };
    r.decide(|d| {
        if intent {
            d.s.compensation_intents.insert(cell_id.to_vec());
        }
        if done {
            d.s.compensation_done.insert(cell_id.to_vec());
        }
        d.s.restored += 1;
        if d.s.bound && d.s.phase == Phase::Gated && d.s.restored >= d.s.expected {
            d.disarm(Deadline::RestoreGate);
            d.open_gate();
        }
    });
}

/// A cooperative stop (`WorkflowStop`, or `max_duration`).
pub fn stop(inst: &OperativeInstance) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| d.stop());
    }
}

/// The registry dropped the instance: every later event finds it done. Its
/// Cells' flusher entries go with it.
pub fn removed(inst: &OperativeInstance) {
    let Some(r) = Run::of(inst) else { return };
    // One step under the lock: an event racing the removal sees it whole.
    r.decide(|d| {
        d.s.phase = Phase::Done;
        d.r.run.cancel.store(true, Ordering::Release);
        d.r.run.pass_dead.store(true, Ordering::Release);
        d.clear_waits();
    });
    for slot in inst.all_slots() {
        if let Some(id) = slot.flusher_entry_id {
            crate::flusher::CellFlusher::instance().deregister(id);
        }
    }
}

/// A message through the entrance: its items for the chain's roots are
/// addressed here, on the reader, and the pass starts at once or waits in the
/// queue. An entry edge that unwinds rides the channel's ladder off the reader.
pub fn message(
    inst: &OperativeInstance,
    channel: &ChannelDescriptor,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    offset: Option<u64>,
) {
    let Some(r) = Run::of(inst) else { return };
    let Some(channel_id) = inst.topology.lookup_channel_id(&channel.name) else {
        return;
    };
    match r.address(channel, &channel.consumers, value, stream, None) {
        Ok(roots) => r.decide(|d| {
            d.arrived(Message {
                channel: channel_id,
                offset,
                roots,
            })
        }),
        Err(error) => entry_ladder(inst, channel_id, value, stream, offset, error),
    }
}

/// A message on its way into the dead-letter queue instead of a pass; the
/// run counts it consumed.
pub fn entry_parked(inst: &OperativeInstance, offset: Option<u64>) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| {
            d.s.parked_offset = d.s.parked_offset.max(offset);
            d.rearm_park_idle();
        });
    }
}

/// A stage's output value, on the worker that emitted it: the edge chain and
/// `cells_for` run here, and the addressed items wait on the sheet for the
/// stage's commit. A chained loop's leaf addresses its value to the roots.
pub fn hold_emit(
    inst: &OperativeInstance,
    channel: Option<&ChannelDescriptor>,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    producer: &Arc<CellSlot>,
) {
    let Some(r) = Run::of(inst) else { return };
    let Some((node, cell)) = r.locate(producer) else {
        return;
    };
    let plugin_bound = channel.is_some_and(|c| c.kind == ChannelKind::PluginBound);
    let addressed = match channel {
        _ if plugin_bound => Ok(Vec::new()),
        _ if r.spec.dag.children[node].is_empty() => Ok(r.carry_of(value, stream, producer)),
        Some(channel) => r.address(channel, &channel.consumers, value, stream, Some(producer)),
        None => Ok(Vec::new()),
    };
    r.decide(|d| {
        let attempt = d.s.attempts.entry((node, cell)).or_default();
        if !plugin_bound {
            attempt.outputs.push(Arc::clone(value));
        }
        match addressed {
            Ok(held) => attempt.held.extend(held),
            Err(error) => {
                attempt.failure.get_or_insert(Failure {
                    node,
                    error: WorkflowError::non_retryable(error),
                    payload: None,
                    attempts: 1,
                    ran: true,
                });
            }
        }
    });
}

/// A plugin-bound output of a workflow Cell: staged for its stage's commit
/// when the run journals, straight to the wire otherwise.
pub fn plugin_output(slot: &Arc<CellSlot>, channel: &str, bytes: Vec<u8>) {
    with_cell(slot, |r, node, cell| {
        if !r.journaling() {
            crate::runner::emit_workflow_output(r.inst.instance_id.to_string(), bytes);
            return;
        }
        r.decide(|d| {
            let attempt = d.s.attempts.entry((node, cell)).or_default();
            attempt.staged.push((channel.to_string(), bytes));
        });
    });
}

/// A tasklet's `complete`: the run ends with `result` once the iteration
/// commits; a later call in the same attempt replaces it.
pub fn complete(slot: &Arc<CellSlot>, result: Vec<u8>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.entry((node, cell)).or_default().completion = Some(result);
        });
    });
}

/// An item's final or non-retryable failure, where a stream would park it.
/// `payload` is the item's wire bytes, what an unreadable message parks.
pub fn note_failure(
    slot: &Arc<CellSlot>,
    error: WorkflowError,
    payload: Option<Vec<u8>>,
    attempts: u32,
) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            let attempt = d.s.attempts.entry((node, cell)).or_default();
            attempt.failure.get_or_insert(Failure {
                node,
                error,
                payload,
                attempts,
                ran: false,
            });
        });
    });
}

/// A retryable attempt failed: what it produced never counts.
pub fn attempt_failed(slot: &Arc<CellSlot>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.remove(&(node, cell));
        });
    });
}

/// One of a stage's items settled, its outputs already held.
pub fn item_settled(slot: &Arc<CellSlot>) {
    with_cell(slot, |r, node, cell| {
        r.decide(|d| d.settled(node, cell, slot))
    });
}

/// Items consumed without running: a dead pass's queue, a disarmed retry.
pub(crate) fn items_dropped(slot: &Arc<CellSlot>, count: usize) {
    if count == 0 {
        return;
    }
    with_cell(slot, |r, node, cell| {
        r.decide(|d| {
            d.s.attempts.remove(&(node, cell));
            d.owe_less(node, count);
        })
    });
}

/// The Cells an idle sweep of `inst` must keep: `None` — every one — unless
/// the run waits between passes, else those a rollback of the declared mode
/// could still reach through a compensation hook. A stream keeps none.
pub(crate) fn reap_holds(inst: &OperativeInstance) -> Option<HashSet<Arc<Vec<u8>>>> {
    let Some(r) = Run::of(inst) else {
        return Some(HashSet::new());
    };
    let sheet = r.run.sheet();
    if sheet.phase != Phase::Waiting {
        return None;
    }
    let reach = plan_compensation(r.spec.rollback_mode, &sheet.committed, &sheet.done);
    Some(
        reach
            .into_iter()
            .filter(|record| r.task(record.stage_idx).compensate_overridden)
            .flat_map(|record| &record.cells)
            .map(|run| Arc::clone(&run.cell.cell_identity))
            .collect(),
    )
}

/// A stage's keyless host value back from the prepare crossing: addressed
/// to the keyed Cells its answer names, which the stage then owes in its
/// place — or, when the crossing's ladder gave up, the stage fails where a
/// stream would park the value.
pub(crate) fn crossed(
    inst: &OperativeInstance,
    node: usize,
    answer: Result<(Value, Arc<FlezhaStream>), String>,
) {
    let Some(r) = Run::of(inst) else { return };
    let items = answer.and_then(|(value, stream)| r.address_crossed(node, &value, &stream));
    r.decide(|d| d.crossed(node, items));
}

/// Whether `slot` belongs to a workflow pass that failed or was cancelled.
pub(crate) fn pass_dead(slot: &CellSlot) -> bool {
    slot.instance
        .upgrade()
        .and_then(|inst| {
            inst.workflow
                .as_ref()
                .map(|run| run.pass_dead.load(Ordering::Acquire))
        })
        .unwrap_or(false)
}

/// A deadline the deferred-wake list promoted; stale when it was re-armed or
/// disarmed since.
pub(crate) fn deadline_due(inst: &OperativeInstance, deadline: Deadline, due: u64) {
    if let Some(r) = Run::of(inst) {
        r.decide(|d| {
            if d.s.armed.get(&deadline) != Some(&due) {
                return;
            }
            d.s.armed.remove(&deadline);
            d.deadline(deadline);
        });
    }
}

fn with_cell(slot: &Arc<CellSlot>, f: impl FnOnce(&Run<'_>, usize, CellPos)) {
    let Some(inst) = slot.instance.upgrade() else {
        return;
    };
    let Some(r) = Run::of(&inst) else { return };
    if let Some((node, cell)) = r.locate(slot) {
        f(&r, node, cell);
    }
}

/// A barrier's controller reply, on the control reader.
fn barrier_acked(inst: &Weak<OperativeInstance>, id: u64, result: Result<(), String>) {
    let Some(inst) = inst.upgrade() else { return };
    let Some(r) = Run::of(&inst) else { return };
    r.decide(|d| {
        let Some(barrier) = d.s.barriers.remove(&id) else {
            return;
        };
        d.disarm(Deadline::Ack(id));
        d.barrier_answered(barrier.target, result);
    });
}

/// A Cell's undo answered, on the thread that completed it.
fn undo_answered(
    inst: &Weak<OperativeInstance>,
    slot: &Arc<CellSlot>,
    outcome: crate::schema::ActivationOutcome,
) {
    crate::poll_core::global().release_and_rearm(slot);
    let Some(inst) = inst.upgrade() else { return };
    let Some(r) = Run::of(&inst) else { return };
    crate::host::settle_workflow_undo(slot, outcome);
    let Some((node, cell)) = r.locate(slot) else {
        return;
    };
    r.decide(|d| d.undone(node, cell, slot));
}

/// A message whose entry edge unwound, re-attempted off the reader as a
/// stream's edge is; parked under the channel when it never enters.
fn entry_ladder(
    inst: &OperativeInstance,
    channel_id: ChannelId,
    value: &Value,
    stream: &Arc<FlezhaStream>,
    offset: Option<u64>,
    error: String,
) {
    let Some(owner) = inst.workflow.as_ref().and_then(|run| run.me.upgrade()) else {
        return;
    };
    let value = Arc::clone(value);
    let stream = Arc::clone(stream);
    let held = Arc::clone(&owner);
    crate::retry::spawn_park_thread(&owner, "flezha-edge-transform-park", move || {
        let Some(channel) = held.topology.channel(channel_id) else {
            return;
        };
        crate::retry::run_entrance_ladder(
            &*held,
            &format!("entry channel `{}` into its chain", channel.name),
            crate::retry::ParkTarget::edge(channel),
            offset,
            None,
            error,
            || {
                let Some(r) = Run::of(&held) else {
                    return Ok(());
                };
                let roots = r.address(channel, &channel.consumers, &value, &stream, None)?;
                r.decide(|d| {
                    d.arrived(Message {
                        channel: channel_id,
                        offset,
                        roots,
                    })
                });
                Ok(())
            },
        );
    });
}

// ---- addressing, off the lock ------------------------------------------------

impl Run<'_> {
    /// A value through `edges` of `channel`: the transform and `cells_for`
    /// run here, and each item is addressed to its Cell.
    pub(super) fn address<'e>(
        &self,
        channel: &ChannelDescriptor,
        edges: impl IntoIterator<Item = &'e ConsumerEdge>,
        value: &Value,
        stream: &Arc<FlezhaStream>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Result<Vec<Release>, String> {
        let mut items = Vec::new();
        let mut keyless = Vec::new();
        for edge in edges {
            if self.asks_host_for_keys(channel, edge, value) {
                if let Some(node) = self.spec.dag.node_of(edge.tasklet) {
                    keyless.push(Release {
                        node,
                        cell: None,
                        item: workflow_item(value, stream),
                        producer: producer.map(Arc::clone),
                    });
                }
                continue;
            }
            crate::routing::deliver_edge(
                self.inst,
                channel,
                edge,
                value,
                stream,
                None,
                false,
                &mut Destination::Held(&mut items),
            )
            .map_err(|e| e.to_string())?;
        }
        let mut releases = self.releases(items, producer);
        releases.extend(keyless);
        Ok(releases)
    }

    /// Whether `value` needs its host to key it for `edge`: a host releases a
    /// workflow value untransformed and unkeyed, and a keyed consumer has no
    /// Cell until the edge's transforms and key have run. An entry value
    /// crossed on its way in.
    fn asks_host_for_keys(
        &self,
        channel: &ChannelDescriptor,
        edge: &ConsumerEdge,
        value: &Value,
    ) -> bool {
        channel.kind != ChannelKind::Entry
            && edge.key.is_none()
            && self
                .inst
                .topology
                .tasklet(edge.tasklet)
                .is_some_and(|t| t.mode == CellMode::Dynamic)
            && value
                .downcast_ref::<HostPayload>()
                .is_some_and(|p| p.keys.is_empty())
    }

    /// An answered host value through the edge into `node`'s tasklet, to the
    /// Cells its keys name; none when the host named none.
    fn address_crossed(
        &self,
        node: usize,
        value: &Value,
        stream: &Arc<FlezhaStream>,
    ) -> Result<Vec<Release>, String> {
        let Some(payload) = value.downcast_ref::<HostPayload>() else {
            return Ok(Vec::new());
        };
        let tasklet = self.spec.dag.order[node];
        let channel = self.inst.topology.channel(payload.channel);
        let edge = channel.and_then(|c| c.consumers.iter().find(|e| e.tasklet == tasklet));
        match (channel, edge) {
            (Some(channel), Some(edge)) if !payload.keys.is_empty() => {
                self.address(channel, [edge], value, stream, None)
            }
            _ => Ok(Vec::new()),
        }
    }

    /// A chained loop's leaf value addressed to the roots of the chain the
    /// loop runs, as the next pass's input; nothing for any other loop.
    fn carry_of(
        &self,
        value: &Value,
        stream: &Arc<FlezhaStream>,
        producer: &Arc<CellSlot>,
    ) -> Vec<Release> {
        if self.spec.loop_mode != LoopMode::Chained {
            return Vec::new();
        }
        let Some(channel) = self.run.sheet().pass.as_ref().map(|pass| pass.channel) else {
            return Vec::new();
        };
        self.to_roots(channel, std::slice::from_ref(value), stream, Some(producer))
    }

    /// Values into every root of `channel`'s chain, with no edge between.
    fn to_roots(
        &self,
        channel: ChannelId,
        values: &[Value],
        stream: &Arc<FlezhaStream>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Vec<Release> {
        let Some(chain) = self.chain(channel) else {
            return Vec::new();
        };
        let mut items = Vec::new();
        for &root in &chain.roots {
            for value in values {
                let tasklet = self.spec.dag.order[root];
                for cell in crate::routing::declared_cells_for(self.inst, tasklet, value) {
                    items.push((cell, workflow_item(value, stream)));
                }
            }
        }
        self.releases(items, producer)
    }

    fn releases(
        &self,
        items: Vec<(Arc<CellSlot>, CellItem)>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Vec<Release> {
        items
            .into_iter()
            .filter_map(|(cell, item)| {
                Some(Release {
                    node: self.spec.dag.node_of(cell.tasklet_id)?,
                    cell: Some(cell),
                    item,
                    producer: producer.map(Arc::clone),
                })
            })
            .collect()
    }

    /// Seed a pass off the lock: the items it starts with, and for a pass the
    /// crash interrupted, the journal's committed producers re-seeded into
    /// their children.
    fn begin_pass(&self, source: Source) {
        let mid = {
            let mut sheet = self.run.sheet();
            if sheet.phase != Phase::Starting {
                return;
            }
            sheet.resume_mid.take()
        };
        let seeded = self.seed(source, mid.as_deref());
        self.decide(|d| match seeded {
            Ok(Some((channel, offset, roots, seed))) => d.start_pass(channel, offset, roots, seed),
            Ok(None) => d.finish_clean(),
            Err(error) => d.compensate_on(error),
        });
    }

    #[allow(clippy::type_complexity)]
    fn seed(
        &self,
        source: Source,
        mid: Option<&MidResume>,
    ) -> Result<Option<(ChannelId, Option<u64>, Vec<Release>, Seed)>, WorkflowError> {
        let (channel, offset, mut roots) = match source {
            Source::Message(message) => (message.channel, message.offset, message.roots),
            Source::Carry(carry) => (carry.channel, None, carry.items),
            Source::Journal { channel, handoff } => {
                let values = seed_from_journal(&self.inst.topology, &handoff, None)?;
                if values.is_empty() && mid.is_none() {
                    return Ok(None);
                }
                let stream = unwired_stream();
                (
                    channel,
                    None,
                    self.to_roots(channel, &values, &stream, None),
                )
            }
        };
        let seed = match mid {
            Some(mid) => {
                roots.retain(|r| !mid.committed_nodes.contains(&r.node));
                resumed_seed(self, mid, channel)?
            }
            None => Seed::default(),
        };
        Ok(Some((channel, offset, roots, seed)))
    }
}

/// A workflow Cell's input: no input-log offset (the pass records it) and
/// never a replay member.
pub(crate) fn workflow_item(value: &Value, stream: &Arc<FlezhaStream>) -> CellItem {
    CellItem::Input(make_cell_payload(value, stream, None, false))
}

/// The connection a workflow value that arrived on none rides — a journal
/// re-seed, an undo. It is never written: a workflow's outputs leave through
/// the coordinator.
pub fn unwired_stream() -> Arc<FlezhaStream> {
    crate::state_sink::secondary_stream().unwrap_or_else(|| {
        static DETACHED: std::sync::OnceLock<Arc<FlezhaStream>> = std::sync::OnceLock::new();
        Arc::clone(
            DETACHED.get_or_init(|| Arc::new(FlezhaStream::Local(Arc::new(|_: &[u8]| Ok(()))))),
        )
    })
}

/// Where a pass's first items come from.
enum Source {
    Message(Message),
    Carry(Carry),
    /// The journal's terminal handoff into the roots of `channel`'s chain.
    Journal {
        channel: ChannelId,
        handoff: Vec<(u32, Vec<u8>)>,
    },
}

// ---- decisions, under the lock -----------------------------------------------

struct Decide<'d, 'r, 'a> {
    r: &'r Run<'a>,
    s: &'d mut PassSheet,
    fx: &'d mut Effects,
}

impl Decide<'_, '_, '_> {
    fn cancelled(&self) -> bool {
        self.r.run.cancelled()
    }

    fn arm(&mut self, deadline: Deadline, after: Duration) {
        let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
        if let Some(old) = self.s.armed.insert(deadline, due) {
            if old != due {
                self.fx.disarm.push((deadline, old));
            }
        }
        self.fx.arm.push((deadline, due));
    }

    fn disarm(&mut self, deadline: Deadline) {
        if let Some(due) = self.s.armed.remove(&deadline) {
            self.fx.disarm.push((deadline, due));
        }
    }

    /// Nothing more to wait for: every deadline disarmed, every owed ack
    /// dropped.
    fn clear_waits(&mut self) {
        for (deadline, due) in self.s.armed.drain() {
            self.fx.disarm.push((deadline, due));
        }
        for (_, barrier) in self.s.barriers.drain() {
            self.fx.drop_acks.push(barrier.reference_id);
        }
    }

    fn terminal(&mut self, status: &str, error: String, result: Vec<u8>) {
        if self.s.phase == Phase::Done {
            return;
        }
        self.s.phase = Phase::Done;
        self.s.pass = None;
        self.r.run.pass_dead.store(true, Ordering::Release);
        self.clear_waits();
        self.fx.completed = Some((status.to_string(), error, result));
    }

    /// `cancelled` when a stop tripped the token, else the error.
    fn abnormal(&self, error: &WorkflowError) -> (String, String) {
        if self.cancelled() {
            ("cancelled".to_string(), String::new())
        } else {
            ("error".to_string(), error.message.clone())
        }
    }

    /// A clean end with nothing left to run: `cancelled` when a stop asked.
    fn finish_clean(&mut self) {
        let status = if self.cancelled() { "cancelled" } else { "ok" };
        self.terminal(status, String::new(), Vec::new());
    }

    /// Whether iteration `iter` takes its seed from a message: every pass of
    /// a `PerInput` loop, the first pass of any loop.
    fn message_fed(&self) -> bool {
        self.r.spec.loop_mode == LoopMode::PerInput || self.s.iter == 0
    }

    fn barrier(&mut self, target: BarrierFor) -> u64 {
        let id = self.s.next_barrier;
        self.s.next_barrier += 1;
        self.s.barriers.insert(
            id,
            Barrier {
                target,
                reference_id: format!(
                    "task-commit-{}-{}-{}",
                    self.r.inst.instance_id,
                    id,
                    crate::util::nonce_suffix()
                ),
                command: None,
                sends: 1,
            },
        );
        id
    }

    fn drop_barrier(&mut self, id: u64) {
        if let Some(barrier) = self.s.barriers.remove(&id) {
            self.fx.drop_acks.push(barrier.reference_id);
        }
        self.disarm(Deadline::Ack(id));
    }

    // -- the restore gate and the journal's resume point --------------------

    /// The journal's plan, as the bind's state and markers describe it.
    fn open_gate(&mut self) {
        let journal = self.s.journal.take();
        let intents = stages_from_intents(self.r, &self.s.compensation_intents);
        if !intents.is_empty() && (journal.is_none() || !self.s.compensation_done.is_empty()) {
            // With a journal only a compensation-done marker proves an undo
            // was in flight: intent markers accumulate during forward
            // progress too.
            self.s.done = intents;
            return self.compensate(AfterUndo::Terminal(
                "error".to_string(),
                "resumed compensation after crash".to_string(),
            ));
        }
        let journal = journal.unwrap_or_default();
        // The last committed iteration asked to end the instance: its commit
        // is durable, so the run ends as it would have before the crash.
        if let Some(result) = journal.completion.clone() {
            return self.terminal("ok", String::new(), result);
        }
        let (committed, in_flight) = journaled_stages(self.r, &journal);
        self.s.committed = committed;
        self.s.done = in_flight;
        self.s.iter = journal.iter;
        // A committed root of some chain IS "this is a mid-iteration resume",
        // and a journal that records a child without its parent re-runs the
        // whole iteration rather than trusting the fragment.
        let root_committed = journal
            .committed_task_idxs
            .iter()
            .any(|&i| self.r.spec.dag.is_root(i as usize));
        let committed_nodes = if root_committed {
            journal
                .committed_task_idxs
                .iter()
                .map(|&i| i as usize)
                .collect()
        } else {
            Vec::new()
        };
        let rewind = rewind_steps(self.r, &journal.rewind_undo);
        self.s.resume_point = Some(MidResume {
            journal,
            committed_nodes,
        });
        // A rewind unwinds the iterations above its target before the loop
        // resumes at it; the journal already folded the forward state back.
        if !rewind.is_empty() {
            self.s.phase = Phase::Compensating;
            self.s.undo = Some(Undo {
                steps: rewind,
                outbox: Vec::new(),
                then: AfterUndo::Resume,
            });
            return self.undo_step();
        }
        self.resume();
    }

    /// Where the journal says the chain goes on: a pass the crash interrupted
    /// resumes with its committed tasklets skipped, else the loop's shape
    /// decides the next seed.
    fn resume(&mut self) {
        let Some(point) = self.s.resume_point.take() else {
            return self.enter_waiting();
        };
        let carried = Source::Journal {
            channel: carried_channel(self.r, &point.journal),
            handoff: point.journal.handoff.clone(),
        };
        if !point.committed_nodes.is_empty() {
            let fed = self.message_fed();
            self.s.resume_mid = Some(Arc::new(point));
            if fed {
                // The pass took its message before the crash; the controller
                // re-forwards it, and that message's pass resumes.
                return self.enter_waiting();
            }
            self.s.phase = Phase::Starting;
            self.fx.start = Some(carried);
            return;
        }
        match self.r.spec.loop_mode {
            LoopMode::Once if self.s.iter > 0 => self.finish_clean(),
            LoopMode::Chained if self.s.iter > 0 => {
                self.s.phase = Phase::Starting;
                self.fx.start = Some(carried);
            }
            _ => self.enter_waiting(),
        }
    }

    // -- messages and passes -----------------------------------------------------

    /// Back at a committed boundary: the next queued message starts its pass,
    /// or the instance waits, and one whose class sleeps arms its park.
    fn enter_waiting(&mut self) {
        self.s.pass = None;
        if self.cancelled() {
            return self.terminal("cancelled", String::new(), Vec::new());
        }
        if self.r.spec.loop_mode == LoopMode::Once && self.s.iter > 0 {
            return self.finish_clean();
        }
        if self.message_fed() {
            if let Some(message) = self.s.queue.pop_front() {
                self.s.phase = Phase::Starting;
                self.fx.start = Some(Source::Message(message));
                return;
            }
        }
        self.s.phase = Phase::Waiting;
        self.rearm_park_idle();
    }

    /// The idle clock restarts: the owed inbox tail's bound while any of it
    /// is still owed (a park then would drop it at the socket), else the park
    /// idle.
    fn rearm_park_idle(&mut self) {
        if self.s.phase != Phase::Waiting || !self.r.inst.sleeps() {
            return;
        }
        let idle = if self.r.inst.owed_inbox() > 0 {
            OWED_INBOX_WAIT
        } else {
            park_idle()
        };
        self.arm(Deadline::ParkIdle, idle);
    }

    fn arrived(&mut self, message: Message) {
        if self.s.phase == Phase::Waiting && self.message_fed() {
            self.disarm(Deadline::ParkIdle);
            self.s.phase = Phase::Starting;
            self.fx.start = Some(Source::Message(message));
        } else {
            self.s.queue.push_back(message);
        }
    }

    fn start_pass(
        &mut self,
        channel: ChannelId,
        offset: Option<u64>,
        roots: Vec<Release>,
        seed: Seed,
    ) {
        if self.s.phase != Phase::Starting {
            return;
        }
        if self.cancelled() {
            return self.compensate(AfterUndo::Terminal("cancelled".to_string(), String::new()));
        }
        let mut pass = Pass::new(channel, offset);
        pass.completion = seed.completion;
        for node in seed.committed_nodes {
            let mut stage = Stage::owing(0);
            stage.state = StageState::Done;
            pass.stages.insert(node, stage);
        }
        pass.banked = seed.banked;
        pass.waiting = seed.waiting;
        let mut ready: BTreeMap<usize, Vec<Release>> = seed.ready;
        for release in roots {
            ready.entry(release.node).or_default().push(release);
        }
        self.s.pass = Some(pass);
        self.s.phase = Phase::Running;
        self.r.run.pass_dead.store(false, Ordering::Release);
        for (node, items) in ready {
            self.start_stage(node, items);
        }
        self.settle_pass();
    }

    fn start_stage(&mut self, node: usize, items: Vec<Release>) {
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let empty = items.is_empty();
        pass.stages.insert(node, Stage::owing(items.len()));
        self.fx.admit.extend(items);
        if empty {
            self.stage_done(node);
        }
    }

    // -- items -------------------------------------------------------------------

    fn settled(&mut self, node: usize, cell: CellPos, slot: &Arc<CellSlot>) {
        let attempt = self
            .s
            .attempts
            .remove(&(node, cell.clone()))
            .unwrap_or_default();
        let Attempt {
            outputs,
            held,
            staged,
            completion,
            failure,
        } = attempt;
        let Some(stage) = self
            .s
            .pass
            .as_mut()
            .and_then(|pass| pass.stages.get_mut(&node))
            .filter(|stage| stage.state == StageState::Running)
        else {
            return;
        };
        // A Cell whose tasklet ran to its end is recorded, so it is undone
        // with the rest, even when an output's edge then failed.
        if failure.as_ref().is_none_or(|f| f.ran) {
            let run = stage
                .cells
                .entry(cell.clone())
                .or_insert_with(|| CellRun::new(Arc::clone(slot)));
            run.outputs.extend(outputs);
            run.staged.extend(staged);
            if completion.is_some() {
                run.completion = completion;
            }
            stage.held.entry(cell).or_default().extend(held);
        }
        if let Some(failure) = failure {
            self.fail(failure);
        }
        self.owe_less(node, 1);
    }

    fn crossed(&mut self, node: usize, items: Result<Vec<Release>, String>) {
        let Some(stage) = self
            .s
            .pass
            .as_mut()
            .and_then(|pass| pass.stages.get_mut(&node))
            .filter(|stage| stage.state == StageState::Running)
        else {
            return;
        };
        match items {
            Ok(items) => {
                stage.owed += items.len();
                self.fx.admit.extend(items);
            }
            Err(error) => self.fail(Failure {
                node,
                error: WorkflowError::non_retryable(error),
                payload: None,
                attempts: 1,
                ran: false,
            }),
        }
        self.owe_less(node, 1);
    }

    fn owe_less(&mut self, node: usize, count: usize) {
        let Some(stage) = self
            .s
            .pass
            .as_mut()
            .and_then(|pass| pass.stages.get_mut(&node))
            .filter(|stage| stage.state == StageState::Running)
        else {
            return;
        };
        stage.owed = stage.owed.saturating_sub(count);
        if stage.owed == 0 {
            self.stage_done(node);
        }
    }

    /// The pass cannot commit: the first failure is the one it ends on, and
    /// every Cell waiting out a retry is disarmed rather than re-run.
    fn fail(&mut self, failure: Failure) {
        if let Some(pass) = self.s.pass.as_mut() {
            pass.failure.get_or_insert(failure);
        }
        self.kill_pass();
    }

    /// A stage's barrier failed: the pass fails on it and the stage owes
    /// nothing more.
    fn stage_failed(&mut self, node: usize, error: String) {
        self.fail(Failure {
            node,
            error: WorkflowError::from(error),
            payload: None,
            attempts: 1,
            ran: false,
        });
        if let Some(stage) = self.s.pass.as_mut().and_then(|p| p.stages.get_mut(&node)) {
            stage.state = StageState::Done;
        }
        self.settle_pass();
    }

    /// No Cell of the pass runs again: what its Cells still have queued is
    /// consumed unrun, and every one waiting out a retry is disarmed.
    fn kill_pass(&mut self) {
        self.r.run.pass_dead.store(true, Ordering::Release);
        self.disarm_retries();
    }

    fn disarm_retries(&mut self) {
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let running: Vec<usize> = pass
            .stages
            .iter()
            .filter(|(_, stage)| stage.state == StageState::Running)
            .map(|(&node, _)| node)
            .collect();
        for node in running {
            self.fx
                .cancel_retries
                .extend(self.r.inst.slots_of(self.r.spec.dag.order[node]));
        }
    }

    // -- stages ------------------------------------------------------------------

    /// Every item the stage owed settled. A healthy pass records the stage
    /// and commits it; a failed or cancelled one records what ran, for undo,
    /// and commits nothing.
    fn stage_done(&mut self, node: usize) {
        let iter = self.s.iter;
        let dead = self.cancelled()
            || self
                .s
                .pass
                .as_ref()
                .is_some_and(|pass| pass.failure.is_some());
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let Some(stage) = pass.stages.get_mut(&node) else {
            return;
        };
        let cells: Vec<CellRun> = stage.cells.values().cloned().collect();
        let asked = node_completion(&cells);
        if dead {
            stage.state = StageState::Done;
        } else {
            pass.completion.note(node, asked.clone());
        }
        if !dead || !cells.is_empty() {
            self.s.done.push(super::compensation::StageRecord {
                stage_idx: node,
                iter,
                cells: cells.clone(),
            });
        }
        if dead {
            return self.settle_pass();
        }
        if self.r.task(node).compensate_overridden && self.r.inst.persistence_enabled() {
            for run in &cells {
                self.fx.markers.push((
                    COMPENSATION_INTENT_PAYLOAD_ID,
                    Arc::clone(&run.cell.cell_identity),
                ));
            }
        }
        if !self.r.journaling() {
            return self.stage_committed(node);
        }
        let id = self.barrier(BarrierFor::Stage(node));
        if let Some(stage) = self.s.pass.as_mut().and_then(|p| p.stages.get_mut(&node)) {
            stage.state = StageState::Committing(id);
        }
        self.fx.frames.push((
            id,
            Frame::Task {
                node,
                iter,
                cells,
                completion: asked,
            },
        ));
    }

    /// The stage's commit is durable: its held items go to its children,
    /// and a child whose producers in this chain have all committed runs. A
    /// leaf's output is the iteration's terminal batch.
    fn stage_committed(&mut self, node: usize) {
        let dead = self.cancelled();
        let dag = &self.r.spec.dag;
        let Some(pass) = self.s.pass.as_mut() else {
            return;
        };
        let Some(stage) = pass.stages.get_mut(&node) else {
            return;
        };
        stage.state = StageState::Done;
        let held: Vec<Release> = std::mem::take(&mut stage.held)
            .into_values()
            .flatten()
            .collect();
        if dead || pass.failure.is_some() {
            return self.settle_pass();
        }
        if dag.children[node].is_empty() {
            let values = stage
                .cells
                .values()
                .flat_map(|run| run.outputs.iter().map(Arc::clone));
            pass.leaf_values.extend(values);
            pass.carry.extend(held);
            return self.settle_pass();
        }
        for release in held {
            pass.banked
                .entry(release.node)
                .or_default()
                .entry(node)
                .or_default()
                .push(release);
        }
        let chain = self.r.chain(pass.channel);
        let mut ready = Vec::new();
        for &child in &dag.children[node] {
            let waiting = pass
                .waiting
                .entry(child)
                .or_insert_with(|| chain.map_or(1, |c| c.parents_in_chain(child)));
            *waiting = waiting.saturating_sub(1);
            if *waiting == 0 {
                pass.waiting.remove(&child);
                let banked = pass.banked.remove(&child).unwrap_or_default();
                ready.push((child, banked.into_values().flatten().collect()));
            }
        }
        for (child, items) in ready {
            self.start_stage(child, items);
        }
        self.settle_pass();
    }

    /// Nothing owed: a failed or cancelled pass is resolved, a healthy one
    /// sealed once every commit is durable.
    fn settle_pass(&mut self) {
        if self.s.phase != Phase::Running {
            return;
        }
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        if pass
            .stages
            .values()
            .any(|stage| stage.state == StageState::Running)
        {
            return;
        }
        if pass.failure.is_some() || self.cancelled() {
            return self.resolve_failure();
        }
        if pass
            .stages
            .values()
            .any(|stage| matches!(stage.state, StageState::Committing(_)))
        {
            return;
        }
        self.seal();
    }

    fn resolve_failure(&mut self) {
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let owed_acks: Vec<u64> = pass
            .stages
            .values()
            .filter_map(|stage| match stage.state {
                StageState::Committing(id) => Some(id),
                _ => None,
            })
            .collect();
        let channel = pass.channel;
        let offset = pass.consumed_offset;
        let failure = pass.failure.clone();
        for id in owed_acks {
            self.drop_barrier(id);
        }
        // A root whose message was unreadable, with nothing of the pass done:
        // the message parks under its entry channel and the pass never
        // happened.
        let message_fed = self.message_fed();
        let unread = failure.as_ref().filter(|f| {
            f.error.unreadable
                && message_fed
                && self.s.done.is_empty()
                && self
                    .r
                    .chain(channel)
                    .is_some_and(|c| c.roots.contains(&f.node))
        });
        if let Some(unread) = unread {
            self.fx.park_unread = Some((channel, offset, unread.clone()));
            self.s.parks_in_flight += 1;
            self.r.run.pass_dead.store(false, Ordering::Release);
            return self.enter_waiting();
        }
        let error = failure
            .map(|f| f.error)
            .unwrap_or_else(|| WorkflowError::from("cancelled"));
        let (status, error) = self.abnormal(&error);
        self.compensate(AfterUndo::Terminal(status, error));
    }

    /// The iteration boundary: a `Once` pass ends here; a loop commits the
    /// iteration and enters the next one on its ack.
    fn seal(&mut self) {
        let iter = self.s.iter;
        let Some(pass) = self.s.pass.as_ref() else {
            return;
        };
        let completion = pass.completion.result();
        if self.r.spec.loop_mode == LoopMode::Once {
            return self.terminal("ok", String::new(), completion.unwrap_or_default());
        }
        if !self.r.journaling() {
            return self.iter_acked();
        }
        let frame = Frame::Iteration {
            iter,
            offset: pass.consumed_offset,
            channel: pass.channel,
            values: pass.leaf_values.clone(),
            completion,
        };
        self.s.phase = Phase::Sealing;
        let id = self.barrier(BarrierFor::Iteration);
        self.fx.frames.push((id, frame));
    }

    fn iter_acked(&mut self) {
        let Some(pass) = self.s.pass.take() else {
            return;
        };
        let done = std::mem::take(&mut self.s.done);
        // Keep-progress never undoes a committed iteration, so it keeps no
        // record of one: nothing then holds its Cells past their eviction.
        if self.r.spec.rollback_mode == RollbackMode::AllOrNothing {
            self.s.committed.push(done);
        }
        self.s.iter += 1;
        if let Some(result) = pass.completion.result() {
            return self.terminal("ok", String::new(), result);
        }
        if self.r.spec.loop_mode != LoopMode::Chained {
            return self.enter_waiting();
        }
        // A chained loop ends on an empty batch; a stop between two passes
        // unwinds what the loop committed.
        if pass.leaf_values.is_empty() {
            return self.finish_clean();
        }
        self.s.phase = Phase::Starting;
        self.fx.start = Some(Source::Carry(Carry {
            channel: pass.channel,
            items: pass.carry,
        }));
    }

    fn barrier_answered(&mut self, target: BarrierFor, result: Result<(), String>) {
        match (target, result) {
            (BarrierFor::Stage(node), Ok(())) => self.stage_committed(node),
            (BarrierFor::Stage(node), Err(error)) => self.stage_failed(
                node,
                format!("commit {node} refused by controller: {error}"),
            ),
            (BarrierFor::Iteration, Ok(())) => {
                if self.s.phase == Phase::Sealing {
                    self.iter_acked();
                }
            }
            (BarrierFor::Iteration, Err(error)) => {
                let error = WorkflowError::from(format!(
                    "iteration commit {} refused by controller: {error}",
                    self.s.iter
                ));
                let (status, error) = self.abnormal(&error);
                self.compensate(AfterUndo::Terminal(status, error));
            }
            (BarrierFor::Undo, result) => {
                if let Err(error) = result {
                    eprintln!(
                        "[flezha] workflow {} compensation journaling failed: {error}",
                        self.r.inst.instance_id
                    );
                }
                if let Some(undo) = self.s.undo.as_mut() {
                    undo.steps.pop_front();
                }
                self.undo_step();
            }
        }
    }

    // -- the stop, the deadlines -------------------------------------------------

    fn stop(&mut self) {
        self.r.run.cancel.store(true, Ordering::Release);
        match self.s.phase {
            Phase::Gated => {
                self.disarm(Deadline::RestoreGate);
                self.open_gate();
            }
            Phase::Waiting => self.terminal("cancelled", String::new(), Vec::new()),
            Phase::Running => {
                self.kill_pass();
                self.settle_pass();
            }
            Phase::Sealing => {
                let owed: Vec<u64> = self
                    .s
                    .barriers
                    .iter()
                    .filter(|(_, b)| b.target == BarrierFor::Iteration)
                    .map(|(&id, _)| id)
                    .collect();
                for id in owed {
                    self.drop_barrier(id);
                }
                self.compensate(AfterUndo::Terminal("cancelled".to_string(), String::new()));
            }
            // A pass being seeded reads the token as it starts; rollback IS
            // the answer to a stop that finds one running.
            Phase::Starting | Phase::Compensating | Phase::Parked | Phase::Done => {}
        }
    }

    fn deadline(&mut self, deadline: Deadline) {
        match deadline {
            Deadline::ParkIdle => {
                if self.s.phase != Phase::Waiting || !self.s.queue.is_empty() {
                    return;
                }
                // An unread message still on its way to the dead-letter queue:
                // the park must carry its offset.
                if self.s.parks_in_flight > 0 {
                    return self.arm(Deadline::ParkIdle, ADMIT_RETRY);
                }
                if !self.r.inst.cells_idle() {
                    return self.arm(Deadline::ParkIdle, park_idle());
                }
                self.park();
            }
            Deadline::MaxDuration => self.stop(),
            Deadline::Ack(id) => self.ack_overdue(id),
            Deadline::RestoreGate => {
                if self.s.phase == Phase::Gated {
                    tracing::warn!(
                        target: "flezha::workflow",
                        expected = self.s.expected,
                        restored = self.s.restored,
                        "workflow crash-resume timed out waiting for Cell restores; \
                         proceeding with partially-restored state"
                    );
                    self.open_gate();
                }
            }
            Deadline::UndoGate => self.undo_step(),
            Deadline::Admit => {
                self.fx.admit.extend(self.s.unadmitted.drain(..));
            }
        }
    }

    /// Suspend at the committed boundary: `IterPark` names the last committed
    /// iteration and the last message parked unread, and the run tears down
    /// without a completion.
    fn park(&mut self) {
        let iter = self.s.iter.saturating_sub(1);
        self.fx.control.push(iter_park(
            &self.r.inst.instance_id,
            iter,
            self.r.spec.wake_after,
            self.s.parked_offset,
        ));
        self.s.phase = Phase::Parked;
        self.clear_waits();
    }

    /// An ack overdue: the barrier goes out again under its reference id (the
    /// controller journals a barrier once by identity), up to
    /// [`BARRIER_SENDS`] sends.
    fn ack_overdue(&mut self, id: u64) {
        let Some(barrier) = self.s.barriers.get_mut(&id) else {
            return;
        };
        if barrier.sends < BARRIER_SENDS {
            if let Some(command) = barrier.command.clone() {
                barrier.sends += 1;
                self.fx
                    .control
                    .push((barrier.reference_id.clone(), command));
                return self.arm(Deadline::Ack(id), ack_timeout());
            }
        }
        let Some(barrier) = self.s.barriers.remove(&id) else {
            return;
        };
        self.fx.drop_acks.push(barrier.reference_id);
        let what = match barrier.target {
            BarrierFor::Stage(node) => format!("commit {node}"),
            BarrierFor::Iteration => format!("iteration commit {}", self.s.iter),
            BarrierFor::Undo => "compensation".to_string(),
        };
        let error = format!(
            "{what} ack timed out after {} attempt(s) of {}s",
            barrier.sends,
            ack_timeout().as_secs()
        );
        match barrier.target {
            BarrierFor::Stage(node) => self.stage_failed(node, error),
            target => self.barrier_answered(target, Err(error)),
        }
    }

    // -- compensation ------------------------------------------------------------

    /// Undo what the run completed, in reverse completion order, then `then`.
    fn compensate(&mut self, then: AfterUndo) {
        self.r.run.pass_dead.store(true, Ordering::Release);
        self.s.pass = None;
        self.s.phase = Phase::Compensating;
        let steps = saga_steps(
            self.r,
            &self.s.committed,
            &self.s.done,
            &self.s.compensation_done,
        );
        self.s.undo = Some(Undo {
            steps,
            outbox: Vec::new(),
            then,
        });
        self.undo_step();
    }

    /// A pass that could not be seeded: undo what is outstanding.
    fn compensate_on(&mut self, error: WorkflowError) {
        let (status, error) = self.abnormal(&error);
        self.compensate(AfterUndo::Terminal(status, error));
    }

    /// The next step of the undo plan, or what follows its end.
    fn undo_step(&mut self) {
        let r = self.r;
        loop {
            let Some(undo) = self.s.undo.as_mut() else {
                return;
            };
            let next = match undo.steps.front() {
                None => None,
                Some(UndoStep::Cell { cell, outputs, .. }) => {
                    self.fx.run_undo = Some((Arc::clone(cell), outputs.clone()));
                    return;
                }
                Some(UndoStep::Record {
                    node,
                    iter,
                    cells_ran,
                }) => Some((*node, *iter, cells_ran.clone())),
                Some(UndoStep::Mark { identity }) => {
                    if r.inst.persistence_enabled() {
                        self.fx
                            .markers
                            .push((COMPENSATION_DONE_PAYLOAD_ID, Arc::clone(identity)));
                    }
                    undo.steps.pop_front();
                    continue;
                }
            };
            let Some((node, iter, cells_ran)) = next else {
                let finished = self.s.undo.take().expect("undo checked above");
                return match finished.then {
                    AfterUndo::Terminal(status, error) => self.terminal(&status, error, Vec::new()),
                    AfterUndo::Resume => self.resume(),
                };
            };
            if !r.journaling() {
                undo.steps.pop_front();
                continue;
            }
            let outbox = std::mem::take(&mut undo.outbox);
            let command = undo_frame(r, node, iter, cells_ran, outbox);
            let id = self.barrier(BarrierFor::Undo);
            self.fx.frames.push((id, Frame::Built(Box::new(command))));
            return;
        }
    }

    /// One Cell's undo answered. A saga journals what the hook staged before
    /// the done marker — the marker stops the hook re-running on resume, so a
    /// crash between the two must fall on the side that re-runs the
    /// (idempotent) hook. A rewind collects it into its tasklet's record.
    fn undone(&mut self, node: usize, cell: CellPos, slot: &Arc<CellSlot>) {
        let attempt = self.s.attempts.remove(&(node, cell)).unwrap_or_default();
        if let Some(failure) = attempt.failure {
            eprintln!(
                "[flezha] workflow {} compensation error: {}",
                self.r.inst.instance_id, failure.error
            );
        }
        let Some(undo) = self.s.undo.as_mut() else {
            return;
        };
        let Some(UndoStep::Cell { iter, marks, .. }) = undo.steps.pop_front() else {
            return;
        };
        let outbox = super::commit::outbox_items(slot, &attempt.staged);
        if !marks {
            undo.outbox.extend(outbox);
            return self.undo_step();
        }
        let identity = Arc::clone(&slot.cell_identity);
        undo.steps.push_front(UndoStep::Mark {
            identity: Arc::clone(&identity),
        });
        if !outbox.is_empty() {
            undo.outbox = outbox;
            undo.steps.push_front(UndoStep::Record {
                node,
                iter,
                cells_ran: vec![identity.as_ref().clone()],
            });
        }
        self.undo_step();
    }
}

/// The completion one tasklet asked for: its greatest cell key's.
fn node_completion(runs: &[CellRun]) -> Option<Vec<u8>> {
    runs.iter()
        .filter_map(|run| Some((&run.cell.cell_key, run.completion.as_ref()?)))
        .max_by(|a, b| a.0.cmp(b.0))
        .map(|(_, result)| result.clone())
}

// ---- effects, after the lock --------------------------------------------------

/// A barrier frame to build and send.
enum Frame {
    Task {
        node: usize,
        iter: u32,
        cells: Vec<CellRun>,
        completion: Option<Vec<u8>>,
    },
    Iteration {
        iter: u32,
        offset: Option<u64>,
        channel: ChannelId,
        values: Vec<Value>,
        completion: Option<Vec<u8>>,
    },
    Built(Box<Command>),
}

#[derive(Default)]
struct Effects {
    markers: Vec<(&'static str, Arc<Vec<u8>>)>,
    frames: Vec<(u64, Frame)>,
    /// Frames sent as they stand: a barrier's re-send, a park.
    control: Vec<(String, Command)>,
    drop_acks: Vec<String>,
    disarm: Vec<(Deadline, u64)>,
    arm: Vec<(Deadline, u64)>,
    admit: Vec<Release>,
    cancel_retries: Vec<Arc<CellSlot>>,
    park_unread: Option<(ChannelId, Option<u64>, Failure)>,
    completed: Option<(String, String, Vec<u8>)>,
    run_undo: Option<(Arc<CellSlot>, Vec<Value>)>,
    start: Option<Source>,
}

impl Run<'_> {
    fn act(&self, fx: Effects) {
        let Effects {
            markers,
            frames,
            control,
            drop_acks,
            disarm,
            arm,
            admit,
            cancel_retries,
            park_unread,
            completed,
            run_undo,
            start,
        } = fx;
        // Intent markers precede the commit: a crash between marker and ack
        // resolves as "not committed, re-run".
        for (payload_id, identity) in markers {
            crate::state_sink::send_compensation_record(payload_id, &identity);
        }
        for (id, frame) in frames {
            self.send_barrier(id, frame);
        }
        for (reference_id, command) in control {
            crate::runner::emit_control_request(reference_id, command);
        }
        for reference_id in drop_acks {
            crate::shutdown::drop_pending_ack(&reference_id);
        }
        let core = crate::poll_core::global();
        for (deadline, due) in disarm {
            core.disarm_instance(self.inst.seq, deadline, due);
        }
        for (deadline, due) in arm {
            core.arm_instance(self.weak(), self.inst.seq, deadline, due);
        }
        self.admit(admit);
        for slot in cancel_retries {
            items_dropped(&slot, core.cancel_retry(&slot));
        }
        if let Some((channel, offset, failure)) = park_unread {
            self.park_unread(channel, offset, failure);
        }
        if let Some((status, error, result)) = completed {
            crate::runner::emit_workflow_completed(
                self.inst.instance_id.to_string(),
                status,
                error,
                result,
            );
        }
        if let Some((slot, outputs)) = run_undo {
            self.run_undo(slot, outputs);
        }
        if let Some(source) = start {
            self.begin_pass(source);
        }
    }

    /// Build a barrier's frame (the journal codecs run here), register its
    /// ack as an event, send it, and keep it for a re-send.
    fn send_barrier(&self, id: u64, frame: Frame) {
        let built = match frame {
            Frame::Task {
                node,
                iter,
                cells,
                completion,
            } => task_frame(self, node, iter, &cells, completion),
            Frame::Iteration {
                iter,
                offset,
                channel,
                values,
                completion,
            } => iteration_frame(self, iter, offset, channel, &values, completion),
            Frame::Built(command) => Ok(*command),
        };
        let reference_id = {
            let sheet = self.run.sheet();
            match sheet.barriers.get(&id) {
                Some(barrier) => barrier.reference_id.clone(),
                None => return,
            }
        };
        let command = match built {
            Ok(command) => command,
            Err(error) => {
                return self.decide(|d| {
                    if let Some(barrier) = d.s.barriers.remove(&id) {
                        d.barrier_answered(barrier.target, Err(error.message));
                    }
                });
            }
        };
        let weak = self.weak();
        crate::shutdown::register_pending_ack(
            reference_id.clone(),
            Box::new(move |result| barrier_acked(&weak, id, result.map(|_| ()))),
        );
        crate::runner::emit_control_request(reference_id, command.clone());
        self.decide(|d| {
            if let Some(barrier) = d.s.barriers.get_mut(&id) {
                barrier.command = Some(command);
                d.arm(Deadline::Ack(id), ack_timeout());
            }
        });
    }

    /// Items into their Cells without waiting: a producing Cell's holdback
    /// takes what a full mailbox refuses; a message's item, which has no
    /// producer, waits on the sheet for a short retry. An item with no Cell
    /// yet crosses to its host for its keys.
    fn admit(&self, releases: Vec<Release>) {
        if releases.is_empty() {
            return;
        }
        let mut backlog = !self.run.sheet().unadmitted.is_empty();
        let mut refused = Vec::new();
        let mut keyless = Vec::new();
        for release in releases {
            let Release {
                node,
                cell,
                item,
                producer,
            } = release;
            let Some(cell) = cell else {
                keyless.push((node, item));
                continue;
            };
            match producer {
                Some(producer) => {
                    crate::admission::admit_from_cell(
                        &producer,
                        &cell,
                        item,
                        OverflowPolicy::DropNothing,
                        true,
                    );
                }
                None if backlog => refused.push(Release {
                    node,
                    cell: Some(cell),
                    item,
                    producer: None,
                }),
                None => {
                    if let Err(item) = crate::admission::try_admit(&cell, item) {
                        backlog = true;
                        refused.push(Release {
                            node,
                            cell: Some(cell),
                            item,
                            producer: None,
                        });
                    }
                }
            }
        }
        if !refused.is_empty() {
            self.decide(|d| {
                d.s.unadmitted.extend(refused);
                d.arm(Deadline::Admit, ADMIT_RETRY);
            });
        }
        self.cross(keyless);
    }

    /// Keyless host values into one prepare crossing per channel they were
    /// released on; each stage owes its values until the answer addresses
    /// them.
    fn cross(&self, keyless: Vec<(usize, CellItem)>) {
        if keyless.is_empty() {
            return;
        }
        let Some(owner) = self.run.me.upgrade() else {
            return;
        };
        let mut by_channel: BTreeMap<ChannelId, Vec<crate::host::StagedEntry>> = BTreeMap::new();
        for (node, item) in keyless {
            let payload = match item {
                CellItem::Input(payload) => payload,
                CellItem::Timer(_) | CellItem::CloseInterval => continue,
            };
            let Some(value) = payload.value.downcast_ref::<HostPayload>() else {
                continue;
            };
            by_channel
                .entry(value.channel)
                .or_default()
                .push(crate::host::StagedEntry {
                    bytes: value.bytes.clone(),
                    consumed_offset: None,
                    replay: false,
                    stream: Arc::clone(&payload.stream),
                    stage: Some(node),
                });
        }
        for (channel_id, entries) in by_channel {
            let Some(channel) = self.inst.topology.channel(channel_id) else {
                continue;
            };
            for unasked in crate::host::stage_prepare(&owner, channel_id, channel, entries) {
                let node = unasked.stage.expect("staged for a stage");
                crossed(
                    self.inst,
                    node,
                    Err(format!(
                        "no consumer of channel `{}` asks its host for keys",
                        channel.name
                    )),
                );
            }
        }
    }

    /// Submit one Cell's undo as a `CompensateRun`, holding the Cell's gate
    /// from here to its answer; a Cell still handed out is tried again.
    fn run_undo(&self, slot: Arc<CellSlot>, outputs: Vec<Value>) {
        if slot.drain_active.swap(true, Ordering::AcqRel) {
            return self.decide(|d| d.arm(Deadline::UndoGate, UNDO_GATE_RETRY));
        }
        let weak = self.weak();
        let answered = Arc::clone(&slot);
        crate::poll_core::global().submit_undo(
            &slot,
            outputs,
            crate::poll_core::CommandReply::new(move |outcome| {
                undo_answered(&weak, &answered, outcome)
            }),
        );
    }

    /// An unreadable message into the dead-letter queue under its entry
    /// channel, off this thread as every park is; the run counts it
    /// consumed once the park is durable.
    fn park_unread(&self, channel: ChannelId, offset: Option<u64>, failure: Failure) {
        let Some(owner) = self.run.me.upgrade() else {
            return;
        };
        let held = Arc::clone(&owner);
        crate::retry::spawn_park_thread(&owner, "flezha-workflow-unread-park", move || {
            if let Some(descriptor) = held.topology.channel(channel) {
                crate::retry::park_exhausted(
                    &format!("entry channel `{}`", descriptor.name),
                    crate::retry::ParkTarget::entry(descriptor),
                    &held.instance_id,
                    offset,
                    failure.payload,
                    failure.error.message,
                    failure.attempts,
                );
            }
            if let Some(r) = Run::of(&held) {
                r.decide(|d| {
                    d.s.parks_in_flight = d.s.parks_in_flight.saturating_sub(1);
                    d.s.parked_offset = d.s.parked_offset.max(offset);
                });
            }
        });
    }
}

#[cfg(all(test, unix))]
#[path = "run_tests.rs"]
mod tests;
