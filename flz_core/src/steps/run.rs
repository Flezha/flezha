//! The events that advance a steps relay's pass sheet, each handled on the
//! thread that brings it: a message on the data reader, a settled item on the
//! worker that ran it, a barrier's ack on the control reader, a deadline on
//! the poller that promotes it. Every event decides under the sheet lock and
//! acts after it: no frame is sent, no item admitted and no author code run
//! while the lock is held, and nothing waits.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use flz_transport::transport::FlezhaStream;

use super::commit::ack_timeout;
use super::effects::{Effects, Frame};
use super::instance::{
    AfterUndo, Barrier, BarrierFor, Carry, CellPos, Deadline, Firing, Message, MidResume,
    PassOrigin, PassSheet, Phase, Release, StepsRun, Value,
};
use super::resume::{resumed_seed, seed_from_journal, Seed};
use super::spec::{Chain, StepsSpec};
use super::StepsError;
use crate::cell::{make_cell_payload, CellItem, CellSlot};
use crate::channel::{ChannelDescriptor, ChannelId, ChannelKind, ConsumerEdge};
use crate::host::{Crossed, HostPayload};
use crate::relay::instance::RelayInstance;
use crate::relay::topology::{CellMode, TaskletDescriptor};
use crate::routing::{Destination, Entering, Routed};

/// How long a message's item waits before a full mailbox is tried again.
pub(super) const ADMIT_RETRY: Duration = Duration::from_millis(5);
/// How long an undo waits for its Cell's handout to be answered.
pub(super) const UNDO_GATE_RETRY: Duration = Duration::from_millis(1);
/// The restore gate's cap: a restore that never lands ends the run `error`
/// rather than hanging the relay or running over state that never landed.
pub(super) const RESTORE_GATE: Duration = Duration::from_secs(30);

/// One steps instance's run with the class facts every event reads.
pub(crate) struct Run<'a> {
    pub(crate) inst: &'a RelayInstance,
    pub(crate) run: &'a StepsRun,
    pub(crate) spec: &'a StepsSpec,
}

impl<'a> Run<'a> {
    pub(crate) fn of(inst: &'a RelayInstance) -> Option<Self> {
        Some(Self {
            inst,
            run: inst.steps.as_ref()?,
            spec: inst.topology.steps.as_ref()?,
        })
    }

    /// Whether this run journals its task boundaries: the one gate on the
    /// `TaskCommit` handshake, and the same fact that makes its Cells write
    /// effect records (`RelayTopology::records_effects`).
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

    pub(crate) fn chain(&self, channel: ChannelId) -> Option<&Chain> {
        self.spec.dag.chain(channel)
    }

    /// The chain a pass of `origin` runs.
    pub(crate) fn origin_chain(&self, origin: &PassOrigin) -> Option<&Chain> {
        match origin {
            PassOrigin::Message { channel } => self.chain(*channel),
            PassOrigin::Timer { node, .. } => self.spec.dag.from_node.get(*node),
        }
    }

    pub(super) fn weak(&self) -> Weak<RelayInstance> {
        self.run.me.clone()
    }

    /// Decide under the sheet lock, then act on what was decided.
    pub(super) fn decide(&self, f: impl FnOnce(&mut Decide<'_, '_, '_>)) {
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

// ---- addressing, off the lock ------------------------------------------------

impl Run<'_> {
    /// Each value through its edge of `channel`: the transform and
    /// `cells_for` run here, and each item is addressed to its Cell.
    pub(super) fn address<'e>(
        &self,
        channel: &ChannelDescriptor,
        routed: impl IntoIterator<Item = Routed<'e>>,
        stream: &Arc<FlezhaStream>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Result<Vec<Release>, String> {
        let mut items = Vec::new();
        let mut keyless = Vec::new();
        let d = &crate::routing::Delivery {
            stream,
            consumed_offset: None,
            call: None,
            replay: false,
        };
        for (edge, value) in routed {
            if self.asks_host_for_keys(channel, edge, value) {
                if let Some(node) = self.spec.dag.node_of(edge.tasklet) {
                    keyless.push(Release {
                        node,
                        cell: None,
                        item: steps_item(value, stream),
                        producer: producer.map(Arc::clone),
                    });
                }
                continue;
            }
            let to = &mut Destination::Held(&mut items);
            crate::routing::deliver_edge(self.inst, channel, edge, value, d, to)
                .map_err(|e| e.to_string())?;
        }
        let mut releases = self.releases(items, producer);
        releases.extend(keyless);
        Ok(releases)
    }

    /// A message's items addressed to the chain's roots, then the message
    /// arrives on the sheet. Inlined: it runs once per message.
    #[inline(always)]
    pub(super) fn arrive<V: Entering>(
        &self,
        channel: &ChannelDescriptor,
        channel_id: ChannelId,
        value: &V,
        stream: &Arc<FlezhaStream>,
        offset: Option<u64>,
        call: Option<&Arc<str>>,
    ) -> Result<(), String> {
        let roots = self.address(channel, value.each(channel), stream, None)?;
        self.decide(|d| {
            d.arrived(Message {
                channel: channel_id,
                offset,
                call: call.cloned(),
                roots,
            })
        });
        Ok(())
    }

    /// Whether `value` needs its host to key it for `edge`: a host releases a
    /// steps relay value untransformed and unkeyed, and a keyed consumer has no
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
    /// Cells its answer named; none when the host named none.
    pub(super) fn address_crossed(
        &self,
        node: usize,
        channel: ChannelId,
        crossed: &Crossed,
        stream: &Arc<FlezhaStream>,
    ) -> Result<Vec<Release>, String> {
        let tasklet = self.spec.dag.order[node];
        let Some(channel) = self.inst.topology.channel(channel) else {
            return Ok(Vec::new());
        };
        let routed = crossed
            .each(channel)
            .filter(|(edge, _)| edge.tasklet == tasklet);
        self.address(channel, routed, stream, None)
    }

    /// A chained loop's leaf value addressed to the roots of the chain the
    /// loop runs, as the next pass's input; nothing for any other loop.
    pub(super) fn carry_of(
        &self,
        value: &Value,
        stream: &Arc<FlezhaStream>,
        producer: &Arc<CellSlot>,
    ) -> Vec<Release> {
        if !self.spec.each.carries_leaf() {
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
                    items.push((cell, steps_item(value, stream)));
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
    pub(super) fn begin_pass(&self, source: Source) {
        if let Source::Firing(firing) = source {
            // A firing that waited may have been served or cancelled since.
            // A Cell in the middle of an invocation holds its set: that answer
            // waits for the assembly, which drops a firing no longer pending.
            let pending = firing.slot.timers.try_lock().map_or(true, |pending| {
                pending.contains(firing.fire.kind, firing.fire.scheduled_at)
            });
            return self.decide(|d| d.start_firing(firing, pending));
        }
        let mid = {
            let mut sheet = self.run.sheet();
            if sheet.phase != Phase::Starting {
                return;
            }
            sheet.resume_mid.take()
        };
        let seeded = self.seed(source, mid.as_deref());
        self.decide(|d| match seeded {
            Ok(Some((origin, message, seed))) => d.start_pass(origin, message, seed),
            Ok(None) => d.finish_clean(),
            Err(error) => d.compensate_on(error),
        });
    }

    #[allow(clippy::type_complexity)]
    fn seed(
        &self,
        source: Source,
        mid: Option<&MidResume>,
    ) -> Result<Option<(PassOrigin, Message, Seed)>, StepsError> {
        let message = |channel| PassOrigin::Message { channel };
        // Only a message's pass carries its offset and its call.
        let without_input = |channel, roots| Message {
            channel,
            offset: None,
            call: None,
            roots,
        };
        let (origin, mut pass) = match source {
            Source::Message(m) => (message(m.channel), m),
            Source::Carry(carry) => (
                message(carry.channel),
                without_input(carry.channel, carry.items),
            ),
            Source::Firing(_) => return Ok(None),
            Source::Resumed { .. } if mid.is_none() => return Ok(None),
            Source::Resumed { origin, channel } => (origin, without_input(channel, Vec::new())),
            Source::Journal { channel, handoff } => {
                let values = seed_from_journal(&self.inst.topology, &handoff, None)?;
                if values.is_empty() && mid.is_none() {
                    return Ok(None);
                }
                let stream = unwired_stream();
                let roots = self.to_roots(channel, &values, &stream, None);
                (message(channel), without_input(channel, roots))
            }
        };
        let seed = match mid {
            Some(mid) => {
                pass.roots
                    .retain(|r| !mid.committed_nodes.contains(&r.node));
                resumed_seed(self, mid, self.resumed_chain(&origin)?)?
            }
            None => Seed::default(),
        };
        Ok(Some((origin, pass, seed)))
    }

    /// The chain an interrupted pass of `origin` resumes over.
    fn resumed_chain(&self, origin: &PassOrigin) -> Result<&Chain, StepsError> {
        self.origin_chain(origin).ok_or_else(|| {
            StepsError::non_retryable(match origin {
                PassOrigin::Message { channel } => format!(
                    "channel {channel} is not an entry channel of steps {}",
                    self.inst.instance_id
                ),
                PassOrigin::Timer { .. } => format!(
                    "the journaled timer pass of steps {} names no tasklet of its chain",
                    self.inst.instance_id
                ),
            })
        })
    }
}

/// A steps relay Cell's input: no input-log offset (the pass records it) and
/// never a replay member.
pub(crate) fn steps_item(value: &Value, stream: &Arc<FlezhaStream>) -> CellItem {
    CellItem::Input(make_cell_payload(value, stream, None, false))
}

/// The connection a steps relay value that arrived on none rides — a journal
/// re-seed, an undo: the runner's secondary stream, or a detached one before
/// any is connected.
pub fn unwired_stream() -> Arc<FlezhaStream> {
    crate::state_sink::secondary_stream().unwrap_or_else(|| {
        static DETACHED: std::sync::OnceLock<Arc<FlezhaStream>> = std::sync::OnceLock::new();
        Arc::clone(
            DETACHED.get_or_init(|| Arc::new(FlezhaStream::Local(Arc::new(|_: &[u8]| Ok(()))))),
        )
    })
}

/// Where a pass's first items come from.
pub(super) enum Source {
    Message(Message),
    Carry(Carry),
    Firing(Firing),
    /// A timer-fed pass the crash interrupted after its root committed.
    Resumed {
        origin: PassOrigin,
        channel: ChannelId,
    },
    /// The journal's terminal handoff into the roots of `channel`'s chain.
    Journal {
        channel: ChannelId,
        handoff: Vec<(u32, Vec<u8>)>,
    },
}

// ---- decisions, under the lock -----------------------------------------------

pub(super) struct Decide<'d, 'r, 'a> {
    pub(super) r: &'r Run<'a>,
    pub(super) s: &'d mut PassSheet,
    pub(super) fx: &'d mut Effects,
}

impl Decide<'_, '_, '_> {
    pub(super) fn cancelled(&self) -> bool {
        self.r.run.cancelled()
    }

    pub(super) fn arm(&mut self, deadline: Deadline, after: Duration) {
        let due = crate::util::wall_now_ms().saturating_add(after.as_millis() as u64);
        if let Some(old) = self.s.armed.insert(deadline, due) {
            if old != due {
                self.fx.disarm.push((deadline, old));
            }
        }
        self.fx.arm.push((deadline, due));
    }

    pub(super) fn disarm(&mut self, deadline: Deadline) {
        if let Some(due) = self.s.armed.remove(&deadline) {
            self.fx.disarm.push((deadline, due));
        }
    }

    /// Nothing more to wait for: every deadline disarmed, every owed ack
    /// dropped.
    pub(super) fn clear_waits(&mut self) {
        for (deadline, due) in self.s.armed.drain() {
            self.fx.disarm.push((deadline, due));
        }
        for (_, barrier) in self.s.barriers.drain() {
            self.fx.drop_acks.push(barrier.reference_id);
        }
    }

    pub(super) fn terminal(&mut self, status: &str, error: String, result: Vec<u8>) {
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
    pub(super) fn abnormal(&self, error: &StepsError) -> (String, String) {
        if self.cancelled() {
            ("cancelled".to_string(), String::new())
        } else {
            ("error".to_string(), error.message.clone())
        }
    }

    /// A clean end with nothing left to run: `cancelled` when a stop asked.
    pub(super) fn finish_clean(&mut self) {
        let status = if self.cancelled() { "cancelled" } else { "ok" };
        self.terminal(status, String::new(), Vec::new());
    }

    /// Whether the current iteration takes its seed from a message.
    pub(super) fn message_fed(&self) -> bool {
        self.r.spec.each.message_fed(self.s.iter)
    }

    pub(super) fn barrier(&mut self, target: BarrierFor) -> u64 {
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

    pub(super) fn drop_barriers(&mut self, ids: Vec<u64>) {
        for id in ids {
            if let Some(barrier) = self.s.barriers.remove(&id) {
                self.fx.drop_acks.push(barrier.reference_id);
            }
            self.disarm(Deadline::Ack(id));
        }
    }

    // -- the stop, the deadlines -------------------------------------------------

    pub(super) fn stop(&mut self) {
        self.r.run.cancel.store(true, Ordering::Release);
        match self.s.phase {
            // Gated, the Cells' state has not all landed: the gate stays shut
            // and the journal and state wait for a later start.
            Phase::Gated | Phase::Waiting => self.terminal("cancelled", String::new(), Vec::new()),
            Phase::Running => {
                self.kill_pass();
                self.settle_pass();
            }
            Phase::Sealing => {
                let owed: Vec<u64> = self
                    .s
                    .barriers
                    .iter()
                    .filter(|(_, b)| b.target.commits_iteration())
                    .map(|(&id, _)| id)
                    .collect();
                self.drop_barriers(owed);
                self.compensate(AfterUndo::Terminal("cancelled".to_string(), String::new()));
            }
            // A pass being seeded reads the token as it starts; rollback IS
            // the answer to a stop that finds one running.
            Phase::Starting | Phase::Compensating | Phase::Parked | Phase::Done => {}
        }
    }

    pub(super) fn deadline(&mut self, deadline: Deadline) {
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
                    return self.arm(Deadline::ParkIdle, self.r.inst.sleep_after());
                }
                self.park();
            }
            Deadline::MaxDuration => self.stop(),
            Deadline::Ack(id) => self.ack_overdue(id),
            Deadline::RestoreGate => {
                if self.s.phase == Phase::Gated {
                    let error = format!(
                        "steps restore gate timed out: {} of {} restores landed within {} s",
                        self.s.restored,
                        self.s.expected,
                        RESTORE_GATE.as_secs()
                    );
                    self.terminal("error", error, Vec::new());
                }
            }
            Deadline::UndoGate => self.undo_step(),
            Deadline::Admit => {
                self.fx.admit.extend(self.s.unadmitted.drain(..));
            }
            Deadline::Complete => {}
        }
    }

    /// Suspend at the committed boundary: `IterPark` names the last committed
    /// iteration, the last message parked unread and the earliest pending
    /// processing-time timer, and the run tears down without a completion.
    fn park(&mut self) {
        self.fx.park = Some((self.s.iter.saturating_sub(1), self.s.parked_offset));
        self.s.phase = Phase::Parked;
        self.clear_waits();
    }

    /// A refused compensation goes out again under its reference id while
    /// [`Barrier::resend`] allows. The refusal consumed the send's waiter, so
    /// the stored command is sent as a new frame, which registers another.
    /// Whether it was sent.
    pub(super) fn resend_refused(&mut self, id: u64) -> bool {
        let Some(command) = self.s.barriers.get_mut(&id).and_then(Barrier::resend) else {
            return false;
        };
        self.fx.frames.push((id, Frame::Built(Box::new(command))));
        true
    }

    /// An ack overdue: the barrier goes out again under its reference id (the
    /// controller journals a barrier once by identity) while
    /// [`Barrier::resend`] allows.
    fn ack_overdue(&mut self, id: u64) {
        let Some(barrier) = self.s.barriers.get_mut(&id) else {
            return;
        };
        if let Some(command) = barrier.resend() {
            self.fx
                .control
                .push((barrier.reference_id.clone(), command));
            return self.arm(Deadline::Ack(id), ack_timeout());
        }
        let Some(barrier) = self.s.barriers.remove(&id) else {
            return;
        };
        self.fx.drop_acks.push(barrier.reference_id);
        let error = format!(
            "{} ack timed out after {} attempt(s) of {}s",
            barrier.target.timeout_label(self.s),
            barrier.sends,
            ack_timeout().as_secs()
        );
        barrier.target.on_timeout(self, error);
    }
}

#[cfg(all(test, unix))]
#[path = "run_tests.rs"]
mod tests;
