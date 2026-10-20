//! The events that advance a steps relay's pass sheet, each handled on the
//! thread that brings it: a message on the data reader, a settled item on the
//! worker that ran it, a barrier's ack on the control reader, a deadline on
//! the poller that promotes it. Every event decides under the sheet lock and
//! acts after it: no frame is sent, no item admitted and no author code run
//! while the lock is held, and nothing waits.

use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::time::Duration;

use super::commit::{ack_timeout, BARRIER_SENDS};
use super::effects::Effects;
use super::instance::{
    AfterUndo, Barrier, BarrierFor, CellPos, Deadline, PassOrigin, PassSheet, Phase, StepsRun,
};
use super::spec::{Chain, StepsSpec};
use super::{Each, StepsError};
use crate::cell::CellSlot;
use crate::channel::ChannelId;
use crate::relay::instance::RelayInstance;
use crate::relay::topology::TaskletDescriptor;

/// How long a message's item waits before a full mailbox is tried again.
pub(super) const ADMIT_RETRY: Duration = Duration::from_millis(5);
/// How long an undo waits for its Cell's handout to be answered.
pub(super) const UNDO_GATE_RETRY: Duration = Duration::from_millis(1);
/// The restore gate's cap: a restore that never lands degrades to a
/// best-effort re-run rather than a hung relay.
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

    /// Whether iteration `iter` takes its seed from a message: every pass of
    /// a `PerInput` loop, the first pass of any loop.
    pub(super) fn message_fed(&self) -> bool {
        self.r.spec.each == Each::PerInput || self.s.iter == 0
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

    pub(super) fn drop_barrier(&mut self, id: u64) {
        if let Some(barrier) = self.s.barriers.remove(&id) {
            self.fx.drop_acks.push(barrier.reference_id);
        }
        self.disarm(Deadline::Ack(id));
    }

    // -- the stop, the deadlines -------------------------------------------------

    pub(super) fn stop(&mut self) {
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
                    .filter(|(_, b)| {
                        matches!(b.target, BarrierFor::Iteration | BarrierFor::DeadLetter)
                    })
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
                    let (restored, expected) = (self.s.restored, self.s.expected);
                    flz_transport::notice!(
                        Warn,
                        "steps crash-resume timed out waiting for Cell restores ({restored} of \
                         {expected}); proceeding with partially-restored state"
                    );
                    self.open_gate();
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
            BarrierFor::Iteration | BarrierFor::DeadLetter => {
                format!("iteration commit {}", self.s.iter)
            }
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
}

#[cfg(all(test, unix))]
#[path = "run_tests.rs"]
mod tests;
