//! A workflow instance's runtime state: its cancellation token and inbox
//! plus, for a structured class, the pre-built Cell slots and the
//! crash-resume gate. Held in the one [`OperativeRegistry`], so every cancel,
//! inbox frame and restore reaches it through the same entry.
//!
//! [`OperativeRegistry`]: crate::operative::registry::OperativeRegistry

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver, Sender};

use super::spec::WorkflowSpec;
use crate::cell::CellSlot;
use crate::channel::ChannelId;
use crate::operative::topology::OperativeTopology;

/// The crash-resume wait's cap; a restore that never lands degrades to a
/// best-effort re-run rather than a hung operative.
const RESUME_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
/// The resume wait's cancel-check cadence.
const RESUME_WAIT_CHUNK: Duration = Duration::from_millis(200);

fn marker_set(set: &Mutex<HashSet<Vec<u8>>>) -> std::sync::MutexGuard<'_, HashSet<Vec<u8>>> {
    set.lock().unwrap_or_else(|p| p.into_inner())
}

fn committed_task(task: &crate::pb::TaskCommittedTask) -> CommittedTask {
    CommittedTask {
        task_idx: task.task_idx,
        iter: task.iter,
        cells_ran: task.cells_ran.clone(),
        completion: task.completes.then(|| task.result.clone()),
    }
}

fn handoff_items(items: &[crate::pb::TaskHandoffItem]) -> Vec<(u32, Vec<u8>)> {
    items
        .iter()
        .map(|item| (item.channel_id, item.payload.clone()))
        .collect()
}

/// One message through the entrance: the entry channel it named, its value
/// as that channel's `wrap_input` made it, its input-log offset (`None`
/// when unjournaled) and whether the controller re-forwarded it.
#[derive(Clone)]
pub struct InboxItem {
    pub channel_id: ChannelId,
    pub value: Arc<dyn Any + Send + Sync>,
    pub offset: Option<u64>,
    pub replay: bool,
}

/// What the entrance hands the driver: a message to run a pass over, or the
/// input-log offset (`None` when unjournaled) of one it parked to the
/// dead-letter queue instead, which the run consumes without a pass.
pub enum Inbound {
    Message(InboxItem),
    Parked(Option<u64>),
}

pub struct WorkflowInstance {
    pub instance_id: Arc<str>,
    /// The class this instance was spawned from.
    pub factory_id: Arc<str>,
    pub spec: Arc<WorkflowSpec>,
    /// Tripped by a stop, a removal or `max_duration`.
    pub cancel: Arc<AtomicBool>,
    inbox_tx: Sender<Inbound>,
    /// Retry ladders still running for this instance, each holding a payload
    /// on its own thread rather than in the inbox.
    pub(crate) ladders: AtomicUsize,
    /// Inbox items the controller still owes this bind; the loop does not
    /// park before they arrive.
    pub owed_inbox: u32,
    /// Pre-built Cells per stage; a restore rebuilds a slot in place
    /// before the gate releases the driver.
    pub stage_cells: Vec<Vec<Arc<CellSlot>>>,
    /// Cell restores to wait for before the first tasklet runs.
    expected: usize,
    restored: Mutex<usize>,
    restored_cv: Condvar,
    /// Cells whose tasklet completed forward and may need undo.
    compensation_intents: Mutex<HashSet<Vec<u8>>>,
    /// Cells whose compensation already ran before the crash.
    compensation_done: Mutex<HashSet<Vec<u8>>>,
    /// The committed task-journal state the bind carried; instance-scoped, so
    /// it rides the bind and not a per-Cell restore.
    task_journal: Option<JournalResume>,
}

/// Committed task-journal state for one crash-resume: the iteration to
/// resume at (after the highest committed one), the entry channel whose
/// chain the last committed iteration ran, the tasklets committed inside the
/// resumed one with what each released, the previous terminal batch, and
/// every forward commit for the compensation planner.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalResume {
    pub committed_task_idxs: Vec<u32>,
    pub iter: u32,
    /// The name of the entry channel the last committed iteration's chain
    /// belongs to; empty before any iteration committed.
    pub entry_channel: String,
    pub handoff: Vec<(u32, Vec<u8>)>,
    pub task_handoffs: Vec<EdgeHandoff>,
    pub committed_tasks: Vec<CommittedTask>,
    /// Tasklets a rewind unwound that nothing has compensated yet.
    pub rewind_undo: Vec<CommittedTask>,
    /// The result the last committed iteration's `complete` asked for.
    pub completion: Option<Vec<u8>>,
}

/// What one committed tasklet of the in-flight iteration released.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EdgeHandoff {
    pub task_idx: u32,
    pub handoff: Vec<(u32, Vec<u8>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedTask {
    pub task_idx: u32,
    pub iter: u32,
    pub cells_ran: Vec<Vec<u8>>,
    /// The result this tasklet's `complete` asked for.
    pub completion: Option<Vec<u8>>,
}

impl From<&crate::pb::TaskJournalState> for JournalResume {
    fn from(journal: &crate::pb::TaskJournalState) -> Self {
        Self {
            committed_task_idxs: journal.committed_task_idxs.clone(),
            iter: journal.iter,
            entry_channel: journal.entry_channel.clone(),
            handoff: handoff_items(&journal.handoff),
            task_handoffs: journal
                .task_handoffs
                .iter()
                .map(|edge| EdgeHandoff {
                    task_idx: edge.task_idx,
                    handoff: handoff_items(&edge.handoff),
                })
                .collect(),
            committed_tasks: journal.committed_tasks.iter().map(committed_task).collect(),
            rewind_undo: journal.rewind_undo.iter().map(committed_task).collect(),
            completion: journal.completed.then(|| journal.result.clone()),
        }
    }
}

impl WorkflowInstance {
    /// The instance and the driver's inbox `Receiver`; `expected` restores
    /// gate the driver (0 runs at once).
    pub fn new(
        instance_id: Arc<str>,
        spec: Arc<WorkflowSpec>,
        stage_cells: Vec<Vec<Arc<CellSlot>>>,
        expected: usize,
        owed_inbox: u32,
        task_journal: Option<JournalResume>,
    ) -> (Arc<Self>, Receiver<Inbound>) {
        let (inbox_tx, inbox_rx) = unbounded();
        let instance = Arc::new(Self {
            instance_id,
            factory_id: Arc::from(spec.topology.factory_id),
            spec,
            cancel: Arc::new(AtomicBool::new(false)),
            inbox_tx,
            ladders: AtomicUsize::new(0),
            stage_cells,
            expected,
            owed_inbox,
            restored: Mutex::new(0),
            restored_cv: Condvar::new(),
            compensation_intents: Mutex::new(HashSet::new()),
            compensation_done: Mutex::new(HashSet::new()),
            task_journal,
        });
        (instance, inbox_rx)
    }

    pub fn task_journal_resume(&self) -> Option<JournalResume> {
        self.task_journal.clone()
    }

    /// An undo pass was in flight before the crash: compensation resume
    /// outranks forward resume.
    pub fn any_compensation_done(&self) -> bool {
        !marker_set(&self.compensation_done).is_empty()
    }

    /// Record a Cell's shipped-back saga markers.
    pub fn note_compensation_markers(&self, cell_identity: &[u8], intent: bool, done: bool) {
        if intent {
            marker_set(&self.compensation_intents).insert(cell_identity.to_vec());
        }
        if done {
            marker_set(&self.compensation_done).insert(cell_identity.to_vec());
        }
    }

    pub fn has_compensation_intent(&self, cell_identity: &[u8]) -> bool {
        marker_set(&self.compensation_intents).contains(cell_identity)
    }

    pub fn has_compensation_done(&self, cell_identity: &[u8]) -> bool {
        marker_set(&self.compensation_done).contains(cell_identity)
    }

    pub fn request_cancel(&self) {
        self.cancel.store(true, Ordering::Release);
    }

    /// Into the driver's inbox; `false` once the driver finished.
    pub fn push_inbox(&self, item: Inbound) -> bool {
        self.inbox_tx.send(item).is_ok()
    }

    pub fn topology(&self) -> &Arc<OperativeTopology> {
        &self.spec.topology
    }

    /// The slots of the stage that runs tasklet `tasklet_id`, in the order
    /// `build_stage_cells` built them: a static stage's in `cell_keys` order.
    pub fn cells_of(&self, tasklet_id: crate::channel::TaskletId) -> Option<&[Arc<CellSlot>]> {
        let position = self
            .spec
            .dag
            .order
            .iter()
            .position(|&id| id == tasklet_id)?;
        self.stage_cells.get(position).map(Vec::as_slice)
    }

    /// One of this instance's slots by wire identity.
    pub fn cell_by_identity(&self, cell_id: &[u8]) -> Option<Arc<CellSlot>> {
        self.stage_cells
            .iter()
            .flatten()
            .find(|slot| slot.cell_identity.as_slice() == cell_id)
            .map(Arc::clone)
    }

    /// One Cell restore landed.
    pub fn note_restored(&self) {
        let mut n = self.restored.lock().unwrap_or_else(|p| p.into_inner());
        *n += 1;
        self.restored_cv.notify_all();
    }

    /// Hold the driver until every expected restore landed, a cancel tripped,
    /// or the cap elapsed.
    pub fn wait_for_restore(&self) {
        if self.expected == 0 {
            return;
        }
        let deadline = Instant::now() + RESUME_WAIT_TIMEOUT;
        let mut n = self.restored.lock().unwrap_or_else(|p| p.into_inner());
        while *n < self.expected {
            if self.cancel.load(Ordering::Acquire) {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                tracing::warn!(
                    target: "flezha::workflow",
                    expected = self.expected,
                    restored = *n,
                    "workflow crash-resume timed out waiting for Cell restores; \
                     proceeding with partially-restored state"
                );
                return;
            }
            let wait = RESUME_WAIT_CHUNK.min(deadline - now);
            let (guard, _) = self
                .restored_cv
                .wait_timeout(n, wait)
                .unwrap_or_else(|p| p.into_inner());
            n = guard;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An instance with no Cells, enough for the resume-count logic.
    fn gate(expected: usize) -> Arc<WorkflowInstance> {
        let factory_id = format!("gate_factory_{}", std::process::id());
        let json = format!(
            r#"{{"factory_id":"{factory_id}","sdk_lang":1,"sdk_version":"0.1.0","persistent":true,
                 "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"}}],"workflow":{{}}}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        let spec = super::super::registry::get_class(&factory_id).expect("class registered");
        WorkflowInstance::new(Arc::from("gate-test"), spec, Vec::new(), expected, 0, None).0
    }

    #[test]
    fn zero_expected_never_blocks() {
        gate(0).wait_for_restore();
    }

    #[test]
    fn gate_releases_once_expected_restores_land() {
        // Recording `expected` restores up front means the next wait returns
        // immediately — the count is the authoritative release signal.
        let g = gate(2);
        g.note_restored();
        g.note_restored();
        g.wait_for_restore();
    }

    #[test]
    fn cancel_short_circuits_the_gate() {
        let g = gate(5);
        g.request_cancel();
        g.wait_for_restore();
    }

    #[test]
    fn note_restored_wakes_a_blocked_waiter() {
        // A driver already parked on the gate is released as soon as the last
        // expected restore lands — the cross-thread wake path.
        let g = gate(1);
        let (started_tx, started_rx) = unbounded::<()>();
        let waiter = {
            let g = Arc::clone(&g);
            std::thread::spawn(move || {
                let _ = started_tx.send(());
                g.wait_for_restore();
            })
        };
        let _ = started_rx.recv();
        g.note_restored();
        waiter.join().expect("waiter joins after its restore lands");
    }
}
