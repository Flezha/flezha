//! A workflow instance's run: the pass sheet its events advance, its cancel
//! token and its compensation markers, carried on the one
//! [`OperativeInstance`](crate::operative::instance::OperativeInstance) type.
//! The sheet is what the journal's resume state describes, kept for the whole
//! run: a live instance and a resumed one are the same data.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use super::compensation::{CellRun, StageRecord, StagedOutput};
use super::WorkflowError;
use crate::cell::{CellItem, CellSlot};
use crate::channel::ChannelId;
use crate::operative::instance::OperativeInstance;

pub(crate) type Value = Arc<dyn Any + Send + Sync>;

/// A Cell's place among its stage's Cells, so they order the same whichever
/// settled first: its declared index, and for a keyed Cell (index 0) its key.
pub(crate) type CellPos = (usize, Option<Arc<str>>);

/// What a bind carries into a workflow instance: the Cell restores to wait
/// for, the input the controller still owes it and the committed journal.
#[derive(Default)]
pub struct Resume {
    pub expected: usize,
    pub owed_inbox: u32,
    pub journal: Option<JournalResume>,
}

/// A workflow deadline on the poll core's deferred-wake list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Deadline {
    /// Waiting at a committed boundary long enough to suspend.
    ParkIdle,
    MaxDuration,
    /// A barrier's ack is overdue: re-send it, or give up on it.
    Ack(u64),
    /// Cell restores that never landed: proceed partially restored.
    RestoreGate,
    /// An undo found its Cell still handed out: try again.
    UndoGate,
    /// A message's item found its Cell's mailbox full: try again.
    Admit,
}

static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct WorkflowRun {
    /// The instance carrying this run, for the events that outlive a call:
    /// wakes and ack callbacks hold it weakly.
    pub(crate) me: Weak<OperativeInstance>,
    /// This run's identity on the deferred-wake list: never reused, where an
    /// instance id comes back with a rehydration.
    pub(crate) seq: u64,
    pub(crate) cancel: AtomicBool,
    /// The pass failed or was cancelled: what its Cells still have queued is
    /// consumed without running.
    pub(crate) pass_dead: AtomicBool,
    pub(crate) sheet: Mutex<PassSheet>,
}

impl WorkflowRun {
    pub fn new(me: Weak<OperativeInstance>) -> Self {
        Self {
            me,
            seq: RUN_SEQ.fetch_add(1, Ordering::Relaxed),
            cancel: AtomicBool::new(false),
            pass_dead: AtomicBool::new(false),
            sheet: Mutex::new(PassSheet::default()),
        }
    }

    /// Take the bind's resume facts; the run waits for [`super::open`].
    pub fn bind(&self, resume: Resume) {
        let mut sheet = self.sheet();
        sheet.expected = resume.expected;
        sheet.owed_inbox = resume.owed_inbox;
        sheet.journal = resume.journal;
    }

    pub(crate) fn sheet(&self) -> MutexGuard<'_, PassSheet> {
        self.sheet.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }
}

/// Where a run is. A stage's own progress lives on the pass, so sibling
/// stages may run, commit and finish in any interleaving within `Running`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Phase {
    /// Waiting for the bind's Cell restores.
    #[default]
    Gated,
    /// At a committed boundary with no pass running.
    Waiting,
    /// A pass is being seeded off the lock.
    Starting,
    Running,
    /// Every stage committed; the iteration's barrier is on the wire.
    Sealing,
    Compensating,
    Parked,
    Done,
}

/// The sheet: everything a run knows between two events.
#[derive(Default)]
pub(crate) struct PassSheet {
    pub(crate) phase: Phase,
    pub(crate) iter: u32,
    /// The bind is complete: the gate may open.
    pub(crate) bound: bool,
    pub(crate) expected: usize,
    pub(crate) restored: usize,
    /// The saga markers the restores shipped back: Cells whose tasklet
    /// completed forward and may need undo, and Cells whose undo already ran
    /// before the crash.
    pub(crate) compensation_intents: HashSet<Vec<u8>>,
    pub(crate) compensation_done: HashSet<Vec<u8>>,
    /// The bind's journal, until the gate opens.
    pub(crate) journal: Option<JournalResume>,
    /// The journal's resume point, while a rewind's undo runs ahead of it.
    pub(crate) resume_point: Option<MidResume>,
    /// A mid-iteration resume that waits for its message's re-forward.
    pub(crate) resume_mid: Option<Arc<MidResume>>,
    /// The fully committed iterations, for an all-or-nothing rollback.
    pub(crate) committed: Vec<Vec<StageRecord>>,
    /// This iteration's finished stages, in completion order.
    pub(crate) done: Vec<StageRecord>,
    pub(crate) pass: Option<Pass>,
    /// Messages that arrived while no pass could start.
    pub(crate) queue: VecDeque<Message>,
    pub(crate) owed_inbox: u32,
    /// The highest input-log offset parked unread, released by the park record.
    pub(crate) parked_offset: Option<u64>,
    /// Unread messages still on their way to the dead-letter queue.
    pub(crate) parks_in_flight: usize,
    /// One attempt's outputs per `(node, Cell)`, until the item settles.
    pub(crate) attempts: HashMap<(usize, CellPos), Attempt>,
    pub(crate) barriers: HashMap<u64, Barrier>,
    pub(crate) next_barrier: u64,
    /// The due time of each armed deadline, so a stale one is recognised.
    pub(crate) armed: HashMap<Deadline, u64>,
    pub(crate) undo: Option<Undo>,
    /// Items a full mailbox refused that no producing Cell can hold.
    pub(crate) unadmitted: VecDeque<Release>,
}

/// One item on its way into a Cell: the stage it belongs to, the Cell, and
/// the producing Cell whose holdback takes it when the mailbox is full
/// (`None` for a message's item and a journal re-seed). A host value bound
/// for a keyed tasklet has no Cell until the host answers its keys.
pub(crate) struct Release {
    pub(crate) node: usize,
    pub(crate) cell: Option<Arc<CellSlot>>,
    pub(crate) item: CellItem,
    pub(crate) producer: Option<Arc<CellSlot>>,
}

/// A message's pass, seeded when it arrived: the entry channel whose chain it
/// runs, its input-log offset and its items for the chain's roots.
pub(crate) struct Message {
    pub(crate) channel: ChannelId,
    pub(crate) offset: Option<u64>,
    pub(crate) roots: Vec<Release>,
}

pub(crate) struct Carry {
    pub(crate) channel: ChannelId,
    pub(crate) items: Vec<Release>,
}

/// A pass the crash interrupted after some of its tasklets committed.
pub(crate) struct MidResume {
    pub(crate) journal: JournalResume,
    pub(crate) committed_nodes: Vec<usize>,
}

/// What one attempt of one item produced. A failed attempt's outputs never
/// count, so they wait here until the item settles.
#[derive(Default)]
pub(crate) struct Attempt {
    pub(crate) outputs: Vec<Value>,
    pub(crate) held: Vec<Release>,
    pub(crate) staged: Vec<StagedOutput>,
    pub(crate) completion: Option<Vec<u8>>,
    pub(crate) failure: Option<Failure>,
}

#[derive(Clone)]
pub(crate) struct Failure {
    pub(crate) node: usize,
    pub(crate) error: WorkflowError,
    /// The failing item's wire bytes, for an unreadable message's park.
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) attempts: u32,
    /// The tasklet ran to its end and only an output's edge failed: the Cell
    /// ran, so it is undone with the rest.
    pub(crate) ran: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StageState {
    Running,
    Committing(u64),
    Done,
}

pub(crate) struct Stage {
    /// Items admitted for the stage and not yet settled.
    pub(crate) owed: usize,
    pub(crate) state: StageState,
    pub(crate) cells: BTreeMap<CellPos, CellRun>,
    /// Settled outputs addressed to the children's Cells, per producing Cell,
    /// released on commit in Cell order whichever settled first.
    pub(crate) held: BTreeMap<CellPos, Vec<Release>>,
}

impl Stage {
    pub(crate) fn owing(owed: usize) -> Self {
        Self {
            owed,
            state: StageState::Running,
            cells: BTreeMap::new(),
            held: BTreeMap::new(),
        }
    }
}

/// One iteration in progress.
pub(crate) struct Pass {
    pub(crate) channel: ChannelId,
    pub(crate) consumed_offset: Option<u64>,
    pub(crate) stages: BTreeMap<usize, Stage>,
    /// Items a join received from each committed producer, waiting for the
    /// rest; released in producer order, whichever committed first.
    pub(crate) banked: HashMap<usize, BTreeMap<usize, Vec<Release>>>,
    pub(crate) waiting: HashMap<usize, usize>,
    pub(crate) completion: Completion,
    /// What the leaves released, the iteration's terminal handoff.
    pub(crate) leaf_values: Vec<Value>,
    /// A chained loop's next pass, addressed to the roots by the leaves.
    pub(crate) carry: Vec<Release>,
    pub(crate) failure: Option<Failure>,
}

impl Pass {
    pub(crate) fn new(channel: ChannelId, consumed_offset: Option<u64>) -> Self {
        Self {
            channel,
            consumed_offset,
            stages: BTreeMap::new(),
            banked: HashMap::new(),
            waiting: HashMap::new(),
            completion: Completion::default(),
            leaf_values: Vec::new(),
            carry: Vec::new(),
            failure: None,
        }
    }
}

/// What a barrier waits to hear: which stage, the iteration, or an undo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BarrierFor {
    Stage(usize),
    Iteration,
    Undo,
}

pub(crate) struct Barrier {
    pub(crate) target: BarrierFor,
    pub(crate) reference_id: String,
    /// Stored once the frame is built, for a re-send under the same id.
    pub(crate) command: Option<crate::pb::command_request::Command>,
    pub(crate) sends: u32,
}

/// A compensation in progress: the Cells still to undo, in order, and what
/// happens once the last one is done.
pub(crate) struct Undo {
    pub(crate) steps: VecDeque<UndoStep>,
    /// A rewind journals one entry per tasklet; this collects its outbox.
    pub(crate) outbox: Vec<crate::pb::TaskOutboxItem>,
    pub(crate) then: AfterUndo,
}

pub(crate) enum UndoStep {
    /// Run one Cell's `compensate` over the outputs it recorded; `marks` says
    /// whether it earns a compensation-done marker (a saga does, a rewind
    /// does not).
    Cell {
        iter: u32,
        cell: Arc<CellSlot>,
        outputs: Vec<Value>,
        marks: bool,
    },
    /// Journal a tasklet's undo: a rewound one's even when empty, a saga
    /// Cell's when its hook staged output.
    Record {
        node: usize,
        iter: u32,
        cells_ran: Vec<Vec<u8>>,
    },
    /// Mark a saga Cell's undo done, so a resume skips it.
    Mark { identity: Arc<Vec<u8>> },
}

pub(crate) enum AfterUndo {
    Terminal(String, String),
    /// A rewind's undo ran; the journal's resume point is next.
    Resume,
}

/// The completion an iteration's tasklets asked for, resolved so it never
/// depends on which Cell answered first: the tasklet latest in the chain's
/// commit order wins, and within one tasklet the greatest cell key.
#[derive(Default)]
pub(crate) struct Completion(Option<(usize, Vec<u8>)>);

impl Completion {
    pub(crate) fn note(&mut self, node: usize, asked: Option<Vec<u8>>) {
        let Some(result) = asked else {
            return;
        };
        if self.0.as_ref().is_none_or(|(held, _)| node > *held) {
            self.0 = Some((node, result));
        }
    }

    pub(crate) fn result(&self) -> Option<Vec<u8>> {
        self.0.as_ref().map(|(_, result)| result.clone())
    }
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
