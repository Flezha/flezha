//! A steps instance's run: the pass sheet its events advance, its cancel
//! token and its compensation markers, carried on the one
//! [`RelayInstance`](crate::relay::instance::RelayInstance) type.
//! The sheet is what the journal's resume state describes, kept for the whole
//! run: a live instance and a resumed one are the same data.

use std::any::Any;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use flz_transport::transport::FlezhaStream;

use super::compensation::{CellRun, RecordSpan, StageRecord};
use super::StepsError;
use crate::cell::{CellItem, CellSlot};
use crate::channel::ChannelId;
use crate::relay::instance::RelayInstance;
use crate::timer::{TimerFire, TimerKind, TimerOp};

pub(crate) type Value = Arc<dyn Any + Send + Sync>;

/// A Cell's place among its stage's Cells, so they order the same whichever
/// settled first: its declared index, and for a keyed Cell (index 0) its key.
pub(crate) type CellPos = (usize, Option<Arc<str>>);

/// What a bind carries into an instance: the input the controller still owes
/// it and, for a steps relay, the Cell restores to wait for and the committed
/// journal.
#[derive(Default)]
pub struct Resume {
    pub expected: usize,
    pub owed_inbox: u32,
    pub journal: Option<JournalResume>,
}

/// An instance's deadline on the poll core's deferred-wake list: `ParkIdle`
/// for either kind, the rest a steps run's.
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
    /// A flow relay asked to end: report it once its Cells hold nothing.
    Complete,
}

pub struct StepsRun {
    /// The instance carrying this run, for the events that outlive a call:
    /// wakes and ack callbacks hold it weakly.
    pub(crate) me: Weak<RelayInstance>,
    pub(crate) cancel: AtomicBool,
    /// The pass failed or was cancelled: what its Cells still have queued is
    /// consumed without running.
    pub(crate) pass_dead: AtomicBool,
    pub(crate) sheet: Mutex<PassSheet>,
}

impl StepsRun {
    pub fn new(me: Weak<RelayInstance>) -> Self {
        Self {
            me,
            cancel: AtomicBool::new(false),
            pass_dead: AtomicBool::new(false),
            sheet: Mutex::new(PassSheet::default()),
        }
    }

    /// Take the bind's resume facts; the run waits for [`super::open`].
    pub fn bind(&self, resume: Resume) {
        let mut sheet = self.sheet();
        sheet.expected = resume.expected;
        let released = resume.journal.as_ref().and_then(|j| j.released_offset);
        sheet.taken = sheet.taken.max(released);
        sheet.journal = resume.journal;
    }

    /// Take a message's input-log offset: `false` for one this instance
    /// already took, which a repeated re-forward delivers again.
    pub(crate) fn take(&self, offset: Option<u64>) -> bool {
        let Some(offset) = offset else {
            return true;
        };
        let mut sheet = self.sheet();
        if sheet.taken.is_some_and(|taken| offset <= taken) {
            return false;
        }
        sheet.taken = Some(offset);
        true
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
    /// The entry channel and consumed offset of the last message-fed pass,
    /// which a timer-fed iteration's commit names in their place.
    pub(crate) entry: Option<(ChannelId, Option<u64>)>,
    /// A chained loop's next carry-fed pass, held while a queued firing
    /// runs first.
    pub(crate) carry: Option<Carry>,
    /// The fully committed iterations, for an all-or-nothing rollback.
    pub(crate) committed: Vec<Vec<StageRecord>>,
    /// This iteration's finished stages, in completion order.
    pub(crate) done: Vec<StageRecord>,
    pub(crate) pass: Option<Pass>,
    /// Triggers that arrived while no pass could start, in arrival order.
    pub(crate) queue: VecDeque<Trigger>,
    /// The highest input-log offset parked unread, journaled as released
    /// before the instance sleeps.
    pub(crate) parked_offset: Option<u64>,
    /// The highest input-log offset this instance took: what its journal
    /// released, raised by every message that arrives. A message at or below
    /// it is a repeat of a re-forward and is dropped.
    pub(crate) taken: Option<u64>,
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

impl PassSheet {
    /// `node`'s stage in the pass in flight, while it is running.
    pub(crate) fn running(&mut self, node: usize) -> Option<&mut Stage> {
        self.pass
            .as_mut()
            .and_then(|pass| pass.stages.get_mut(&node))
            .filter(|stage| stage.state == StageState::Running)
    }
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
/// runs, its input-log offset, the outside call waiting on it and its items
/// for the chain's roots.
pub(crate) struct Message {
    pub(crate) channel: ChannelId,
    pub(crate) offset: Option<u64>,
    pub(crate) call: Option<Arc<str>>,
    pub(crate) roots: Vec<Release>,
}

/// A chained loop's carry: the leaf batch its commit recorded, and the same
/// values addressed to the roots of the loop's chain.
pub(crate) struct Carry {
    pub(crate) channel: ChannelId,
    pub(crate) values: Vec<Value>,
    pub(crate) items: Vec<Release>,
}

/// What starts a pass.
pub(crate) enum Trigger {
    Message(Message),
    Firing(Firing),
}

/// A due timer on a Cell of the instance, waiting for its pass.
pub(crate) struct Firing {
    pub(crate) node: usize,
    pub(crate) slot: Arc<CellSlot>,
    pub(crate) fire: TimerFire,
}

impl Firing {
    pub(crate) fn new(node: usize, slot: &Arc<CellSlot>, fire: TimerFire) -> Self {
        Self {
            node,
            slot: Arc::clone(slot),
            fire,
        }
    }

    /// The firing as the item its Cell runs.
    pub(crate) fn into_release(self) -> Release {
        Release {
            node: self.node,
            cell: Some(self.slot),
            item: CellItem::Timer(self.fire),
            producer: None,
        }
    }

    pub(crate) fn key(&self) -> FiringKey {
        FiringKey {
            cell_key: self.slot.cell_key.clone(),
            kind: self.fire.kind,
            scheduled_at: self.fire.scheduled_at,
        }
    }
}

/// Which timer fired: its Cell's key, its kind and its scheduled timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FiringKey {
    pub cell_key: Option<Arc<str>>,
    pub kind: TimerKind,
    pub scheduled_at: u64,
}

impl FiringKey {
    pub(crate) fn to_wire(&self) -> crate::pb::TimerTrigger {
        crate::pb::TimerTrigger {
            cell_key: self.cell_key.as_deref().unwrap_or_default().to_string(),
            event_time: self.kind == TimerKind::EventTime,
            scheduled_at: self.scheduled_at,
        }
    }

    fn from_wire(trigger: &crate::pb::TimerTrigger) -> Self {
        Self {
            cell_key: (!trigger.cell_key.is_empty()).then(|| Arc::from(trigger.cell_key.as_str())),
            kind: if trigger.event_time {
                TimerKind::EventTime
            } else {
                TimerKind::ProcessingTime
            },
            scheduled_at: trigger.scheduled_at,
        }
    }
}

/// What a pass runs: the chain of the entry channel its message arrived on,
/// or the chain of the tasklet whose timer fired.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PassOrigin {
    Message { channel: ChannelId },
    Timer { node: usize, firing: FiringKey },
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
    pub(crate) record: Option<RecordSpan>,
    pub(crate) completion: Option<Vec<u8>>,
    pub(crate) removals: Vec<TimerOp>,
    pub(crate) failure: Option<Failure>,
}

#[derive(Clone)]
pub(crate) struct Failure {
    pub(crate) node: usize,
    /// The failing Cell's key; `None` for a declared Cell and for a failure
    /// no Cell raised.
    pub(crate) cell_key: Option<Arc<str>>,
    pub(crate) error: StepsError,
    /// The failing item's wire bytes, for an unreadable message's park.
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) attempts: u32,
    /// The tasklet ran to its end and only an output's edge failed: the Cell
    /// ran, so it is undone with the rest.
    pub(crate) ran: bool,
    /// The controller refused or never acked a stage's commit: the pass
    /// failed on its barrier, not on a tasklet.
    pub(crate) refused: bool,
}

impl Failure {
    /// One attempt's failure with nothing to park.
    pub(crate) fn new(node: usize, cell_key: Option<Arc<str>>, error: StepsError) -> Self {
        Self {
            node,
            cell_key,
            error,
            payload: None,
            attempts: 1,
            ran: false,
            refused: false,
        }
    }
}

/// A processing-time registration waiting to be armed: the Cell, the due time
/// and the connection its firing rides.
pub(crate) type HeldArm = (Arc<CellSlot>, u64, Arc<FlezhaStream>);

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
    /// An event-time firing the stage owes before its Cell's assembly has
    /// generated it.
    pub(crate) claim: Option<FiringKey>,
    /// Processing-time timers its items registered, off the wake list until
    /// the stage is done.
    pub(crate) arms: Vec<HeldArm>,
}

impl Stage {
    pub(crate) fn owing(owed: usize) -> Self {
        Self {
            owed,
            state: StageState::Running,
            cells: BTreeMap::new(),
            held: BTreeMap::new(),
            claim: None,
            arms: Vec::new(),
        }
    }

    /// Hold a registration its item made until the stage is done, so the timer
    /// never comes due inside the stage and joins it, and drop a held one a
    /// cancel names; `true` when `op` was held, `false` for a cancel, which is
    /// still disarmed.
    pub(crate) fn hold(
        &mut self,
        slot: &Arc<CellSlot>,
        op: &TimerOp,
        stream: &Arc<FlezhaStream>,
    ) -> bool {
        let at = op.scheduled_at;
        if !op.register {
            self.arms
                .retain(|(cell, due, _)| *due != at || !Arc::ptr_eq(cell, slot));
            return false;
        }
        self.arms.push((Arc::clone(slot), at, Arc::clone(stream)));
        true
    }
}

/// One iteration in progress. `channel` is the entry channel its commit
/// names, which for a timer-fed pass is the last message-fed pass's.
pub(crate) struct Pass {
    pub(crate) origin: PassOrigin,
    pub(crate) channel: ChannelId,
    pub(crate) consumed_offset: Option<u64>,
    /// The outside call the pass's message carried: every output its stages
    /// settle on a sink answers it. `None` for a timer-fed or a
    /// carry-fed pass.
    pub(crate) call: Option<Arc<str>>,
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
    /// The input-log offset that names this pass's effects: its message's,
    /// and none for a firing, which took no input of its own.
    pub(crate) fn effect_offset(&self) -> Option<u64> {
        match self.origin {
            PassOrigin::Timer { .. } => None,
            PassOrigin::Message { .. } => self.consumed_offset,
        }
    }

    pub(crate) fn new(
        origin: PassOrigin,
        channel: ChannelId,
        consumed_offset: Option<u64>,
        call: Option<Arc<str>>,
    ) -> Self {
        Self {
            origin,
            channel,
            consumed_offset,
            call,
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

/// What a barrier waits to hear: which stage, the iteration, the iteration
/// of a failed turn, or an undo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BarrierFor {
    Stage(usize),
    Iteration,
    DeadLetter,
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
    /// The Cells whose hook wrote a record, collected for the tasklet's
    /// `compensated` entry.
    pub(crate) cells: Vec<crate::pb::CommittedCell>,
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
    /// Cell's when its hook wrote a record.
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
        trigger: task.trigger.as_ref().map(FiringKey::from_wire),
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
    /// The input-log offset the last committed iteration consumed.
    pub consumed_offset: Option<u64>,
    /// The highest input-log offset the journal released, by an iteration
    /// commit or an input dead-lettered unread.
    pub released_offset: Option<u64>,
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
    /// The firing that opened the pass this tasklet was the root of.
    pub trigger: Option<FiringKey>,
}

impl JournalResume {
    /// The firing that opened the in-flight iteration, when its root
    /// committed with one.
    pub(crate) fn in_flight_trigger(&self) -> Option<(usize, &FiringKey)> {
        self.firings()
            .find(|(iter, ..)| *iter == self.iter)
            .map(|(_, node, firing)| (node, firing))
    }

    /// Every firing a committed root stage names: its iteration, its tasklet
    /// and the firing. None of them may fire again.
    pub(crate) fn firings(&self) -> impl Iterator<Item = (u32, usize, &FiringKey)> {
        self.committed_tasks
            .iter()
            .filter_map(|task| Some((task.iter, task.task_idx as usize, task.trigger.as_ref()?)))
    }
}

impl From<&crate::pb::TaskJournalState> for JournalResume {
    fn from(journal: &crate::pb::TaskJournalState) -> Self {
        Self {
            committed_task_idxs: journal.committed_task_idxs.clone(),
            iter: journal.iter,
            entry_channel: journal.entry_channel.clone(),
            consumed_offset: journal.consumed_offset,
            released_offset: journal.released_offset,
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
