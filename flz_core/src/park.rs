//! The dead-letter park outside the effectively-once tier: a `park_payload`
//! frame plus a wait for the controller's `ParkPayloadAck`, re-sent until
//! acked. The invariant: the cell advances only once the park is durable
//! — delay, never loss. Under the tier the park rides the effect record.
//!
//! A payload the routing seam could not deliver is handed to the one park
//! owner of the process, which climbs its retry ladder and holds its park
//! until acked: a held payload costs its bytes, not a thread. The owner holds
//! at most [`PENDING_BOUND`] payloads; a hand-off past that waits for a free
//! slot, so the caller slows down and nothing is dropped.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::flusher::build_cell_frame;
use crate::retry::{Climb, Ladder};
use crate::routing::EntryView;
use crate::state_sink::{self, PARK_PAYLOAD_PAYLOAD_ID};
use crate::util::LockRecover;

/// Retry attempts, process-wide; rides the heartbeat.
pub static RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub static PARKED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Hand-offs that found the park owner full and waited for a free slot.
pub static PARK_STALLS_TOTAL: AtomicU64 = AtomicU64::new(0);

static PARK_SEQ: AtomicU64 = AtomicU64::new(0);

/// Wire body of a `park_payload` frame; field order is the contract, and
/// the entry is the same [`DlqEntryBody`] a tier-on park embeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkBody {
    /// Echoed in `ParkPayloadAck`; the controller's dedup handle for re-sends.
    pub park_id: String,
    pub origin_offset: Option<u64>,
    pub entry: crate::flusher::DlqEntryBody,
    /// The token of the outside call waiting on the parked input, if any
    /// (`flz.reply_to`), so the controller can answer it `call.failed`.
    pub reply_to: Option<String>,
}

const ACK_RESEND_INTERVAL: Duration = Duration::from_secs(2);
/// Warn every this many re-sends.
const WARN_EVERY_RESENDS: u64 = 5;

/// Payloads the park owner holds at most before a hand-off waits.
pub const PENDING_BOUND: usize = 4096;
static BOUND: AtomicUsize = AtomicUsize::new(PENDING_BOUND);
const START_RETRY: Duration = Duration::from_millis(100);

/// Acked park ids and the payloads the owner holds, under one lock: an ack,
/// a relay leaving, a hand-off and a freed slot all wake [`WOKEN`].
static REGISTRY: LazyLock<Mutex<Registry>> = LazyLock::new(Default::default);
static WOKEN: Condvar = Condvar::new();

pub fn mint_park_id(cell_label: &str) -> String {
    format!(
        "{}#{}",
        cell_label,
        PARK_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock_recover()
}

/// Sleep on [`WOKEN`] until woken, or until `at` when there is one.
fn sleep_until(
    reg: MutexGuard<'static, Registry>,
    at: Option<Instant>,
) -> MutexGuard<'static, Registry> {
    match at {
        Some(at) => {
            let timeout = at.saturating_duration_since(Instant::now());
            WOKEN
                .wait_timeout(reg, timeout)
                .unwrap_or_else(PoisonError::into_inner)
                .0
        }
        None => WOKEN.wait(reg).unwrap_or_else(PoisonError::into_inner),
    }
}

/// Record an ack for the park waiting on it and wake its waiter; an ack
/// for no waiting park, a re-send's duplicate or one after its relay left,
/// is dropped.
pub fn complete_ack(park_id: &str) {
    let mut reg = registry();
    if let Some(&key) = reg.awaiting.get(park_id) {
        reg.acked_keys.push(key);
    } else if reg.blocking.contains(park_id) {
        reg.acked.insert(park_id.to_string());
    }
    drop(reg);
    WOKEN.notify_all();
}

/// The wire bytes of an input for the DLQ entry; only `Vec<u8>` / `String`
/// entries round-trip, the rest park without bytes.
pub fn extract_payload_bytes<T: std::any::Any>(input: &T) -> Option<Vec<u8>> {
    let any: &dyn std::any::Any = input;
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        Some(bytes.clone())
    } else {
        any.downcast_ref::<String>().map(|s| s.as_bytes().to_vec())
    }
}

/// What runs once a park is let go of: acked, dropped for want of a
/// controller, or given up because its relay left.
pub(crate) type Then = Box<dyn FnOnce() + Send>;

/// One tier-off park, re-sent until acked. A run with no controller, local
/// mode included, logs and drops it; so does a relay that left this runner,
/// because the controller acks a park only from the runner the relay is
/// bound to, and the input is now its new owner's.
pub(crate) struct Park {
    identity: Vec<u8>,
    body: ParkBody,
    frame: Option<Vec<u8>>,
    resend_at: Option<Instant>,
    resends: u64,
    then: Option<Then>,
}

impl Park {
    /// Mint and build one tier-off park: a Cell's poison input, or a payload a
    /// ladder could not deliver; `then` runs once it is let go of.
    pub(crate) fn new(
        cell_identity: Vec<u8>,
        cell_label: &str,
        origin_offset: Option<u64>,
        reply_to: Option<&Arc<str>>,
        entry: crate::flusher::DlqEntryBody,
        then: Option<Then>,
    ) -> Self {
        Park {
            identity: cell_identity,
            body: ParkBody {
                park_id: mint_park_id(cell_label),
                origin_offset,
                entry,
                reply_to: reply_to.map(|token| token.to_string()),
            },
            frame: None,
            resend_at: None,
            resends: 0,
            then,
        }
    }

    /// One look at the park; `Some(at)` while it is held, to be looked at
    /// again then, or sooner on its ack or its relay's leave.
    fn advance(&mut self, relay: &dyn EntryView, acked: bool) -> Option<Instant> {
        if acked {
            return self.let_go();
        }
        if self.frame.is_none() && !state_sink::controller_attached() {
            eprintln!(
                "[flezha] DLQ park impossible without a controller transport; dropping poison \
                 payload on channel `{}` after {} attempts: {}",
                self.body.entry.channel, self.body.entry.attempts, self.body.entry.error
            );
            return self.let_go();
        }
        if relay.removed() {
            tracing::warn!(
                target: "flezha::dlq",
                park_id = %self.body.park_id,
                channel = %self.body.entry.channel,
                "dead-letter park dropped unacked: its relay left this runner"
            );
            return self.let_go();
        }
        let now = Instant::now();
        if let Some(at) = self.resend_at {
            if now < at {
                return Some(at);
            }
            self.note_resend();
        }
        let frame = self.frame.get_or_insert_with(|| {
            let body = postcard::to_allocvec(&self.body)
                .expect("postcard serialization of ParkBody cannot fail");
            build_cell_frame(
                &self.identity,
                PARK_SEQ.load(Ordering::Relaxed),
                &[0u8; 16],
                &body,
            )
        });
        state_sink::send(PARK_PAYLOAD_PAYLOAD_ID, frame.clone());
        self.resend_at = Some(now + ACK_RESEND_INTERVAL);
        self.resend_at
    }

    fn note_resend(&mut self) {
        self.resends += 1;
        if self.resends.is_multiple_of(WARN_EVERY_RESENDS) {
            tracing::warn!(
                target: "flezha::dlq",
                park_id = %self.body.park_id,
                channel = %self.body.entry.channel,
                resends = self.resends,
                "dead-letter park still unacked; cell held (re-sending)"
            );
        }
    }

    fn let_go(&mut self) -> Option<Instant> {
        if let Some(then) = self.then.take() {
            then();
        }
        None
    }
}

/// Ship a Cell's own tier-off park and hold the calling Cell until it is let
/// go of: park-then-advance.
pub fn park_tier_off(
    cell_identity: &[u8],
    cell_label: &str,
    origin_offset: Option<u64>,
    reply_to: Option<&Arc<str>>,
    entry: crate::flusher::DlqEntryBody,
    relay: &dyn EntryView,
) {
    let identity = cell_identity.to_vec();
    let mut park = Park::new(identity, cell_label, origin_offset, reply_to, entry, None);
    let id = park.body.park_id.clone();
    registry().blocking.insert(id.clone());
    let mut acked = false;
    while let Some(at) = park.advance(relay, acked) {
        let mut reg = registry();
        loop {
            acked = reg.acked.remove(&id);
            if acked || Instant::now() >= at || relay.removed() {
                break;
            }
            reg = sleep_until(reg, Some(at));
        }
    }
    let mut reg = registry();
    reg.blocking.remove(&id);
    reg.acked.remove(&id);
}

/// Wake every park waiting on an ack to look again at its relay. A waiter
/// looks under the registry lock and sleeps without releasing it in between,
/// so this wake cannot fall between the two.
pub(crate) fn wake_waiters() {
    registry().relays_left = true;
    WOKEN.notify_all();
}

/// What the owner holds a payload for: its ladder, then its park.
pub(crate) enum Work {
    Climbing(Ladder),
    Parking(Park),
}

/// The owner's two threads: one ships parks, re-sends them and takes their
/// acks, the other runs ladder attempts, so author code in an attempt never
/// holds up an ack.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Park = 0,
    Climb = 1,
}

/// A payload the owner holds for `relay`, counted among the relay's running
/// ladders until let go of. `bounded` while it holds one of the owner's
/// [`PENDING_BOUND`] slots: given up while it waits on a consumer's room or
/// a host's answer, so a hand-off waiting for a slot never waits on a Cell
/// that may be the very one it holds up, and taken back once that wait ends.
struct Pending {
    relay: Arc<dyn EntryView>,
    work: Work,
    due: Instant,
    bounded: bool,
}

impl Pending {
    fn lane(&self) -> Lane {
        match self.work {
            Work::Climbing(_) => Lane::Climb,
            Work::Parking(_) => Lane::Park,
        }
    }

    fn park_id(&self) -> Option<&String> {
        match &self.work {
            Work::Parking(park) => Some(&park.body.park_id),
            Work::Climbing(_) => None,
        }
    }

    /// One turn on its lane; `Some` while the payload is still held.
    fn turn(mut self, acked: bool) -> Option<Self> {
        let next = match &mut self.work {
            Work::Climbing(ladder) => {
                let climb = ladder.advance();
                self.bounded = !matches!(climb, Climb::Waiting(_));
                match climb {
                    Climb::Again(at) | Climb::Waiting(at) => Some(at),
                    Climb::Over(park) => {
                        self.work = Work::Parking(*park?);
                        Some(Instant::now())
                    }
                }
            }
            Work::Parking(park) => park.advance(&*self.relay, acked),
        };
        self.due = next?;
        Some(self)
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.relay.ladders().fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct Registry {
    /// Acks for the parks Cells wait on themselves, each taken by its waiter.
    acked: HashSet<String>,
    blocking: HashSet<String>,
    pending: HashMap<u64, Pending>,
    /// Per lane, when each held payload is next due.
    due: [BTreeSet<(Instant, u64)>; 2],
    /// The owner's parks by park id, for as long as each is held.
    awaiting: HashMap<String, u64>,
    /// Owner parks acked since the park lane last looked.
    acked_keys: Vec<u64>,
    /// Payloads holding a slot, those out on a lane for a turn included.
    held: usize,
    next_key: u64,
    relays_left: bool,
}

impl Registry {
    fn insert(&mut self, key: u64, pending: Pending) {
        if let Some(id) = pending.park_id() {
            if !self.awaiting.contains_key(id) {
                self.awaiting.insert(id.clone(), key);
            }
        }
        self.due[pending.lane() as usize].insert((pending.due, key));
        self.pending.insert(key, pending);
    }

    fn take(&mut self, key: u64) -> Option<(u64, Pending)> {
        let pending = self.pending.remove(&key)?;
        self.due[pending.lane() as usize].remove(&(pending.due, key));
        Some((key, pending))
    }

    /// What `lane` turns now, with whether it was acked: acked parks, every
    /// park once a relay left, then whatever is due.
    fn take_ready(&mut self, lane: Lane, now: Instant) -> Vec<(u64, Pending, bool)> {
        let mut keys = Vec::new();
        if lane == Lane::Park {
            keys.extend(self.acked_keys.drain(..).map(|key| (key, true)));
            if std::mem::take(&mut self.relays_left) {
                keys.extend(self.awaiting.values().map(|key| (*key, false)));
            }
        }
        let due = self.due[lane as usize].iter();
        keys.extend(
            due.take_while(|(at, _)| *at <= now)
                .map(|(_, key)| (*key, false)),
        );
        (keys.into_iter())
            .filter_map(|(key, acked)| self.take(key).map(|(key, p)| (key, p, acked)))
            .collect()
    }

    /// Put back what a turn left held; let go of the rest, slot and ack
    /// index with it.
    fn settle(
        &mut self,
        key: u64,
        park_id: Option<String>,
        was_bounded: bool,
        kept: Option<Pending>,
    ) {
        // Only the wait itself is outside the bound: a payload past it takes
        // its slot back, over the bound if need be.
        let still_bounded = kept.as_ref().is_some_and(|p| p.bounded);
        match (was_bounded, still_bounded) {
            (true, false) => self.held -= 1,
            (false, true) => self.held += 1,
            _ => {}
        }
        match kept {
            Some(pending) => self.insert(key, pending),
            None => {
                park_id.map(|id| self.awaiting.remove(&id));
            }
        }
    }

    fn next_due(&self, lane: Lane) -> Option<Instant> {
        self.due[lane as usize].first().map(|(at, _)| *at)
    }
}

thread_local! {
    static ON_LANE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// One lane of the owner, for the life of the process.
fn serve(lane: Lane) {
    ON_LANE.set(true);
    let mut reg = registry();
    loop {
        let ready = reg.take_ready(lane, Instant::now());
        if ready.is_empty() {
            let next = reg.next_due(lane);
            reg = sleep_until(reg, next);
            continue;
        }
        drop(reg);
        let turned: Vec<_> = (ready.into_iter())
            .map(|(key, pending, acked)| {
                let (park_id, bounded) = (pending.park_id().cloned(), pending.bounded);
                (key, park_id, bounded, turn_contained(pending, acked))
            })
            .collect();
        reg = registry();
        for (key, park_id, bounded, kept) in turned {
            reg.settle(key, park_id, bounded, kept);
        }
        WOKEN.notify_all();
    }
}

/// A turn whose author code unwound lets its payload go, as the thread it
/// once ran on died with it, and the lane lives on.
fn turn_contained(pending: Pending, acked: bool) -> Option<Pending> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pending.turn(acked))).unwrap_or_else(
        |panic| {
            eprintln!(
                "[flezha] a held payload's ladder unwound and was let go: {}",
                crate::util::format_panic(panic)
            );
            None
        },
    )
}

static STARTED: Mutex<[bool; 2]> = Mutex::new([false; 2]);

/// Start the owner's lanes, once per process.
pub(crate) fn start_owner() -> std::io::Result<()> {
    let mut started = STARTED.lock_recover();
    for (lane, name) in [
        (Lane::Park, "flezha-park-owner"),
        (Lane::Climb, "flezha-park-ladders"),
    ] {
        if !started[lane as usize] {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || serve(lane))?;
            started[lane as usize] = true;
        }
    }
    Ok(())
}

/// Hand a payload to the park owner, which holds it until it is delivered
/// or its park is let go of. The caller waits, counted and logged once as a
/// stall, while the owner holds [`PENDING_BOUND`] payloads or cannot start;
/// a lane of the owner never waits, as it would wait on itself.
pub(crate) fn hand_off<V: EntryView>(relay: &Arc<V>, work: Work) {
    relay.ladders().fetch_add(1, Ordering::AcqRel);
    let relay: Arc<dyn EntryView> = relay.clone();
    let mut reg = registry();
    if !ON_LANE.get() && reg.held >= BOUND.load(Ordering::Relaxed) {
        stall(format_args!(
            "the park owner is full ({} payloads held)",
            reg.held
        ));
        while reg.held >= BOUND.load(Ordering::Relaxed) {
            reg = sleep_until(reg, None);
        }
    }
    reg.held += 1;
    let key = reg.next_key;
    reg.next_key += 1;
    let (due, bounded) = (Instant::now(), true);
    reg.insert(
        key,
        Pending {
            relay,
            work,
            due,
            bounded,
        },
    );
    drop(reg);
    WOKEN.notify_all();
    let mut stalled = false;
    while let Err(e) = start_owner() {
        if !std::mem::replace(&mut stalled, true) {
            stall(format_args!("the park owner cannot start yet ({e})"));
        }
        std::thread::sleep(START_RETRY);
    }
}

fn stall(why: std::fmt::Arguments<'_>) {
    PARK_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!("[flezha] {why}; this hand-off waits");
}

/// Lower the bound for a test that drives the owner full.
#[cfg(test)]
pub(crate) fn set_pending_bound(bound: usize) {
    BOUND.store(bound, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails when: a park whose relay left the runner re-sends forever and
    /// holds its Cell's worker, because the controller acks a park only from
    /// the runner the relay is bound to.
    #[test]
    fn a_park_stops_waiting_once_its_relay_leaves_the_runner() {
        let _guard = state_sink::test_guard();
        state_sink::reset_for_test();
        let sends = Arc::new(AtomicU64::new(0));
        let sent = Arc::clone(&sends);
        state_sink::install_global_sink(Arc::new(move |payload_id: &str, _frame: &[u8]| {
            if payload_id == PARK_PAYLOAD_PAYLOAD_ID {
                sent.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }));
        let relay = crate::test_support::host_class::instance("moved", &["in"], &[], &[]);
        let parking = Arc::clone(&relay);
        let entry = crate::flusher::DlqEntryBody {
            channel: "input".to_string(),
            error: "poison".to_string(),
            attempts: 3,
            payload: None,
        };
        let parked = std::thread::spawn(move || {
            park_tier_off(b"moved", "moved/tasklet/key", None, None, entry, &*parking);
        });
        crate::test_support::spin_until("the first park send", || sends.load(Ordering::SeqCst) > 0);

        relay.shutdown_background_threads();
        assert!(
            crate::test_support::wait_until(
                ACK_RESEND_INTERVAL / 2,
                Duration::from_millis(5),
                || parked.is_finished()
            ),
            "the park still waits on a relay that is gone"
        );
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "a gone relay's park is not re-sent"
        );
        state_sink::reset_for_test();
    }

    /// Fails when: an ack for a park nobody waits on, a duplicate of a
    /// re-send or one after its relay left, is kept for ever.
    #[test]
    fn an_ack_for_no_waiting_park_is_dropped() {
        complete_ack("kb278#stale");
        assert!(!registry().acked.contains("kb278#stale"));
    }

    #[test]
    fn ack_registry_round_trips_and_consumes_once() {
        let take_ack = |id: &str| registry().acked.remove(id);
        registry().blocking.insert("seg#1".to_string());
        complete_ack("seg#1");
        assert!(take_ack("seg#1"));
        assert!(!take_ack("seg#1"), "ack is consumed exactly once");
        assert!(!take_ack("seg#never"), "unknown id is not acked");
    }

    #[test]
    fn payload_bytes_extraction_covers_entry_types() {
        assert_eq!(
            extract_payload_bytes(&b"raw".to_vec()),
            Some(b"raw".to_vec())
        );
        assert_eq!(
            extract_payload_bytes(&"text".to_string()),
            Some(b"text".to_vec())
        );
        #[derive(Clone)]
        struct Opaque;
        assert_eq!(extract_payload_bytes(&Opaque), None);
    }
}
