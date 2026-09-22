//! Tasklet-requested spawn of a workflow operative
//! (`tasklet-requested-spawn`).
//!
//! [`request`] ships one `SpawnWorkflowRequest` on the control plane and hands
//! the caller a [`SpawnHandle`] at once. The request never blocks the
//! tasklet and its failure is never the tasklet's failure: everything the
//! controller decides comes back through the handle, and a handle nobody reads
//! reports a failure to the log instead of vanishing. The Rust SDK holds the
//! handle directly; every other host reaches the same one through
//! `flz_core_ffi`'s spawn request and result read.

use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::shutdown::AckResult;

/// How long after its request a spawn result is awaited before it is reported
/// as timed out: the deadline every request over [`crate::shutdown::ask`]
/// waits out.
pub const SPAWN_RESULT_TIMEOUT: Duration = crate::shutdown::REPLY_TIMEOUT;

/// What to do when the requested id is already running.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IfRunning {
    /// Deliver the payload to the running instance's inbox.
    #[default]
    Deliver,
    /// Leave the running instance alone and report
    /// [`SpawnError::AlreadyRunning`].
    Error,
}

impl IfRunning {
    pub fn as_wire(self) -> &'static str {
        match self {
            IfRunning::Deliver => "deliver",
            IfRunning::Error => "error",
        }
    }

    /// The policy a host named, where an empty word is the default.
    pub fn from_wire(word: &str) -> Option<Self> {
        match word {
            "" | "deliver" => Some(IfRunning::Deliver),
            "error" => Some(IfRunning::Error),
            _ => None,
        }
    }
}

/// The instance a spawn names. Built only from an id, so a policy for a
/// running instance cannot be written for a spawn that has none: without an
/// id the controller generates one and always starts a new instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnOptions {
    id: String,
    if_running: IfRunning,
}

impl SpawnOptions {
    pub fn id(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            if_running: IfRunning::default(),
        }
    }

    pub fn if_running(mut self, if_running: IfRunning) -> Self {
        self.if_running = if_running;
        self
    }
}

/// How a spawn request reached an instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnOutcome {
    /// A fresh instance was started with the payload as its first input.
    Spawned,
    /// The id was taken: the payload became one more input of the running
    /// instance.
    Delivered,
    /// The invocation was a replay of already-processed input, so no request
    /// was issued. Spawning is a side effect of live processing only.
    SkippedOnReplay,
}

impl SpawnOutcome {
    pub fn as_wire(self) -> &'static str {
        match self {
            SpawnOutcome::Spawned => "spawned",
            SpawnOutcome::Delivered => "delivered",
            SpawnOutcome::SkippedOnReplay => "skipped_on_replay",
        }
    }
}

/// A spawn that reached an instance: the id it resolved to (the caller's, or
/// the one the controller generated) and how it got there. A replayed spawn
/// without an id answers an empty id, because the generated one exists only
/// in the live run's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnResult {
    pub id: String,
    pub outcome: SpawnOutcome,
}

/// Why a spawn request reached no instance. None of these fault the requesting
/// tasklet: they resolve its handle.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SpawnError {
    #[error("an instance of `{0}` is already running and if_running was Error")]
    AlreadyRunning(String),

    #[error("no connected runner registers factory `{0}`")]
    FactoryUnknown(String),

    #[error("factory `{0}` is a stream, and only a workflow is spawned")]
    FactoryNotWorkflow(String),

    #[error("the controller refused the spawn: {0}")]
    Refused(String),

    #[error("no spawn result within {}s", SPAWN_RESULT_TIMEOUT.as_secs())]
    Timeout,

    #[error("no control plane to request a spawn on")]
    Unavailable,
}

impl SpawnError {
    pub fn as_wire(&self) -> &'static str {
        match self {
            SpawnError::AlreadyRunning(_) => "already_running",
            SpawnError::FactoryUnknown(_) => "factory_unknown",
            SpawnError::FactoryNotWorkflow(_) => "factory_not_workflow",
            SpawnError::Refused(_) => "refused",
            SpawnError::Timeout => "timeout",
            SpawnError::Unavailable => "unavailable",
        }
    }
}

/// The ack a controller's spawn reply wakes its handle with. The reply reports
/// two values through a channel that carries one string, and an outcome word
/// never holds a tab, so the first tab splits them whatever the id holds.
pub fn encode_ack(outcome: &str, instance_id: &str) -> String {
    format!("{outcome}\t{instance_id}")
}

/// The outcome word and the id of an ack [`encode_ack`] wrote.
fn decode_ack(ack: &str) -> (&str, &str) {
    ack.split_once('\t').unwrap_or((ack, ""))
}

/// The async result of a spawn request. Reading it is optional: a handle
/// dropped before its result arrives keeps waiting in the background and logs
/// a warning if the request failed, so a fire-and-forget requester never loses
/// the diagnosis.
#[must_use = "a SpawnHandle reports the spawn's outcome; drop it deliberately if you \
              only want failures logged"]
pub struct SpawnHandle {
    instance_id: String,
    factory_id: String,
    /// The waiter a timed-out read drops, so the runner keeps none.
    reference_id: String,
    deadline: Instant,
    state: State,
}

/// A handle is waiting for a reply, or holds its answer, which every read
/// hands out again.
enum State {
    Pending(Receiver<AckResult>),
    Resolved {
        answer: Result<SpawnResult, SpawnError>,
        /// Whether a read has handed the answer out, so a failure nobody read
        /// is the one reported.
        read: bool,
    },
}

impl State {
    /// A state with nothing left to report, for a handle whose pending reply
    /// has been handed to the reaper.
    fn handed_off() -> Self {
        State::Resolved {
            answer: Err(SpawnError::Timeout),
            read: true,
        }
    }
}

impl SpawnHandle {
    fn pending(
        instance_id: String,
        factory_id: String,
        reference_id: String,
        deadline: Instant,
        rx: Receiver<AckResult>,
    ) -> Self {
        Self {
            instance_id,
            factory_id,
            reference_id,
            deadline,
            state: State::Pending(rx),
        }
    }

    /// A handle that already knows its answer: nothing was sent.
    fn resolved(
        instance_id: String,
        factory_id: String,
        result: Result<SpawnResult, SpawnError>,
    ) -> Self {
        Self {
            instance_id,
            factory_id,
            reference_id: String::new(),
            deadline: Instant::now(),
            state: State::Resolved {
                answer: result,
                read: false,
            },
        }
    }

    /// The id the request named; empty when the controller was asked to
    /// generate one.
    pub fn id(&self) -> &str {
        &self.instance_id
    }

    /// Block for the controller's answer, up to the request's deadline.
    /// Consumes the handle, so the drop-time warning never double-reports what
    /// the caller has seen.
    pub fn wait(mut self) -> Result<SpawnResult, SpawnError> {
        self.wait_timeout(Duration::MAX)
            .unwrap_or(Err(SpawnError::Timeout))
    }

    /// The answer if it has already arrived, without blocking. `None` while it
    /// has not.
    pub fn try_result(&mut self) -> Option<Result<SpawnResult, SpawnError>> {
        self.wait_timeout(Duration::ZERO)
    }

    /// Wait up to `timeout` for the answer; `None` means it has not arrived. A
    /// read that reaches the request's deadline resolves
    /// [`SpawnError::Timeout`]. Once resolved, every read answers the same.
    pub fn wait_timeout(&mut self, timeout: Duration) -> Option<Result<SpawnResult, SpawnError>> {
        if !self.settle(timeout) {
            return None;
        }
        let State::Resolved { answer, read } = &mut self.state else {
            return None;
        };
        *read = true;
        Some(answer.clone())
    }

    /// Take the reply into the handle if it arrives within `timeout`; `false`
    /// while it has not.
    fn settle(&mut self, timeout: Duration) -> bool {
        let State::Pending(rx) = &self.state else {
            return true;
        };
        let left = self.deadline.saturating_duration_since(Instant::now());
        let answer = match rx.recv_timeout(timeout.min(left)) {
            Ok(reply) => interpret(&self.instance_id, &self.factory_id, reply),
            Err(RecvTimeoutError::Timeout) if timeout < left => return false,
            Err(_) => {
                crate::shutdown::drop_pending_ack(&self.reference_id);
                Err(SpawnError::Timeout)
            }
        };
        self.state = State::Resolved {
            answer,
            read: false,
        };
        true
    }
}

impl Drop for SpawnHandle {
    fn drop(&mut self) {
        if self.settle(Duration::ZERO) {
            if let State::Resolved {
                answer: Err(error),
                read: false,
            } = &self.state
            {
                warn(&self.instance_id, &self.factory_id, error);
            }
            return;
        }
        let pending = std::mem::replace(&mut self.state, State::handed_off());
        reap(SpawnHandle {
            instance_id: std::mem::take(&mut self.instance_id),
            factory_id: std::mem::take(&mut self.factory_id),
            reference_id: std::mem::take(&mut self.reference_id),
            deadline: self.deadline,
            state: pending,
        });
    }
}

/// Map a controller reply onto the public result. `already_running`,
/// `factory_unknown` and `factory_not_workflow` arrive as outcomes rather than
/// errors because they are the controller's decision, not a failed exchange.
fn interpret(
    instance_id: &str,
    factory_id: &str,
    reply: AckResult,
) -> Result<SpawnResult, SpawnError> {
    let ack = reply.map_err(SpawnError::Refused)?;
    let (outcome, resolved) = decode_ack(&ack);
    let id = if resolved.is_empty() {
        instance_id
    } else {
        resolved
    };
    let outcome = match outcome {
        "spawned" => SpawnOutcome::Spawned,
        "delivered" => SpawnOutcome::Delivered,
        "already_running" => return Err(SpawnError::AlreadyRunning(id.to_string())),
        "factory_unknown" => return Err(SpawnError::FactoryUnknown(factory_id.to_string())),
        "factory_not_workflow" => {
            return Err(SpawnError::FactoryNotWorkflow(factory_id.to_string()))
        }
        other => return Err(SpawnError::Refused(format!("unknown outcome `{other}`"))),
    };
    Ok(SpawnResult {
        id: id.to_string(),
        outcome,
    })
}

fn warn(instance_id: &str, factory_id: &str, error: &SpawnError) {
    crate::shutdown::say(&format!(
        "[flezha] spawn_workflow(factory `{factory_id}`, id `{instance_id}`) failed: {error}"
    ));
}

/// Hand an unread pending handle to the reaper, which waits out what is left
/// of its deadline so its failure still reaches the log. One thread for the
/// whole process, started on the first such handle: a requester that never
/// reads its handles must not pay a thread per request.
fn reap(handle: SpawnHandle) {
    static REAPER: OnceLock<Sender<SpawnHandle>> = OnceLock::new();
    let tx = REAPER.get_or_init(|| {
        let (tx, work) = mpsc::channel::<SpawnHandle>();
        std::thread::Builder::new()
            .name("flz-spawn-results".to_string())
            .spawn(move || {
                for mut handle in work {
                    if let Some(Err(error)) = handle.wait_timeout(Duration::MAX) {
                        warn(&handle.instance_id, &handle.factory_id, &error);
                    }
                }
            })
            .expect("spawn the flezha spawn-result reaper thread");
        tx
    });
    // A reaper that is gone hands the handle back; this keeps its drop from
    // handing it straight back here.
    if let Err(mpsc::SendError(mut handle)) = tx.send(handle) {
        handle.state = State::handed_off();
    }
}

/// Ship a spawn request and hand back its handle. Without options the
/// controller generates the id and always starts a new instance. A replay
/// sends nothing and answers [`SpawnOutcome::SkippedOnReplay`]. A run with
/// nothing listening on the control plane (a transport-less unit test or
/// bench, or a closed runner) resolves at once rather than waiting out the
/// deadline for a reply that cannot come.
pub fn request(
    factory_id: String,
    payload: Vec<u8>,
    options: Option<SpawnOptions>,
    replay: bool,
) -> SpawnHandle {
    let SpawnOptions { id, if_running } = options.unwrap_or_else(|| SpawnOptions::id(""));
    if replay {
        let skipped = SpawnResult {
            id: id.clone(),
            outcome: SpawnOutcome::SkippedOnReplay,
        };
        return SpawnHandle::resolved(id, factory_id, Ok(skipped));
    }

    let reference_id = format!("spawn-workflow-{}-{}", id, crate::util::nonce_suffix());
    let command = crate::pb::command_request::Command::SpawnWorkflowRequest(
        crate::pb::SpawnWorkflowRequest {
            instance_id: id.clone(),
            factory_id: factory_id.clone(),
            payload,
            if_running: if_running.as_wire().to_string(),
        },
    );

    let deadline = Instant::now() + SPAWN_RESULT_TIMEOUT;
    match crate::shutdown::ask(reference_id.clone(), command) {
        Ok(rx) => SpawnHandle::pending(id, factory_id, reference_id, deadline, rx),
        Err(_) => SpawnHandle::resolved(id, factory_id, Err(SpawnError::Unavailable)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pending handle over a channel this test answers, due at `deadline`.
    fn pending_until(deadline: Instant) -> (mpsc::SyncSender<AckResult>, SpawnHandle) {
        let (tx, rx) = mpsc::sync_channel(1);
        let handle = SpawnHandle::pending(
            "w1".to_string(),
            "worker".to_string(),
            "ref-w1".to_string(),
            deadline,
            rx,
        );
        (tx, handle)
    }

    /// Resolve a handle against a controller reply without a control plane.
    fn answer(reply: AckResult) -> Result<SpawnResult, SpawnError> {
        let (tx, handle) = pending_until(Instant::now() + SPAWN_RESULT_TIMEOUT);
        tx.send(reply).unwrap();
        handle.wait()
    }

    fn resolved(id: &str, outcome: SpawnOutcome) -> Result<SpawnResult, SpawnError> {
        Ok(SpawnResult {
            id: id.to_string(),
            outcome,
        })
    }

    #[test]
    fn a_spawn_and_a_delivery_carry_the_id_they_resolved_to() {
        assert_eq!(
            answer(Ok(encode_ack("spawned", "worker-k7m2qzxw4pd3fn2a"))),
            resolved("worker-k7m2qzxw4pd3fn2a", SpawnOutcome::Spawned)
        );
        assert_eq!(
            answer(Ok(encode_ack("delivered", "w1"))),
            resolved("w1", SpawnOutcome::Delivered)
        );
    }

    #[test]
    fn a_reply_without_an_id_resolves_to_the_requested_one() {
        assert_eq!(
            answer(Ok("spawned".to_string())),
            resolved("w1", SpawnOutcome::Spawned)
        );
    }

    #[test]
    fn the_ack_splits_on_the_first_tab_only() {
        assert_eq!(
            decode_ack(&encode_ack("spawned", "a\tb")),
            ("spawned", "a\tb")
        );
        assert_eq!(decode_ack("spawned"), ("spawned", ""));
    }

    #[test]
    fn the_error_policy_and_an_unknown_factory_come_back_as_errors() {
        assert_eq!(
            answer(Ok(encode_ack("already_running", "w1"))),
            Err(SpawnError::AlreadyRunning("w1".to_string()))
        );
        assert_eq!(
            answer(Ok(encode_ack("factory_unknown", ""))),
            Err(SpawnError::FactoryUnknown("worker".to_string()))
        );
        assert_eq!(
            answer(Ok(encode_ack("factory_not_workflow", ""))),
            Err(SpawnError::FactoryNotWorkflow("worker".to_string())),
            "a stream factory is its own answer, neither unknown nor refused"
        );
    }

    #[test]
    fn a_refusal_carries_the_controllers_reason() {
        assert_eq!(
            answer(Err("quota_exceeded".to_string())),
            Err(SpawnError::Refused("quota_exceeded".to_string()))
        );
    }

    #[test]
    fn a_reply_that_can_never_arrive_times_out_rather_than_hanging_forever() {
        // The sender is dropped, so the wait ends on disconnect rather than
        // burning the full deadline.
        let (tx, handle) = pending_until(Instant::now() + SPAWN_RESULT_TIMEOUT);
        drop(tx);
        assert_eq!(handle.wait(), Err(SpawnError::Timeout));
    }

    #[test]
    fn a_read_waits_only_until_the_request_deadline_and_drops_the_waiter() {
        let reference_id = format!("spawn-deadline-{}", crate::util::nonce_suffix());
        let rx = crate::shutdown::pending_ack_channel(reference_id.clone());
        let mut handle = SpawnHandle::pending(
            "w1".to_string(),
            "worker".to_string(),
            reference_id.clone(),
            Instant::now() + Duration::from_millis(50),
            rx,
        );

        let started = Instant::now();
        assert_eq!(
            handle.wait_timeout(Duration::from_secs(5)),
            Some(Err(SpawnError::Timeout))
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the read waited past the request's deadline: {:?}",
            started.elapsed()
        );
        assert!(
            !crate::shutdown::has_pending_ack(&reference_id),
            "a timed-out read must leave no waiter behind"
        );
    }

    #[test]
    fn try_result_leaves_a_pending_handle_pending_then_keeps_its_answer() {
        let (tx, mut handle) = pending_until(Instant::now() + SPAWN_RESULT_TIMEOUT);
        assert!(handle.try_result().is_none());

        tx.send(Ok(encode_ack("spawned", "w1"))).unwrap();
        assert_eq!(
            handle.try_result(),
            Some(resolved("w1", SpawnOutcome::Spawned))
        );
        assert_eq!(
            handle.try_result(),
            Some(resolved("w1", SpawnOutcome::Spawned)),
            "a resolved handle answers every read the same"
        );
    }

    #[test]
    fn a_replay_answers_the_id_it_was_given() {
        // A request that went out would resolve to anything but a skip.
        let named = request(
            "worker".to_string(),
            b"p".to_vec(),
            Some(SpawnOptions::id("w1")),
            true,
        );
        let unnamed = request("worker".to_string(), b"p".to_vec(), None, true);

        assert_eq!(named.wait(), resolved("w1", SpawnOutcome::SkippedOnReplay));
        assert_eq!(unnamed.wait(), resolved("", SpawnOutcome::SkippedOnReplay));
    }

    #[test]
    fn every_answer_has_the_word_a_host_reads() {
        assert_eq!(SpawnOutcome::Spawned.as_wire(), "spawned");
        assert_eq!(SpawnOutcome::Delivered.as_wire(), "delivered");
        assert_eq!(SpawnOutcome::SkippedOnReplay.as_wire(), "skipped_on_replay");
        let words: Vec<&str> = [
            SpawnError::AlreadyRunning("w1".to_string()),
            SpawnError::FactoryUnknown("worker".to_string()),
            SpawnError::FactoryNotWorkflow("worker".to_string()),
            SpawnError::Refused("quota".to_string()),
            SpawnError::Timeout,
            SpawnError::Unavailable,
        ]
        .iter()
        .map(SpawnError::as_wire)
        .collect();
        assert_eq!(
            words,
            [
                "already_running",
                "factory_unknown",
                "factory_not_workflow",
                "refused",
                "timeout",
                "unavailable"
            ]
        );
        assert_eq!(IfRunning::default(), IfRunning::Deliver);
        for policy in [IfRunning::Deliver, IfRunning::Error] {
            assert_eq!(IfRunning::from_wire(policy.as_wire()), Some(policy));
        }
        assert_eq!(IfRunning::from_wire(""), Some(IfRunning::Deliver));
        assert_eq!(IfRunning::from_wire("later"), None);
    }

    /// fails when: two unnamed spawns issued at once share a reference id, so
    /// the second waiter replaces the first and that handle times out.
    #[test]
    fn requests_issued_at_once_never_share_a_reference_id() {
        let nonces: Vec<u128> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..2_000)
                        .map(|_| crate::util::nonce_suffix())
                        .collect::<Vec<_>>()
                })
            })
            .flat_map(|worker| worker.join().expect("nonce worker"))
            .collect();
        let distinct: std::collections::HashSet<u128> = nonces.iter().copied().collect();

        assert_eq!(distinct.len(), nonces.len());
    }
}
