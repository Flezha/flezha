//! The prepare crossing: an entry payload whose consumers need keys or an
//! event time the core cannot derive from bytes is staged and crossed to the
//! host as a [`schema::ActivationKind::KeyRequest`] before it is enqueued.
//! The invariant: an end-of-slice broadcast never overtakes a slice member
//! still waiting on its answer.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use super::decl::HostClock;
use super::entity::HostPayload;
use crate::channel::{ChannelDescriptor, ChannelKind};
use crate::operative::instance::StreamInstance;
use crate::operative::topology::{EntityMode, OperativeTopology};
use crate::poll_core::EnginePayload;
use crate::schema;
use crate::util::LockRecover;

/// Per-instance staging for the prepare crossing, plus the deferred
/// end-of-slice broadcast.
#[derive(Default)]
pub struct HostPrepareState {
    queues: HashMap<u32, HostPrepareQueue>,
    /// Slice members staged or in a crossing; the broadcast waits for zero.
    replay_staged: usize,
    pending_replay_end: bool,
}

#[derive(Default)]
struct HostPrepareQueue {
    queued: VecDeque<StagedEntry>,
    inflight: bool,
}

#[derive(Clone)]
pub struct StagedEntry {
    pub bytes: Vec<u8>,
    pub consumed_offset: Option<u64>,
    pub replay: bool,
    pub stream: Arc<flz_transport::transport::FlezhaStream>,
}

/// A batched prepare crossing, handed to the host as a
/// [`schema::ActivationKind::KeyRequest`].
pub struct PrepareCrossing {
    pub instance: Arc<StreamInstance>,
    pub channel: crate::channel::ChannelId,
    pub want_keys: bool,
    pub want_event_time: bool,
    pub entries: Vec<StagedEntry>,
    /// A ladder's single-payload re-crossing awaits here.
    pub reply:
        Option<crossbeam::channel::Sender<Vec<Result<schema::PrepareAnswer, schema::WorkError>>>>,
}

/// Cap per crossing.
const PREPARE_BATCH_MAX: usize = 256;

/// `(want_keys, want_event_time)`; `(false, false)` is no crossing.
pub fn prepare_wants(topology: &OperativeTopology, channel: &ChannelDescriptor) -> (bool, bool) {
    let mut want_keys = false;
    let mut want_event_time = false;
    for edge in &channel.consumers {
        let Some(desc) = topology.procedure(edge.procedure) else {
            continue;
        };
        let Some(facts) = desc.host_facts.as_ref() else {
            continue;
        };
        match desc.mode {
            EntityMode::Dynamic => want_keys = true,
            EntityMode::Static if desc.entities_for_overridden => want_keys = true,
            _ => {}
        }
        if facts.clock == HostClock::EventTime {
            want_event_time = true;
        }
    }
    (want_keys, want_event_time)
}

/// Stage an entry payload for the crossing, or hand it back when none is
/// needed.
pub fn maybe_stage_prepare(
    instance: &Arc<StreamInstance>,
    channel_id: crate::channel::ChannelId,
    channel: &ChannelDescriptor,
    payload: Vec<u8>,
    consumed_offset: Option<u64>,
    replay: bool,
    stream: &Arc<flz_transport::transport::FlezhaStream>,
) -> Option<Vec<u8>> {
    if channel.kind != ChannelKind::Entry {
        return Some(payload);
    }
    let (want_keys, want_event_time) = prepare_wants(&instance.topology, channel);
    if !want_keys && !want_event_time {
        return Some(payload);
    }
    {
        let mut state = instance.host_prepare.lock_recover();
        if replay {
            state.replay_staged += 1;
        }
        state
            .queues
            .entry(channel_id)
            .or_default()
            .queued
            .push_back(StagedEntry {
                bytes: payload,
                consumed_offset,
                replay,
                stream: Arc::clone(stream),
            });
    }
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    None
}

/// The end-of-slice broadcast, deferred behind slice members still in the
/// crossing.
pub fn deferred_end_replay_slice(instance: &Arc<StreamInstance>) {
    {
        let mut state = instance.host_prepare.lock_recover();
        if state.replay_staged > 0 {
            state.pending_replay_end = true;
            return;
        }
    }
    crate::routing::end_replay_slice(instance);
}

/// Submit the channel's next crossing unless one is in flight.
fn submit_next_crossing(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
) {
    let entries = {
        let mut state = instance.host_prepare.lock_recover();
        let Some(queue) = state.queues.get_mut(&channel_id) else {
            return;
        };
        if queue.inflight || queue.queued.is_empty() {
            return;
        }
        queue.inflight = true;
        let take = queue.queued.len().min(PREPARE_BATCH_MAX);
        queue.queued.drain(..take).collect::<Vec<_>>()
    };
    crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
        instance: Arc::clone(instance),
        channel: channel_id,
        want_keys,
        want_event_time,
        entries,
        reply: None,
    }));
}

/// A completed crossing: deliver the answered, ladder the failed, re-arm
/// the next batch.
pub(super) fn apply_prepare_outcome(
    crossing: PrepareCrossing,
    outcome: schema::ActivationOutcome,
) -> Result<(), crate::error::Error> {
    let answers = answers_for(outcome, crossing.entries.len());
    let PrepareCrossing {
        instance,
        channel,
        want_keys,
        want_event_time,
        entries,
        reply,
    } = crossing;
    if let Some(reply) = reply {
        let _ = reply.send(answers);
        return Ok(());
    }
    let Some(channel_desc) = instance.topology.channel(channel) else {
        return Err(crate::error::Error::Answer(
            "prepare crossing names an unknown channel".to_string(),
        ));
    };
    let mut replay_resolved = 0usize;
    for (entry, answer) in entries.into_iter().zip(answers) {
        if entry.replay {
            replay_resolved += 1;
        }
        match answer {
            Ok(ans) => deliver_prepared(&instance, channel, channel_desc, entry, ans),
            Err(e) => spawn_prepare_ladder(
                Arc::clone(&instance),
                channel,
                want_keys,
                want_event_time,
                entry,
                e,
            ),
        }
    }
    finish_crossing(
        &instance,
        channel,
        want_keys,
        want_event_time,
        replay_resolved,
    );
    Ok(())
}

/// One answer per entry; a short answer fails what it left unanswered.
fn answers_for(
    outcome: schema::ActivationOutcome,
    entries: usize,
) -> Vec<Result<schema::PrepareAnswer, schema::WorkError>> {
    let mut answers = match outcome {
        schema::ActivationOutcome::Prepared(answers) => answers,
        schema::ActivationOutcome::Failed(err) => {
            vec![Err(err); entries]
        }
        _ => {
            vec![
                Err(schema::WorkError {
                    message: "prepare crossing answered with a non-prepare outcome".to_string(),
                    non_retryable: false,
                });
                entries
            ]
        }
    };
    if answers.len() < entries {
        answers.resize(
            entries,
            Err(schema::WorkError {
                message: "prepare crossing answered fewer payloads than asked".to_string(),
                non_retryable: false,
            }),
        );
    }
    answers
}

fn deliver_prepared(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    channel: &ChannelDescriptor,
    entry: StagedEntry,
    answer: schema::PrepareAnswer,
) {
    let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
        bytes: entry.bytes,
        channel: channel_id,
        keys: answer.keys,
        event_time: answer.event_time,
        metadata: Vec::new(),
    });
    crate::routing::route_to_consumers(
        instance,
        channel,
        &value,
        &entry.stream,
        entry.consumed_offset,
        entry.replay,
        None,
    );
}

/// A failed prepare climbs the entry channel's ladder, one blocking
/// re-crossing per attempt.
fn spawn_prepare_ladder(
    instance: Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    entry: StagedEntry,
    first_error: schema::WorkError,
) {
    crate::retry::spawn_park_thread("flezha-prepare-park", move || {
        let channel = instance
            .topology
            .channel(channel_id)
            .expect("prepare channel present in topology");
        crate::retry::run_retry_park_ladder(
            &format!("prepare for entry channel `{}`", channel.name),
            &channel.name,
            &channel.name,
            channel.retry,
            &instance.instance_id,
            "_entry",
            entry.consumed_offset,
            Some(entry.bytes.clone()),
            first_error.message,
            || {
                let (tx, rx) = crossbeam::channel::bounded(1);
                crate::poll_core::global().submit_direct(EnginePayload::Prepare(PrepareCrossing {
                    instance: Arc::clone(&instance),
                    channel: channel_id,
                    want_keys,
                    want_event_time,
                    entries: vec![entry.clone()],
                    reply: Some(tx),
                }));
                let mut answers = rx
                    .recv_timeout(std::time::Duration::from_secs(30))
                    .map_err(|_| "prepare re-attempt not answered".to_string())?;
                match answers.pop() {
                    Some(Ok(ans)) => {
                        deliver_prepared(&instance, channel_id, channel, entry.clone(), ans);
                        Ok(())
                    }
                    Some(Err(e)) => Err(e.message),
                    None => Err("prepare re-attempt answered nothing".to_string()),
                }
            },
        );
    });
}

/// Release the inflight gate, re-arm the next batch, fire a deferred
/// broadcast once the last slice member resolved.
fn finish_crossing(
    instance: &Arc<StreamInstance>,
    channel_id: u32,
    want_keys: bool,
    want_event_time: bool,
    replay_resolved: usize,
) {
    let fire_broadcast = {
        let mut state = instance.host_prepare.lock_recover();
        state.replay_staged = state.replay_staged.saturating_sub(replay_resolved);
        if let Some(queue) = state.queues.get_mut(&channel_id) {
            queue.inflight = false;
        }
        state.replay_staged == 0
            && state.pending_replay_end
            && std::mem::take(&mut state.pending_replay_end)
    };
    submit_next_crossing(instance, channel_id, want_keys, want_event_time);
    if fire_broadcast {
        crate::routing::end_replay_slice(instance);
    }
}
