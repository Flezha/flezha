//! The activation crossing: a handed-out engine activation becomes the schema
//! form the C ABI ships ([`to_schema`]), and the host's completion is applied
//! back ([`apply_result`], [`abandon_activation`]). The invariant on the way
//! in: an item's effect record is folded only after every edge push it
//! produced has landed, so no consumed offset is durable ahead of a value the
//! replay could never re-produce.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use flz_transport::transport::FlezhaStream;

use super::cell::{cell_ref_of, BytesBuffer, HostPayload, HostTaskletFacts};
use super::decl::HostClock;
use super::prepare::{apply_prepare_outcome, PrepareCrossing};
use crate::cell::{CellItem, CellSlot};
use crate::error::{Code, Error};
use crate::flusher::{DlqEntryBody, InvocationEffects, OutputRecord, SettlePolicy};
use crate::poll_core::{EngineActivation, EnginePayload, ItemAttempt, PollCore};
use crate::schema::{self, TimerOp};
use crate::util::LockRecover;

/// The schema form of a handed-out activation. `now` is stamped here — the
/// host never reads ambient time for `ctx.now()`.
pub fn to_schema(act: &EngineActivation) -> schema::Activation {
    match &act.payload {
        EnginePayload::Command {
            slot,
            change_seq,
            kind,
            ..
        } => schema::Activation {
            token: act.token,
            target: cell_ref_of(slot),
            kind: kind.clone(),
            change_seq: *change_seq,
            factory: slot.topology.factory_id.to_string(),
        },
        EnginePayload::Prepare(crossing) => schema::Activation {
            token: act.token,
            target: prepare_target(crossing),
            kind: schema::ActivationKind::KeyRequest(schema::KeyRequest {
                channel: crossing.channel,
                payloads: crossing.entries.iter().map(|e| e.bytes.clone()).collect(),
                want_keys: crossing.want_keys,
                want_event_time: crossing.want_event_time,
            }),
            change_seq: 0,
            factory: crossing.instance.topology.factory_id.to_string(),
        },
        EnginePayload::Work { slot, items } => schema::Activation {
            token: act.token,
            target: cell_ref_of(slot),
            kind: schema::ActivationKind::Work(work_items_to_schema(slot, items)),
            change_seq: slot.change_seq.load(Ordering::Acquire),
            factory: slot.topology.factory_id.to_string(),
        },
    }
}

/// A prepare crossing is addressed to the channel's first consumer.
fn prepare_target(crossing: &PrepareCrossing) -> schema::CellRef {
    schema::CellRef {
        operator: crossing.instance.instance_id.to_string(),
        tasklet: crossing
            .instance
            .topology
            .channel(crossing.channel)
            .and_then(|c| c.consumers.first())
            .map(|e| e.tasklet)
            .unwrap_or(0),
        cell_key: None,
    }
}

fn work_items_to_schema(slot: &CellSlot, items: &[ItemAttempt]) -> Vec<schema::WorkItem> {
    let now_fallback = host_clock_now(slot);
    crate::poll_core::host_items(items)
        .map(|work| match &work.item {
            CellItem::Input(payload) => {
                let (channel, bytes, metadata) =
                    host_input_parts(payload.value.downcast_ref::<HostPayload>(), slot);
                schema::WorkItem::Input(schema::InputItem {
                    channel,
                    payload: bytes,
                    metadata,
                    consumed_offset: payload.consumed_offset,
                    replay: payload.replay,
                    now: work.now.unwrap_or(now_fallback),
                })
            }
            CellItem::Timer(fire) => schema::WorkItem::Timer(schema::TimerItem {
                kind: fire.kind,
                scheduled_at: fire.scheduled_at,
            }),
            // Filtered out above; a firing item is the harmless shape.
            CellItem::CloseInterval => schema::WorkItem::Timer(schema::TimerItem {
                kind: schema::TimerKind::ProcessingTime,
                scheduled_at: 0,
            }),
        })
        .collect()
}

/// The declared clock's value, once per handout; the engine stamps event-time
/// items, wall and logical clocks evaluate here.
fn host_clock_now(slot: &CellSlot) -> u64 {
    let clock = slot
        .dispatcher
        .host_facts()
        .map(|f| f.clock)
        .unwrap_or(HostClock::Wall);
    match clock {
        HostClock::Logical => slot.change_seq.load(Ordering::Acquire),
        _ => crate::util::wall_now_ms(),
    }
}

/// The channel, payload and metadata one item carries across. A value that
/// is not a `HostPayload` cannot reach a host class; the fallback is an empty
/// payload the host's decoder rejects.
fn host_input_parts(
    payload: Option<&HostPayload>,
    slot: &CellSlot,
) -> (u32, Vec<u8>, Vec<(String, String)>) {
    match payload {
        Some(p) => (p.channel, p.bytes.clone(), p.metadata.clone()),
        None => (input_channel_of(slot), Vec::new(), Vec::new()),
    }
}

fn input_channel_of(slot: &CellSlot) -> u32 {
    slot.topology
        .tasklet(slot.tasklet_id)
        .and_then(|p| p.input_channel_ids.first().copied())
        .unwrap_or(0)
}

/// Apply a host completion to its outstanding activation, then drive the
/// engine's completion protocol (release, ladder, or park).
pub fn apply_result(
    core: &PollCore,
    act: EngineActivation,
    result: schema::ActivationResult,
) -> Result<(), Error> {
    let EngineActivation { token, payload } = act;
    let (slot, items) = match payload {
        EnginePayload::Command { reply, .. } => {
            // A command interaction: whoever enqueued it owns the cell gate
            // and validates the outcome; this side only answers it.
            reply.answer(result.outcome);
            return Ok(());
        }
        EnginePayload::Prepare(crossing) => {
            return apply_prepare_outcome(crossing, result.outcome);
        }
        EnginePayload::Work { slot, items } => (slot, items),
    };
    match result.outcome {
        schema::ActivationOutcome::Work(effects) => {
            settle_items(core, token, &slot, items, effects)
        }
        schema::ActivationOutcome::Failed(err) => {
            answer_failed_activation(core, token, &slot, items, err);
            Ok(())
        }
        // A malformed answer still answers: the cell is released with its
        // items intact rather than retired behind a handout nobody will
        // complete.
        _ => {
            core.abandon(token, items);
            Err(Error::Answer(
                Code::HostWrongOutcome.with([("command", "work".into())]),
            ))
        }
    }
}

/// Whole-activation failure: ladder the head with the rest behind it, or
/// park it when non-retryable/final, resolving with an empty effect.
fn answer_failed_activation(
    core: &PollCore,
    token: u64,
    slot: &Arc<CellSlot>,
    items: Vec<ItemAttempt>,
    err: schema::WorkError,
) {
    let head_final = items.first().map(|w| w.is_final).unwrap_or(false);
    if err.non_retryable || head_final {
        if let Some(head) = items.first() {
            settle_item(
                slot,
                head,
                empty_effect(),
                Some((err.message.clone(), head.attempt)),
            );
        }
        // The head parked; the tail never ran and is owed a fresh
        // round, not a retry.
        let rest: Vec<ItemAttempt> = items.into_iter().skip(1).collect();
        core.abandon(token, rest);
    } else {
        core.retry_after_failure(token, items, err.message);
    }
}

/// Settle each item against its effect in order; a retryable failure ends
/// the batch and re-hands the failed item with the rest behind it.
fn settle_items(
    core: &PollCore,
    token: u64,
    slot: &Arc<CellSlot>,
    items: Vec<ItemAttempt>,
    effects: Vec<schema::ItemEffect>,
) -> Result<(), Error> {
    let mut queue: std::collections::VecDeque<ItemAttempt> = items.into();
    let bounded = slot.topology.bounded.is_some();
    for mut effect in effects {
        if !bounded {
            refuse_completion(&mut effect);
        }
        let Some(work) = queue.pop_front() else {
            core.abandon(token, Vec::new());
            return Err(Error::Answer(Code::CompletionTooManyEffects.with([])));
        };
        // A bounded operator's unreadable message rides its ladder before it parks;
        // an unbounded operator's parks at once.
        let verdict = match effect.verdict.clone() {
            schema::Verdict::Unreadable { message } if bounded => {
                schema::Verdict::RetryableError { message }
            }
            verdict => verdict,
        };
        match verdict {
            schema::Verdict::Ok => {
                settle_item(slot, &work, effect, None);
                crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            schema::Verdict::RetryableError { message } => {
                if work.is_final {
                    settle_item(slot, &work, effect, Some((message, work.attempt)));
                    crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                // Keep the failed attempt's changes out of the flusher's
                // reach until the item resolves — the resolving effect
                // re-takes them, so durable state cannot diverge from the
                // host's memory.
                stash_pending_attempt(slot, effect.changes);
                let mut rest = Vec::with_capacity(queue.len() + 1);
                rest.push(work);
                rest.extend(queue);
                core.retry_after_failure(token, rest, message);
                return Ok(());
            }
            schema::Verdict::NonRetryableError { message }
            | schema::Verdict::Unreadable { message } => {
                settle_item(slot, &work, effect, Some((message, work.attempt)));
                crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    finish_batch(core, token, queue);
    Ok(())
}

/// An unbounded operator never completes: an item whose tasklet asked to fails as the
/// author's own non-retryable error would, carrying nothing it recorded.
pub(super) fn refuse_completion(effect: &mut schema::ItemEffect) {
    if effect.complete.is_some() && effect.verdict == schema::Verdict::Ok {
        *effect = schema::ItemEffect {
            verdict: schema::Verdict::NonRetryableError {
                message: crate::bounded::complete_outside_bounded().text(),
            },
            consumed_offset: effect.consumed_offset,
            event_time: effect.event_time,
            ..empty_effect()
        };
    }
}

/// Anything still queued behind a parked head is owed a fresh round, or a
/// healthy payload behind a poison one vanishes.
fn finish_batch(core: &PollCore, token: u64, queue: std::collections::VecDeque<ItemAttempt>) {
    let rest: Vec<ItemAttempt> = queue.into_iter().collect();
    if rest.is_empty() {
        core.complete(token);
    } else {
        core.abandon(token, rest);
    }
}

/// Answer a polled activation the host cannot finish, without processing it
/// — the ABI's third answer, for a veneer that threw or whose thread died. A
/// work batch re-hands its items; a command, step or prepare crossing
/// releases its blocked enqueuer with a failure.
pub fn abandon_activation(core: &PollCore, act: EngineActivation) -> Result<(), Error> {
    let EngineActivation { token, payload } = act;
    match payload {
        EnginePayload::Work { items, .. } => match core.abandon(token, items) {
            crate::poll_core::Answered::Released => Ok(()),
            crate::poll_core::Answered::UnknownToken => Err(Error::Answer(
                Code::UnknownActivationToken.with([("token", token.into())]),
            )),
        },
        other => apply_result(
            core,
            EngineActivation {
                token,
                payload: other,
            },
            schema::ActivationResult {
                token,
                outcome: schema::ActivationOutcome::Failed(schema::WorkError {
                    message: "host abandoned the activation".to_string(),
                    non_retryable: false,
                }),
            },
        ),
    }
}

pub(crate) fn empty_effect() -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: Vec::new(),
        outputs: Vec::new(),
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
        complete: None,
    }
}

fn stash_pending_attempt(slot: &Arc<CellSlot>, changes: Vec<Vec<u8>>) {
    if changes.is_empty() {
        return;
    }
    let mut buffer = slot.flush_buffer.lock_recover();
    if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
        bytes.pending_attempt.extend(changes);
    }
}

/// Settle one item: fold its changes and timer ops per the tier, route its
/// outputs, and mint the park where the typed layer does — inside the effect
/// record under the tier, through the blocking park otherwise.
pub(super) fn settle_item(
    slot: &Arc<CellSlot>,
    work: &ItemAttempt,
    effect: schema::ItemEffect,
    park: Option<(String, u32)>,
) {
    let Some(facts) = slot.dispatcher.host_facts() else {
        return;
    };
    let Some(stream) = stream_of(&work.item) else {
        return;
    };
    if slot.topology.bounded.is_some() {
        return settle_bounded_item(slot, facts, work, &stream, effect, park);
    }
    let parked = park.is_some();
    let timers = committed_timers(&work.item, effect.timers, parked);
    // A failed attempt's captured outputs must not ship — the invocation did
    // not complete.
    let outputs = if parked { Vec::new() } else { effect.outputs };
    let (edge_outputs, wire_outputs) = split_outputs(outputs);
    let consumed_offset = consumed_offset_of(&work.item);
    let call = call_of(&work.item);
    let dlq = park.map(|(error, attempts)| park_entry(slot, &work.item, error, attempts));
    let keeps_edges = slot.keeps_edges(consumed_offset);
    let edges = route_edge_outputs(
        slot,
        &stream,
        consumed_offset,
        call,
        edge_outputs,
        keeps_edges,
    );
    let effects = InvocationEffects {
        changes: effect.changes,
        timers: timers.clone(),
        outputs: Vec::new(),
        edges,
        offset: consumed_offset,
        event_time: effect.event_time,
        checkpointed: effect.checkpoint,
        parked: dlq.is_some(),
    };
    let (tier_off_dlq, plugin_outputs) = settle_into_buffer(
        slot,
        facts,
        effects,
        wire_outputs,
        call,
        dlq,
        hold_of(&work.item),
    );
    commit_timers(slot, &timers, &stream);
    crate::timer::arm_interval_deadline(slot);
    ship_plugin_outputs(slot, &stream, plugin_outputs);
    if let Some(entry) = tier_off_dlq {
        // Tier-off park: ship the park frame and hold this cell until the
        // controller acks the DLQ append as durable — park-then-advance.
        crate::park::park_tier_off(
            &slot.cell_identity,
            slot.dispatcher.cell_label(),
            consumed_offset,
            call,
            entry,
        );
    }
}

/// A host's answer to a bounded operator undo: its changes and plugin outputs settled
/// as an item's are, into the undo's own record; a failed hook reported.
pub(crate) fn settle_bounded_undo(slot: &Arc<CellSlot>, outcome: schema::ActivationOutcome) {
    match outcome {
        schema::ActivationOutcome::Work(effects) => {
            let Some(facts) = slot.dispatcher.host_facts() else {
                return;
            };
            let stream = crate::bounded::unwired_stream();
            for effect in effects {
                let (_edge, wire) = split_outputs(effect.outputs);
                settle_bounded_effects(
                    slot,
                    facts,
                    effect.changes,
                    Vec::new(),
                    effect.event_time,
                    wire,
                    &stream,
                );
            }
        }
        schema::ActivationOutcome::Failed(error) => crate::bounded::note_failure(
            slot,
            crate::bounded::BoundedError::from(error.message),
            None,
            1,
        ),
        _ => {}
    }
}

/// A bounded operator Cell's changes, durable timer ops and plugin outputs. A class
/// that journals writes them as one effect record, sent at once and named on
/// the pass sheet for the stage's barrier; one that does not sends the outputs
/// to the wire with their channel, key and metadata.
fn settle_bounded_effects(
    slot: &Arc<CellSlot>,
    facts: &HostTaskletFacts,
    changes: Vec<Vec<u8>>,
    timers: Vec<TimerOp>,
    event_time: u64,
    wire_outputs: Vec<schema::Output>,
    stream: &Arc<FlezhaStream>,
) {
    let channels: Vec<String> = wire_outputs
        .iter()
        .filter_map(|output| match &output.target {
            schema::OutputTarget::Plugin { channel } => Some(channel.clone()),
            schema::OutputTarget::Edge { .. } => None,
        })
        .collect();
    let effects = InvocationEffects {
        changes,
        timers,
        outputs: Vec::new(),
        edges: Vec::new(),
        offset: None,
        event_time,
        checkpointed: false,
        parked: false,
    };
    let call = crate::bounded::pass_call(slot);
    let (_park, plugin_outputs) = settle_into_buffer(
        slot,
        facts,
        effects,
        wire_outputs,
        call.as_ref(),
        None,
        None,
    );
    ship_plugin_outputs(slot, stream, plugin_outputs);
    if !slot.topology.records_effects(slot.persistence_enabled) {
        return;
    }
    let mut buffer = slot.flush_buffer.lock_recover();
    crate::bounded::ship_stage_record(slot, &mut **buffer, &facts.header(), channels);
}

/// A bounded operator Cell's item: its changes and plugin outputs settled into its
/// record, its edge outputs held for the stage's commit and its completion
/// noted; or, where an unbounded operator would park it, its stage failed with no dead
/// letter and only its changes recorded. Either way the stage owes it no
/// longer.
fn settle_bounded_item(
    slot: &Arc<CellSlot>,
    facts: &HostTaskletFacts,
    work: &ItemAttempt,
    stream: &Arc<FlezhaStream>,
    effect: schema::ItemEffect,
    park: Option<(String, u32)>,
) {
    let unreadable = matches!(effect.verdict, schema::Verdict::Unreadable { .. });
    let timers = committed_timers(&work.item, effect.timers, park.is_some());
    commit_timers(slot, &timers, stream);
    let mut durable = timers;
    crate::timer::hold_firing_removal(slot, &mut durable, firing_of(&work.item));
    let outputs = if park.is_some() {
        Vec::new()
    } else {
        effect.outputs
    };
    let (edge_outputs, wire_outputs) = split_outputs(outputs);
    settle_bounded_effects(
        slot,
        facts,
        effect.changes,
        durable,
        effect.event_time,
        wire_outputs,
        stream,
    );
    match park {
        Some((error, attempts)) => {
            let error = if unreadable {
                crate::bounded::BoundedError::unreadable(error)
            } else {
                crate::bounded::BoundedError::non_retryable(error)
            };
            let payload = match &work.item {
                CellItem::Input(p) => p
                    .value
                    .downcast_ref::<HostPayload>()
                    .map(|hp| hp.bytes.clone()),
                CellItem::Timer(_) | CellItem::CloseInterval => None,
            };
            crate::bounded::note_failure(slot, error, payload, attempts);
        }
        None => hold_bounded_edges(slot, stream, edge_outputs, effect.complete),
    }
    crate::bounded::item_settled(slot);
}

/// A host bounded stage emits one untransformed output per released value,
/// naming its producing channel; the consumer applies the edge on delivery.
fn hold_bounded_edges(
    slot: &Arc<CellSlot>,
    stream: &Arc<FlezhaStream>,
    edge_outputs: Vec<schema::Output>,
    complete: Option<Vec<u8>>,
) {
    let Some(instance) = slot.instance.upgrade() else {
        return;
    };
    for output in edge_outputs {
        let schema::OutputTarget::Edge { channel, .. } = output.target else {
            continue;
        };
        let Some(descriptor) = instance.topology.channel(channel) else {
            continue;
        };
        let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
            bytes: output.payload,
            channel,
            keys: output.key.into_iter().collect(),
            event_time: output.event_time,
            metadata: output.metadata,
        });
        crate::bounded::hold_emit(&instance, Some(descriptor), &value, stream, slot);
    }
    if let Some(result) = complete {
        crate::bounded::complete(slot, result);
    }
}

/// The connection an item's outputs and park ride; a close takes the live
/// secondary.
fn stream_of(item: &CellItem) -> Option<Arc<FlezhaStream>> {
    match item {
        CellItem::Input(p) => Some(Arc::clone(&p.stream)),
        CellItem::Timer(f) => Some(Arc::clone(&f.stream)),
        CellItem::CloseInterval => crate::state_sink::secondary_stream(),
    }
}

/// `(edge, wire)`: edge values deliver in-process, wire outputs ride the
/// tier's path.
fn split_outputs(outputs: Vec<schema::Output>) -> (Vec<schema::Output>, Vec<schema::Output>) {
    outputs
        .into_iter()
        .partition(|o| matches!(o.target, schema::OutputTarget::Edge { .. }))
}

/// The firing an item is, if it is one.
fn firing_of(item: &CellItem) -> Option<(crate::timer::TimerKind, u64)> {
    match item {
        CellItem::Timer(fire) => Some((fire.kind, fire.scheduled_at)),
        CellItem::Input(_) | CellItem::CloseInterval => None,
    }
}

/// The input's hold on its instance's processed prefix; the item keeps its
/// own until it is dropped.
fn hold_of(item: &CellItem) -> Option<Arc<crate::prefix::Hold>> {
    match item {
        CellItem::Input(p) => p.hold.clone(),
        CellItem::Timer(_) | CellItem::CloseInterval => None,
    }
}

/// The offset is core-authoritative; a firing consumes nothing.
fn consumed_offset_of(item: &CellItem) -> Option<u64> {
    match item {
        CellItem::Input(p) => p.consumed_offset,
        CellItem::Timer(_) | CellItem::CloseInterval => None,
    }
}

/// The outside call waiting on the item's input; a firing answers nobody.
fn call_of(item: &CellItem) -> Option<&Arc<str>> {
    match item {
        CellItem::Input(p) => p.call.as_ref(),
        CellItem::Timer(_) | CellItem::CloseInterval => None,
    }
}

/// A firing commits its own removal first, whatever the host reported: the
/// pending set is the core's, and a timer left in it fires again. A failed
/// invocation commits nothing else, so a poison firing parks once instead of
/// firing forever.
fn committed_timers(item: &CellItem, timers: Vec<TimerOp>, parked: bool) -> Vec<TimerOp> {
    let removal = firing_of(item).map(|(kind, at)| TimerOp::cancel(kind, at));
    match removal {
        _ if parked => removal.into_iter().collect(),
        Some(op) if !timers.contains(&op) => std::iter::once(op).chain(timers).collect(),
        _ => timers,
    }
}

fn park_entry(slot: &Arc<CellSlot>, item: &CellItem, error: String, attempts: u32) -> DlqEntryBody {
    crate::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[flezha] tasklet {} parking poison payload to DLQ after {} attempt(s): {}",
        slot.dispatcher.tasklet_name(),
        attempts,
        error
    );
    DlqEntryBody {
        channel: park_channel_of(slot),
        error,
        attempts,
        payload: match item {
            CellItem::Input(p) => p
                .value
                .downcast_ref::<HostPayload>()
                .map(|hp| hp.bytes.clone()),
            CellItem::Timer(_) | CellItem::CloseInterval => None,
        },
    }
}

/// Edge values into consumer mailboxes, BEFORE the effect record is folded,
/// so no consumed offset is durable ahead of an undelivered value; where the
/// Cell records effects, the values its record keeps for a restore to deliver
/// again.
fn route_edge_outputs(
    slot: &Arc<CellSlot>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    call: Option<&Arc<str>>,
    edge_outputs: Vec<schema::Output>,
    record: bool,
) -> Vec<crate::flusher::EdgeRecord> {
    let mut recorded = Vec::new();
    if edge_outputs.is_empty() {
        return recorded;
    }
    let Some(instance) = slot.instance.upgrade() else {
        return recorded;
    };
    for output in edge_outputs {
        let schema::OutputTarget::Edge { channel, consumer } = output.target else {
            continue;
        };
        let value: Arc<dyn Any + Send + Sync> = Arc::new(HostPayload {
            bytes: output.payload,
            channel,
            keys: output.key.into_iter().collect(),
            event_time: output.event_time,
            metadata: output.metadata,
        });
        if record {
            recorded.extend(crate::routing::edge_record(
                &instance,
                channel,
                consumer,
                consumed_offset,
                &value,
            ));
        }
        match crate::routing::deliver_host_output(
            &instance,
            channel,
            consumer,
            &value,
            stream,
            consumed_offset,
            call,
            slot,
        ) {
            // A push this Cell could not deliver is HELD, not lost:
            // it sits in the producer's holdback and the Cell is
            // handed no further work until it lands. The remaining
            // outputs of this completion still route — rule 2 appends
            // them behind the held one rather than letting them
            // overtake it.
            Ok(_) => {}
            Err(e) => tracing::warn!(
                target: "flezha::host",
                cell = slot.dispatcher.cell_label(),
                "edge output dropped: {e}"
            ),
        }
    }
    recorded
}

/// Fold the invocation into the buffer per its Cell's policy; a Cell that
/// records no effects hands its wire outputs and park back to ship after the
/// buffer ops. Both routes leave from here, so the answers are stamped here.
fn settle_into_buffer(
    slot: &Arc<CellSlot>,
    facts: &HostTaskletFacts,
    mut effects: InvocationEffects<Vec<u8>>,
    mut wire_outputs: Vec<schema::Output>,
    call: Option<&Arc<str>>,
    dlq: Option<DlqEntryBody>,
    hold: Option<Arc<crate::prefix::Hold>>,
) -> (Option<DlqEntryBody>, Vec<schema::Output>) {
    stamp_answers(slot, call, &mut wire_outputs);
    let mut buffer = slot.flush_buffer.lock_recover();
    let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() else {
        return (None, Vec::new());
    };
    let records_effects = slot.topology.records_effects(slot.persistence_enabled);
    let mut plugin_outputs = Vec::new();
    if records_effects {
        effects.outputs = output_records(wire_outputs);
    } else {
        plugin_outputs = wire_outputs;
    }
    let tier_off_dlq = crate::flusher::settle_effects(
        bytes,
        SettlePolicy {
            records_effects,
            checkpoint: facts.checkpoint,
            buffered: slot.flusher_entry_id.is_some(),
        },
        effects,
        dlq,
    );
    crate::cell::keep_hold(slot, bytes, hold);
    (tier_off_dlq, plugin_outputs)
}

/// Every output of a `response()` end carries `flz.reply_to`, the call's
/// token or an empty value when its input carried none; no other output
/// carries the key, whatever the host put in its metadata.
fn stamp_answers(slot: &CellSlot, call: Option<&Arc<str>>, outputs: &mut [schema::Output]) {
    for output in outputs {
        let schema::OutputTarget::Plugin { channel } = &output.target else {
            continue;
        };
        output
            .metadata
            .retain(|(key, _)| key != crate::routing::CALL_KEY);
        let response = slot
            .topology
            .lookup_channel_id(channel)
            .and_then(|id| slot.topology.channel(id))
            .is_some_and(|descriptor| descriptor.response);
        if response {
            output.metadata.push((
                crate::routing::CALL_KEY.to_string(),
                call.map_or_else(String::new, |token| token.to_string()),
            ));
        }
    }
}

fn output_records(wire_outputs: Vec<schema::Output>) -> Vec<OutputRecord> {
    wire_outputs
        .into_iter()
        .filter_map(|o| match o.target {
            schema::OutputTarget::Plugin { channel } => Some(OutputRecord {
                channel,
                key: o.key.unwrap_or_default(),
                metadata: o.metadata,
                payload: o.payload,
            }),
            schema::OutputTarget::Edge { .. } => None,
        })
        .collect()
}

fn commit_timers(slot: &Arc<CellSlot>, timers: &[TimerOp], stream: &Arc<FlezhaStream>) {
    if timers.is_empty() {
        return;
    }
    {
        let mut pending = slot.timers.lock_recover();
        for op in timers {
            pending.apply(op);
        }
    }
    crate::timer::apply_ops(slot, timers, stream);
}

/// Ship the tier-off plugin outputs. The frame's payload id is the OPERATOR
/// ID — what the controller routes `:on_output` by.
fn ship_plugin_outputs(
    slot: &Arc<CellSlot>,
    stream: &Arc<FlezhaStream>,
    plugin_outputs: Vec<schema::Output>,
) {
    for output in plugin_outputs {
        match output.target {
            schema::OutputTarget::Plugin { channel } => {
                let mut metadata: HashMap<String, String> = output.metadata.into_iter().collect();
                metadata.insert("channel".to_string(), channel);
                if let Err(error) = crate::routing::send_plugin_frame(
                    stream,
                    &slot.instance_id,
                    &output.payload,
                    &metadata,
                ) {
                    tracing::error!(
                        target: "flezha::host",
                        instance_id = %slot.instance_id,
                        %error,
                        "plugin output refused; it is dropped and the fold carries on"
                    );
                }
            }
            schema::OutputTarget::Edge { .. } => unreachable!("edge outputs partitioned above"),
        }
    }
}

fn park_channel_of(slot: &Arc<CellSlot>) -> String {
    slot.topology
        .tasklet(slot.tasklet_id)
        .and_then(|d| d.input_channel_ids.first())
        .and_then(|&id| slot.topology.channel(id))
        .map(|c| c.name.to_string())
        .unwrap_or_else(|| "<unwired>".to_string())
}
