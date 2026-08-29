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

use super::decl::HostClock;
use super::entity::{entity_ref_of, BytesBuffer, HostPayload, HostProcedureFacts};
use super::prepare::{apply_prepare_outcome, PrepareCrossing};
use crate::entity::{EntityItem, EntitySlot};
use crate::flusher::{DlqEntryBody, InvocationEffects, OutputRecord, SettlePolicy};
use crate::poll_core::{EngineActivation, EnginePayload, ItemAttempt, PollCore};
use crate::schema::{self, TimerOp};
use crate::util::LockRecover;

/// Translate a handed-out engine activation into the schema form the C ABI
/// ships. Wall-clock `now` is stamped here — the host never reads ambient
/// time for `ctx.now()`.
pub fn to_schema(act: &EngineActivation) -> schema::Activation {
    match &act.payload {
        EnginePayload::Command {
            target,
            change_seq,
            factory,
            kind,
            ..
        } => schema::Activation {
            token: act.token,
            target: target.clone(),
            kind: kind.clone(),
            change_seq: *change_seq,
            factory: factory.clone(),
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
        EnginePayload::WorkflowStep(step) => schema::Activation {
            token: act.token,
            target: entity_ref_of(&step.slot),
            kind: workflow_step_kind(step),
            change_seq: step.slot.change_seq.load(Ordering::Acquire),
            factory: step.slot.topology.factory_id.to_string(),
        },
        EnginePayload::Work { slot, items } => schema::Activation {
            token: act.token,
            target: entity_ref_of(slot),
            kind: schema::ActivationKind::Work(work_items_to_schema(slot, items)),
            change_seq: slot.change_seq.load(Ordering::Acquire),
            factory: slot.topology.factory_id.to_string(),
        },
    }
}

/// A prepare crossing is addressed to the channel's first consumer; the host
/// answers for the channel, not for an entity.
fn prepare_target(crossing: &PrepareCrossing) -> schema::EntityRef {
    schema::EntityRef {
        operative: crossing.instance.instance_id.to_string(),
        procedure: crossing
            .instance
            .topology
            .channel(crossing.channel)
            .and_then(|c| c.consumers.first())
            .map(|e| e.procedure)
            .unwrap_or(0),
        entity_key: None,
    }
}

/// One handout's items in schema form.
fn work_items_to_schema(slot: &EntitySlot, items: &[ItemAttempt]) -> Vec<schema::WorkItem> {
    let now_fallback = host_clock_now(slot);
    items
        .iter()
        // A close is consumed by the batch assembler; it never becomes
        // host work, and filtering keeps the effect-to-item zip aligned
        // if that invariant ever loosened.
        .filter(|work| !matches!(work.item, EntityItem::CloseInterval))
        .map(|work| match &work.item {
            EntityItem::Input(payload) => {
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
            EntityItem::Timer(fire) => schema::WorkItem::Timer(schema::TimerItem {
                kind: fire.kind,
                scheduled_at: fire.scheduled_at,
            }),
            // Filtered out above; a firing item is the harmless shape.
            EntityItem::CloseInterval => schema::WorkItem::Timer(schema::TimerItem {
                kind: schema::TimerKind::ProcessingTime,
                scheduled_at: 0,
            }),
        })
        .collect()
}

/// One workflow step as the host receives it: a stage batch is an ordinary
/// `Work` activation (the driver's ladder and barriers stay driver-side, so the
/// host runs exactly one attempt), an undo is a `CompensateRun`.
fn workflow_step_kind(step: &crate::workflow::step::WorkflowStep) -> schema::ActivationKind {
    use crate::workflow::step::StepWork;
    match &step.work {
        StepWork::Items(items) => {
            let now = host_clock_now(&step.slot);
            schema::ActivationKind::Work(
                items
                    .iter()
                    .map(|value| {
                        let (channel, bytes, metadata) =
                            host_input_parts(value.downcast_ref::<HostPayload>(), &step.slot);
                        schema::WorkItem::Input(schema::InputItem {
                            channel,
                            payload: bytes,
                            metadata,
                            consumed_offset: None,
                            replay: false,
                            now,
                        })
                    })
                    .collect(),
            )
        }
        StepWork::Compensate(outputs) => schema::ActivationKind::CompensateRun {
            outputs: outputs
                .iter()
                .filter_map(|value| value.downcast_ref::<HostPayload>())
                .map(|payload| payload.bytes.clone())
                .collect(),
        },
    }
}

/// The declared clock's value for one handout. The engine stamps `now` for
/// event-time items; wall and logical clocks evaluate here, once per handout —
/// the host never reads ambient time (or its own counters) for `ctx.now()`.
fn host_clock_now(slot: &EntitySlot) -> u64 {
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

/// The channel, payload and metadata one item carries across. A value that is
/// not a `HostPayload` cannot reach a host class — its codecs mint them — so
/// the fallback is defensive: an empty payload on the entity's input channel,
/// which the host's own decoder rejects as a work failure.
fn host_input_parts(
    payload: Option<&HostPayload>,
    slot: &EntitySlot,
) -> (u32, Vec<u8>, Vec<(String, String)>) {
    match payload {
        Some(p) => (p.channel, p.bytes.clone(), p.metadata.clone()),
        None => (input_channel_of(slot), Vec::new(), Vec::new()),
    }
}

fn input_channel_of(slot: &EntitySlot) -> u32 {
    slot.topology
        .procedure(slot.procedure_id)
        .and_then(|p| p.input_channel_ids.first().copied())
        .unwrap_or(0)
}

/// Apply a host completion to its outstanding activation: buffer the change
/// bytes, fold and arm timer ops, ship plugin outputs, then drive the
/// engine's completion protocol (release, ladder, or park).
pub fn apply_result(
    core: &PollCore,
    act: EngineActivation,
    result: schema::ActivationResult,
) -> Result<(), String> {
    let EngineActivation { token, payload } = act;
    let (slot, items) = match payload {
        EnginePayload::Command { reply, .. } => {
            // A command interaction: the blocked command thread owns the
            // entity gate and validates the outcome; this side only
            // rendezvous-routes.
            let _ = reply.send(result.outcome);
            return Ok(());
        }
        EnginePayload::WorkflowStep(step) => {
            // A workflow stage step: its driver is parked at the barrier this
            // answer feeds, so the fold happens here rather than on a thread
            // waiting between the two.
            crate::workflow::step::complete_workflow_step(step, result.outcome);
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
        // A malformed answer still answers: the entity is released with its
        // items intact rather than retired behind a handout nobody will
        // complete.
        _ => {
            core.abandon(token, items);
            Err("work activation answered with a non-work outcome".to_string())
        }
    }
}

/// Whole-activation failure: ladder the head item with the rest behind it,
/// or park it when non-retryable/final. The host reported no per-item
/// effect, so the park resolves with an empty one (any pending-attempt
/// changes still flush with it).
fn answer_failed_activation(
    core: &PollCore,
    token: u64,
    slot: &Arc<EntitySlot>,
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

/// Settle each item against its effect in order; a retryable failure ends the
/// batch and re-hands the failed item with the rest behind it.
fn settle_items(
    core: &PollCore,
    token: u64,
    slot: &Arc<EntitySlot>,
    items: Vec<ItemAttempt>,
    effects: Vec<schema::ItemEffect>,
) -> Result<(), String> {
    let mut queue: std::collections::VecDeque<ItemAttempt> = items.into();
    for effect in effects {
        let Some(work) = queue.pop_front() else {
            core.abandon(token, Vec::new());
            return Err("completion carries more effects than items".to_string());
        };
        match effect.verdict.clone() {
            schema::Verdict::Ok => {
                settle_item(slot, &work, effect, None);
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            schema::Verdict::RetryableError { message } => {
                if work.is_final {
                    settle_item(slot, &work, effect, Some((message, work.attempt)));
                    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
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
            schema::Verdict::NonRetryableError { message } => {
                settle_item(slot, &work, effect, Some((message, work.attempt)));
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    finish_batch(core, token, queue);
    Ok(())
}

/// A parked head stops the host mid-batch ("items after the failed one
/// are re-handed by the core"); anything still queued is owed a fresh
/// round — dropping it here is how a healthy payload behind a poison one
/// would vanish.
fn finish_batch(core: &PollCore, token: u64, queue: std::collections::VecDeque<ItemAttempt>) {
    let rest: Vec<ItemAttempt> = queue.into_iter().collect();
    if rest.is_empty() {
        core.complete(token);
    } else {
        core.abandon(token, rest);
    }
}

/// Answer a polled activation the host cannot finish, without processing it.
///
/// The ABI's third answer, and the one no unwind can reach across the
/// boundary: a veneer whose language threw, or whose thread died, has no way
/// to call `complete`, and per-entity exclusivity would retire the entity for
/// the life of the process. One function so every payload kind is answered the
/// way its own waiter expects — a work batch re-hands its items and consumes
/// no ladder attempt, while a command, workflow step or prepare crossing has a
/// blocked enqueuer that must be released with a failure rather than left
/// parked.
pub fn abandon_activation(core: &PollCore, act: EngineActivation) -> Result<(), String> {
    let EngineActivation { token, payload } = act;
    match payload {
        EnginePayload::Work { items, .. } => match core.abandon(token, items) {
            crate::poll_core::Answered::Released => Ok(()),
            crate::poll_core::Answered::UnknownToken => Err(format!(
                "unknown or already-answered activation token {token}"
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

/// The effect a whole-activation failure resolves its head item with: the
/// host reported nothing per-item.
fn empty_effect() -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: Vec::new(),
        outputs: Vec::new(),
        consumed_offset: None,
        event_time: 0,
        timers: Vec::new(),
        checkpoint: false,
        dlq: None,
    }
}

/// Hold a failed-but-retryable attempt's changes until its item resolves.
fn stash_pending_attempt(slot: &Arc<EntitySlot>, changes: Vec<Vec<u8>>) {
    if changes.is_empty() {
        return;
    }
    let mut buffer = slot.flush_buffer.lock_recover();
    if let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() {
        bytes.pending_attempt.extend(changes);
    }
}

/// Settle one item — the host boundary's mirror of the typed layer's
/// per-invocation effect block: fold the item's changes and timer ops into
/// the entity per its tier, route its outputs, and (when `park` is set) mint
/// the DLQ entry exactly where the typed layer does — inside the effect
/// record for the effectively-once tier (atomic with the offset advance),
/// through the blocking tier-off park otherwise.
pub(super) fn settle_item(
    slot: &Arc<EntitySlot>,
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
    let parked = park.is_some();
    let timers = committed_timers(&work.item, effect.timers, parked);
    // A failed attempt's captured outputs must not ship — the invocation did
    // not complete.
    let outputs = if parked { Vec::new() } else { effect.outputs };
    let (edge_outputs, wire_outputs) = split_outputs(outputs);
    let consumed_offset = consumed_offset_of(&work.item);
    let dlq = park.map(|(error, attempts)| park_entry(slot, &work.item, error, attempts));
    route_edge_outputs(slot, &stream, consumed_offset, edge_outputs);
    let effects = InvocationEffects {
        changes: effect.changes,
        timers: timers.clone(),
        outputs: Vec::new(),
        offset: consumed_offset,
        event_time: effect.event_time,
        checkpointed: effect.checkpoint,
        parked: dlq.is_some(),
    };
    let (tier_off_dlq, plugin_outputs) =
        settle_into_buffer(slot, facts, effects, wire_outputs, dlq);
    commit_timers(slot, &timers, &stream);
    crate::timer::arm_interval_deadline(slot);
    ship_plugin_outputs(slot, &stream, plugin_outputs);
    if let Some(entry) = tier_off_dlq {
        // Tier-off park: ship the park frame and hold this entity until the
        // controller acks the DLQ append as durable — park-then-advance.
        crate::park::park_tier_off(
            &slot.entity_identity,
            slot.dispatcher.entity_label(),
            consumed_offset,
            entry,
        );
    }
}

/// The connection an item's outputs and park ride. A close is never resolved
/// as host work (the assembler consumes it); without a connection of its own
/// it takes the live secondary, as a chained deadline does.
fn stream_of(item: &EntityItem) -> Option<Arc<FlezhaStream>> {
    match item {
        EntityItem::Input(p) => Some(Arc::clone(&p.stream)),
        EntityItem::Timer(f) => Some(Arc::clone(&f.stream)),
        EntityItem::CloseInterval => crate::state_sink::secondary_stream(),
    }
}

/// Edge values deliver in-process to their named consumer (producer-side
/// evaluation: the host already ran transforms and key); wire outputs ride
/// the tier's path. `(edge, wire)`.
fn split_outputs(outputs: Vec<schema::Output>) -> (Vec<schema::Output>, Vec<schema::Output>) {
    outputs
        .into_iter()
        .partition(|o| matches!(o.target, schema::OutputTarget::Edge { .. }))
}

/// The offset is core-authoritative: the item the engine handed out knows
/// what it consumed; a firing consumes nothing.
fn consumed_offset_of(item: &EntityItem) -> Option<u64> {
    match item {
        EntityItem::Input(p) => p.consumed_offset,
        EntityItem::Timer(_) | EntityItem::CloseInterval => None,
    }
}

/// A failed invocation commits no timer registrations — except a poison
/// firing, which still records its removal so it parks once instead of
/// firing forever.
fn committed_timers(item: &EntityItem, timers: Vec<TimerOp>, parked: bool) -> Vec<TimerOp> {
    if !parked {
        return timers;
    }
    match item {
        EntityItem::Timer(fire) => vec![TimerOp::cancel(fire.kind, fire.scheduled_at)],
        EntityItem::Input(_) | EntityItem::CloseInterval => Vec::new(),
    }
}

/// The dead-letter entry a park mints, counted and reported as it is minted.
fn park_entry(
    slot: &Arc<EntitySlot>,
    item: &EntityItem,
    error: String,
    attempts: u32,
) -> DlqEntryBody {
    crate::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[flezha] procedure {} parking poison payload to DLQ after {} attempt(s): {}",
        slot.dispatcher.procedure_name(),
        attempts,
        error
    );
    DlqEntryBody {
        channel: park_channel_of(slot),
        error,
        attempts,
        payload: match item {
            EntityItem::Input(p) => p
                .value
                .downcast_ref::<HostPayload>()
                .map(|hp| hp.bytes.clone()),
            EntityItem::Timer(_) | EntityItem::CloseInterval => None,
        },
    }
}

/// Edge values into consumer mailboxes — the offset propagates exactly as
/// the typed emit path propagates it.
///
/// BEFORE the effect record is folded, and that order is the guarantee: an
/// edge output is the one output kind with no durable copy (it is excluded
/// from the transactional outbox), so the only thing that can re-produce it
/// is the producer re-running its input. Fold first and a crash in the
/// ~5 ms until the flusher runs leaves the offset durable, the replay floor
/// skipping that input, and the consumer never fed — a permanent loss under
/// a contract that promises effectively-once. Holding the item's effects
/// until every push has landed makes the item unresolved while a delivery
/// is outstanding, exactly as `stash_pending_attempt` holds a failed
/// attempt's changes. The typed path emits from inside `ctx.emit` during
/// `run()`, before the effect block exists, and already satisfies this.
fn route_edge_outputs(
    slot: &Arc<EntitySlot>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    edge_outputs: Vec<schema::Output>,
) {
    if edge_outputs.is_empty() {
        return;
    }
    let Some(instance) = slot.instance.upgrade() else {
        return;
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
        match crate::routing::deliver_host_output(
            &instance,
            channel,
            consumer,
            &value,
            stream,
            consumed_offset,
            slot,
        ) {
            // A push this Entity could not deliver is HELD, not lost:
            // it sits in the producer's holdback and the Entity is
            // handed no further work until it lands. The remaining
            // outputs of this completion still route — rule 2 appends
            // them behind the held one rather than letting them
            // overtake it.
            Ok(_) => {}
            Err(e) => tracing::warn!(
                target: "flezha::host",
                entity = slot.dispatcher.entity_label(),
                "edge output dropped: {e}"
            ),
        }
    }
}

/// Fold the invocation into the Entity's buffer per its tier. Under the tier
/// the wire outputs ride the effect record's transactional outbox; outside
/// it they are handed back to ship on the wire directly, and so is the park,
/// which ships as its own blocking frame after the buffer ops.
fn settle_into_buffer(
    slot: &Arc<EntitySlot>,
    facts: &HostProcedureFacts,
    mut effects: InvocationEffects<Vec<u8>>,
    wire_outputs: Vec<schema::Output>,
    dlq: Option<DlqEntryBody>,
) -> (Option<DlqEntryBody>, Vec<schema::Output>) {
    let mut buffer = slot.flush_buffer.lock_recover();
    let Some(bytes) = buffer.as_any_mut().downcast_mut::<BytesBuffer>() else {
        return (None, Vec::new());
    };
    let mut plugin_outputs = Vec::new();
    if facts.effectively_once {
        effects.outputs = output_records(wire_outputs);
    } else {
        plugin_outputs = wire_outputs;
    }
    let tier_off_dlq = crate::flusher::settle_effects(
        bytes,
        SettlePolicy {
            effectively_once: facts.effectively_once,
            checkpoint: facts.checkpoint,
            buffered: slot.flusher_entry_id.is_some(),
        },
        effects,
        dlq,
    );
    (tier_off_dlq, plugin_outputs)
}

/// The plugin outputs as the effect record carries them.
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

/// Fold the committed timer ops into the pending set and arm them.
fn commit_timers(slot: &Arc<EntitySlot>, timers: &[TimerOp], stream: &Arc<FlezhaStream>) {
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

/// Ship the tier-off plugin outputs on the wire. Empty on a park and under
/// the tier (where outputs ride the effect record's transactional outbox
/// instead). The frame's payload id is the OPERATIVE ID — that is what the
/// controller routes `:on_output` by; the entity label would name nothing.
fn ship_plugin_outputs(
    slot: &Arc<EntitySlot>,
    stream: &Arc<FlezhaStream>,
    plugin_outputs: Vec<schema::Output>,
) {
    for output in plugin_outputs {
        match output.target {
            schema::OutputTarget::Plugin { channel } => {
                let mut metadata: HashMap<String, String> = output.metadata.into_iter().collect();
                metadata.insert("channel".to_string(), channel);
                crate::routing::send_plugin_frame(
                    stream,
                    &slot.instance_id,
                    &output.payload,
                    &metadata,
                );
            }
            schema::OutputTarget::Edge { .. } => unreachable!("edge outputs partitioned above"),
        }
    }
}

fn park_channel_of(slot: &Arc<EntitySlot>) -> String {
    slot.topology
        .procedure(slot.procedure_id)
        .and_then(|d| d.input_channel_ids.first())
        .and_then(|&id| slot.topology.channel(id))
        .map(|c| c.name.to_string())
        .unwrap_or_else(|| "<unwired>".to_string())
}
