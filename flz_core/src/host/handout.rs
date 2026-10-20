//! The activation crossing's way out: a handed-out engine activation becomes
//! the schema form the C ABI ships; the way back is [`super::activation`].

use std::sync::atomic::Ordering;

use super::cell::{cell_ref_of, HostPayload};
use super::decl::HostClock;
use super::prepare::PrepareCrossing;
use crate::cell::{CellItem, CellSlot};
use crate::poll_core::{EngineActivation, EnginePayload, ItemAttempt};
use crate::schema;

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
        EnginePayload::Work {
            slot,
            items,
            change_seq,
        } => schema::Activation {
            token: act.token,
            target: cell_ref_of(slot),
            kind: schema::ActivationKind::Work(work_items_to_schema(slot, items)),
            change_seq: *change_seq,
            factory: slot.topology.factory_id.to_string(),
        },
        EnginePayload::Leave {
            target, factory, ..
        } => schema::Activation {
            token: act.token,
            target: target.clone(),
            kind: schema::ActivationKind::Leave { whole_relay: true },
            change_seq: 0,
            factory: factory.to_string(),
        },
    }
}

/// A prepare crossing is addressed to the channel's first consumer.
fn prepare_target(crossing: &PrepareCrossing) -> schema::CellRef {
    schema::CellRef {
        relay: crossing.instance.instance_id.to_string(),
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
