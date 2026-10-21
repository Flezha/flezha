//! The metadata a host's plugin outputs leave with, stamped by the core
//! whatever the host put there: the answer token on a called input's sink
//! outputs, the call facts on a `call(...)` end's, the identity on a keyed
//! channel's. No other output carries the first two, so neither can be
//! written by a tasklet.

use std::sync::Arc;

use crate::cell::CellSlot;
use crate::channel::{
    ChannelDescriptor, CALL_ANSWER_CHANNEL_KEY, CALL_CHANNEL_KEY, CALL_CREATE_KEY,
    CALL_RESPONSE_KEY, CALL_TARGET_KEY, DEADLINE_AT_KEY,
};
use crate::routing::REPLY_TO_KEY;
use crate::schema;

/// The keys only the core writes on a plugin output.
const CORE_KEYS: [&str; 7] = [
    REPLY_TO_KEY,
    CALL_TARGET_KEY,
    CALL_CREATE_KEY,
    CALL_RESPONSE_KEY,
    CALL_CHANNEL_KEY,
    CALL_ANSWER_CHANNEL_KEY,
    DEADLINE_AT_KEY,
];

/// Every sink output of a batch whose input carried a call's token carries
/// `flz.reply_to` with that token; a batch with none stamps nothing. Every
/// output of a call end carries the end's stamps, its deadline taken from
/// `clock`, read once and only for a batch with a call end, and its `flz.key`
/// from the key the host computed, on both routes. A keyed output of a Cell
/// that records no effects carries its identity at `change_seq`, read once.
pub(super) fn stamp_plugin_ends(
    slot: &CellSlot,
    call: Option<&Arc<str>>,
    outputs: &mut [schema::Output],
    change_seq: impl Fn() -> u64,
    mut clock: impl FnMut() -> u64,
) {
    let (mut now_ms, mut seq_read, mut ordinal) = (None, None, 0);
    let identity = !slot.topology.records_effects(slot.keeps_state);
    for output in outputs {
        let schema::OutputTarget::Plugin { channel } = &output.target else {
            continue;
        };
        output.metadata.retain(|(key, _)| !core_key(key));
        let Some(descriptor) = slot
            .topology
            .lookup_channel_id(channel)
            .and_then(|id| slot.topology.channel(id))
        else {
            continue;
        };
        if descriptor.call.is_some() {
            output.metadata.retain(|(key, _)| key != "flz.key");
        }
        let key = output.key.as_deref();
        let read_once = || *now_ms.get_or_insert_with(&mut clock);
        end_stamps(descriptor, call, key, read_once, &mut output.metadata);
        if identity {
            let seq = || *seq_read.get_or_insert_with(&change_seq);
            let label = slot.dispatcher.cell_label();
            let keyed = descriptor.keyed;
            output_identity(label, keyed, seq, &mut ordinal, key, &mut output.metadata);
        }
    }
}

/// The stamps one plugin output of `descriptor` leaves with: the answer
/// token on a sink's whose input carried a call, the end's facts on a call
/// end's, its deadline read off `now_ms` only there.
pub fn end_stamps(
    descriptor: &ChannelDescriptor,
    call: Option<&Arc<str>>,
    key: Option<&str>,
    now_ms: impl FnOnce() -> u64,
    metadata: &mut impl Extend<(String, String)>,
) {
    if let Some(token) = call.filter(|_| descriptor.sink) {
        metadata.extend([(REPLY_TO_KEY.to_string(), token.to_string())]);
    }
    if let Some(end) = &descriptor.call {
        metadata.extend(end.stamps(key, now_ms()));
    }
}

/// The identity a keyed plugin output leaves with where its Cell records no
/// effects (the controller stamps those at flush): the Cell, its place
/// `<change_seq>.<ordinal>` and its key. Every plugin output of the
/// invocation takes the next ordinal, stamped or not.
pub fn output_identity(
    cell_label: &str,
    keyed: bool,
    change_seq: impl FnOnce() -> u64,
    ordinal: &mut u32,
    key: Option<&str>,
    metadata: &mut impl Extend<(String, String)>,
) {
    if keyed {
        let seq = format!("{}.{ordinal}", change_seq());
        metadata.extend([
            ("flz.cell".to_string(), cell_label.to_string()),
            ("flz.seq".to_string(), seq),
        ]);
        metadata.extend(key.map(|key| ("flz.key".to_string(), key.to_string())));
    }
    *ordinal += 1;
}

/// One prefix test settles every key a host may freely write.
fn core_key(key: &str) -> bool {
    key.starts_with("flz.") && CORE_KEYS.contains(&key)
}
