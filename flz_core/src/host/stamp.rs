//! The metadata a host's plugin outputs leave with, stamped by the core
//! whatever the host put there: the answer token on a called input's sink
//! outputs, the call facts on a `call(...)` end's. No other output carries
//! either, so neither can be written by a tasklet.

use std::sync::Arc;

use crate::cell::CellSlot;
use crate::channel::{
    CALL_ANSWER_CHANNEL_KEY, CALL_CHANNEL_KEY, CALL_CREATE_KEY, CALL_RESPONSE_KEY, CALL_TARGET_KEY,
    DEADLINE_AT_KEY,
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
/// from the key the host computed, on both routes.
pub(super) fn stamp_plugin_ends(
    slot: &CellSlot,
    call: Option<&Arc<str>>,
    outputs: &mut [schema::Output],
    mut clock: impl FnMut() -> u64,
) {
    let mut now_ms = None;
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
        if let Some(token) = call {
            if descriptor.sink {
                output
                    .metadata
                    .push((REPLY_TO_KEY.to_string(), token.to_string()));
            }
        }
        if let Some(end) = &descriptor.call {
            output.metadata.retain(|(key, _)| key != "flz.key");
            let now_ms = *now_ms.get_or_insert_with(&mut clock);
            let stamps = end.stamps(output.key.as_deref(), now_ms);
            output.metadata.extend(stamps);
        }
    }
}

/// One prefix test settles every key a host may freely write.
fn core_key(key: &str) -> bool {
    key.starts_with("flz.") && CORE_KEYS.contains(&key)
}
