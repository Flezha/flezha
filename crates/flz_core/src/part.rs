//! A part: a preview of the output an invocation is producing, sent while it
//! runs (`output-parts`).
//!
//! [`send`] frames one part on its invocation's own stream and never waits:
//! a part the wire cannot take at once is dropped and counted by reason. A
//! part is never durable, never held for a reconnect and never replayed, so
//! nothing here touches the effect record, the outbox or the ring. The Rust
//! SDK calls [`send`] from its context; every other host reaches it through
//! `flz_core_ffi`'s part entry.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use flz_transport::service::payloader::{self, HEADER_LEN};
use flz_transport::transport::FlezhaStream;
use serde_json::Value;

use crate::cell::{CellItem, CellSlot};
use crate::error::{Code, Error};
use crate::poll_core::ItemAttempt;
use crate::state_sink::{Class, Verdict};

/// The reserved data-plane payload id a part travels under. An older
/// controller reads it as an instance it has no route for and drops it.
pub const PART_PAYLOAD_ID: &str = "output_part";

static PARTS_SENT: AtomicU64 = AtomicU64::new(0);
static DROPPED_NO_STREAM: AtomicU64 = AtomicU64::new(0);
static DROPPED_REFUSED: AtomicU64 = AtomicU64::new(0);
static DROPPED_TOO_LARGE: AtomicU64 = AtomicU64::new(0);

/// The runner's part counters, as the heartbeat reports them.
pub fn counters() -> [(&'static str, u64); 4] {
    [
        ("parts_sent", PARTS_SENT.load(Ordering::Relaxed)),
        (
            "parts_dropped_no_stream",
            DROPPED_NO_STREAM.load(Ordering::Relaxed),
        ),
        (
            "parts_dropped_refused",
            DROPPED_REFUSED.load(Ordering::Relaxed),
        ),
        (
            "parts_dropped_too_large",
            DROPPED_TOO_LARGE.load(Ordering::Relaxed),
        ),
    ]
}

/// What became of a part the invocation was allowed to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    Written,
    /// The invocation replays an input already processed: a part is a side
    /// effect of live processing only.
    Replayed,
    NoStream,
    Refused,
    TooLarge,
}

/// Send one part of the invocation `work` on `slot`, numbered `*sequence`,
/// which is advanced for every part the call accepts, dropped or not, so a
/// listener reads a drop as a gap. Refused with `part.no_output` when the
/// tasklet's output channel does not end in a sink.
pub fn send(
    slot: &CellSlot,
    work: &ItemAttempt,
    sequence: &mut u32,
    payload: &[u8],
) -> Result<Sent, Error> {
    send_within(
        slot,
        work,
        sequence,
        payload,
        flz_transport::transport::config::max_frame_bytes(),
    )
}

fn send_within(
    slot: &CellSlot,
    work: &ItemAttempt,
    sequence: &mut u32,
    payload: &[u8],
    max_frame: usize,
) -> Result<Sent, Error> {
    let channel = previewed_channel(slot)?;
    let (stream, replay) = match &work.item {
        CellItem::Input(input) => (Some(&input.stream), input.replay),
        CellItem::Timer(fire) => (Some(&fire.stream), false),
        CellItem::CloseInterval => (None, false),
    };
    if replay {
        return Ok(Sent::Replayed);
    }
    let number = *sequence;
    *sequence += 1;
    let sent = match stream.filter(|s| !is_local(s)) {
        None => Sent::NoStream,
        Some(stream) => {
            let metadata = stamps(slot, work.attempt, number, channel);
            match payloader::build_from_binary(PART_PAYLOAD_ID, payload, metadata) {
                Ok(frame) if frame.len() - HEADER_LEN <= max_frame => {
                    match crate::state_sink::write(Class::Preview, Some(stream), frame) {
                        Verdict::Written => Sent::Written,
                        Verdict::Held | Verdict::Refused(_) => Sent::Refused,
                    }
                }
                _ => Sent::TooLarge,
            }
        }
    };
    count(sent);
    Ok(sent)
}

/// A context kept past its invocation, or naming an item its activation does
/// not hold; `tasklet` when the caller still knows it.
pub fn outside_invocation(tasklet: Option<&str>) -> Error {
    let details = tasklet.map_or_else(serde_json::Map::new, |name| {
        serde_json::Map::from_iter([("tasklet".to_string(), Value::from(name))])
    });
    Error::Part(Code::PartOutsideInvocation.error(details))
}

/// A local run has no controller, so no listener: its stream is the owner's
/// in-process release path, which carries outputs only.
fn is_local(stream: &FlezhaStream) -> bool {
    matches!(stream, FlezhaStream::Local(_))
}

/// The tasklet's output channel, when it ends in a sink.
fn previewed_channel(slot: &CellSlot) -> Result<&str, Error> {
    let channel = slot
        .topology
        .tasklet(slot.tasklet_id)
        .and_then(|tasklet| tasklet.output_channel_id)
        .and_then(|id| slot.topology.channel(id));
    match channel {
        Some(channel) if channel.sink => Ok(&channel.name),
        _ => Err(Error::Part(Code::PartNoOutput.with([
            ("tasklet", slot.dispatcher.tasklet_name().into()),
            ("relay", slot.instance_id.as_ref().into()),
            ("channel", channel.map_or("", |c| c.name.as_ref()).into()),
        ]))),
    }
}

fn stamps(slot: &CellSlot, attempt: u32, number: u32, channel: &str) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        ("channel".to_string(), channel.to_string()),
        ("flz.part".to_string(), number.to_string()),
        ("flz.part_attempt".to_string(), attempt.to_string()),
        (
            "flz.cell".to_string(),
            slot.dispatcher.cell_label().to_string(),
        ),
        ("flz.relay".to_string(), slot.instance_id.to_string()),
    ]);
    if let Some(key) = &slot.cell_key {
        metadata.insert("flz.key".to_string(), key.to_string());
    }
    metadata
}

fn count(sent: Sent) {
    let counter = match sent {
        Sent::Written => &PARTS_SENT,
        Sent::NoStream => &DROPPED_NO_STREAM,
        Sent::Refused => &DROPPED_REFUSED,
        Sent::TooLarge => &DROPPED_TOO_LARGE,
        Sent::Replayed => return,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

#[cfg(test)]
#[path = "part_tests.rs"]
mod tests;
