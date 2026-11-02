//! Effect results (`relay-determinism`): what `ctx.effect` keeps so a re-run of
//! the invocation reads the result of an outside call instead of making it
//! again.
//!
//! The controller is the authority: [`keep`] ships a result and returns only
//! once the controller has it synced, and a bind ships the instance's kept
//! results back before anything re-runs ([`landed`]). This module's map is the
//! runner's copy, which [`lookup`] serves. A Cell takes its inputs in offset
//! order, so a call at a later offset drops the Cell's earlier entries; a re-run
//! of those comes only with a new bind, which ships them again.
//!
//! An invocation that took no input-log offset has no identity: a host calls
//! neither function and only runs the function.

use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Mutex, OnceLock};

use flz_transport::errors::FlezhaError;

use crate::error::Code;
use crate::origin::{InvocationOrigin, Phase};
use crate::util::LockRecover;

/// The longest name an effect may carry, in bytes.
pub const MAX_NAME_BYTES: usize = 255;

/// The encoded-result cap when `FLEZHA_EFFECT_MAX_BYTES` is unset.
pub const DEFAULT_MAX_BYTES: usize = 4 * 1024 * 1024;

/// The ack prefix naming a result kept before this one, hex after it.
const FIRST_PREFIX: &str = "first:";

/// The encoded-result cap, `FLEZHA_EFFECT_MAX_BYTES` or the default, read once.
pub fn max_bytes() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        flz_transport::env::env_parse_positive("FLEZHA_EFFECT_MAX_BYTES", DEFAULT_MAX_BYTES)
    })
}

/// Refuse an encoded result over [`max_bytes`] with `effect.too_large`.
pub fn fits(name: &str, encoded: &[u8]) -> Result<(), FlezhaError> {
    let limit = max_bytes();
    if encoded.len() > limit {
        return Err(Code::EffectTooLarge.with([
            ("name", name.into()),
            ("bytes", encoded.len().into()),
            ("limit", limit.into()),
        ]));
    }
    Ok(())
}

/// Refuse a name that is empty or over [`MAX_NAME_BYTES`].
pub fn check_name(name: &str) -> Result<(), FlezhaError> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES {
        return Err(Code::EffectBadName.with([("name", format!("{name:?}").into())]));
    }
    Ok(())
}

/// `effect.nested`, for a host whose call came from inside another effect's
/// function.
pub fn nested(name: &str) -> FlezhaError {
    Code::EffectNested.with([("name", name.into())])
}

/// `effect.outside_invocation`, for a call through a returned context.
pub fn outside_invocation(name: &str) -> FlezhaError {
    Code::EffectOutsideInvocation.with([("name", name.into())])
}

/// A kept result, and whether an invocation on this runner already took it.
struct Held {
    payload: Vec<u8>,
    claimed: bool,
}

/// One Cell's results: offset, then identity.
type CellResults = BTreeMap<u64, HashMap<String, Held>>;

/// Instance, then Cell.
type Kept = HashMap<String, HashMap<Vec<u8>, CellResults>>;

fn kept() -> &'static Mutex<Kept> {
    static KEPT: OnceLock<Mutex<Kept>> = OnceLock::new();
    KEPT.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The instance a Cell identity names; empty for one that does not decode.
fn instance_of(cell: &[u8]) -> String {
    crate::flusher::decode_cell_identity(cell)
        .map(|(instance_id, _, _)| instance_id)
        .unwrap_or_default()
}

fn store(
    kept: &mut Kept,
    cell: &[u8],
    offset: u64,
    identity: String,
    payload: Vec<u8>,
    claimed: bool,
) {
    kept.entry(instance_of(cell))
        .or_default()
        .entry(cell.to_vec())
        .or_default()
        .entry(offset)
        .or_default()
        .entry(identity)
        .or_insert(Held { payload, claimed });
}

/// Which call an `effect` is: its ordinal and identity, and the result kept
/// for it, if this runner holds one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub ordinal: u32,
    pub identity: String,
    pub kept: Option<Vec<u8>>,
}

/// Resolve the call `name` makes as its invocation's `ordinal`-th of that name.
///
/// Several items of one Cell can take one offset (a steps pass hands a stage
/// every item it addressed to that Cell), and each counts its calls from 0. A
/// result an invocation kept or read is claimed, so another item's first
/// attempt passes over it to the next ordinal, which is the order the items
/// ran in the first time. A retry (`attempt` above 1) reads a claimed result
/// as its own. The Cell's entries below the offset are dropped first: it has
/// moved past them.
pub fn resolve(
    origin: InvocationOrigin<'_>,
    phase: Phase,
    name: &str,
    ordinal: u32,
    attempt: u32,
) -> Resolved {
    let mut ordinal = ordinal;
    let Some(offset) = origin.offset else {
        return Resolved {
            ordinal,
            identity: String::new(),
            kept: None,
        };
    };
    let mut kept = kept().lock_recover();
    let mut results = kept
        .get_mut(&instance_of(origin.cell_identity))
        .and_then(|cells| cells.get_mut(origin.cell_identity))
        .and_then(|results| {
            *results = results.split_off(&offset);
            results.get_mut(&offset)
        });
    loop {
        let identity = origin.effect_identity(phase, name, ordinal);
        match results.as_mut().and_then(|at| at.get_mut(&identity)) {
            Some(held) if held.claimed && attempt <= 1 => ordinal += 1,
            Some(held) => {
                held.claimed = true;
                let kept = Some(held.payload.clone());
                return Resolved {
                    ordinal,
                    identity,
                    kept,
                };
            }
            None => {
                return Resolved {
                    ordinal,
                    identity,
                    kept: None,
                }
            }
        }
    }
}

/// Ship a result the function returned and wait for the controller to keep it,
/// over the control plane's request deadline. `None` when this result was kept,
/// so the call returns it; the result the controller kept first for the
/// identity otherwise, which the call returns instead. A refused, unanswered or
/// unshippable request is `effect.not_recorded`, which fails the invocation so
/// it runs again.
pub fn keep(
    origin: InvocationOrigin<'_>,
    name: &str,
    identity: String,
    payload: Vec<u8>,
) -> Result<Option<Vec<u8>>, FlezhaError> {
    fits(name, &payload)?;
    let Some(offset) = origin.offset else {
        return Ok(None);
    };
    let not_recorded = |reason: String| {
        Code::EffectNotRecorded.with([("name", name.into()), ("reason", reason.into())])
    };
    let reference_id = format!("effect-{identity}-{}", crate::util::nonce_suffix());
    let command = crate::pb::command_request::Command::EffectResult(crate::pb::EffectResult {
        relay_id: instance_of(origin.cell_identity),
        identity: identity.clone(),
        offset,
        name: name.to_string(),
        payload: payload.clone(),
        cell_identity: origin.cell_identity.to_vec(),
    });
    let rx = crate::shutdown::ask(reference_id.clone(), command)
        .map_err(|_| not_recorded("the runner has no control plane".to_string()))?;
    let ack = match rx.recv_timeout(crate::shutdown::REPLY_TIMEOUT) {
        Ok(Ok(ack)) => ack,
        Ok(Err(refusal)) => return Err(not_recorded(refusal.message)),
        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
            crate::shutdown::drop_pending_ack(&reference_id);
            return Err(not_recorded(format!(
                "no answer within {}s",
                crate::shutdown::REPLY_TIMEOUT.as_secs()
            )));
        }
    };
    let first = match ack.strip_prefix(FIRST_PREFIX) {
        Some(hex) => {
            Some(decode_hex(hex).ok_or_else(|| not_recorded("an unreadable answer".to_string()))?)
        }
        None => None,
    };
    let kept_result = first.clone().unwrap_or(payload);
    store(
        &mut kept().lock_recover(),
        origin.cell_identity,
        offset,
        identity,
        kept_result,
        true,
    );
    Ok(first)
}

/// The ack string an `EffectResultAck` wakes [`keep`] with: empty when the sent
/// result was kept, the first result otherwise.
pub fn encode_ack(first: bool, payload: &[u8]) -> String {
    match first {
        true => format!("{FIRST_PREFIX}{}", crate::util::hex(payload)),
        false => String::new(),
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(hex.get(at..at + 2)?, 16).ok())
        .collect()
}

/// Take the results a bind shipped; answers how many.
pub fn landed(results: Vec<crate::pb::EffectResult>) -> usize {
    let count = results.len();
    let mut kept = kept().lock_recover();
    for result in results {
        store(
            &mut kept,
            &result.cell_identity,
            result.offset,
            result.identity,
            result.payload,
            false,
        );
    }
    count
}

/// Drop everything kept for an instance this runner no longer holds.
pub fn forget(instance_id: &str) {
    kept().lock_recover().remove(instance_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::origin::Phase;

    fn origin(cell: &[u8], offset: u64) -> InvocationOrigin<'_> {
        InvocationOrigin {
            cell_identity: cell,
            offset: Some(offset),
        }
    }

    /// A Cell identity of an instance unique to the test, so tests sharing
    /// the process-wide map never meet.
    fn cell(instance: &str, key: &str) -> Vec<u8> {
        crate::flusher::encode_cell_identity(instance, "task", key)
    }

    fn shipped(
        cell: &[u8],
        offset: u64,
        identity: &str,
        payload: &[u8],
    ) -> crate::pb::EffectResult {
        crate::pb::EffectResult {
            relay_id: String::new(),
            identity: identity.to_string(),
            offset,
            name: "model".to_string(),
            payload: payload.to_vec(),
            cell_identity: cell.to_vec(),
        }
    }

    fn instance(name: &str) -> String {
        format!("{name}-{}", crate::util::nonce_suffix())
    }

    #[test]
    fn a_name_is_non_empty_and_at_most_255_bytes() {
        assert!(check_name("model").is_ok());
        assert!(check_name(&"m".repeat(MAX_NAME_BYTES)).is_ok());
        for bad in [String::new(), "m".repeat(MAX_NAME_BYTES + 1)] {
            assert_eq!(check_name(&bad).unwrap_err().code, "effect.bad_name");
        }
    }

    /// The kept result for `name`'s first call at `offset`, by a first attempt.
    fn first(cell: &[u8], offset: u64, name: &str) -> Resolved {
        resolve(origin(cell, offset), Phase::Run, name, 0, 1)
    }

    #[test]
    fn a_shipped_result_is_served_to_its_identity_only() {
        let (a, b) = (
            cell(&instance("served"), "a"),
            cell(&instance("served"), "b"),
        );
        let model = origin(&a, 4).effect_identity(Phase::Run, "model", 0);
        assert_eq!(landed(vec![shipped(&a, 4, &model, b"answer")]), 1);

        assert_eq!(first(&a, 4, "model").kept, Some(b"answer".to_vec()));
        assert_eq!(first(&a, 4, "tool").kept, None);
        assert_eq!(first(&b, 4, "model").kept, None);
    }

    /// Two items of one Cell take one offset in one pass: each first attempt
    /// takes the next result in the order they ran, and a retry reads its own.
    #[test]
    fn items_of_one_cell_at_one_offset_take_their_results_in_order() {
        let a = cell(&instance("items"), "a");
        let at = origin(&a, 4);
        let ids: Vec<String> = (0..2)
            .map(|n| at.effect_identity(Phase::Run, "model", n))
            .collect();
        landed(vec![
            shipped(&a, 4, &ids[0], b"first"),
            shipped(&a, 4, &ids[1], b"second"),
        ]);

        let one = first(&a, 4, "model");
        assert_eq!((one.ordinal, one.kept), (0, Some(b"first".to_vec())));
        let two = first(&a, 4, "model");
        assert_eq!((two.ordinal, two.kept), (1, Some(b"second".to_vec())));
        let three = first(&a, 4, "model");
        assert_eq!(
            (three.ordinal, three.kept),
            (2, None),
            "a third item calls out"
        );
        assert_eq!(
            resolve(at, Phase::Run, "model", 0, 2).kept,
            Some(b"first".to_vec()),
            "a retry reads the result it kept"
        );
    }

    /// A Cell that moved to a later offset never re-runs an earlier one
    /// without a bind, which ships what is still kept.
    #[test]
    fn a_later_offset_drops_the_cells_earlier_results() {
        let instance = instance("pruned");
        let (a, b) = (cell(&instance, "a"), cell(&instance, "b"));
        let id = |c: &[u8], offset| origin(c, offset).effect_identity(Phase::Run, "model", 0);
        landed(vec![
            shipped(&a, 4, &id(&a, 4), b"4"),
            shipped(&a, 7, &id(&a, 7), b"7"),
            shipped(&b, 4, &id(&b, 4), b"b"),
        ]);

        assert_eq!(first(&a, 7, "model").kept, Some(b"7".to_vec()));
        assert_eq!(first(&a, 4, "model").kept, None);
        assert_eq!(
            first(&b, 4, "model").kept,
            Some(b"b".to_vec()),
            "another Cell keeps its own"
        );
    }

    #[test]
    fn a_forgotten_instance_serves_nothing() {
        let instance = instance("forgotten");
        let a = cell(&instance, "a");
        let id = origin(&a, 1).effect_identity(Phase::Run, "model", 0);
        landed(vec![shipped(&a, 1, &id, b"1")]);
        forget(&instance);
        assert_eq!(first(&a, 1, "model").kept, None);
    }

    #[test]
    fn a_result_over_the_cap_is_refused_before_anything_is_sent() {
        let a = cell(&instance("capped"), "a");
        let refusal = keep(
            origin(&a, 1),
            "model",
            "id".to_string(),
            vec![0; max_bytes() + 1],
        )
        .unwrap_err();
        assert_eq!(refusal.code, "effect.too_large");
        assert_eq!(refusal.details["limit"], DEFAULT_MAX_BYTES);
    }

    #[test]
    fn a_runner_without_a_control_plane_records_nothing() {
        let a = cell(&instance("unplugged"), "a");
        let refusal = keep(origin(&a, 1), "model", "id".to_string(), b"r".to_vec()).unwrap_err();
        assert_eq!(refusal.code, "effect.not_recorded");
    }

    #[test]
    fn the_first_result_round_trips_through_the_ack() {
        assert_eq!(encode_ack(false, b"x"), "");
        let ack = encode_ack(true, &[0, 1, 0xfe, b'a']);
        assert_eq!(
            decode_hex(ack.strip_prefix(FIRST_PREFIX).unwrap()),
            Some(vec![0, 1, 0xfe, b'a'])
        );
        assert_eq!(decode_hex("abc"), None);
    }
}
