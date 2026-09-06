//! Erlang-term message builders shared by the driver loop and the client.
//! Each sends a `{:flz_*, ...}` tuple to the owning Elixir process.

use std::sync::{Arc, Mutex};

use flz_transport::transport::send_gate::{GateChange, Transition};
use rustler::{Encoder, Env, LocalPid, OwnedEnv, ResourceArc, Term};

use crate::atoms;
use crate::driver::CloseReason;
use crate::frame_buffer::ParsedFrame;
use crate::resources::StreamHandle;

/// Send one `{:flz_*, ...}` tuple to `owner`. Every message below is this
/// shape; only the elements differ.
fn send_tuple(owner: LocalPid, elements: impl for<'a> FnOnce(Env<'a>) -> Vec<Term<'a>>) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        rustler::types::tuple::make_tuple(env, &elements(env))
    });
}

pub(crate) fn send_data_msg(owner: LocalPid, stream_id: u64, frame: ParsedFrame) {
    send_tuple(owner, |env| {
        let metadata_pairs: Vec<(Term, Term)> = frame
            .metadata
            .iter()
            .map(|(k, v)| (k.as_str().encode(env), v.as_str().encode(env)))
            .collect();

        let metadata_term = Term::map_from_pairs(env, &metadata_pairs)
            .unwrap_or_else(|_| rustler::types::atom::nil().encode(env));

        let mut msg_binary = rustler::NewBinary::new(env, frame.message.len());
        msg_binary.as_mut_slice().copy_from_slice(&frame.message);

        vec![
            atoms::flz_data().encode(env),
            stream_id.encode(env),
            frame.payload_id.as_str().encode(env),
            rustler::Binary::from(msg_binary).encode(env),
            metadata_term,
        ]
    });
}

/// Tell a stream's owner it has ended, as `{:flz_closed, stream_id, reason}`.
/// The reason is rendered from the one declared set, so the same wire event
/// reads the same whichever transport observed it.
pub(crate) fn send_closed_msg(owner: LocalPid, stream_id: u64, reason: &CloseReason) {
    let reason_owned = reason.to_string();
    send_tuple(owner, |env| {
        vec![
            atoms::flz_closed().encode(env),
            stream_id.encode(env),
            reason_owned.as_str().encode(env),
        ]
    });
}

/// A peer whose connection died before it ever opened a stream was refused at
/// the handshake (an untrusted or absent client certificate lands here). The
/// acceptor owns the decision of how loud that is, so report it rather than
/// writing to stderr from the driver thread.
pub(crate) fn send_refused_msg(acceptor: LocalPid, reason: &str) {
    let reason_owned = reason.to_string();
    send_tuple(acceptor, |env| {
        vec![
            atoms::flz_refused().encode(env),
            reason_owned.as_str().encode(env),
        ]
    });
}

pub(crate) fn send_connection_msg(acceptor: LocalPid, stream_handle: ResourceArc<StreamHandle>) {
    send_tuple(acceptor, |env| {
        vec![
            atoms::flz_connection().encode(env),
            stream_handle.encode(env),
        ]
    });
}

/// Send `{:flz_stream, stream_handle, type, index, conn_id}` for a QUIC data
/// stream once its type prefix is read. `conn_id` is the control stream's nif
/// id — a stable per-connection routing key the controller uses to bind data
/// streams to the connection that owns the session token.
pub(crate) fn send_stream_msg(
    acceptor: LocalPid,
    stream_handle: ResourceArc<StreamHandle>,
    stream_type: u8,
    index: u8,
    conn_id: u64,
) {
    send_tuple(acceptor, |env| {
        vec![
            atoms::flz_stream().encode(env),
            stream_handle.encode(env),
            (stream_type as u64).encode(env),
            (index as u64).encode(env),
            conn_id.encode(env),
        ]
    });
}

/// Tell a stream's owner that its send gate crossed a watermark, as
/// `{:flz_send_gate, stream_id, raised?}` — `true` means senders must hold,
/// `false` wakes them. It rides the same driver -> owner route as
/// `{:flz_data, ...}` rather than opening a second one. The loop reports
/// here; a depth that moved without crossing says nothing and sends nothing.
pub(crate) fn report_gate(owner: &Arc<Mutex<LocalPid>>, stream_id: u64, change: Transition) {
    let Some(change) = change else {
        return;
    };
    send_tuple(*owner.lock().unwrap(), |env| {
        vec![
            atoms::flz_send_gate().encode(env),
            stream_id.encode(env),
            (change == GateChange::Raised).encode(env),
        ]
    });
}
