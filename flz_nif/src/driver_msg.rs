//! Erlang-term message builders shared by the UDS and QUIC drivers.
//! Each sends a `{:flz_*, ...}` tuple to the owning Elixir process.

use rustler::{Encoder, LocalPid, OwnedEnv, ResourceArc, Term};

use crate::atoms;
use crate::frame_buffer::ParsedFrame;
use crate::resources::StreamHandle;

pub(crate) fn send_data_msg(owner: LocalPid, stream_id: u64, frame: ParsedFrame) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        let metadata_pairs: Vec<(Term, Term)> = frame
            .metadata
            .iter()
            .map(|(k, v)| (k.as_str().encode(env), v.as_str().encode(env)))
            .collect();

        let metadata_term = Term::map_from_pairs(env, &metadata_pairs)
            .unwrap_or_else(|_| rustler::types::atom::nil().encode(env));

        let mut msg_binary = rustler::NewBinary::new(env, frame.message.len());
        msg_binary.as_mut_slice().copy_from_slice(&frame.message);

        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_data().encode(env),
                stream_id.encode(env),
                frame.payload_id.as_str().encode(env),
                rustler::Binary::from(msg_binary).encode(env),
                metadata_term,
            ],
        )
    });
}

pub(crate) fn send_closed_msg(owner: LocalPid, stream_id: u64, reason: &str) {
    let reason_owned = reason.to_string();
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&owner, |env| {
        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_closed().encode(env),
                stream_id.encode(env),
                reason_owned.as_str().encode(env),
            ],
        )
    });
}

pub(crate) fn send_connection_msg(acceptor: LocalPid, stream_handle: ResourceArc<StreamHandle>) {
    let mut env = OwnedEnv::new();
    let _ = env.send_and_clear(&acceptor, |env| {
        rustler::types::tuple::make_tuple(
            env,
            &[
                atoms::flz_connection().encode(env),
                stream_handle.encode(env),
            ],
        )
    });
}
