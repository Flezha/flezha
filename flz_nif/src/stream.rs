use std::collections::HashMap;

use rustler::{Encoder, Env, LocalPid, NifResult, Term};

use crate::atoms;
use crate::resources::StreamHandleArc;
use crate::uds_driver::ServerCommand;

/// Send raw binary data on a stream.
#[rustler::nif]
fn send_data<'a>(
    env: Env<'a>,
    stream: StreamHandleArc,
    data: rustler::Binary<'a>,
) -> NifResult<Term<'a>> {
    if let Some(refusal) = refusal(env, &stream) {
        return Ok(refusal);
    }
    let _ = stream.cmd_tx.send(ServerCommand::SendData {
        stream_id: stream.stream_id,
        data: data.as_slice().to_vec(),
    });
    stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Build a binary frame in Rust and send it on the stream.
#[rustler::nif]
fn send_frame<'a>(
    env: Env<'a>,
    stream: StreamHandleArc,
    payload_id: String,
    message: rustler::Binary<'a>,
    metadata: HashMap<String, String>,
) -> NifResult<Term<'a>> {
    if let Some(refusal) = refusal(env, &stream) {
        return Ok(refusal);
    }
    let _ = stream.cmd_tx.send(ServerCommand::SendFrame {
        stream_id: stream.stream_id,
        payload_id,
        message: message.as_slice().to_vec(),
        metadata,
    });
    stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// The gate read every outbound frame pays: one relaxed atomic load, on a
/// crossing the sender already makes. A refused frame never leaves the caller,
/// and the caller is the one told.
fn refusal<'a>(env: Env<'a>, stream: &StreamHandleArc) -> Option<Term<'a>> {
    if !stream.gate.is_raised() {
        return None;
    }
    stream.gate.refuse();
    Some((atoms::error(), atoms::send_buffer_full()).encode(env))
}

/// Enable active mode: the driver delivers incoming frames as Erlang messages.
#[rustler::nif]
fn set_active(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::SetActive {
        stream_id: stream.stream_id,
    });
    stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Disable active mode: the driver buffers incoming frames.
#[rustler::nif]
fn set_passive(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::SetPassive {
        stream_id: stream.stream_id,
    });
    stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Close a stream.
#[rustler::nif]
fn close(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::CloseStream {
        stream_id: stream.stream_id,
    });
    stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Return the driver-assigned stream id for this handle. Matches the id sent in
/// `{:flz_data, stream_id, ...}` / `{:flz_closed, stream_id, ...}`, letting the
/// owner correlate a freshly accepted `{:flz_connection, handle}` with the
/// frames and close events that later arrive for the same stream.
#[rustler::nif]
fn stream_id(stream: StreamHandleArc) -> u64 {
    stream.stream_id
}

/// Transfer ownership of a stream to a different Elixir process.
#[rustler::nif]
fn controlling_process(env: Env, stream: StreamHandleArc, new_owner: LocalPid) -> NifResult<Term> {
    *stream.owner.lock().unwrap() = new_owner;

    let _ = stream.cmd_tx.send(ServerCommand::UpdateOwner {
        stream_id: stream.stream_id,
        new_owner,
    });
    stream.waker.wake();

    Ok(atoms::ok().encode(env))
}
