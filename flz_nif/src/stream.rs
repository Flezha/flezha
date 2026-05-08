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
    let _ = stream.cmd_tx.send(ServerCommand::SendData {
        stream_id: stream.stream_id,
        data: data.as_slice().to_vec(),
    });
    let _ = stream.waker.wake();
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
    let _ = stream.cmd_tx.send(ServerCommand::SendFrame {
        stream_id: stream.stream_id,
        payload_id,
        message: message.as_slice().to_vec(),
        metadata,
    });
    let _ = stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Enable active mode: the driver delivers incoming frames as Erlang messages.
#[rustler::nif]
fn set_active(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::SetActive {
        stream_id: stream.stream_id,
    });
    let _ = stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Disable active mode: the driver buffers incoming frames.
#[rustler::nif]
fn set_passive(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::SetPassive {
        stream_id: stream.stream_id,
    });
    let _ = stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Close a stream.
#[rustler::nif]
fn close(env: Env, stream: StreamHandleArc) -> NifResult<Term> {
    let _ = stream.cmd_tx.send(ServerCommand::CloseStream {
        stream_id: stream.stream_id,
    });
    let _ = stream.waker.wake();
    Ok(atoms::ok().encode(env))
}

/// Transfer ownership of a stream to a different Elixir process.
#[rustler::nif]
fn controlling_process(env: Env, stream: StreamHandleArc, new_owner: LocalPid) -> NifResult<Term> {
    *stream.owner.lock().unwrap() = new_owner;

    let _ = stream.cmd_tx.send(ServerCommand::UpdateOwner {
        stream_id: stream.stream_id,
        new_owner,
    });
    let _ = stream.waker.wake();

    Ok(atoms::ok().encode(env))
}
