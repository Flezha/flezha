use std::collections::HashMap;

use rustler::{Encoder, Env, LocalPid, NifResult, Term};

use crate::atoms;
use crate::resources::StreamHandleArc;
use crate::uds_driver::ServerCommand;

/// Build a binary frame in Rust and send it on the stream.
#[rustler::nif]
fn send_frame<'a>(
    env: Env<'a>,
    stream: StreamHandleArc,
    payload_id: String,
    message: rustler::Binary<'a>,
    metadata: HashMap<String, String>,
) -> NifResult<Term<'a>> {
    if stream.gate.is_raised() {
        return Ok((atoms::error(), atoms::send_buffer_full()).encode(env));
    }

    // The gate weighs what this call is about to queue as well as what the
    // driver already holds, and it is raised HERE rather than on the driver
    // thread. The bit lives on the far side of an unbounded command channel,
    // so a sender reading only what the driver has buffered is refused after
    // it has queued the whole burst — and the ceiling then turns those frames
    // away behind an answer that said they were sent.
    let reserved = message.len();
    stream.gate.queued(reserved);

    let queued = stream.cmd_tx.send(ServerCommand::SendFrame {
        stream_id: stream.stream_id,
        payload_id,
        message: message.as_slice().to_vec(),
        metadata,
    });
    stream.waker.wake();
    Ok(match queued {
        // The command channel is unbounded, so the only way a send fails is
        // that the driver thread is gone — and then the frame is lost. Saying
        // so is the whole of the answer: a discarded result here was a drop
        // with no counter, no log, and `:ok` to the sender. Nothing was
        // queued, so the reservation is discharged here instead of by a
        // driver that will never see it.
        Err(_) => {
            stream.gate.release(reserved);
            (atoms::error(), atoms::driver_gone()).encode(env)
        }
        Ok(()) => atoms::ok().encode(env),
    })
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
