use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use mio::net::UnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use rustler::LocalPid;

use crate::driver_msg::{send_closed_msg, send_connection_msg, send_data_msg};
use crate::driver_stats::{DriverStats, StreamSnapshot};
use crate::frame_buffer::{FrameBuffer, ParsedFrame};
use crate::resources::StreamHandle;

const LISTENER_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);
const STREAM_TOKEN_BASE: usize = 2;

/// Default per-stream send-buffer cap (8 MiB) — a few seconds of peak per-stream
/// throughput. Override with `FLEZHA_MAX_WRITE_BUFFER_BYTES`.
const DEFAULT_MAX_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024;

fn max_write_buffer_bytes() -> usize {
    std::env::var("FLEZHA_MAX_WRITE_BUFFER_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_BYTES)
}

/// Commands sent from NIF functions to the driver thread.
pub enum ServerCommand {
    SendData {
        stream_id: u64,
        data: Vec<u8>,
    },
    SendFrame {
        stream_id: u64,
        payload_id: String,
        message: Vec<u8>,
        metadata: HashMap<String, String>,
    },
    SetActive {
        stream_id: u64,
    },
    SetPassive {
        stream_id: u64,
    },
    CloseStream {
        stream_id: u64,
    },
    UpdateOwner {
        stream_id: u64,
        new_owner: LocalPid,
    },
    Shutdown,
}

struct StreamState {
    stream: mio::net::UnixStream,
    token: Token,
    owner: Arc<Mutex<LocalPid>>,
    frame_buffer: FrameBuffer,
    active: bool,
    passive_queue: Vec<ParsedFrame>,
    write_buffer: Vec<u8>,
    // Hard cap on the per-stream send buffer: past it `enqueue_write` drops the
    // newest frame so a slow/non-reading runner cannot grow controller memory
    // without bound (mirrors the SDK bounded-mailbox DropNewest).
    max_write_buffer_bytes: usize,
    // Liveness counters (single-threaded driver, no contention). Copied into
    // the DriverStats snapshot once per loop iteration.
    write_buffer_dropped: u64,
    frames_written: u64,
    frames_read: u64,
    last_write_ms: u64,
    last_read_ms: u64,
}

/// Start result passed back from driver thread to NIF via channel.
pub struct DriverReady {
    pub waker: Arc<Waker>,
}

/// Run the UDS server driver loop. Called from a spawned thread.
///
/// Creates its own Poll + Waker, sends the Waker back to the NIF via `ready_tx`,
/// then enters the event loop.
pub fn run_uds_driver(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<std::io::Result<DriverReady>>,
    stats: Arc<DriverStats>,
) {
    match init_and_run(socket_path, acceptor_pid, cmd_rx, cmd_tx, ready_tx, stats) {
        Ok(()) => {}
        Err(e) => eprintln!("[flz_nif] UDS driver fatal error: {}", e),
    }
}

fn init_and_run(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<std::io::Result<DriverReady>>,
    stats: Arc<DriverStats>,
) -> std::io::Result<()> {
    // Clean up stale socket file
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    // Ensure parent directory exists, owner-only: directory traversal is the
    // UDS authentication boundary — a local same-uid peer IS the node owner.
    // Best-effort on the parent (a socket placed in a shared dir like /tmp is
    // not ours to chmod); the socket file itself is hardened unconditionally
    // below, and the controller's own socket dir is created 0700 here and by
    // `Flezha.Socket.Support.prepare_socket_dir/0`.
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    let poll = Poll::new()?;

    let mut listener = UnixListener::bind(&socket_path)?;

    // Owner-only socket; the 0o700 parent already gates the pre-chmod window.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    }
    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

    // Signal ready to the NIF
    let _ = ready_tx.send(Ok(DriverReady {
        waker: Arc::clone(&waker),
    }));

    driver_loop(
        socket_path,
        acceptor_pid,
        cmd_rx,
        cmd_tx,
        &waker,
        poll,
        listener,
        stats,
    )
}

// reason: the loop owns the listener, command channel, waker, poll registry and
// the shared stats handle; bundling them into a struct would not reduce the
// genuine state the loop must thread.
#[allow(clippy::too_many_arguments)]
fn driver_loop(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    _waker: &Arc<Waker>,
    mut poll: Poll,
    listener: UnixListener,
    stats: Arc<DriverStats>,
) -> std::io::Result<()> {
    let mut events = Events::with_capacity(256);
    let mut streams: HashMap<u64, StreamState> = HashMap::new();
    let mut next_stream_id: u64 = 1;
    let mut next_token_id: usize = STREAM_TOKEN_BASE;
    let mut token_to_stream: HashMap<Token, u64> = HashMap::new();
    let mut read_buf = vec![0u8; 65536];

    loop {
        let _ = poll.poll(&mut events, Some(std::time::Duration::from_millis(50)));

        // Liveness heartbeat: bump once per iteration before any work, so a
        // frozen counter (with a stale last_loop_ms) pinpoints a wedged thread.
        let now = stats.now_ms();
        stats.mark_loop(now, cmd_rx.len() as u64);

        // Collect events that need stream reads (to avoid borrow issues)
        let mut readable_streams: Vec<u64> = Vec::new();
        let mut writable_streams: Vec<u64> = Vec::new();
        let mut accept_needed = false;

        for event in events.iter() {
            match event.token() {
                LISTENER_TOKEN => {
                    accept_needed = true;
                }
                WAKER_TOKEN => {
                    // Commands handled below
                }
                token => {
                    if let Some(&stream_id) = token_to_stream.get(&token) {
                        if event.is_readable() {
                            readable_streams.push(stream_id);
                        }
                        if event.is_writable() {
                            writable_streams.push(stream_id);
                        }
                    }
                }
            }
        }

        // Accept new connections
        if accept_needed {
            accept_connections(
                &listener,
                &mut poll,
                &mut streams,
                &mut token_to_stream,
                &mut next_stream_id,
                &mut next_token_id,
                acceptor_pid,
                &cmd_tx,
                _waker,
            );
        }

        // Flush writable streams (backpressure relief)
        for stream_id in writable_streams {
            if flush_write_buffer(stream_id, &mut streams, &mut poll, now) {
                if let Some(state) = streams.get(&stream_id) {
                    let owner = *state.owner.lock().unwrap();
                    send_closed_msg(owner, stream_id, "write_error");
                }
                close_stream_internal(stream_id, &mut streams, &mut token_to_stream, &mut poll);
            }
        }

        // Read from streams
        for stream_id in readable_streams {
            let closed = read_stream_data(stream_id, &mut streams, &mut read_buf, now);
            if closed {
                close_stream_internal(stream_id, &mut streams, &mut token_to_stream, &mut poll);
            }
        }

        // Process commands
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                ServerCommand::SendData { stream_id, data } => {
                    if enqueue_write(stream_id, data, &mut streams, &mut poll, now) {
                        if let Some(state) = streams.get(&stream_id) {
                            let owner = *state.owner.lock().unwrap();
                            send_closed_msg(owner, stream_id, "write_error");
                        }
                        close_stream_internal(
                            stream_id,
                            &mut streams,
                            &mut token_to_stream,
                            &mut poll,
                        );
                    }
                }
                ServerCommand::SendFrame {
                    stream_id,
                    payload_id,
                    message,
                    metadata,
                } => {
                    let frame = flz_transport::service::payloader::build_from_binary(
                        &payload_id,
                        &message,
                        metadata,
                    );
                    if enqueue_write(stream_id, frame, &mut streams, &mut poll, now) {
                        if let Some(state) = streams.get(&stream_id) {
                            let owner = *state.owner.lock().unwrap();
                            send_closed_msg(owner, stream_id, "write_error");
                        }
                        close_stream_internal(
                            stream_id,
                            &mut streams,
                            &mut token_to_stream,
                            &mut poll,
                        );
                    }
                }
                ServerCommand::SetActive { stream_id } => {
                    if let Some(state) = streams.get_mut(&stream_id) {
                        state.active = true;
                        let owner = *state.owner.lock().unwrap();
                        let queued: Vec<_> = state.passive_queue.drain(..).collect();
                        for frame in queued {
                            send_data_msg(owner, stream_id, frame);
                        }
                    }
                }
                ServerCommand::SetPassive { stream_id } => {
                    if let Some(state) = streams.get_mut(&stream_id) {
                        state.active = false;
                    }
                }
                ServerCommand::CloseStream { stream_id } => {
                    close_stream_internal(stream_id, &mut streams, &mut token_to_stream, &mut poll);
                }
                ServerCommand::UpdateOwner {
                    stream_id,
                    new_owner,
                } => {
                    if let Some(state) = streams.get_mut(&stream_id) {
                        *state.owner.lock().unwrap() = new_owner;
                    }
                }
                ServerCommand::Shutdown => {
                    for (_, mut state) in streams.drain() {
                        let _ = poll.registry().deregister(&mut state.stream);
                    }
                    let _ = std::fs::remove_file(&socket_path);
                    return Ok(());
                }
            }
        }

        // After processing all commands, try to flush pending writes
        let ids_with_data: Vec<u64> = streams
            .iter()
            .filter(|(_, s)| !s.write_buffer.is_empty())
            .map(|(&id, _)| id)
            .collect();
        for stream_id in ids_with_data {
            if flush_write_buffer(stream_id, &mut streams, &mut poll, now) {
                if let Some(state) = streams.get(&stream_id) {
                    let owner = *state.owner.lock().unwrap();
                    send_closed_msg(owner, stream_id, "write_error");
                }
                close_stream_internal(stream_id, &mut streams, &mut token_to_stream, &mut poll);
            }
        }

        // Rebuild the per-stream liveness snapshot once per iteration (off the
        // per-frame path). A frozen loop means this never refreshes — matching
        // the stale last_loop_ms that flags the wedge.
        let snap: HashMap<u64, StreamSnapshot> = streams
            .iter()
            .map(|(&id, s)| {
                (
                    id,
                    StreamSnapshot {
                        write_buffer_len: s.write_buffer.len() as u64,
                        write_buffer_dropped: s.write_buffer_dropped,
                        frames_written: s.frames_written,
                        frames_read: s.frames_read,
                        last_write_ms: s.last_write_ms,
                        last_read_ms: s.last_read_ms,
                    },
                )
            })
            .collect();
        stats.set_streams(snap);
    }
}

/// Enqueue data for writing to a stream. Attempts immediate flush.
/// Returns true if the stream is broken (write error).
fn enqueue_write(
    stream_id: u64,
    data: Vec<u8>,
    streams: &mut HashMap<u64, StreamState>,
    poll: &mut Poll,
    now: u64,
) -> bool {
    if let Some(state) = streams.get_mut(&stream_id) {
        if would_overflow_write_buffer(
            state.write_buffer.len(),
            data.len(),
            state.max_write_buffer_bytes,
        ) {
            // DropNewest: the peer is not draining; drop rather than grow the
            // buffer without bound. Counted, not silent.
            state.write_buffer_dropped += 1;
            return false;
        }

        state.frames_written += 1;
        state.write_buffer.extend_from_slice(&data);
        return try_flush(state, poll, now);
    }
    false
}

/// Whether enqueuing `incoming` bytes onto a `buffered`-byte send buffer would
/// exceed `cap`. An empty buffer always accepts (so a single oversized frame on
/// an idle stream is never dropped); once non-empty, the buffer stays at or
/// below `cap`.
fn would_overflow_write_buffer(buffered: usize, incoming: usize, cap: usize) -> bool {
    buffered != 0 && buffered + incoming > cap
}

/// Attempt to write buffered data to the stream.
/// If WouldBlock, registers for WRITABLE events so we retry later.
/// Returns true if the stream is broken (write error).
fn try_flush(state: &mut StreamState, poll: &mut Poll, now: u64) -> bool {
    while !state.write_buffer.is_empty() {
        match state.stream.write(&state.write_buffer) {
            Ok(0) => break,
            Ok(n) => {
                state.write_buffer.drain(..n);
                state.last_write_ms = now;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                let _ = poll.registry().reregister(
                    &mut state.stream,
                    state.token,
                    Interest::READABLE | Interest::WRITABLE,
                );
                return false;
            }
            Err(_) => {
                state.write_buffer.clear();
                return true;
            }
        }
    }

    if state.write_buffer.is_empty() {
        let _ = poll
            .registry()
            .reregister(&mut state.stream, state.token, Interest::READABLE);
    }
    false
}

/// Flush write buffer for a specific stream by ID.
/// Returns true if the stream is broken (write error).
fn flush_write_buffer(
    stream_id: u64,
    streams: &mut HashMap<u64, StreamState>,
    poll: &mut Poll,
    now: u64,
) -> bool {
    if let Some(state) = streams.get_mut(&stream_id) {
        return try_flush(state, poll, now);
    }
    false
}

// reason: accept loop needs the listener plus all per-connection bookkeeping (stream/token maps, id counters, acceptor pid, mio waker); splitting would just bundle them into an inline struct
#[allow(clippy::too_many_arguments)]
fn accept_connections(
    listener: &UnixListener,
    poll: &mut Poll,
    streams: &mut HashMap<u64, StreamState>,
    token_to_stream: &mut HashMap<Token, u64>,
    next_stream_id: &mut u64,
    next_token_id: &mut usize,
    acceptor_pid: LocalPid,
    cmd_tx: &Sender<ServerCommand>,
    waker: &Arc<Waker>,
) {
    loop {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                let stream_id = *next_stream_id;
                *next_stream_id += 1;
                let token = Token(*next_token_id);
                *next_token_id += 1;

                let owner = Arc::new(Mutex::new(acceptor_pid));

                if let Err(e) = poll
                    .registry()
                    .register(&mut stream, token, Interest::READABLE)
                {
                    eprintln!("[flz_nif] Failed to register stream: {}", e);
                    continue;
                }

                let stream_handle = rustler::ResourceArc::new(StreamHandle {
                    stream_id,
                    cmd_tx: cmd_tx.clone(),
                    waker: Arc::clone(waker),
                    owner: Arc::clone(&owner),
                });

                streams.insert(
                    stream_id,
                    StreamState {
                        stream,
                        token,
                        owner,
                        frame_buffer: FrameBuffer::new(),
                        active: false,
                        passive_queue: Vec::new(),
                        write_buffer: Vec::new(),
                        max_write_buffer_bytes: max_write_buffer_bytes(),
                        write_buffer_dropped: 0,
                        frames_written: 0,
                        frames_read: 0,
                        last_write_ms: 0,
                        last_read_ms: 0,
                    },
                );
                token_to_stream.insert(token, stream_id);

                send_connection_msg(acceptor_pid, stream_handle);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => {
                eprintln!("[flz_nif] Accept error: {}", e);
                break;
            }
        }
    }
}

fn close_stream_internal(
    stream_id: u64,
    streams: &mut HashMap<u64, StreamState>,
    token_to_stream: &mut HashMap<Token, u64>,
    poll: &mut Poll,
) {
    if let Some(mut state) = streams.remove(&stream_id) {
        token_to_stream.remove(&state.token);
        let _ = poll.registry().deregister(&mut state.stream);
        // Stream is dropped here, closing the fd
    }
}

/// Read available data from a stream, feed into FrameBuffer, deliver frames.
/// Returns true if the stream is closed.
fn read_stream_data(
    stream_id: u64,
    streams: &mut HashMap<u64, StreamState>,
    read_buf: &mut [u8],
    now: u64,
) -> bool {
    let state = match streams.get_mut(&stream_id) {
        Some(s) => s,
        None => return false,
    };

    loop {
        match state.stream.read(read_buf) {
            Ok(0) => {
                // EOF — connection closed
                let owner = *state.owner.lock().unwrap();
                send_closed_msg(owner, stream_id, "connection_closed");
                return true;
            }
            Ok(n) => {
                let frames = state.frame_buffer.feed(&read_buf[..n]);
                let owner = *state.owner.lock().unwrap();
                state.last_read_ms = now;
                state.frames_read += frames.len() as u64;

                if state.active {
                    for frame in frames {
                        send_data_msg(owner, stream_id, frame);
                    }
                } else {
                    state.passive_queue.extend(frames);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return false,
            Err(_) => {
                let owner = *state.owner.lock().unwrap();
                send_closed_msg(owner, stream_id, "read_error");
                return true;
            }
        }
    }
}

// send_data_msg / send_closed_msg / send_connection_msg live in
// crate::driver_msg (shared with the QUIC driver).

#[cfg(test)]
mod tests {
    use super::*;

    // R2d: without the cap, `enqueue_write` appends to `write_buffer` on every
    // `WouldBlock` and it grows without bound. The cap predicate bounds it:
    // DropNewest once the buffer is non-empty and the frame would exceed the cap.
    #[test]
    fn write_buffer_cap_drops_newest_past_bound() {
        let cap = 1024;

        // Empty buffer always accepts (a single oversized frame on an idle stream
        // is never dropped).
        assert!(!would_overflow_write_buffer(0, 4096, cap));

        // Filling exactly to the cap is allowed; one byte past it is dropped.
        assert!(!would_overflow_write_buffer(512, 512, cap));
        assert!(would_overflow_write_buffer(512, 513, cap));

        // Already at the cap: every further frame is dropped — workflow, not 1:1.
        assert!(would_overflow_write_buffer(cap, 1, cap));
        assert!(would_overflow_write_buffer(cap, 4096, cap));
    }

    #[test]
    fn max_write_buffer_bytes_defaults_when_unset() {
        // No env override in the test process → the documented default.
        assert_eq!(max_write_buffer_bytes(), DEFAULT_MAX_WRITE_BUFFER_BYTES);
    }
}
