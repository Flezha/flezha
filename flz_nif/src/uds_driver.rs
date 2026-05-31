use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use mio::net::UnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use rustler::LocalPid;

use crate::driver_msg::{send_closed_msg, send_connection_msg, send_data_msg};
use crate::frame_buffer::{FrameBuffer, ParsedFrame};
use crate::resources::StreamHandle;

const LISTENER_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);
const STREAM_TOKEN_BASE: usize = 2;

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
) {
    match init_and_run(socket_path, acceptor_pid, cmd_rx, cmd_tx, ready_tx) {
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
) -> std::io::Result<()> {
    // Clean up stale socket file
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let poll = Poll::new()?;

    let mut listener = UnixListener::bind(&socket_path)?;
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
    )
}

fn driver_loop(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    _waker: &Arc<Waker>,
    mut poll: Poll,
    listener: UnixListener,
) -> std::io::Result<()> {
    let mut events = Events::with_capacity(256);
    let mut streams: HashMap<u64, StreamState> = HashMap::new();
    let mut next_stream_id: u64 = 1;
    let mut next_token_id: usize = STREAM_TOKEN_BASE;
    let mut token_to_stream: HashMap<Token, u64> = HashMap::new();
    let mut read_buf = vec![0u8; 65536];

    loop {
        let _ = poll.poll(&mut events, Some(std::time::Duration::from_millis(50)));

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
            flush_write_buffer(stream_id, &mut streams, &mut poll);
        }

        // Read from streams
        for stream_id in readable_streams {
            let closed = read_stream_data(stream_id, &mut streams, &mut read_buf);
            if closed {
                close_stream_internal(stream_id, &mut streams, &mut token_to_stream, &mut poll);
            }
        }

        // Process commands
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                ServerCommand::SendData { stream_id, data } => {
                    enqueue_write(stream_id, data, &mut streams, &mut poll);
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
                    enqueue_write(stream_id, frame, &mut streams, &mut poll);
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
            flush_write_buffer(stream_id, &mut streams, &mut poll);
        }
    }
}

/// Enqueue data for writing to a stream. Attempts immediate flush.
fn enqueue_write(
    stream_id: u64,
    data: Vec<u8>,
    streams: &mut HashMap<u64, StreamState>,
    poll: &mut Poll,
) {
    if let Some(state) = streams.get_mut(&stream_id) {
        state.write_buffer.extend_from_slice(&data);
        // Try to flush immediately
        try_flush(state, poll);
    }
}

/// Attempt to write buffered data to the stream.
/// If WouldBlock, registers for WRITABLE events so we retry later.
fn try_flush(state: &mut StreamState, poll: &mut Poll) {
    while !state.write_buffer.is_empty() {
        match state.stream.write(&state.write_buffer) {
            Ok(0) => break, // Can't make progress
            Ok(n) => {
                state.write_buffer.drain(..n);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Socket buffer full — register for WRITABLE to retry later
                let _ = poll.registry().reregister(
                    &mut state.stream,
                    state.token,
                    Interest::READABLE | Interest::WRITABLE,
                );
                return;
            }
            Err(_) => {
                // Write error — discard buffer, stream is likely broken
                state.write_buffer.clear();
                return;
            }
        }
    }

    // Buffer fully flushed — only need READABLE events now
    if state.write_buffer.is_empty() {
        let _ = poll
            .registry()
            .reregister(&mut state.stream, state.token, Interest::READABLE);
    }
}

/// Flush write buffer for a specific stream by ID.
fn flush_write_buffer(stream_id: u64, streams: &mut HashMap<u64, StreamState>, poll: &mut Poll) {
    if let Some(state) = streams.get_mut(&stream_id) {
        try_flush(state, poll);
    }
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
