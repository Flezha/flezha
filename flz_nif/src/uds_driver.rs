use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::config::{max_write_buffer_bytes, would_overflow_write_buffer};
use mio::net::UnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use rustler::LocalPid;

use crate::driver_msg::{send_closed_msg, send_connection_msg};
use crate::driver_stats::{DriverStats, StreamSnapshot};
use crate::frame_buffer::{FrameBuffer, Inbox};
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
    inbox: Inbox,
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
    let outcome = bind_listener(&socket_path, &ready_tx).and_then(|(poll, listener, waker)| {
        UdsDriver {
            socket_path,
            acceptor_pid,
            cmd_tx,
            waker,
            poll,
            listener,
            stats,
            streams: HashMap::new(),
            token_to_stream: HashMap::new(),
            next_stream_id: 1,
            next_token_id: STREAM_TOKEN_BASE,
        }
        .run(cmd_rx)
    });
    if let Err(e) = outcome {
        eprintln!("[flz_nif] UDS driver fatal error: {}", e);
    }
}

/// Bind the listening socket, owner-only, and hand the NIF its waker.
///
/// The parent directory is created owner-only because directory traversal
/// is the UDS authentication boundary — a local same-uid peer IS the node
/// owner. Best-effort on the parent (a socket placed in a shared dir like
/// /tmp is not ours to chmod); the socket file itself is hardened
/// unconditionally, and the controller's own socket dir is created 0700 here
/// and by `Flezha.Socket.Support.prepare_socket_dir/0`.
fn bind_listener(
    socket_path: &PathBuf,
    ready_tx: &Sender<std::io::Result<DriverReady>>,
) -> std::io::Result<(Poll, UnixListener, Arc<Waker>)> {
    // Clean up stale socket file
    if socket_path.exists() {
        let _ = std::fs::remove_file(socket_path);
    }

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    let poll = Poll::new()?;

    let mut listener = UnixListener::bind(socket_path)?;

    // Owner-only socket; the 0o700 parent already gates the pre-chmod window.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    }
    poll.registry()
        .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

    // Signal ready to the NIF
    let _ = ready_tx.send(Ok(DriverReady {
        waker: Arc::clone(&waker),
    }));

    Ok((poll, listener, waker))
}

/// What one poll pass found: whether the listener is readable, and the
/// streams that are.
struct PassEvents {
    accept_needed: bool,
    readable: Vec<u64>,
    writable: Vec<u64>,
}

/// The UDS server driver: the listener, every accepted stream, and the
/// bookkeeping the loop threads through them.
struct UdsDriver {
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    waker: Arc<Waker>,
    poll: Poll,
    listener: UnixListener,
    stats: Arc<DriverStats>,
    streams: HashMap<u64, StreamState>,
    token_to_stream: HashMap<Token, u64>,
    next_stream_id: u64,
    next_token_id: usize,
}

impl UdsDriver {
    fn run(mut self, cmd_rx: Receiver<ServerCommand>) -> std::io::Result<()> {
        let mut events = Events::with_capacity(256);
        let mut read_buf = vec![0u8; 65536];

        loop {
            let _ = self
                .poll
                .poll(&mut events, Some(std::time::Duration::from_millis(50)));

            // Liveness heartbeat: bump once per iteration before any work, so a
            // frozen counter (with a stale last_loop_ms) pinpoints a wedged thread.
            let now_ms = self.stats.now_ms();
            self.stats.mark_loop(now_ms, cmd_rx.len() as u64);

            let pass = self.collect_events(&events);
            if pass.accept_needed {
                self.accept_connections();
            }
            // Flush writable streams (backpressure relief)
            self.flush_dirty(pass.writable, now_ms);
            for stream_id in pass.readable {
                if self.read_stream_data(stream_id, &mut read_buf, now_ms) {
                    self.close_stream(stream_id);
                }
            }
            while let Ok(cmd) = cmd_rx.try_recv() {
                if self.handle_command(cmd, now_ms) {
                    return Ok(());
                }
            }
            // After processing all commands, try to flush pending writes
            let dirty: Vec<u64> = self
                .streams
                .iter()
                .filter(|(_, s)| !s.write_buffer.is_empty())
                .map(|(&id, _)| id)
                .collect();
            self.flush_dirty(dirty, now_ms);
            let streams = self.snapshot_streams();
            self.stats.set_streams(streams);
        }
    }

    /// Sort one pass's events by what they ask for; the waker's own token
    /// means only that commands are queued.
    fn collect_events(&self, events: &Events) -> PassEvents {
        let mut pass = PassEvents {
            accept_needed: false,
            readable: Vec::new(),
            writable: Vec::new(),
        };
        for event in events.iter() {
            match event.token() {
                LISTENER_TOKEN => pass.accept_needed = true,
                WAKER_TOKEN => {}
                token => {
                    if let Some(&stream_id) = self.token_to_stream.get(&token) {
                        if event.is_readable() {
                            pass.readable.push(stream_id);
                        }
                        if event.is_writable() {
                            pass.writable.push(stream_id);
                        }
                    }
                }
            }
        }
        pass
    }

    /// One NIF command. `true` means the driver was asked to shut down: every
    /// stream is deregistered and the socket file removed.
    fn handle_command(&mut self, cmd: ServerCommand, now_ms: u64) -> bool {
        match cmd {
            ServerCommand::SendData { stream_id, data } => {
                self.enqueue_or_close(stream_id, data, now_ms)
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
                self.enqueue_or_close(stream_id, frame, now_ms);
            }
            ServerCommand::SetActive { stream_id } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    let owner = *state.owner.lock().unwrap();
                    state.inbox.activate(owner, stream_id);
                }
            }
            ServerCommand::SetPassive { stream_id } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    state.inbox.deactivate();
                }
            }
            ServerCommand::CloseStream { stream_id } => self.close_stream(stream_id),
            ServerCommand::UpdateOwner {
                stream_id,
                new_owner,
            } => {
                if let Some(state) = self.streams.get_mut(&stream_id) {
                    *state.owner.lock().unwrap() = new_owner;
                }
            }
            ServerCommand::Shutdown => {
                for (_, mut state) in self.streams.drain() {
                    let _ = self.poll.registry().deregister(&mut state.stream);
                }
                let _ = std::fs::remove_file(&self.socket_path);
                return true;
            }
        }
        false
    }

    /// Queue `data` on a stream and flush what it will take now; a broken
    /// stream is reported to its owner and closed.
    fn enqueue_or_close(&mut self, stream_id: u64, data: Vec<u8>, now_ms: u64) {
        if enqueue_write(stream_id, data, &mut self.streams, &mut self.poll, now_ms) {
            self.report_and_close(stream_id, "write_error");
        }
    }

    /// Flush the buffered writes of `stream_ids`; a broken stream is reported
    /// to its owner and closed.
    fn flush_dirty(&mut self, stream_ids: Vec<u64>, now_ms: u64) {
        for stream_id in stream_ids {
            if flush_write_buffer(stream_id, &mut self.streams, &mut self.poll, now_ms) {
                self.report_and_close(stream_id, "write_error");
            }
        }
    }

    fn report_and_close(&mut self, stream_id: u64, reason: &str) {
        if let Some(state) = self.streams.get(&stream_id) {
            let owner = *state.owner.lock().unwrap();
            send_closed_msg(owner, stream_id, reason);
        }
        self.close_stream(stream_id);
    }

    /// The per-stream liveness snapshot, rebuilt once per iteration (off the
    /// per-frame path). A frozen loop means this never refreshes — matching
    /// the stale last_loop_ms that flags the wedge.
    fn snapshot_streams(&self) -> HashMap<u64, StreamSnapshot> {
        self.streams
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
            .collect()
    }

    fn accept_connections(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _addr)) => self.register_stream(stream),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("[flz_nif] Accept error: {}", e);
                    break;
                }
            }
        }
    }

    /// Register one accepted connection with the poll and hand its handle to
    /// the acceptor.
    fn register_stream(&mut self, mut stream: mio::net::UnixStream) {
        let stream_id = self.next_stream_id;
        self.next_stream_id += 1;
        let token = Token(self.next_token_id);
        self.next_token_id += 1;

        let owner = Arc::new(Mutex::new(self.acceptor_pid));

        if let Err(e) = self
            .poll
            .registry()
            .register(&mut stream, token, Interest::READABLE)
        {
            eprintln!("[flz_nif] Failed to register stream: {}", e);
            return;
        }

        let stream_handle = rustler::ResourceArc::new(StreamHandle {
            stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: crate::resources::StreamWaker::Mio(Arc::clone(&self.waker)),
            owner: Arc::clone(&owner),
        });

        self.streams.insert(
            stream_id,
            StreamState {
                stream,
                token,
                owner,
                frame_buffer: FrameBuffer::new(),
                inbox: Inbox::new(),
                write_buffer: Vec::new(),
                max_write_buffer_bytes: max_write_buffer_bytes(),
                write_buffer_dropped: 0,
                frames_written: 0,
                frames_read: 0,
                last_write_ms: 0,
                last_read_ms: 0,
            },
        );
        self.token_to_stream.insert(token, stream_id);

        send_connection_msg(self.acceptor_pid, stream_handle);
    }

    fn close_stream(&mut self, stream_id: u64) {
        if let Some(mut state) = self.streams.remove(&stream_id) {
            self.token_to_stream.remove(&state.token);
            let _ = self.poll.registry().deregister(&mut state.stream);
            // Stream is dropped here, closing the fd
        }
    }

    /// Read available data from a stream, feed into FrameBuffer, deliver frames.
    /// Returns true if the stream is closed.
    fn read_stream_data(&mut self, stream_id: u64, read_buf: &mut [u8], now_ms: u64) -> bool {
        let state = match self.streams.get_mut(&stream_id) {
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
                    let frames = match state.frame_buffer.feed(&read_buf[..n]) {
                        Ok(frames) => frames,
                        Err(e) => {
                            eprintln!(
                                "[flz_nif] UDS frame too large on stream {}: {}",
                                stream_id, e
                            );
                            let owner = *state.owner.lock().unwrap();
                            send_closed_msg(owner, stream_id, "frame_too_large");
                            return true;
                        }
                    };
                    let owner = *state.owner.lock().unwrap();
                    state.last_read_ms = now_ms;
                    state.frames_read += frames.len() as u64;
                    state.inbox.deliver(owner, stream_id, frames);
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
}

/// Enqueue data for writing to a stream. Attempts immediate flush.
/// Returns true if the stream is broken (write error).
fn enqueue_write(
    stream_id: u64,
    data: Vec<u8>,
    streams: &mut HashMap<u64, StreamState>,
    poll: &mut Poll,
    now_ms: u64,
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
        return flush_pending(state, poll, now_ms);
    }
    false
}

/// Attempt to write buffered data to the stream.
/// If WouldBlock, registers for WRITABLE events so we retry later.
/// Returns true if the stream is broken (write error).
fn flush_pending(state: &mut StreamState, poll: &mut Poll, now_ms: u64) -> bool {
    while !state.write_buffer.is_empty() {
        match state.stream.write(&state.write_buffer) {
            Ok(0) => break,
            Ok(n) => {
                state.write_buffer.drain(..n);
                state.last_write_ms = now_ms;
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
    now_ms: u64,
) -> bool {
    if let Some(state) = streams.get_mut(&stream_id) {
        return flush_pending(state, poll, now_ms);
    }
    false
}
