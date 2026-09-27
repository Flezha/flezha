//! The UDS wire: a listening socket, the connections accepted on it, and the
//! `mio` registration that says which of them the poll should report. Every
//! job that is not one of those is [`super::DriverLoop`]'s.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{Receiver, Sender};
use flz_transport::transport::close_reason::CloseReason;
use mio::net::UnixListener;
use mio::{Events, Interest, Poll, Token, Waker};
use rustler::LocalPid;

use super::{
    DriverReady, Ended, Pass, ReadBuf, ServerCommand, StreamState, Streams, Transport, Written,
};
use crate::driver_msg::send_connection_msg;
use crate::driver_stats::DriverStats;

/// The listener answers on the token every wire's own source uses; a stream
/// takes the next free one.
const STREAM_TOKEN_BASE: usize = 2;

/// How the UDS wire addresses one stream: the socket itself, and the poll token
/// that reports it.
pub(crate) struct UdsWire {
    stream: mio::net::UnixStream,
    token: Token,
}

/// Run the UDS server driver. Called from a spawned thread.
pub fn run(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_rx: Receiver<ServerCommand>,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: Sender<std::io::Result<DriverReady>>,
    stats: Arc<DriverStats>,
) {
    let wire = bind(socket_path, acceptor_pid, cmd_tx, &ready_tx);
    super::drive(wire, stats, cmd_rx, "UDS");
}

/// Bind the listening socket, owner-only, and open the poll it answers on.
///
/// The parent directory is created owner-only because directory traversal
/// is the UDS authentication boundary — a local same-uid peer IS the node
/// owner. Best-effort on the parent (a socket placed in a shared dir like
/// /tmp is not ours to chmod); the socket file itself is hardened
/// unconditionally, and the controller's own socket dir is created 0700 here
/// and by `Flezha.Socket.Support.prepare_socket_dir/0`.
fn bind(
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    ready_tx: &Sender<std::io::Result<DriverReady>>,
) -> std::io::Result<Uds> {
    // Clean up stale socket file
    if socket_path.exists() {
        let _ = std::fs::remove_file(&socket_path);
    }

    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    let mut listener = UnixListener::bind(&socket_path)?;

    // Owner-only socket; the 0o700 parent already gates the pre-chmod window.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    }

    let (poll, waker) = super::open_poll(&mut listener, Interest::READABLE, ready_tx)?;

    Ok(Uds {
        socket_path,
        acceptor_pid,
        cmd_tx,
        waker,
        poll,
        listener,
        events: Events::with_capacity(256),
        token_to_stream: HashMap::new(),
        next_stream_id: 1,
        next_token_id: STREAM_TOKEN_BASE,
    })
}

/// The process id of the peer on the other end of `stream`, as the kernel
/// recorded it when the peer connected. It is shown, never trusted: a runner
/// proves its namespace with its credential, not with this.
fn peer_pid(stream: &mio::net::UnixStream) -> Option<u32> {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `ucred` is plain data, and `len` tells the kernel its size.
        let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut len,
            )
        };
        (rc == 0 && cred.pid > 0).then_some(cred.pid as u32)
    }
    #[cfg(target_os = "macos")]
    {
        let mut pid: libc::pid_t = 0;
        let mut len = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
        // SAFETY: `pid` is plain data, and `len` tells the kernel its size.
        let rc = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&mut pid as *mut libc::pid_t).cast(),
                &mut len,
            )
        };
        (rc == 0 && pid > 0).then_some(pid as u32)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = fd;
        None
    }
}

/// The UDS wire.
pub(crate) struct Uds {
    socket_path: PathBuf,
    acceptor_pid: LocalPid,
    cmd_tx: Sender<ServerCommand>,
    waker: Arc<Waker>,
    poll: Poll,
    listener: UnixListener,
    events: Events,
    token_to_stream: HashMap<Token, u64>,
    next_stream_id: u64,
    next_token_id: usize,
}

impl Uds {
    fn accept_connections(&mut self, streams: &mut Streams<UdsWire>) {
        loop {
            match self.listener.accept() {
                Ok((stream, _addr)) => self.register_stream(stream, streams),
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
    fn register_stream(
        &mut self,
        mut stream: mio::net::UnixStream,
        streams: &mut Streams<UdsWire>,
    ) {
        let id = self.next_stream_id;
        self.next_stream_id += 1;
        let pid = peer_pid(&stream);
        let token = Token(self.next_token_id);
        self.next_token_id += 1;

        if let Err(e) = self
            .poll
            .registry()
            .register(&mut stream, token, Interest::READABLE)
        {
            eprintln!("[flz_nif] Failed to register stream: {}", e);
            return;
        }

        let (state, handle) = StreamState::accept(
            id,
            self.acceptor_pid,
            &self.cmd_tx,
            &self.waker,
            UdsWire { stream, token },
            // A UDS peer completed no handshake: its namespace entitlement
            // comes from the credential it presents, never from the socket.
            None,
            pid,
        );
        streams.insert(id, state);
        self.token_to_stream.insert(token, id);

        send_connection_msg(self.acceptor_pid, handle);
    }

    /// Ask the poll for writability only while there is something to write, so
    /// an idle stream does not wake the loop on every pass.
    fn want_writable(&self, state: &mut StreamState<UdsWire>, wanted: bool) {
        let interest = if wanted {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        let _ = self
            .poll
            .registry()
            .reregister(&mut state.wire.stream, state.wire.token, interest);
    }
}

impl Transport for Uds {
    type Wire = UdsWire;

    fn wait(&mut self) {
        let _ = self
            .poll
            .poll(&mut self.events, Some(Duration::from_millis(50)));
    }

    /// Sort one pass's events by what they ask for; the waker's own token means
    /// only that commands are queued.
    fn pass(&mut self, streams: &mut Streams<UdsWire>) -> Pass {
        let mut pass = Pass::default();
        let mut accept_needed = false;
        for event in self.events.iter() {
            match event.token() {
                super::SOURCE_TOKEN => accept_needed = true,
                super::WAKER_TOKEN => {}
                token => {
                    if let Some(&id) = self.token_to_stream.get(&token) {
                        if event.is_readable() {
                            pass.readable.push(id);
                        }
                        if event.is_writable() {
                            pass.writable.push(id);
                        }
                    }
                }
            }
        }
        if accept_needed {
            self.accept_connections(streams);
        }
        pass
    }

    fn read(
        &mut self,
        state: &mut StreamState<UdsWire>,
        into: &mut ReadBuf,
    ) -> Option<CloseReason> {
        match state.wire.stream.read(into.spare()) {
            // EOF — the peer ended the connection.
            Ok(0) => Some(CloseReason::ConnectionClosed),
            Ok(n) => {
                into.commit(n);
                None
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(e) => Some(e.kind().into()),
        }
    }

    fn write(&mut self, state: &mut StreamState<UdsWire>) -> Result<Written, CloseReason> {
        let mut took = 0;
        while !state.write_buffer.is_empty() {
            match state.wire.stream.write(&state.write_buffer) {
                Ok(0) => break,
                Ok(n) => {
                    state.write_buffer.drain(..n);
                    took += n;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    self.want_writable(state, true);
                    return Ok(Written::Took(took));
                }
                Err(_) => return Err(CloseReason::WriteError),
            }
        }
        if state.write_buffer.is_empty() {
            self.want_writable(state, false);
        }
        Ok(Written::Took(took))
    }

    /// A write to a Unix socket has already reached the medium.
    fn flush(&mut self) {}

    fn release(&mut self, state: &mut StreamState<UdsWire>, _ended: Ended) {
        self.token_to_stream.remove(&state.wire.token);
        let _ = self.poll.registry().deregister(&mut state.wire.stream);
        // The socket is dropped with the state, closing the fd.
    }

    fn shutdown(&mut self, streams: &mut Streams<UdsWire>) {
        for state in streams.values_mut() {
            let _ = self.poll.registry().deregister(&mut state.wire.stream);
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

/// What the conformance table drives the UDS half of the contract against.
#[cfg(test)]
pub(super) mod conformance {
    use super::*;

    /// A UDS wire with no listener traffic behind it, holding one already
    /// connected socket as its only stream.
    pub(crate) fn wire(
        driver_end: std::os::unix::net::UnixStream,
        id: u64,
    ) -> (Uds, StreamState<UdsWire>) {
        // The table runs its cases in parallel, so each fixture needs a
        // listener path of its own even though every case uses one stream id.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        driver_end.set_nonblocking(true).unwrap();
        let dir = std::env::temp_dir().join(format!("flz-conformance-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "uds-{}.sock",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);

        let mut listener = UnixListener::bind(&path).unwrap();
        let (poll, waker, cmd_tx, acceptor_pid) =
            super::super::support::detached_poll(&mut listener);

        let mut stream = mio::net::UnixStream::from_std(driver_end);
        let token = Token(STREAM_TOKEN_BASE);
        poll.registry()
            .register(&mut stream, token, Interest::READABLE)
            .unwrap();

        let wire = Uds {
            socket_path: path,
            acceptor_pid,
            cmd_tx,
            waker,
            poll,
            listener,
            events: Events::with_capacity(8),
            token_to_stream: HashMap::from([(token, id)]),
            next_stream_id: id + 1,
            next_token_id: STREAM_TOKEN_BASE + 1,
        };
        let state = super::super::support::detached_stream(id, UdsWire { stream, token });
        (wire, state)
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    // fails when: the peer's process id is not read, so `flz runner list`
    // cannot say which process a runner is.
    #[test]
    fn the_peer_pid_is_the_process_on_the_other_end() {
        let (ours, _theirs) = mio::net::UnixStream::pair().expect("a socket pair");
        assert_eq!(super::peer_pid(&ours), Some(std::process::id()));
    }
}
