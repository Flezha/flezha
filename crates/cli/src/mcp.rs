//! `flz mcp` — the MCP server an AI assistant starts, on stdin and stdout.
//!
//! It holds no MCP of its own. Each line is one JSON-RPC message, answered by
//! `Flezha.Mcp.Stdio` the way every verb is answered: on the controller through
//! `flzvm rpc`, as the caller's token, or, while no controller can be reached,
//! in a VM of its own through `flzvm eval`, which serves the knowledge tools and
//! refuses the operations. Each line asks again, so a controller started later
//! is found without restarting the assistant.
//!
//! Messages are answered one at a time, in order, so one assistant never runs
//! more than one VM against the controller. Two exceptions are heard while one
//! runs: a `ping`, answered beside it in a VM of its own, since its answer never
//! needs a controller; and `notifications/cancelled`, which kills the cancelled
//! request's child and leaves the request unanswered. SIGINT and SIGTERM kill
//! the running children before `flz mcp` exits.

use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::os::fd::IntoRawFd;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::{process, thread};

use flz_transport::utility::socket_dir;
use serde_json::{json, Value};

use crate::args::Reading;
use crate::rpc::{self, text, Out, Stop, ARGUMENTS};

/// Set when SIGINT or SIGTERM arrives: every running child is killed, and no
/// message is answered after it.
static STOPPING: AtomicBool = AtomicBool::new(false);

/// Held by each lane while it answers a message, so a signal waits for the
/// lane's child to be killed and reaped before the process exits.
static BUSY: [Mutex<()>; 2] = [Mutex::new(()), Mutex::new(())];

/// One message to answer, with the flag a cancellation of it sets.
struct Job {
    line: String,
    id: Option<String>,
    cancelled: Arc<AtomicBool>,
}

struct Server<'a> {
    token: Option<&'a str>,
    socket: String,
    /// The requests not yet answered, by id as JSON, so a cancellation finds
    /// its request, and one for a request answered or never sent finds none.
    pending: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

pub(crate) fn serve(read: &Reading) {
    if let Ok(woken) = catch_stop_signals() {
        thread::spawn(move || stop_on(woken));
    }

    let server = Server {
        token: read.token(),
        socket: socket_dir::control_socket_path().unwrap_or_default(),
        pending: Mutex::default(),
    };
    let (calls, call_jobs) = mpsc::channel();
    let (pings, ping_jobs) = mpsc::channel();

    thread::scope(|scope| {
        scope.spawn(|| server.lane(&BUSY[0], call_jobs, false));
        scope.spawn(|| server.lane(&BUSY[1], ping_jobs, true));
        server.read(calls, pings);
    });
}

impl Server<'_> {
    /// Hand each line to its lane, or act on a cancellation at once. At the
    /// end of input the lanes answer what they hold, then end.
    fn read(&self, calls: Sender<Job>, pings: Sender<Job>) {
        // Bytes, not `lines()`: one line that is not UTF-8 must not end the
        // session. It reaches the core as text the parser then refuses.
        for bytes in io::stdin().lock().split(b'\n').map_while(Result::ok) {
            let line = String::from_utf8_lossy(&bytes);
            if line.trim().is_empty() {
                continue;
            }

            let message = serde_json::from_str::<Value>(&line).unwrap_or_default();
            let id = message
                .get("id")
                .filter(|id| id.is_string() || id.is_number())
                .map(Value::to_string);
            let lane = match (message.get("method").and_then(Value::as_str), &id) {
                (Some("notifications/cancelled"), None) => {
                    self.cancel(&message["params"]["requestId"]);
                    continue;
                }
                (Some("ping"), _) => &pings,
                _ => &calls,
            };

            let job = Job {
                line: line.into_owned(),
                id,
                cancelled: Arc::default(),
            };
            if let Some(id) = &job.id {
                self.pending().insert(id.clone(), job.cancelled.clone());
            }
            let _ = lane.send(job);
        }
    }

    fn cancel(&self, request_id: &Value) {
        if let Some(cancelled) = self.pending().get(&request_id.to_string()) {
            cancelled.store(true, Ordering::SeqCst);
        }
    }

    fn pending(&self) -> MutexGuard<'_, HashMap<String, Arc<AtomicBool>>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Answer `jobs` one at a time, in order; a `local` lane never asks the
    /// controller.
    fn lane(&self, busy: &Mutex<()>, jobs: Receiver<Job>, local: bool) {
        for job in jobs {
            let _busy = busy.lock().unwrap_or_else(PoisonError::into_inner);
            let stop = || job.cancelled.load(Ordering::SeqCst) || STOPPING.load(Ordering::SeqCst);
            if !stop() {
                self.answer(&job.line, local, &stop);
            }
            if let Some(id) = &job.id {
                self.pending().remove(id);
            }
        }
    }

    fn answer(&self, line: &str, local: bool, stop: Stop) {
        // The line is the request's arguments line: an assistant's tool
        // arguments never ride a command line.
        let online = rpc::request(Some(line.to_string()), &[], self.token, false, |list| {
            format!("Flezha.Mcp.Stdio.answer({}, {})", ARGUMENTS, list)
        });
        let offline = rpc::request(Some(line.to_string()), &[], None, false, |_| {
            format!(
                "Flezha.Mcp.Stdio.offline({}, {})",
                ARGUMENTS,
                text(&self.socket)
            )
        });

        // Stdout is the protocol's alone: a line the child printed that is not
        // a reply, such as a boot-time log line, goes to stderr, where a client
        // keeps a server's log.
        let mut replied = false;
        let mut pass = |line: &str| {
            let reply = line.starts_with('{');
            if reply {
                replied = true;
            } else {
                eprintln!("{}", line);
            }
            reply
        };

        let mut run =
            |command, request| rpc::child(command, request, Out::Filter(&mut pass), Some(stop));
        let reached = if local {
            None
        } else {
            Some(run("rpc", &online))
        };
        let answered = match reached {
            Some(answered) if !matches!(answered, Ok(ref a) if a.unreachable) => answered,
            // A request cancelled meanwhile starts no VM only to kill it.
            _ if stop() => return,
            _ => run("eval", &offline),
        };

        // A stopped request gets no answer: MCP sends none for a cancelled one.
        // A child that failed before replying has said why on stderr; the
        // client still gets an answer to its request rather than waiting.
        if !stop() && !replied && !matches!(answered, Ok(ref answer) if answer.status.success()) {
            internal_error(line);
        }
    }
}

/// The socket end the signal handler writes a signal's number to.
static WAKE: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_stop_signal(signal: libc::c_int) {
    let byte = signal as u8;
    // SAFETY: `write` is async-signal-safe, and `byte` outlives the call.
    unsafe { libc::write(WAKE.load(Ordering::SeqCst), (&byte as *const u8).cast(), 1) };
}

/// Hand SIGINT and SIGTERM to `stop_on`, through the socket returned. A child
/// starts with neither caught: exec resets a handler.
fn catch_stop_signals() -> io::Result<UnixStream> {
    let (woken, wake) = UnixStream::pair()?;
    WAKE.store(wake.into_raw_fd(), Ordering::SeqCst);
    for signal in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: the handler calls only `write`, which is async-signal-safe.
        unsafe { libc::signal(signal, on_stop_signal as *const () as libc::sighandler_t) };
    }
    Ok(woken)
}

/// On SIGINT or SIGTERM, stop every lane's child, wait until each lane has
/// reaped it, and exit with the shell's code for that signal.
fn stop_on(mut woken: UnixStream) {
    let mut signal = [0];
    if woken.read_exact(&mut signal).is_err() {
        return;
    }

    STOPPING.store(true, Ordering::SeqCst);
    let _idle: Vec<_> = BUSY.iter().map(Mutex::lock).collect();
    process::exit(128 + i32::from(signal[0]));
}

/// The JSON-RPC internal error for `line`: under its id when it is a request,
/// under a null id when it cannot be read, and nothing for a notification.
fn internal_error(line: &str) {
    let id = match serde_json::from_str::<Value>(line) {
        Ok(request) => request.get("id").cloned(),
        Err(_) => Some(Value::Null),
    };

    if let Some(id) = id {
        let error = json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32_603, "message": "Internal error"}});
        let mut out = io::stdout().lock();
        let _ = writeln!(out, "{}", error);
        let _ = out.flush();
    }
}
