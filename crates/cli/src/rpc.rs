//! The one place `flz` sends a request to a controller, the one place it starts
//! the sibling `flzvm`, and the one place a failure becomes an exit code.
//!
//! Every verb that reaches a controller is the same shape: one request on the
//! controller's UDS control socket naming a published operation, its arguments
//! as one JSON array and the options it carries. The controller renders the
//! answer, and a failure with its class's exit code, and sends both back as
//! frames, so `flz` copies them through and adds nothing. `flz` renders only
//! what it decides itself: its command line, a controller it cannot reach or
//! that does not accept its request, and an `flzvm` it cannot start. The
//! caller's token and output format join the options here and nowhere else: no
//! verb can be gated on the controller and token-less on the wire, and the
//! token travels only inside the request, over the owner-only socket.

use std::io::{self, ErrorKind, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::process::{exit, Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use std::{env, thread};

use flz_transport::errors::FlezhaError;
use flz_transport::service::payloader;
use flz_transport::utility::socket_dir;
use serde_json::{json, Map, Value};

use crate::args::{OptSpec, Reading, AFTER, DEFAULT_WINDOW_MS, DURATION_MS, LIMIT};

/// One option a request carries: its key and its value.
pub(crate) type Opt<'a> = (&'a str, Value);

/// The request protocol this `flz` speaks. A controller refuses one it does
/// not, so a mismatched pair fails clearly rather than oddly.
const PROTOCOL: u64 = 1;

/// How long a controller has to accept a request. Acceptance is immediate; a
/// controller that never sends one does not know the request at all.
const ACCEPT_WINDOW: Duration = Duration::from_secs(5);

static JSON: OnceLock<bool> = OnceLock::new();

/// Record the output format the command line asked for, before anything can
/// refuse it: a refusal of the command line itself honours `--json` too.
pub(crate) fn set_json(json: bool) {
    let _ = JSON.set(json);
}

/// End the run with `error`, rendered in the format the caller asked for on
/// stderr, with the exit code of its class.
pub(crate) fn fail(error: FlezhaError) -> ! {
    let format = if JSON.get().copied().unwrap_or(false) {
        "json"
    } else {
        "text"
    };
    eprintln!("{}", error.render(format));
    exit(error.exit_code());
}

/// Run the published operation `name` on the controller with `args`, the
/// caller's token and format joining `opts`. Returns only when it succeeded,
/// so the caller's next line is its success path.
pub(crate) fn door(name: &str, args: &[Value], opts: &[Opt], read: &Reading) {
    settle(send(
        &door_request(name, args, opts, read),
        Out::Inherit,
        None,
    ));
}

/// The request `door` sends, for the calls that read its answer themselves.
pub(crate) fn door_request(name: &str, args: &[Value], opts: &[Opt], read: &Reading) -> Value {
    let mut opts: Map<String, Value> = (opts.iter())
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect();
    if read.output_format() == "json" {
        opts.insert("output".into(), Value::from("json"));
    }
    cli_request(name, args, opts, read.token())
}

/// The request that runs the published operation `name` with `args`.
fn cli_request(name: &str, args: &[Value], opts: Map<String, Value>, token: Option<&str>) -> Value {
    request("cli", json!({ "name": name, "args": args }), opts, token)
}

/// The request `flz mcp` answers one protocol line with.
pub(crate) fn mcp_request(line: &str, token: Option<&str>) -> Value {
    request("mcp", json!({ "line": line }), Map::new(), token)
}

/// One request: the protocol, the entry the controller runs it through, that
/// entry's own fields, and the options, the caller's token among them when it
/// has one.
fn request(entry: &str, fields: Value, mut opts: Map<String, Value>, token: Option<&str>) -> Value {
    if let Some(token) = token {
        opts.insert("token".into(), Value::from(token));
    }
    let mut request = json!({ "protocol": PROTOCOL, "entry": entry, "opts": opts });
    if let (Value::Object(request), Value::Object(fields)) = (&mut request, fields) {
        request.extend(fields);
    }
    request
}

/// How a request ended.
pub(crate) enum Ended {
    /// The code the controller's `cli_exit` carried.
    Exit(i32),
    /// No socket, a refused connection, or one that closed before `cli_exit`.
    Unreachable,
    /// A controller that did not accept the request in time.
    Unaccepted,
}

/// Where what the controller writes goes: stdout through; stdout line by line
/// through a filter that says which lines to pass on; or stdout kept and
/// stderr dropped, for a probe that prints nothing.
pub(crate) enum Out<'a> {
    Inherit,
    Filter(&'a mut dyn FnMut(&str) -> bool),
    Capture(&'a mut Vec<u8>),
}

/// What ends a request before it ends: asked while it runs, and once it says
/// so the connection is closed, or the child killed and reaped.
pub(crate) type Stop<'a> = &'a (dyn Fn() -> bool + Sync);

/// Send `request` to the controller and copy its answer to `out` and stderr as
/// it arrives. The one function every request goes through.
pub(crate) fn send(request: &Value, out: Out, stop: Option<Stop>) -> Ended {
    match open(request, ACCEPT_WINDOW) {
        Ok(stream) => answer(stream, out, stop),
        Err(ended) => ended,
    }
}

/// Copy an accepted request's answer, closing the connection once `stop` says
/// so, which ends the read and, on the controller, the call.
fn answer(mut stream: UnixStream, out: Out, stop: Option<Stop>) -> Ended {
    let done = AtomicBool::new(false);
    let watched = stop.zip(stream.try_clone().ok());

    thread::scope(|scope| {
        if let Some((stop, closer)) = watched {
            let done = &done;
            scope.spawn(move || {
                if stopped_before(stop, || done.load(Ordering::SeqCst)) {
                    let _ = closer.shutdown(Shutdown::Both);
                }
            });
        }
        let ended = copy(&mut stream, &mut Sink::new(out));
        done.store(true, Ordering::SeqCst);
        ended
    })
}

/// Connect, send `request`, and wait up to `accept` for `cli_accepted`.
fn open(request: &Value, accept: Duration) -> Result<UnixStream, Ended> {
    let path = socket_dir::control_socket_path().map_err(|_| Ended::Unreachable)?;
    let mut stream = UnixStream::connect(path).map_err(|_| Ended::Unreachable)?;
    let frame = payloader::build_without_metadata("cli_request", request.to_string().as_bytes());
    stream.write_all(&frame).map_err(|_| Ended::Unreachable)?;

    stream
        .set_read_timeout(Some(accept))
        .map_err(|_| Ended::Unreachable)?;
    match payloader::extract_payload(&mut stream) {
        Ok((id, _, _)) if id == "cli_accepted" => {}
        Ok(_) => return Err(Ended::Unaccepted),
        Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
            return Err(Ended::Unaccepted)
        }
        Err(_) => return Err(Ended::Unreachable),
    }
    stream
        .set_read_timeout(None)
        .map_err(|_| Ended::Unreachable)?;
    Ok(stream)
}

/// Copy the controller's frames until `cli_exit` or the connection's end.
fn copy(stream: &mut UnixStream, sink: &mut Sink) -> Ended {
    loop {
        let Ok((id, bytes, _)) = payloader::extract_payload(&mut *stream) else {
            sink.finish();
            return Ended::Unreachable;
        };
        match id.as_str() {
            "cli_stdout" => sink.stdout(&bytes),
            "cli_stderr" => sink.stderr(&bytes),
            "cli_exit" => {
                sink.finish();
                return Ended::Exit(exit_code(&bytes));
            }
            _ => {}
        }
    }
}

fn exit_code(frame: &[u8]) -> i32 {
    serde_json::from_slice::<Value>(frame)
        .ok()
        .and_then(|exit| exit["code"].as_i64())
        .and_then(|code| i32::try_from(code).ok())
        .unwrap_or(1)
}

/// Ask `stop` until `done` says the work has ended; `true` when `stop` came
/// first.
fn stopped_before(stop: Stop, mut done: impl FnMut() -> bool) -> bool {
    while !done() {
        if stop() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

/// `out`, and the part of a line a filter has not been handed yet.
struct Sink<'a> {
    out: Out<'a>,
    pending: Vec<u8>,
}

impl<'a> Sink<'a> {
    fn new(out: Out<'a>) -> Self {
        Sink {
            out,
            pending: Vec::new(),
        }
    }

    fn stdout(&mut self, bytes: &[u8]) {
        match &mut self.out {
            Out::Inherit => {
                let mut stdout = io::stdout().lock();
                let _ = stdout.write_all(bytes);
                let _ = stdout.flush();
            }
            Out::Capture(kept) => kept.extend_from_slice(bytes),
            Out::Filter(_) => {
                self.pending.extend_from_slice(bytes);
                while let Some(end) = self.pending.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = self.pending.drain(..=end).collect();
                    self.line(&line[..end]);
                }
            }
        }
    }

    fn stderr(&mut self, bytes: &[u8]) {
        if !matches!(self.out, Out::Capture(_)) {
            let _ = io::stderr().write_all(bytes);
        }
    }

    /// The last line, when the answer did not end with a newline.
    fn finish(&mut self) {
        if !self.pending.is_empty() {
            let line = std::mem::take(&mut self.pending);
            self.line(&line);
        }
    }

    fn line(&mut self, line: &[u8]) {
        if let Out::Filter(keep) = &mut self.out {
            let line = String::from_utf8_lossy(line);
            if keep(&line) {
                println!("{}", line);
            }
        }
    }
}

/// `door`, for a verb whose job is to wait for the controller: when it could
/// not be reached at all, nothing is printed or exited, so the verb can ask
/// again.
pub(crate) fn run_if_reachable(request: &Value) -> Result<(), ()> {
    match send(request, Out::Inherit, None) {
        Ended::Unreachable => Err(()),
        ended => {
            settle(ended);
            Ok(())
        }
    }
}

/// The one rule every verb's request ends with: success returns, a controller
/// that could not be reached is `controller.unreachable`, one that did not
/// accept the request is `cli.protocol_unsupported`, and any other failure
/// exits with the controller's own code, whose rendering it has already sent.
pub(crate) fn settle(ended: Ended) {
    match ended {
        Ended::Exit(0) => {}
        Ended::Exit(code) => exit(code),
        Ended::Unreachable => unreachable(),
        Ended::Unaccepted => fail(FlezhaError::with(
            "cli.protocol_unsupported",
            [
                ("got", Value::from(PROTOCOL)),
                ("supported", Value::from("none")),
            ],
        )),
    }
}

/// End the run for a file `flz` could not write.
pub(crate) fn write_failed(path: impl std::fmt::Display, reason: impl std::fmt::Display) -> ! {
    fail(FlezhaError::with(
        "cli.write_failed",
        [
            ("path", Value::String(path.to_string())),
            ("reason", Value::String(reason.to_string())),
        ],
    ))
}

/// End the run for a controller that could not be reached.
pub(crate) fn unreachable() -> ! {
    let socket = socket_dir::control_socket_path().unwrap_or_default();
    fail(FlezhaError::with(
        "controller.unreachable",
        [("socket", Value::String(socket))],
    ))
}

/// Run `module.fun` in a VM of its own (`flzvm eval`), for a verb that reaches
/// no controller: it works where none runs, and no controller authorizes it, so
/// it carries no token. The child prints its own answer and exits with its
/// class's code, which `flz` passes through.
pub(crate) fn eval(module: &str, fun: &str, positional: &[String], read: &Reading) {
    let mut parts = positional.to_vec();
    parts.push(match read.output_format() {
        "json" => format!("[output: {}]", text("json")),
        _ => "[]".to_string(),
    });
    let expression = format!("{}.{}({})", module, fun, parts.join(", "));
    verdict(child(&expression, "", Out::Inherit, None));
}

/// Run `flzvm eval <expression>` with `stdin` as its whole input; its stdout
/// goes to `out`, its stderr through.
pub(crate) fn child(
    expression: &str,
    stdin: &str,
    out: Out,
    stop: Option<Stop>,
) -> io::Result<ExitStatus> {
    let stdout = match out {
        Out::Inherit => Stdio::inherit(),
        _ => Stdio::piped(),
    };
    let mut child = spawn(expression, stdin, stdout)?;

    // The child is waited for beside the read, so a stop is heard while its
    // stdout is still open; killing it closes that stdout and ends the read.
    let piped = child.stdout.take();
    thread::scope(|scope| {
        let waiter = scope.spawn(|| wait(&mut child, stop));
        if let Some(mut piped) = piped {
            let mut sink = Sink::new(out);
            let mut chunk = [0; 8192];
            while let Ok(read @ 1..) = piped.read(&mut chunk) {
                sink.stdout(&chunk[..read]);
            }
            sink.finish();
        }
        waiter.join().expect("the waiter does not panic")
    })
}

/// Start `flzvm eval <expression>` and write its stdin. The child gets no
/// other, so nothing it reads can be a line meant for `flz` itself.
fn spawn(expression: &str, stdin: &str, stdout: Stdio) -> io::Result<Child> {
    let mut cmd = flzvm();
    cmd.arg("eval")
        .arg(expression)
        .stdin(Stdio::piped())
        .stdout(stdout);
    // A `flz` killed outright (SIGKILL, the OOM killer) takes the child with
    // it. The signal fires when the spawning thread exits, and every caller
    // waits for its child on that thread.
    #[cfg(target_os = "linux")]
    // SAFETY: `prctl` is async-signal-safe and touches no memory of the parent.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut cmd, || {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    let mut child = cmd.spawn()?;

    // Written beside the child and closed once written, so a stop is heard
    // however long the child takes to read. A child that exits before reading
    // closes the pipe; its own status and stderr say why.
    if let Some(mut pipe) = child.stdin.take() {
        let input = stdin.to_string();
        thread::spawn(move || {
            let _ = pipe.write_all(input.as_bytes());
        });
    }

    Ok(child)
}

/// Wait for `child`, killing and reaping it once `stop` says so; with nothing
/// to stop for, a plain wait.
fn wait(child: &mut Child, stop: Option<Stop>) -> io::Result<ExitStatus> {
    // A failed check counts as ended: the wait below reports why.
    if stop.is_some_and(|stop| stopped_before(stop, || !matches!(child.try_wait(), Ok(None)))) {
        let _ = child.kill();
    }
    child.wait()
}

/// The status of a child `flz` started, judged by one rule: a non-zero code is
/// the child's, which has already said why.
pub(crate) fn verdict(status: io::Result<ExitStatus>) {
    match status {
        Err(e) => not_runnable(e),
        Ok(status) if status.success() => {}
        Ok(status) => exit(status.code().unwrap_or(1)),
    }
}

/// An unbuilt release and the wrong working directory are the same caller
/// condition, and only the path says which: `flz` looks for its sibling next to
/// itself and nowhere else.
pub(crate) fn not_runnable(e: io::Error) -> ! {
    fail(FlezhaError::with(
        "cli.flzvm_not_runnable",
        [
            ("path", Value::String(flzvm_path())),
            ("reason", Value::String(e.to_string())),
        ],
    ))
}

/// The namespace a scoped verb names: the one written, `default` when none was.
pub(crate) fn namespace(read: &Reading) -> Value {
    match read.namespace() {
        "" => Value::from("default"),
        written => Value::from(written),
    }
}

/// The `opts` a paged listing sends: `limit`, `after`, and a `filter` of the
/// filter flags it declares, each under its filter key. `None` when none was
/// written, so the listing answers exactly what it answers without them.
pub(crate) fn page(read: &Reading, filters: &[(&str, &OptSpec)]) -> Option<Value> {
    let filter: Map<String, Value> = (filters.iter())
        .filter_map(|(key, spec)| Some((key.to_string(), Value::from(read.opt(spec.long)?))))
        .collect();
    let mut opts = Map::new();
    if let Some(limit) = read.num::<u64>(LIMIT.long) {
        opts.insert("limit".into(), Value::from(limit));
    }
    if let Some(cursor) = read.opt(AFTER.long) {
        opts.insert("after".into(), Value::from(cursor));
    }
    if !filter.is_empty() {
        opts.insert("filter".into(), Value::Object(filter));
    }
    (!opts.is_empty()).then_some(Value::Object(opts))
}

/// The read verbs — a namespace, an output format, a progress line only text
/// mode prints naming what is being fetched, and the row's operation taking the
/// namespace, and the page asked for when one was.
pub(crate) fn scoped_read(read: &Reading, noun: &str, page: Option<Value>) {
    let namespace = namespace(read);
    let shown = namespace.as_str().unwrap_or_default();
    progress_line(
        read,
        &format!("Fetching {} for namespace '{}'...", noun, shown),
    );

    let args: Vec<Value> = [Some(namespace), page].into_iter().flatten().collect();
    door(read.operation(), &args, &[], read);
}

/// A line that says what `flz` is about to do, printed in text mode only: under
/// `--json`, stdout carries the answer and nothing else.
pub(crate) fn progress_line(read: &Reading, line: &str) {
    if read.output_format() == "text" {
        println!("{}", line);
    }
}

/// The window a bounded stream runs for, in milliseconds: `--duration-ms`, or
/// the default every streaming verb shares.
pub(crate) fn window(read: &Reading) -> Option<Value> {
    Some(Value::from(
        read.num::<u64>(DURATION_MS.long)
            .unwrap_or(DEFAULT_WINDOW_MS),
    ))
}

/// A `Command` for the sibling `flzvm`, for the verbs that start a VM of their
/// own: `controller start`, the plugin-root verbs and `flz mcp` offline.
pub(crate) fn flzvm() -> Command {
    Command::new(flzvm_path())
}

pub(crate) fn flzvm_path() -> String {
    let current_exe = env::current_exe().expect("Failed to get current executable path");
    let current_dir = current_exe
        .parent()
        .expect("Failed to get current directory");

    current_dir
        .join(format!("flzvm{}", exe_suffix()))
        .to_str()
        .expect("Failed to convert path to string")
        .to_string()
}

fn exe_suffix() -> String {
    if cfg!(target_os = "windows") {
        ".bat".to_string()
    } else {
        String::new()
    }
}

/// `value` as an Elixir string literal, for the expressions `flzvm eval` runs.
/// A JSON string is one, and escapes a backslash as well as a quote. An Elixir
/// string also interpolates `#{`, so that is escaped too: a value is never
/// code the VM runs. The line separators and bidirectional controls Elixir
/// refuses raw in a literal are written as `\uXXXX`.
pub(crate) fn text(value: &str) -> String {
    let json = serde_json::Value::String(value.to_string()).to_string();
    let mut literal = String::with_capacity(json.len());

    for c in json.chars() {
        match c {
            '\u{85}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202A}'..='\u{202E}'
            | '\u{2066}'..='\u{2069}' => literal.push_str(&format!("\\u{:04X}", c as u32)),
            c => literal.push(c),
        }
    }

    literal.replace("#{", "\\#{")
}

pub(crate) const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;

/// Whether a controller is serving on this node — `flz reset` and
/// `flz auth rebootstrap` both refuse to run while one is.
///
/// A `boot_await` request that does not wait, a readiness read of this
/// controller alone served to any caller, answered with any exit code inside
/// the timeout means up: a controller still starting answers not ready, and is
/// up all the same. Every other outcome (no socket, a refused connection, no
/// acceptance, the timeout) means not up, so the caller proceeds. `FLEZHA_RESET_PROBE_OVERRIDE`
/// short-circuits it to `up` or `down` for both callers' tests.
pub(crate) fn controller_is_up(probe_timeout_ms: u64) -> bool {
    if let Some(v) = flz_transport::env::env_raw("FLEZHA_RESET_PROBE_OVERRIDE") {
        match v.as_str() {
            "up" => return true,
            "down" => return false,
            _ => {}
        }
    }

    let timeout = Duration::from_millis(probe_timeout_ms.max(1));
    let deadline = Instant::now() + timeout;
    let probe = cli_request("Controller.boot_await", &[json!(0)], Map::new(), None);
    let Ok(stream) = open(&probe, timeout) else {
        return false;
    };

    let mut verdict = Vec::new();
    let ended = answer(
        stream,
        Out::Capture(&mut verdict),
        Some(&|| Instant::now() >= deadline),
    );
    matches!(ended, Ended::Exit(_))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The token and the output format ride the options, beside whatever the
    /// verb carries, and the arguments travel as written.
    #[test]
    fn a_request_carries_its_options_beside_its_arguments() {
        let fields = json!({ "name": "d\ne", "init_values": { "a": "x\"y" } });
        let opts = Map::from_iter([("duration_ms".to_string(), Value::from(500))]);
        let built = request(
            "cli",
            json!({ "name": "Schedule.add", "args": [fields] }),
            opts,
            Some("t"),
        );
        assert_eq!(
            built,
            json!({
                "protocol": 1,
                "entry": "cli",
                "name": "Schedule.add",
                "args": [{ "name": "d\ne", "init_values": { "a": "x\"y" } }],
                "opts": { "duration_ms": 500, "token": "t" }
            })
        );
    }

    /// A line `flz mcp` answers is sent as written, with the token beside it.
    #[test]
    fn an_mcp_request_carries_its_line() {
        assert_eq!(
            mcp_request("{\"id\":1}", None),
            json!({ "protocol": 1, "entry": "mcp", "line": "{\"id\":1}", "opts": {} })
        );
    }

    /// A value is data in the VM, never code: `#{` stays text, and a
    /// backslash before it stays a backslash.
    #[test]
    fn a_value_never_interpolates() {
        assert_eq!(text("a#{1+1}b"), r#""a\#{1+1}b""#);
        assert_eq!(text(r"\#{x}"), r#""\\\#{x}""#);
        assert_eq!(text("# {x} #"), r##""# {x} #""##);
    }

    /// Elixir refuses these raw in a string literal, so they are escaped.
    #[test]
    fn a_separator_or_bidi_control_is_escaped() {
        assert_eq!(
            text("a\u{2028}b\u{85}c\u{202E}d\u{2069}"),
            r#""a\u2028b\u0085c\u202Ed\u2069""#
        );
    }

    /// A filter is handed whole lines, however the frames split them.
    #[test]
    fn a_filter_sees_whole_lines_across_frames() {
        let mut seen = Vec::new();
        let mut keep = |line: &str| {
            seen.push(line.to_string());
            false
        };
        let mut sink = Sink::new(Out::Filter(&mut keep));
        sink.stdout(b"one\ntw");
        sink.stdout(b"o\nthree");
        sink.finish();
        drop(sink);
        assert_eq!(seen, ["one", "two", "three"]);
    }

    /// `flz reset help` quotes the default as a literal; nothing else can keep
    /// the screen and the constant from drifting apart.
    #[test]
    fn the_reset_screen_quotes_the_default_probe_timeout() {
        let quoted = format!("(default {})", DEFAULT_PROBE_TIMEOUT_MS);
        let reset = crate::FLZ.verbs.iter().find(|verb| verb.name == "reset");
        assert!(
            crate::help::verb_screen(&crate::FLZ, reset.expect("flz reset")).contains(&quoted),
            "flz reset help must say {}",
            quoted
        );
    }
}
