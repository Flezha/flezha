//! The one place `flz` starts the sibling `flzvm`, the one place a request is
//! built, and the one place a failure becomes an exit code.
//!
//! Every verb that reaches a controller is the same shape: one door call
//! naming a published operation, its arguments as one JSON array and the
//! options it carries, run as `flzvm rpc <expr>`. The controller renders the
//! answer, and a failure on this process's stderr with its class's exit code,
//! so `flz` passes that code through and adds nothing. `flz` renders only what
//! it decides itself: its command line, a controller it cannot reach, and an
//! `flzvm` it cannot start. Every request ends with one options list, and the
//! caller's token and output format join that list here and nowhere else: no
//! verb can be gated on the controller and token-less on the wire. The token
//! itself travels on the child's stdin, never its command line, which any local
//! user can read.

use std::io::{BufRead, BufReader, Write};
use std::process::{exit, Child, ChildStderr, Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use std::{env, io, thread};

use flz_transport::errors::FlezhaError;
use flz_transport::utility::socket_dir;
use serde_json::Value;

use crate::args::{Reading, DEFAULT_WINDOW_MS, DURATION_MS};

/// One option a request carries: its key and its value already rendered as an
/// Elixir literal — `text` for a string, `to_string` for a number, `"true"`.
pub(crate) type Opt<'a> = (&'a str, String);

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

/// The one function every request to a running controller calls: the door's
/// own name for the operation, its arguments as one JSON array, and the
/// options list.
const DOOR: &str = "Flezha.Service.Cli.call";

/// Run the published operation `name` on the controller with `args`, the
/// caller's token and format appended to `opts`. Returns only when the child
/// succeeded, so the caller's next line is its success path.
pub(crate) fn door(name: &str, args: &[Value], opts: &[Opt], read: &Reading) {
    settle(call(&door_request(name, args, opts, read), Out::Inherit));
}

/// The request `door` sends, for the calls that do not go through `door`.
pub(crate) fn door_request<'a>(
    name: &str,
    args: &[Value],
    opts: &[Opt],
    read: &'a Reading,
) -> Request<'a> {
    door_call(
        name,
        args,
        opts,
        read.token(),
        read.output_format() == "json",
    )
}

/// `serde_json` keeps an object's keys sorted, so one invocation builds one
/// request, byte for byte.
fn door_call<'a>(
    name: &str,
    args: &[Value],
    opts: &[Opt],
    token: Option<&'a str>,
    json: bool,
) -> Request<'a> {
    request(opts, token, json, |list| {
        format!(
            "{}({}, {}, {})",
            DOOR,
            text(name),
            text(&Value::Array(args.to_vec()).to_string()),
            list
        )
    })
}

/// One request for a `flzvm` child: its expression, and the token that
/// expression's options list reads from the child's stdin. Only `request`
/// builds one with a token, so an expression that reads a token never goes
/// without it. No `Debug`: it holds the token.
pub(crate) struct Request<'a> {
    expression: String,
    token: Option<&'a str>,
}

impl Request<'_> {
    /// An expression with no options list, so no token to read.
    pub(crate) fn bare(expression: String) -> Request<'static> {
        Request {
            expression,
            token: None,
        }
    }

    pub(crate) fn expression(&self) -> &str {
        &self.expression
    }
}

/// The request `build` makes of the options list `keyword_opts_fragment`
/// renders, holding the token that list reads.
pub(crate) fn request<'a>(
    opts: &[Opt],
    token: Option<&'a str>,
    json: bool,
    build: impl FnOnce(&str) -> String,
) -> Request<'a> {
    Request {
        expression: build(&keyword_opts_fragment(opts, token.is_some(), json)),
        token,
    }
}

/// Run `module.fun` in a VM of its own (`flzvm eval`), for a verb that reaches
/// no controller: it works where none runs, and no controller authorizes it, so
/// it carries no token. The child prints its own answer and exits with its
/// class's code, which `flz` passes through.
pub(crate) fn eval(module: &str, fun: &str, positional: &[String], read: &Reading) {
    settle(child(
        "eval",
        &eval_request(module, fun, positional, read),
        Out::Inherit,
        None,
    ));
}

/// The request `eval` runs, for a verb that reads the child's stdout itself.
pub(crate) fn eval_request(
    module: &str,
    fun: &str,
    positional: &[String],
    read: &Reading,
) -> Request<'static> {
    request(&[], None, read.output_format() == "json", |list| {
        let mut parts = positional.to_vec();
        parts.push(list.to_string());
        format!("{}.{}({})", module, fun, parts.join(", "))
    })
}

/// What `elixir --rpc-eval` says, with exit 1, whenever it could not reach the
/// node: one that is not up yet, one whose cookie is not this shell's, a name
/// nobody holds, and a node lost mid-call all read the same. Its exit code is
/// the one an internal failure has, so this line is the only difference.
const NOCONNECTION: &str = "--rpc-eval : RPC failed with reason :noconnection";

/// `run`, for a verb whose job is to wait for the node: when the node could
/// not be reached at all, nothing is printed or exited, so the verb can ask
/// again.
pub(crate) fn run_if_reachable(request: &Request) -> Result<(), ()> {
    match call(request, Out::Inherit) {
        Ok(answer) if answer.unreachable => Err(()),
        answer => {
            settle(answer);
            Ok(())
        }
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

/// What one `flzvm rpc` answered: its status, and whether it said the node
/// could not be reached.
pub(crate) struct Answer {
    pub(crate) status: ExitStatus,
    pub(crate) unreachable: bool,
}

/// Where a call's stdout goes: through, or line by line through a filter that
/// says which lines to pass on.
pub(crate) enum Out<'a> {
    Inherit,
    Filter(&'a mut dyn FnMut(&str) -> bool),
}

pub(crate) fn call(request: &Request, out: Out) -> io::Result<Answer> {
    child("rpc", request, out, None)
}

/// What ends a child before it exits: asked while the child runs, and once it
/// says so the child is killed and reaped.
pub(crate) type Stop<'a> = &'a (dyn Fn() -> bool + Sync);

/// `call`, with the `flzvm` command named: `rpc` evaluates on the controller,
/// `eval` in a VM of its own. The request's token is the child's whole stdin;
/// it gets no other, so nothing it reads can be a line meant for `flz` itself.
pub(crate) fn child(
    command: &str,
    request: &Request,
    out: Out,
    stop: Option<Stop>,
) -> io::Result<Answer> {
    let mut spawn = flzvm();
    spawn
        .arg(command)
        .arg(&request.expression)
        .stdin(Stdio::piped())
        .stdout(match out {
            Out::Inherit => Stdio::inherit(),
            Out::Filter(_) => Stdio::piped(),
        })
        .stderr(Stdio::piped());
    // A `flz` killed outright (SIGKILL, the OOM killer) takes the child with
    // it. The signal fires when the spawning thread exits, and every caller
    // waits for its child on that thread.
    #[cfg(target_os = "linux")]
    // SAFETY: `prctl` is async-signal-safe and touches no memory of the parent.
    unsafe {
        std::os::unix::process::CommandExt::pre_exec(&mut spawn, || {
            libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
            Ok(())
        });
    }
    let mut child = spawn.spawn()?;

    // Closed once written. A child that exits before reading closes the pipe;
    // its own status and stderr say why, so the failed write adds nothing.
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(request.token.unwrap_or_default().as_bytes());
    }

    let stderr = child.stderr.take().expect("stderr is piped");
    let forwarder = thread::spawn(move || forward_stderr(stderr));

    // The child is waited for beside the read, so a stop is heard while its
    // stdout is still open; killing it closes that stdout and ends the read.
    let piped = child.stdout.take();
    let status = thread::scope(|scope| {
        let waiter = scope.spawn(|| wait(&mut child, stop));
        if let (Some(piped), Out::Filter(keep)) = (piped, out) {
            for line in BufReader::new(piped).lines().map_while(Result::ok) {
                if keep(&line) {
                    println!("{}", line);
                }
            }
        }
        waiter.join().expect("the waiter does not panic")
    })?;
    let unreachable = forwarder.join().unwrap_or(false);

    Ok(Answer {
        status,
        unreachable,
    })
}

/// Wait for `child`, killing and reaping it once `stop` says so; with nothing
/// to stop for, a plain wait.
fn wait(child: &mut Child, stop: Option<Stop>) -> io::Result<ExitStatus> {
    let Some(stop) = stop else {
        return child.wait();
    };

    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if stop() {
            let _ = child.kill();
            return child.wait();
        }
        thread::sleep(Duration::from_millis(25));
    }
}

// The child's stderr, passed through line by line as it arrives so a streaming
// verb's failure is not held back, except the one line that says the node
// could not be reached: `flz` answers that one itself.
fn forward_stderr(stderr: ChildStderr) -> bool {
    let mut unreachable = false;
    let mut out = io::stderr();

    for line in BufReader::new(stderr).split(b'\n').map_while(Result::ok) {
        if line == NOCONNECTION.as_bytes() {
            unreachable = true;
        } else {
            let _ = out.write_all(&line);
            let _ = out.write_all(b"\n");
        }
    }

    unreachable
}

/// The one rule every verb's call ends with: success returns, a child that
/// could not reach the controller is `controller.unreachable`, and any other
/// failure exits with the child's own code, whose rendering it has already
/// written.
pub(crate) fn settle(answer: io::Result<Answer>) {
    match answer {
        Err(e) => not_runnable(e),
        Ok(answer) if answer.status.success() => {}
        Ok(answer) if answer.unreachable => unreachable(),
        Ok(answer) => exit(answer.status.code().unwrap_or(1)),
    }
}

/// The status of a child `flz` started for something other than an rpc, judged
/// by the same rule: a non-zero code is the child's.
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

/// The read verbs — a namespace, an output format, a progress line only text
/// mode prints naming what is being fetched, and the row's operation taking the
/// namespace.
pub(crate) fn scoped_read(read: &Reading, noun: &str) {
    let namespace = namespace(read);
    let shown = namespace.as_str().unwrap_or_default();
    progress_line(
        read,
        &format!("Fetching {} for namespace '{}'...", noun, shown),
    );

    door(read.operation(), &[namespace], &[], read);
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

/// A `Command` for the sibling `flzvm`, for the three verbs that read its
/// stdout or pass it a subcommand of their own.
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

/// `value` as an Elixir string literal. A JSON string is one, and escapes a
/// backslash as well as a quote — the one renderer for every argument that
/// can carry JSON or free text, so no call site invents its own. An Elixir
/// string also interpolates `#{`, so that is escaped too: a value is never
/// code the controller runs. The line separators and bidirectional controls
/// Elixir refuses raw in a literal are written as `\uXXXX`.
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

/// The options list every request ends with: `opts` in the order given, then
/// `output: "json"` when the caller asked for JSON, then the caller's identity
/// token when the caller has one — `[]` when it carries none of them. The
/// token's value never enters the list: the list reads it from the stdin
/// `child` hands it on.
fn keyword_opts_fragment(opts: &[Opt], token: bool, json: bool) -> String {
    let mut entries: Vec<String> = opts
        .iter()
        .map(|(key, value)| format!("{}: {}", key, value))
        .collect();

    if json {
        entries.push(format!("output: {}", text("json")));
    }
    // `child` writes the token and closes stdin, so the read ends exactly at
    // the token's last byte.
    if token {
        entries.push("token: IO.read(:eof)".to_string());
    }

    format!("[{}]", entries.join(", "))
}

pub(crate) const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;

/// Whether a controller is serving on this node — `flz reset` and
/// `flz auth rebootstrap` both refuse to run while one is.
///
/// A successful `boot_status` RPC — a liveness read served to any caller —
/// with non-empty stdout inside the timeout means up; every other outcome (no
/// node, timeout, distribution failure) means not up, so the caller proceeds.
/// `FLEZHA_RESET_PROBE_OVERRIDE` short-circuits it to `up` or `down` for both
/// callers' tests.
pub(crate) fn controller_is_up(probe_timeout_ms: u64) -> bool {
    if let Some(v) = flz_transport::env::env_raw("FLEZHA_RESET_PROBE_OVERRIDE") {
        match v.as_str() {
            "up" => return true,
            "down" => return false,
            _ => {}
        }
    }

    let mut child = match flzvm()
        .arg("rpc")
        .arg(&door_call("Controller.boot_status", &[], &[], None, false).expression)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let deadline = Instant::now() + Duration::from_millis(probe_timeout_ms);
    match wait(&mut child, Some(&|| Instant::now() >= deadline)) {
        Ok(status) if status.success() => {
            let mut out = String::new();
            if let Some(mut stdout) = child.stdout.take() {
                let _ = std::io::Read::read_to_string(&mut stdout, &mut out);
            }
            !out.trim().is_empty()
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The token closes the options list as code that reads it from stdin, so
    /// its value is never on the child's command line; a call with nothing
    /// else still ends with the list.
    #[test]
    fn the_token_closes_the_options_list_and_is_read_from_stdin() {
        assert_eq!(
            door_call("Dlq.list", &[], &[], Some("abc\"def"), false).expression,
            r#"Flezha.Service.Cli.call("Dlq.list", "[]", [token: IO.read(:eof)])"#
        );
        assert_eq!(
            door_call("Dlq.list", &[Value::Null], &[], None, false).expression,
            r#"Flezha.Service.Cli.call("Dlq.list", "[null]", [])"#
        );
    }

    /// A value is data on the controller, never code: `#{` stays text, and a
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

    /// The arguments are one JSON array inside one Elixir string: a quote and a
    /// backslash inside a value stay inside it, an object's keys come out in one
    /// order, and the options keep theirs with the token last.
    #[test]
    fn arguments_are_one_escaped_json_array_and_options_keep_their_order() {
        let fields =
            serde_json::json!({ "name": "d", "init_values": { "a": "x\"y" }, "cron": "0 8 * * *" });
        let opts = [("duration_ms", "500".to_string())];
        assert_eq!(
            door_call("Schedule.add", &[fields], &opts, Some("t"), true).expression,
            r#"Flezha.Service.Cli.call("Schedule.add", "[{\"cron\":\"0 8 * * *\",\"init_values\":{\"a\":\"x\\\"y\"},\"name\":\"d\"}]", [duration_ms: 500, output: "json", token: IO.read(:eof)])"#
        );
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
