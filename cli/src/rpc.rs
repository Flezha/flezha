//! The one place `flz` starts the sibling `flzvm`, the one place a request is
//! built, and the one place a refused RPC becomes an exit code.
//!
//! Every verb is the same shape — a `CliManager` wire name, its positional
//! arguments, the options it carries, run as `flzvm rpc <expr>`, exit non-zero
//! if it did — so the shape lives here and each verb contributes only its
//! arguments and its failure policy. Every request ends with one options list,
//! and the caller's token joins that list here and nowhere else: no verb can be
//! gated on the controller and token-less on the wire.

use std::io::Write;
use std::process::{exit, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};
use std::{env, io};

use crate::args::{Reading, DEFAULT_WINDOW_MS, DURATION_MS};

/// What a verb exits with when `flzvm` refuses.
#[derive(Clone, Copy)]
pub(crate) enum Exit {
    /// This code, whatever the child answered.
    Fixed(i32),
    /// The child's own code. The `i32` is what a child that was signalled —
    /// and therefore carries no code — becomes.
    Propagate(i32),
}

impl Exit {
    /// The code to use when the child never ran, or answered nothing.
    fn fallback(self) -> i32 {
        match self {
            Exit::Fixed(n) | Exit::Propagate(n) => n,
        }
    }
}

/// A verb's failure policy: what it says, and what it exits with. The two
/// halves are independent across this tree — a verb that panics on a failed
/// spawn may still be silent on a refusal — so the policy carries both rather
/// than collapsing them and moving an operator-visible code.
#[derive(Clone, Copy)]
pub(crate) struct Failure {
    /// Said on stderr when the child ran and refused. A single `{}` takes the
    /// child's exit status.
    refused: Option<&'static str>,
    exit: Exit,
    /// What a verb calls a child it could not start at all; `None` takes the
    /// shared sentence. Either way the line names the path and the reason.
    unspawnable: Option<&'static str>,
}

impl Failure {
    pub(crate) const fn new(exit: Exit) -> Self {
        Failure {
            refused: None,
            exit,
            unspawnable: None,
        }
    }

    pub(crate) const fn saying(mut self, refused: &'static str) -> Self {
        self.refused = Some(refused);
        self
    }

    pub(crate) const fn diagnosing(mut self, what: &'static str) -> Self {
        self.unspawnable = Some(what);
        self
    }
}

/// The seventeen verbs that name the failure and exit 1.
pub(crate) const REFUSED: Failure =
    Failure::new(Exit::Fixed(1)).saying("Error: Command execution failed.");

/// The verbs that answer with the child's own code and say nothing.
pub(crate) const PROPAGATE: Failure = Failure::new(Exit::Propagate(1));

/// `flz controller status` and `mem-top`: 3 means the controller socket is
/// unreachable, whatever the child answered.
pub(crate) const UNREACHABLE: Failure = Failure::new(Exit::Fixed(3));

/// `flz audit` and `flz observe`: the child's own code, and a failed spawn
/// diagnosed rather than panicked.
pub(crate) const DIAGNOSED: Failure =
    Failure::new(Exit::Propagate(3)).diagnosing("error: failed to spawn rpc");

/// The verbs whose refusal is reported without being explained — the RPC has
/// already said why on its own stderr.
pub(crate) const UNKNOWN_STATUS: Failure =
    Failure::new(Exit::Fixed(1)).saying("Unknown status returned.");

/// Exit 1 adding nothing to what the RPC printed.
pub(crate) const QUIET: Failure = Failure::new(Exit::Fixed(1));

/// Said when a verb that named nothing of its own could not start `flzvm`. An
/// unbuilt release and the wrong working directory are the same operator
/// condition, and only the path says which: `flz` looks for its sibling next
/// to itself and nowhere else.
const UNSPAWNABLE: &str = "Error: could not run";

/// One option a request carries: its key and its value already rendered as an
/// Elixir literal — `text` for a string, `to_string` for a number, `"true"`.
pub(crate) type Opt<'a> = (&'a str, String);

/// Run one `CliManager` call on the controller: `fun` with `positional` (each
/// an Elixir literal) and `opts`, the caller's token appended. Returns only
/// when the child succeeded, so the caller's next line is its success path.
pub(crate) fn run(
    fun: &str,
    positional: &[String],
    opts: &[Opt],
    token: Option<&str>,
    failure: Failure,
) {
    verdict(
        flzvm()
            .arg("rpc")
            .arg(expression(fun, positional, opts, token))
            .status(),
        failure,
    );
}

/// The request `run` sends, for the calls that do not go through `run`.
pub(crate) fn expression(
    fun: &str,
    positional: &[String],
    opts: &[Opt],
    token: Option<&str>,
) -> String {
    let mut parts: Vec<String> = positional.to_vec();
    parts.push(keyword_opts_fragment(opts, token));
    format!("Flezha.Service.CliManager.{}({})", fun, parts.join(", "))
}

/// The request whose answer is what the controller printed rather than the
/// code it exited with. The status is still `verdict`'s to judge, so a verb
/// that reads stdout does not also decide for itself whether the call worked.
pub(crate) fn captured(expression: String, failure: Failure) -> String {
    match flzvm()
        .arg("rpc")
        .arg(expression)
        .stderr(Stdio::inherit())
        .output()
    {
        Ok(output) => {
            verdict(Ok(output.status), failure);
            String::from_utf8_lossy(&output.stdout).into_owned()
        }
        Err(e) => {
            verdict(Err(e), failure);
            String::new()
        }
    }
}

/// What `elixir --rpc-eval` says, with exit 1, whenever it could not reach the
/// node: one that is not up yet, one whose cookie is not this shell's, a name
/// nobody holds, and a node lost mid-call all read the same. Its exit code is
/// the one a reached node's refusal has, so this line is the only difference.
const NOCONNECTION: &str = "--rpc-eval : RPC failed with reason :noconnection";

/// `run`, for a verb whose job is to wait for the node: when the node could
/// not be reached at all, the child's status comes back and nothing is printed
/// or exited, so the verb can ask again. The child's stderr is held until it
/// exits, which is why the verbs that stream a failure use `run`.
pub(crate) fn run_if_reachable(expression: String, failure: Failure) -> Result<(), ExitStatus> {
    let output = flzvm()
        .arg("rpc")
        .arg(expression)
        .stdout(Stdio::inherit())
        .output();

    if let Ok(output) = &output {
        if !output.status.success()
            && String::from_utf8_lossy(&output.stderr)
                .lines()
                .any(|line| line == NOCONNECTION)
        {
            return Err(output.status);
        }
        let _ = io::stderr().write_all(&output.stderr);
    }

    verdict(output.map(|output| output.status), failure);
    Ok(())
}

/// Apply a failure policy to a child that has already been waited on.
pub(crate) fn verdict(status: io::Result<ExitStatus>, failure: Failure) {
    let status = match status {
        Ok(status) => status,
        Err(e) => {
            eprintln!(
                "{}: {} ({})",
                failure.unspawnable.unwrap_or(UNSPAWNABLE),
                flzvm_path(),
                e
            );
            exit(failure.exit.fallback());
        }
    };

    if status.success() {
        return;
    }

    if let Some(line) = failure.refused {
        eprintln!("{}", line.replace("{}", &status.to_string()));
    }

    exit(match failure.exit {
        Exit::Fixed(n) => n,
        Exit::Propagate(n) => status.code().unwrap_or(n),
    });
}

/// The read verbs — a namespace, an output format, a progress line only text
/// mode prints, and one RPC taking both. `progress` names what is being
/// fetched; `None` prints no line, which is what `flz cluster status` does.
pub(crate) fn scoped_read(read: &Reading, rpc_fun: &str, progress: Option<&str>) {
    let namespace = read.namespace();
    let output_format = read.output_format();

    if let Some(noun) = progress {
        if output_format == "text" {
            let display_namespace = if namespace.is_empty() {
                "default"
            } else {
                namespace
            };
            println!("Fetching {} for namespace '{}'...", noun, display_namespace);
        }
    }

    run(
        rpc_fun,
        &[text(namespace), text(output_format)],
        &[],
        read.token(),
        REFUSED,
    );
}

/// The verbs whose whole request is a rendering choice: `flz audit stats`,
/// `flz observe sinks`, `flz controller status`. They differ in what they ask
/// for and in what a refusal costs, and in nothing else.
pub(crate) fn rendered(read: &Reading, rpc_fun: &str, failure: Failure) {
    run(
        rpc_fun,
        &[text(read.output_format())],
        &[],
        read.token(),
        failure,
    );
}

/// The window a bounded stream runs for, as the option its request carries.
/// The two verbs that tail name different subjects and share this.
pub(crate) fn window(read: &Reading) -> Opt<'static> {
    let ms = read
        .num::<u64>(DURATION_MS.long)
        .unwrap_or(DEFAULT_WINDOW_MS);

    ("duration_ms", ms.to_string())
}

/// The verbs that name one thing and do one thing to it: a dead-letter entry,
/// an Entity path, a schedule, a plugin, a namespace, an observability source.
/// They differ in the slot's name and in what a refusal costs, and in nothing
/// else.
pub(crate) fn one_arg(read: &Reading, slot: &str, rpc_fun: &str, failure: Failure) {
    run(rpc_fun, &[text(read.pos(slot))], &[], read.token(), failure);
}

/// The authz verbs that take nothing but the caller's token: the three list
/// and identity reads.
pub(crate) fn token_only(read: &Reading, rpc_fun: &str) {
    run(rpc_fun, &[], &[], read.token(), PROPAGATE);
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
/// can carry JSON or free text, so no call site invents its own.
pub(crate) fn text(value: &str) -> String {
    serde_json::Value::String(value.to_string()).to_string()
}

/// The options list every request ends with: `opts` in the order given, then
/// the caller's identity token when there is one — `[]` when it carries
/// neither. The token is the walk's answer: the `--token` flag wherever it was
/// written, else the environment variable its declaration names.
fn keyword_opts_fragment(opts: &[Opt], token: Option<&str>) -> String {
    let mut entries: Vec<String> = opts
        .iter()
        .map(|(key, value)| format!("{}: {}", key, value))
        .collect();

    if let Some(token) = token {
        entries.push(format!("token: {}", text(token)));
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
    if let Ok(v) = env::var("FLEZHA_RESET_PROBE_OVERRIDE") {
        match v.as_str() {
            "up" => return true,
            "down" => return false,
            _ => {}
        }
    }

    let mut child = match flzvm()
        .arg("rpc")
        .arg(expression("boot_status", &[], &[], None))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return false,
    };

    let timeout = Duration::from_millis(probe_timeout_ms);
    let started = Instant::now();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return false;
                }
                let mut out = String::new();
                if let Some(mut stdout) = child.stdout.take() {
                    let _ = std::io::Read::read_to_string(&mut stdout, &mut out);
                }
                return !out.trim().is_empty();
            }
            Ok(None) => {
                if started.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return false;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A token with a quote must not break out of the Elixir keyword literal,
    /// and a verb with nothing else still ends with the list.
    #[test]
    fn the_token_closes_the_options_list_and_is_escaped() {
        assert_eq!(
            expression("x", &[], &[], Some("abc\"def")),
            "Flezha.Service.CliManager.x([token: \"abc\\\"def\"])"
        );
        assert_eq!(
            expression("x", &[], &[], None),
            "Flezha.Service.CliManager.x([])"
        );
    }

    /// Options keep the order they were given, a quote and a backslash inside a
    /// value both stay inside it, and the token closes the list.
    #[test]
    fn options_keep_their_order_and_every_value_is_escaped() {
        let opts = [
            ("cron", text("0 8 * * *")),
            ("init_values", text("{\"a\":\"x\\\"y\"}")),
        ];
        assert_eq!(
            expression("schedule_add", &[text("d")], &opts, Some("t")),
            "Flezha.Service.CliManager.schedule_add(\"d\", [cron: \"0 8 * * *\", init_values: \"{\\\"a\\\":\\\"x\\\\\\\"y\\\"}\", token: \"t\"])"
        );
    }

    /// `flz reset help` quotes the default as a literal; nothing else can keep
    /// the screen and the constant from drifting apart.
    #[test]
    fn the_reset_screen_quotes_the_default_probe_timeout() {
        let quoted = format!("(default {})", DEFAULT_PROBE_TIMEOUT_MS);
        assert!(
            crate::help::RESET.contains(&quoted),
            "flz reset help must say {}",
            quoted
        );
    }
}
