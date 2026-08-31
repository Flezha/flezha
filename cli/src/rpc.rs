//! The one place `flz` starts the sibling `flzvm`, and the one place a refused
//! RPC becomes an exit code.
//!
//! Every verb is the same shape — build an Elixir expression, run
//! `flzvm rpc <expr>`, exit non-zero if it did — so the shape lives here and
//! each verb contributes only its expression and its failure policy.

use std::process::{exit, Command, ExitStatus};
use std::time::{Duration, Instant};
use std::{env, io};

use crate::args;

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
    /// Said on stderr as `"{what}: {error}"` when the child could not be
    /// started at all. `None` panics, which is what most of the tree does.
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

/// Run one `CliManager` call on the controller. Returns only when the child
/// succeeded, so the caller's next line is its success path.
pub(crate) fn run(expr: String, failure: Failure) {
    verdict(flzvm().arg("rpc").arg(expr).status(), failure);
}

/// Apply a failure policy to a child that has already been waited on.
pub(crate) fn verdict(status: io::Result<ExitStatus>, failure: Failure) {
    let status = match status {
        Ok(status) => status,
        Err(e) => match failure.unspawnable {
            Some(what) => {
                eprintln!("{}: {}", what, e);
                exit(failure.exit.fallback());
            }
            // The payload `.expect("Failed to execute command")` produced at
            // every one of these sites before they shared this one.
            None => panic!("Failed to execute command: {:?}", e),
        },
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
pub(crate) fn scoped_read(args: &[String], rpc_fun: &str, progress: Option<&str>) {
    let namespace = args::namespace(args);
    let output_format = args::output_format(args);

    if let Some(noun) = progress {
        if output_format == "text" {
            let display_namespace = if namespace.is_empty() {
                "default"
            } else {
                &namespace
            };
            println!("Fetching {} for namespace '{}'...", noun, display_namespace);
        }
    }

    run(
        format!(
            "Flezha.Service.CliManager.{}(\"{}\", \"{}\")",
            rpc_fun, namespace, output_format
        ),
        REFUSED,
    );
}

/// The authz verbs that take one argument and the caller's token:
/// `role unbind`, `token revoke`, `namespace delete`, `namespace show`.
pub(crate) fn one_arg_authz(args: &[String], rpc_fun: &str, usage: &str) {
    if args.len() < 2 {
        eprintln!("{}", usage);
        exit(1);
    }

    run(
        format!(
            "Flezha.Service.CliManager.{}(\"{}\", {})",
            rpc_fun,
            escape_rpc_arg(&args[1]),
            token_opts_fragment(&args[2..]),
        ),
        PROPAGATE,
    );
}

/// The authz verbs that take nothing but the caller's token: the three list
/// and identity reads.
pub(crate) fn token_only(args: &[String], rpc_fun: &str) {
    run(
        format!(
            "Flezha.Service.CliManager.{}({})",
            rpc_fun,
            token_opts_fragment(&args[1..]),
        ),
        PROPAGATE,
    );
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

/// Escape a value for interpolation into the quoted Elixir string literals of
/// an RPC expression. Every argument that can carry JSON or free text goes
/// through this — one escaper, so no call site invents its own.
pub(crate) fn escape_rpc_arg(value: &str) -> String {
    value.replace('"', "\\\"")
}

/// The caller's identity token: `--token <t>` flag, else the FLEZHA_TOKEN
/// environment variable. The CLI process resolves it (the controller's own
/// env is NOT the caller's) and threads it into every gated verb's opts.
pub(crate) fn auth_token(args: &[String]) -> Option<String> {
    if let Some(t) = args::extract_param(args, "--token", "--token") {
        if !t.is_empty() {
            return Some(t);
        }
    }

    env::var("FLEZHA_TOKEN").ok().filter(|v| !v.is_empty())
}

/// Elixir keyword-list fragment carrying the identity token for a CliManager
/// wrapper's `opts` parameter: `[token: "…"]`, or `[]` when unauthenticated.
pub(crate) fn token_opts_fragment(args: &[String]) -> String {
    match auth_token(args) {
        Some(t) => format!("[token: \"{}\"]", escape_rpc_arg(&t)),
        None => "[]".to_string(),
    }
}

/// JSON opts object (as an Elixir string literal) for wrappers taking an
/// `opts_json` argument: `"{\"token\": \"…\"}"`, or `"{}"`.
pub(crate) fn token_opts_json_literal(args: &[String]) -> String {
    match auth_token(args) {
        Some(t) => {
            let json = serde_json::json!({ "token": t }).to_string();
            serde_json::Value::String(json).to_string()
        }
        None => "\"{}\"".to_string(),
    }
}

pub(crate) const DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;

/// Whether a controller is serving on this node — `flz reset` and
/// `flz auth rebootstrap` both refuse to run while one is.
///
/// A successful `controller_status` RPC with non-empty stdout inside the
/// timeout means up; every other outcome (no node, timeout, distribution
/// failure) means not up, so the caller proceeds. `FLEZHA_RESET_PROBE_OVERRIDE`
/// short-circuits it to `up` or `down` for both callers' tests.
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
        .arg("Flezha.Service.CliManager.controller_status(\"text\")")
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
    use crate::args::owned as v;

    /// A token with a quote must not break out of the Elixir keyword literal.
    #[test]
    fn token_opts_fragment_uses_the_flag_and_escapes() {
        let args = v(&["--token", "abc\"def"]);
        assert_eq!(token_opts_fragment(&args), "[token: \"abc\\\"def\"]");
    }

    #[test]
    fn token_opts_json_literal_is_a_decodable_elixir_string() {
        let args = v(&["--token", "flzt_abc"]);
        let literal = token_opts_json_literal(&args);
        // The literal itself is a JSON string whose content is a JSON object.
        let inner: String = serde_json::from_str(&literal).expect("outer string");
        let parsed: serde_json::Value = serde_json::from_str(&inner).expect("inner object");
        assert_eq!(parsed["token"], "flzt_abc");
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
