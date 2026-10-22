//! The fixture `flz`'s integration tests share: a copy of the binary under
//! test with a `flzvm` shim beside it.
//!
//! `flz` resolves `flzvm` next to its own executable rather than on `PATH`, so
//! standing in for `flzvm` means copying `flz` somewhere writable first. What
//! the shim does with the call — refuse it, echo it — is each test's own.

#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use tempfile::TempDir;

/// A `flzvm` that prints each argument on its own line, then its whole stdin,
/// and succeeds, so the request `flz` built is readable on stdout.
pub const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$@\"\ncat\nexit 0\n";

/// How an expression binds the arguments line of its stdin, ahead of its call.
pub const READ_ARGUMENTS: &str = "arguments = IO.read(:line); ";

/// The call every request to a running controller makes.
pub const DOOR: &str = "Flezha.Service.Cli.call(";

/// The door call a run against `ECHOING` reached, with the arguments line its
/// stdin carried written where the call names it.
pub fn door_request(stdout: &str) -> Option<String> {
    let mut lines = stdout.lines().skip_while(|line| {
        !line
            .strip_prefix(READ_ARGUMENTS)
            .is_some_and(|call| call.starts_with(DOOR))
    });
    let call = lines.next()?.strip_prefix(READ_ARGUMENTS)?;
    let arguments = lines.next().unwrap_or("<no arguments line>");
    Some(call.replacen(", arguments, ", &format!(", {arguments}, "), 1))
}

/// The stdin a run over every verb gives each one: `flz auth bootstrap` reads
/// its token from it, and no other verb reads it.
pub const BOOTSTRAP_INPUT: &str = "bootstrap-token\n";

/// Run `cmd` with `input` as its whole stdin.
pub fn output_fed(mut cmd: Command, input: &str) -> Output {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flz runs");
    // A run that exits before reading closes the pipe; its output says why.
    let _ = child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes());
    child.wait_with_output().expect("flz ends")
}

pub struct Shim {
    _tmp: TempDir,
    flz: PathBuf,
    flzvm: PathBuf,
}

impl Shim {
    /// `script` is the whole `flzvm` shim, `#!` line included.
    pub fn new(flz_bin: &str, script: &str) -> Self {
        Shim::beside(flz_bin, Some(script))
    }

    /// A `flz` with NO sibling at all: the checkout where the release was
    /// never built, or the directory it was not built in.
    pub fn without_flzvm(flz_bin: &str) -> Self {
        Shim::beside(flz_bin, None)
    }

    fn beside(flz_bin: &str, script: Option<&str>) -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let flz = tmp.path().join("flz");
        // A link opens no write handle, so a test thread forking meanwhile
        // cannot hold one across this binary's exec (`Text file busy`).
        fs::hard_link(flz_bin, &flz)
            .or_else(|_| fs::copy(flz_bin, &flz).map(drop))
            .expect("place the flz under test");
        let flzvm = tmp.path().join("flzvm");
        if let Some(script) = script {
            fs::write(&flzvm, script).expect("write shim");
            fs::set_permissions(&flzvm, fs::Permissions::from_mode(0o755)).expect("chmod shim");
        }
        Shim {
            _tmp: tmp,
            flz,
            flzvm,
        }
    }

    /// Where this `flz` looks for its sibling, and so what a refusal that
    /// names the path it could not run has to say.
    pub fn flzvm_path(&self) -> &PathBuf {
        &self.flzvm
    }

    /// The copied `flz`, for a caller that adds environment of its own.
    pub fn cmd(&self) -> Command {
        Command::new(&self.flz)
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().expect("flz runs")
    }
}
