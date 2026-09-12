//! The fixture `flz`'s integration tests share: a copy of the binary under
//! test with a `flzvm` shim beside it.
//!
//! `flz` resolves `flzvm` next to its own executable rather than on `PATH`, so
//! standing in for `flzvm` means copying `flz` somewhere writable first. What
//! the shim does with the call — refuse it, echo it — is each test's own.

#![allow(dead_code)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

use tempfile::TempDir;

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
        fs::copy(flz_bin, &flz).expect("copy the flz under test");
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
