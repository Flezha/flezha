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
}

impl Shim {
    /// `script` is the whole `flzvm` shim, `#!` line included.
    pub fn new(flz_bin: &str, script: &str) -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let flz = tmp.path().join("flz");
        fs::copy(flz_bin, &flz).expect("copy the flz under test");
        let shim = tmp.path().join("flzvm");
        fs::write(&shim, script).expect("write shim");
        fs::set_permissions(&shim, fs::Permissions::from_mode(0o755)).expect("chmod shim");
        Shim { _tmp: tmp, flz }
    }

    /// The copied `flz`, for a caller that adds environment of its own.
    pub fn cmd(&self) -> Command {
        Command::new(&self.flz)
    }

    pub fn run(&self, args: &[&str]) -> Output {
        self.cmd().args(args).output().expect("flz runs")
    }
}
