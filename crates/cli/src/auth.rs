use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::Path;

use flz_transport::errors::FlezhaError;
use serde_json::Value;

use crate::args::{app_dir, storage_root, Reading, STORAGE_ROOT};
use crate::dispatch::{Arg, Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, controller_is_up, DEFAULT_PROBE_TIMEOUT_MS};

pub(crate) const TABLE: Command = Command {
    path: "flz auth",
    title: "Authentication bootstrap and identity.",
    notes: Some(help::AUTH),
    verbs: &[
        Verb::door("bootstrap")
            .sending(&[Arg::Built(bootstrap_token)])
            .strict()
            .calls(&["Namespace.bootstrap"]),
        Verb::door("whoami").calls(&["Namespace.whoami"]),
        Verb::new("rebootstrap", handle_rebootstrap)
            .reading(&[STORAGE_ROOT])
            .about("Break-glass: recover a lost admin@_system."),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

const CLAIM: &str = "flz auth bootstrap < storage/app/auth/bootstrap.token";

/// The one-time token, as the first line of `flz`'s own stdin: a credential
/// written on a command line is readable by any local user. A terminal is
/// refused rather than waited on, since nothing would say it is being read.
fn bootstrap_token(_read: &Reading) -> Option<Value> {
    let stdin = io::stdin();
    let mut line = String::new();
    if !stdin.is_terminal() {
        let _ = stdin.lock().read_line(&mut line);
    }
    match line.trim() {
        "" => rpc::fail(FlezhaError::with(
            "usage.missing_argument",
            [("usage", CLAIM.into())],
        )),
        token => Some(Value::from(token)),
    }
}

// Break-glass recovery for a lost admin@_system. Both readings need filesystem
// access to `storage/app/auth/` and nothing else — the same trust the original
// bootstrap assumes, and already enough to stop the controller.
//
// Controller UP: mint a bootstrap token and write it where `Flezha.AuthZ.Bootstrap`
// already reads one, then print it. The claim replaces the lost admin, and no
// dataflow stops.
//
// Controller DOWN: write the REBOOTSTRAP marker; the next boot consumes it,
// invalidates the stale admin bindings/tokens, and re-enters the ordinary
// bootstrap flow with a fresh token.
fn handle_rebootstrap(read: &Reading) {
    let auth_dir = flz_transport::env::env_dir("FLEZHA_AUTHZ_DIR")
        .unwrap_or_else(|| app_dir(&storage_root(read)).join("auth"));

    if !auth_dir.is_dir() {
        rpc::fail(FlezhaError::with(
            "cli.auth_dir_missing",
            [("path", auth_dir.display().to_string().into())],
        ));
    }

    if controller_is_up(DEFAULT_PROBE_TIMEOUT_MS) {
        recover_online(&auth_dir);
    } else {
        recover_offline(&auth_dir);
    }
}

fn recover_online(auth_dir: &Path) {
    let path = auth_dir.join("bootstrap.token");
    let token = mint_bootstrap_token();

    write_owner_only(&path, token.as_bytes());

    println!("Bootstrap token written to {}.", path.display());
    println!("Claim it against the RUNNING controller:");
    println!("    flz auth bootstrap < {}", path.display());
    println!(
        "The claim unbinds every admin@_system and revokes its _system admin tokens \
         before binding the new admin, and it is audited as a recovery."
    );
}

fn recover_offline(auth_dir: &Path) {
    let marker = auth_dir.join("REBOOTSTRAP");

    write_owner_only(&marker, b"rebootstrap requested\n");

    println!("Recovery marker written to {}.", marker.display());
    println!(
        "Start the controller: it will invalidate the previous admin@_system binding \
         and its _system admin tokens, then write a fresh bootstrap.token to claim."
    );
}

/// 256 bits from the OS entropy source, base16. The value is a credential, so it
/// does not come from the decorrelation noise `flz_transport` ships.
fn mint_bootstrap_token() -> String {
    let mut raw = [0u8; 32];
    if let Err(e) = getrandom::fill(&mut raw) {
        rpc::fail(FlezhaError::with(
            "cli.entropy_unavailable",
            [("reason", e.to_string().into())],
        ));
    }
    raw.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Write `bytes` to `path` readable by its owner alone — the file holds a
/// credential, and the directory's mode is not enough on its own.
fn write_owner_only(path: &Path, bytes: &[u8]) {
    if let Err(e) = fs::File::create(path).and_then(|mut f| f.write_all(bytes)) {
        rpc::write_failed(path.display(), e);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Err(e) = fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
            rpc::write_failed(path.display(), e);
        }
    }
}
