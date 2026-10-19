use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use flz_transport::errors::FlezhaError;
use serde_json::Value;

use crate::args::{app_data_root, Positional, Reading, APP_DATA_ROOT};
use crate::dispatch::{Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, controller_is_up, DEFAULT_PROBE_TIMEOUT_MS};

pub(crate) const TABLE: Command = Command {
    path: "flz auth",
    title: "Authentication bootstrap and identity.",
    notes: Some(help::AUTH),
    verbs: &[
        Verb::new("bootstrap", handle_bootstrap)
            .taking(&[Positional::required("<token>")])
            .calls(&["Namespace.bootstrap"]),
        Verb::new("whoami", rpc::bare).calls(&["Namespace.whoami"]),
        Verb::new("rebootstrap", handle_rebootstrap)
            .reading(&[APP_DATA_ROOT])
            .about("Break-glass: recover a lost admin@_system."),
    ],
    missing: Missing::Screen,
    unknown_prints_help: false,
};

fn handle_bootstrap(read: &Reading) {
    rpc::door(
        "Namespace.bootstrap",
        &[Value::from(read.pos("<token>"))],
        &[],
        read,
    );
}

// Break-glass recovery for a lost admin@_system. Both readings need filesystem
// access to `app_data/auth/` and nothing else — the same trust the original
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
    let auth_dir = match std::env::var("FLEZHA_AUTHZ_DIR") {
        Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => app_data_root(read).join("auth"),
    };

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
    println!("    flz auth bootstrap {}", token);
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
    if let Err(e) = getrandom::getrandom(&mut raw) {
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
