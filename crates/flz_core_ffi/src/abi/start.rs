//! Starting the runner loop, and stopping it.

use std::ffi::{c_char, CStr};
use std::time::Duration;

use flz_core::Code;

use crate::{set_init_error, Error, FlzContext, FLZ_ERR, FLZ_OK};

/// Connect to the controller and start the runner loop. Transport
/// configuration (socket path, `FLEZHA_TRANSPORT`) rides the same
/// environment the Rust SDK reads. A connect the controller refuses fails
/// with the controller's own error (for example `runner.namespace_not_proven`).
///
/// # Safety
/// `ctx` must be live; `namespace` must be a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn flz_core_start(ctx: *mut FlzContext, namespace: *const c_char) -> i32 {
    live!(mut ctx);
    let Ok(namespace) = CStr::from_ptr(namespace).to_str() else {
        return ctx.fail(Error::Utf8("namespace"));
    };
    start_runner(ctx, namespace, Vec::new())
}

/// [`flz_core_start`], with the connection settings handed over as a JSON
/// object instead of read from the environment: `socket_files_path`,
/// `transport`, `quic_addresses`, `ca_cert_path`, `quic_client_cert`,
/// `quic_client_key` and `namespace_secret`, each a string standing in for
/// its `FLEZHA_*` variable. A key left out resolves as `flz_core_start`
/// resolves it. An unknown key or a value that is not a string is refused
/// before anything connects. The process environment is never written.
///
/// # Safety
/// `ctx` must be live; `namespace` must be a NUL-terminated string;
/// `settings`/`settings_len` must describe a readable buffer.
#[no_mangle]
pub unsafe extern "C" fn flz_core_start_with(
    ctx: *mut FlzContext,
    namespace: *const c_char,
    settings: *const u8,
    settings_len: usize,
) -> i32 {
    live!(mut ctx);
    let Ok(namespace) = CStr::from_ptr(namespace).to_str() else {
        return ctx.fail(Error::Utf8("namespace"));
    };
    let settings = if settings.is_null() {
        &[][..]
    } else {
        std::slice::from_raw_parts(settings, settings_len)
    };
    match connect_settings(settings) {
        Ok(handed_over) => start_runner(ctx, namespace, handed_over),
        Err(refusal) => ctx.fail(refusal),
    }
}

/// The settings document as `(variable, value)` pairs.
fn connect_settings(bytes: &[u8]) -> Result<Vec<(&'static str, String)>, Error> {
    let refused = |setting: &str, reason| Error::ConnectSetting {
        setting: setting.to_string(),
        reason,
    };
    let document: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)
        .map_err(|_| refused("", "the settings are not a JSON object"))?;
    document
        .into_iter()
        .map(|(key, value)| {
            let var = flz_transport::env::CONNECT_SETTINGS
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, var)| *var)
                .ok_or_else(|| refused(&key, "not a connection setting"))?;
            match value {
                serde_json::Value::String(value) => Ok((var, value)),
                _ => Err(refused(&key, "its value is not a string")),
            }
        })
        .collect()
}

fn start_runner(
    ctx: &mut FlzContext,
    namespace: &str,
    handed_over: Vec<(&'static str, String)>,
) -> i32 {
    if flz_core::poll_core::global().is_shut_down() {
        return ctx.fail(Error::ShutDown);
    }
    ctx.status(ctx.session.start(namespace, handed_over))
}

/// Ship every live relay's state as a `RunnerShutdownNotification`, then
/// wake every blocked poller with [`FLZ_POLL_SHUTTING_DOWN`] and stop the
/// runner loop. Unpolled work follows the existing durability rules (it is
/// journaled controller-side and re-forwarded on the next bind).
///
/// Returns at once. The notification serializes each Cell through the host,
/// which needs the caller's loops still polling, so the sequence runs on its
/// own thread and the core reports shutting down only once it is done. That
/// thread reads nothing of `ctx`, so freeing the context cannot race it. The
/// context's session ends at once, so a host's cleanup undeclare unregisters
/// locally; the runner loop is abandoned last: a stopping runner that
/// reconnected would be handed work it will never do.
///
/// # Safety
/// `ctx` must be live.
#[no_mangle]
pub unsafe extern "C" fn flz_core_shutdown(ctx: *mut FlzContext) {
    let Some(ctx) = ctx.as_ref() else {
        return;
    };
    ctx.session.end();
    let sequence = || {
        flz_core::shutdown::exit(flz_core::shutdown::Exit {
            wait: Some(Duration::ZERO),
            abandon: true,
            ..Default::default()
        })
    };
    if std::thread::Builder::new()
        .name("flz-shutdown".into())
        .spawn(sequence)
        .is_err()
    {
        flz_transport::notice!(Error, "no shutdown thread: stopping without shipping state");
        flz_core::poll_core::global().shutdown();
    }
}

/// The host installs the SIGTERM and SIGINT handlers itself, so a start installs
/// none and the host hands a signal over through [`flz_core_terminate`]. Call it
/// before the first [`flz_core_start`]: once the core has installed its own it
/// answers [`FLZ_ERR`], `runtime.runner_already_running` in
/// [`flz_core_init_error`], and the core's handler stays. Idempotent.
///
/// [`flz_core_init_error`]: crate::flz_core_init_error
#[no_mangle]
pub extern "C" fn flz_core_host_owns_signals() -> i32 {
    match flz_core::shutdown::host_owns_signals() {
        Ok(()) => FLZ_OK,
        Err(_) => {
            set_init_error(Error::Refused(Code::RunnerAlreadyRunning.with([])));
            FLZ_ERR
        }
    }
}

/// End the runner as the core's own termination-signal handler does: ship every
/// relay's state, drain within the same grace, and exit the process with 0. It
/// does not return. `reason` names the signal in the notice it writes.
///
/// # Safety
/// `reason` must be NULL or a NUL-terminated string.
#[no_mangle]
pub unsafe extern "C" fn flz_core_terminate(reason: *const c_char) -> ! {
    let reason = if reason.is_null() {
        std::borrow::Cow::Borrowed("signal")
    } else {
        CStr::from_ptr(reason).to_string_lossy()
    };
    flz_core::shutdown::terminate(&reason)
}

/// Watch standard input and end the process when it reaches end of file, on a
/// thread the core owns, so no host reads descriptor 0 itself
/// ([`flz_core::shutdown::watch_stdin_eof`] says why). The watch ends the runner
/// as the SIGTERM handler does, with the same grace. Idempotent: the second
/// call installs nothing.
#[no_mangle]
pub extern "C" fn flz_core_watch_stdin_eof() {
    flz_core::shutdown::watch_stdin_eof();
}
