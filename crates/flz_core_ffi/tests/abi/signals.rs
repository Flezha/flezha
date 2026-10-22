//! A host that owns its termination signals: `flz_core_host_owns_signals`
//! keeps the core's handler out, `flz_core_terminate` ends the runner the way
//! that handler does, and a host that calls neither keeps the handler. Each
//! case runs in a child process of this test binary: a signal disposition is
//! process-wide, and `terminate` ends the process.

use std::ffi::{c_int, CStr, CString};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core_ffi::*;
use flz_transport::transport::FlezhaStream;

use crate::harness::{abi_complete, ok_effect, Controller};

const CHILD: &str = "FLZ_ABI_SIGNALS_CHILD";

/// Run `test` of this module alone in a child: `None` inside it, its output
/// outside.
fn child(test: &str) -> Option<Output> {
    if std::env::var_os(CHILD).is_some() {
        return None;
    }
    Some(
        Command::new(std::env::current_exe().expect("the test binary"))
            .args(["--exact", &format!("signals::{test}"), "--nocapture"])
            .env(CHILD, "1")
            .output()
            .expect("the child runs"),
    )
}

fn assert_passed(out: &Output) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// The handler `signal` runs now.
fn disposition(signal: c_int) -> libc::sighandler_t {
    // SAFETY: a read of the current action into a zeroed out-parameter.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        assert_eq!(libc::sigaction(signal, std::ptr::null(), &mut action), 0);
        action.sa_sigaction
    }
}

extern "C" fn host_handler(_: c_int) {}

/// [`host_handler`] as a disposition.
fn host() -> libc::sighandler_t {
    host_handler as *const () as libc::sighandler_t
}

/// A context, connected to a fake controller and running one relay.
fn started() -> (*mut FlzContext, Controller) {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null());
    let controller = Controller::start(&crate::lifecycle::socket_dir());
    crate::lifecycle::declares_and_starts(ctx, &controller);
    (ctx, controller)
}

#[test]
fn the_core_installs_no_handler_for_a_host_that_owns_signals() {
    let Some(out) = child("the_core_installs_no_handler_for_a_host_that_owns_signals") else {
        // SAFETY: installs a plain function with no captured state.
        unsafe {
            libc::signal(libc::SIGTERM, host());
            libc::signal(libc::SIGINT, host());
        }
        assert_eq!(flz_core_host_owns_signals(), FLZ_OK);
        assert_eq!(
            flz_core_host_owns_signals(),
            FLZ_OK,
            "a second call is a no-op"
        );
        let _running = started();
        for signal in [libc::SIGTERM, libc::SIGINT] {
            assert_eq!(disposition(signal), host());
        }
        return;
    };
    assert_passed(&out);
}

#[test]
fn terminate_drains_like_the_signal_does() {
    let Some(out) = child("terminate_drains_like_the_signal_does") else {
        assert_eq!(flz_core_host_owns_signals(), FLZ_OK);
        let (ctx, _controller) = started();
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let act =
            crate::lifecycle::routes_two_entry_payloads(ctx, &Arc::new(FlezhaStream::unix(sock)));
        // The activation is still in flight when the host hands the signal
        // over; the drain waits for it.
        let ctx_addr = ctx as usize;
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let result = schema::ActivationResult {
                token: act.token,
                outcome: schema::ActivationOutcome::Work(vec![
                    ok_effect(None, vec![]),
                    ok_effect(None, vec![]),
                ]),
            };
            abi_complete(ctx_addr as *mut FlzContext, &result);
            println!("in-flight activation completed");
        });
        let reason = CString::new("SIGTERM").unwrap();
        // SAFETY: a NUL-terminated reason; the call ends the process.
        unsafe { flz_core_terminate(reason.as_ptr()) }
    };
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{stdout}{stderr}");
    assert!(
        stdout.contains("in-flight activation completed"),
        "the drain did not wait for the activation in flight: {stdout}{stderr}"
    );
    assert!(
        stderr.contains("termination signal received (SIGTERM); draining"),
        "{stderr}"
    );
}

#[test]
fn owning_signals_after_start_is_refused() {
    let Some(out) = child("owning_signals_after_start_is_refused") else {
        let _running = started();
        let installed = disposition(libc::SIGTERM);
        assert_eq!(flz_core_host_owns_signals(), FLZ_ERR);
        // SAFETY: the refusal left a NUL-terminated document on this thread.
        let document = unsafe { CStr::from_ptr(flz_core_init_error()) }.to_string_lossy();
        assert!(
            document.contains("\"code\":\"runtime.runner_already_running\""),
            "{document}"
        );
        assert_eq!(disposition(libc::SIGTERM), installed, "the handler stays");
        return;
    };
    assert_passed(&out);
}

#[test]
fn a_host_that_calls_neither_keeps_the_core_handler() {
    let Some(out) = child("a_host_that_calls_neither_keeps_the_core_handler") else {
        assert_eq!(disposition(libc::SIGTERM), libc::SIG_DFL);
        let _running = started();
        let deadline = Instant::now() + Duration::from_secs(5);
        while disposition(libc::SIGTERM) == libc::SIG_DFL && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        for signal in [libc::SIGTERM, libc::SIGINT] {
            assert_ne!(
                disposition(signal),
                libc::SIG_DFL,
                "the core installs its handler"
            );
        }
        return;
    };
    assert_passed(&out);
}
