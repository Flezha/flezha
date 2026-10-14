//! `flz_core_start_with`: the connection settings a host hands over take the
//! place of the environment, a key left out falls back to it, and a key the
//! core does not define is refused before anything connects. A start the
//! controller refuses leaves no runner dialling behind it.
//!
//! The starting scenarios each run in a child process of this test binary,
//! because the runner is process-global.

use std::ffi::CString;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use flz_core_ffi::*;
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::harness::{last_document, last_error, Controller};

const CHILD: &str = "FLZ_ABI_SETTINGS_CHILD";

fn in_child(test: &str) {
    let status = Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, "1")
        .status()
        .expect("run the scenario in a child");
    assert!(status.success(), "{test} failed in its child");
}

fn dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("flz-set-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_with(ctx: *mut FlzContext, settings: &str) -> i32 {
    let ns = CString::new("settings_ns").unwrap();
    unsafe { flz_core_start_with(ctx, ns.as_ptr(), settings.as_ptr(), settings.len()) }
}

fn context() -> *mut FlzContext {
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir("id").join("runner.id"));
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null());
    ctx
}

#[test]
fn an_explicit_socket_directory_wins_and_leaves_the_environment() {
    if std::env::var_os(CHILD).is_none() {
        return in_child("settings::an_explicit_socket_directory_wins_and_leaves_the_environment");
    }
    let named_by_env = dir("env");
    let handed_over = dir("handed");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &named_by_env);
    let controller = Controller::start(&handed_over);

    let ctx = context();
    let settings = format!(
        r#"{{"socket_files_path": "{}", "transport": "uds"}}"#,
        handed_over.display()
    );
    assert_eq!(start_with(ctx, &settings), FLZ_OK, "{}", last_error(ctx));
    assert_eq!(
        controller.accepted(),
        1,
        "dialled the handed-over directory"
    );
    assert_eq!(
        std::env::var_os("FLEZHA_SOCKET_FILES_PATH"),
        Some(named_by_env.into_os_string()),
        "the environment is untouched"
    );
}

#[test]
fn an_absent_key_falls_back_to_the_environment() {
    if std::env::var_os(CHILD).is_none() {
        return in_child("settings::an_absent_key_falls_back_to_the_environment");
    }
    let named_by_env = dir("fallback");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &named_by_env);
    let controller = Controller::start(&named_by_env);

    let ctx = context();
    assert_eq!(
        start_with(ctx, r#"{"transport": "uds"}"#),
        FLZ_OK,
        "{}",
        last_error(ctx)
    );
    assert_eq!(
        controller.accepted(),
        1,
        "dialled the environment's directory"
    );
}

#[test]
fn an_unknown_key_or_a_wrong_type_is_refused_before_connecting() {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    for (settings, setting) in [
        (r#"{"socket_dir": "/nowhere"}"#, "socket_dir"),
        (r#"{"transport": 1}"#, "transport"),
        ("[]", ""),
    ] {
        assert_eq!(start_with(ctx, settings), FLZ_ERR);
        let refusal = last_document(ctx);
        assert_eq!(refusal.code, "runtime.invalid_connect_setting");
        assert_eq!(
            refusal.details.get("setting").and_then(|v| v.as_str()),
            Some(setting)
        );
    }
    let ns = CString::new("settings_ns").unwrap();
    let no_settings = unsafe { flz_core_start_with(ctx, ns.as_ptr(), std::ptr::null(), 0) };
    assert_eq!(no_settings, FLZ_ERR, "a NULL document is refused, not read");
    assert_eq!(last_document(ctx).code, "runtime.invalid_connect_setting");
    unsafe { flz_core_context_free(ctx) };
}

#[test]
fn a_refused_start_leaves_no_runner_dialling() {
    if std::env::var_os(CHILD).is_none() {
        return in_child("settings::a_refused_start_leaves_no_runner_dialling");
    }
    let dir = dir("refused");
    let path = flz_transport::utility::socket_dir::control_socket_path_in(&dir).unwrap();
    let listener = UnixListener::bind(path).unwrap();
    let dials = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&dials);
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            counted.fetch_add(1, Ordering::SeqCst);
            let stream = FlezhaStream::unix(stream);
            let _connect = payloader::extract_payload(&stream);
            let refusal = flz_core::pb::CommandRequest {
                reference_id: String::new(),
                command: Some(flz_core::pb::command_request::Command::ConnectRefused(
                    flz_core::pb::ConnectRefused {
                        error: Some(flz_core::pb::Error {
                            code: "runner.namespace_not_proven".into(),
                            class: "denied".into(),
                            message: "refused by the test".into(),
                            details_json: "{}".into(),
                            hint: String::new(),
                        }),
                    },
                )),
            };
            let frame =
                payloader::build_without_metadata("control_command", &refusal.encode_to_vec());
            let _ = stream.write_frame(&frame);
        }
    });

    let ctx = context();
    let settings = format!(r#"{{"socket_files_path": "{}"}}"#, dir.display());
    assert_eq!(start_with(ctx, &settings), FLZ_ERR);
    assert_eq!(last_document(ctx).code, "runner.namespace_not_proven");

    // Past several rungs of the reconnect ladder a runner left behind would climb.
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        dials.load(Ordering::SeqCst),
        1,
        "the refused runner dialled again"
    );

    // A plain start after it reads the environment, not what the refused
    // start handed over.
    let named_by_env = std::env::temp_dir().join(format!("flz-set-{}-plain", std::process::id()));
    std::fs::create_dir_all(&named_by_env).unwrap();
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &named_by_env);
    let controller = Controller::start(&named_by_env);
    let ctx = context();
    let ns = CString::new("settings_ns").unwrap();
    assert_eq!(
        unsafe { flz_core_start(ctx, ns.as_ptr()) },
        FLZ_OK,
        "{}",
        last_error(ctx)
    );
    assert_eq!(
        controller.accepted(),
        1,
        "dialled the environment's directory"
    );
}
