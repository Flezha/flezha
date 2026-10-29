use std::ffi::{c_char, CStr};
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;

use flz_core::pb;
use flz_transport::errors::FlezhaError;

use super::*;

/// A host's own refusal renders as the catalogue says; details that are
/// not a JSON object render nothing.
#[test]
fn a_host_refusal_renders_through_the_catalogue() {
    let code = CString::new("runtime.identity_too_long").unwrap();
    let details = CString::new(r#"{"part":"tasklet name","value":"t","limit":255}"#).unwrap();
    let document = unsafe { flz_core_error_document(code.as_ptr(), details.as_ptr()) };
    let document = unsafe { CStr::from_ptr(document) }
        .to_string_lossy()
        .into_owned();
    let error = FlezhaError::from_document(&document).expect("an error document");
    assert_eq!(error.code, "runtime.identity_too_long");
    assert_eq!(
        error.message,
        "the tasklet name 't' is longer than 255 bytes"
    );

    let list = CString::new("[]").unwrap();
    assert!(unsafe { flz_core_error_document(code.as_ptr(), list.as_ptr()) }.is_null());
}

/// A refused check leaves one error document naming every issue, and
/// hands nothing out through the buffer.
#[test]
fn a_refused_check_names_every_issue_in_the_error_document() {
    let decl = br#"{"factory_id":"ffi_check","sdk_lang":1,"sdk_version":"0.1.0",
        "durable":{"false":{"sleep_after":1000}},"tasklets":[],"channels":[]}"#;
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len = 0;
    let status =
        unsafe { flz_core_check_class(ctx, decl.as_ptr(), decl.len(), &mut buf, &mut len) };

    assert_eq!(status, FLZ_ERR);
    assert!(buf.is_null(), "a refusal rides last_error alone");
    let document = unsafe { CStr::from_ptr(flz_core_last_error(ctx)) }
        .to_string_lossy()
        .into_owned();
    let error = FlezhaError::from_document(&document).expect("an error document");
    let codes: Vec<&str> = error.details["issues"]
        .as_array()
        .expect("details.issues")
        .iter()
        .filter_map(|issue| issue["code"].as_str())
        .collect();
    assert_eq!(codes, ["topology.no_tasklets", "topology.unknown_field"]);
    assert_eq!(error.code, codes[0], "the first issue names the refusal");
    unsafe { flz_core_context_free(ctx) };
}

static NOTICES: Mutex<Vec<String>> = Mutex::new(Vec::new());

extern "C" fn noted(_level: i32, text: *const c_char) {
    // SAFETY: the core hands a NUL-terminated text borrowed for the call.
    let text = unsafe { CStr::from_ptr(text) }
        .to_string_lossy()
        .into_owned();
    NOTICES.lock().unwrap().push(text);
}

/// A connect the controller refuses fails the start with the
/// controller's own error. The runner is process-global, so this is the
/// only start this test binary makes.
#[test]
fn a_refused_connect_fails_the_start_with_the_controllers_error() {
    let dir = std::env::temp_dir().join(format!("flz-ffi-refused-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("socket dir");
    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    let path = flz_transport::utility::socket_dir::control_socket_path_in(&dir)
        .expect("control socket path");
    let listener = UnixListener::bind(path).expect("bind the control socket");

    let refusal = FlezhaError::new("runner.namespace_not_proven", Default::default());
    let frame = flz_core::command::framed_request(
        String::new(),
        pb::command_request::Command::ConnectRefused(pb::ConnectRefused {
            error: Some(pb::Error::from(&refusal)),
        }),
    );
    let controller = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("the runner connects");
        let _ = stream.read(&mut [0u8; 4096]);
        stream.write_all(&frame).expect("write the refusal");
    });

    flz_core_set_log_callback(Some(noted));
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    let namespace = CString::new("unproven").expect("a namespace");
    let status = unsafe { flz_core_start(ctx, namespace.as_ptr()) };
    controller.join().expect("the controller answered");
    let said = "the controller refused the connect: error: ";
    assert!(NOTICES.lock().unwrap().iter().any(|n| n.starts_with(said)));

    assert_eq!(status, FLZ_ERR, "a refused connect is not a started runner");
    let document = unsafe { CStr::from_ptr(flz_core_last_error(ctx)) }
        .to_string_lossy()
        .into_owned();
    assert_eq!(FlezhaError::from_document(&document), Some(refusal));
}

/// Threads failing on one context at once each read their own refusal: the
/// last error is kept per thread, and only for the context it was recorded on.
#[test]
fn threads_failing_on_one_context_each_read_their_own_error() {
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION) as usize;
    let threads: Vec<_> = (0..8)
        .map(|t| {
            std::thread::spawn(move || {
                for i in 0..300 {
                    let id = CString::new(format!("factory-{t}-{i}")).unwrap();
                    let ctx = ctx as *mut FlzContext;
                    assert_eq!(
                        unsafe { flz_core_undeclare_class(ctx, id.as_ptr()) },
                        FLZ_ERR
                    );
                    let document = unsafe { CStr::from_ptr(flz_core_last_error(ctx)) }
                        .to_string_lossy()
                        .into_owned();
                    assert!(
                        document.contains(&format!("'factory-{t}-{i}'")),
                        "thread {t} read another refusal: {document}"
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread
            .join()
            .expect("a thread read its own refusal every time");
    }
    let other = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(
        unsafe { flz_core_last_error(other) }.is_null(),
        "a refusal is read back only on the context it was recorded on"
    );
    unsafe {
        flz_core_context_free(other);
        flz_core_context_free(ctx as *mut FlzContext);
    }
}

/// A context the allocator places where a freed one was reads none of the
/// freed context's errors: an error is kept under its context's id, which no
/// later context shares, never under its address.
#[test]
fn a_context_at_a_freed_contexts_address_has_no_last_error() {
    let id = CString::new("never-declared").unwrap();
    let freed = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert_eq!(
        unsafe { flz_core_undeclare_class(freed, id.as_ptr()) },
        FLZ_ERR
    );
    let freed_id = unsafe { (*freed).id };
    unsafe { flz_core_context_free(freed) };
    let fresh = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert_ne!(unsafe { (*fresh).id }, freed_id);
    assert!(unsafe { flz_core_last_error(fresh) }.is_null());
    unsafe { flz_core_context_free(fresh) };
}
