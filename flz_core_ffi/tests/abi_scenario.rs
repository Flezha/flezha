//! The C-ABI scenario suite (spec `sdk-core-abi`): every capability the
//! crate API has must be reachable through `flz_core_ffi` itself.
//!
//! One sequential `#[test]`: the context, the engine and the fake controller
//! are process-global state, so the phases run in order inside a single
//! test fn (the `runner_lifecycle` pattern).

use std::ffi::CString;
use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core_ffi::*;
use prost::Message;

type CapturedFrame = (String, Vec<u8>);

/// Accept control connections, capture every runner→controller frame, and
/// hold the write half so the test can push controller→runner commands.
fn start_fake_controller(
    dir: &std::path::Path,
) -> (
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<CapturedFrame>>>,
    Arc<Mutex<Option<flz_transport::transport::FlezhaStream>>>,
) {
    let listener =
        UnixListener::bind(dir.join("runner_control_channel.sock")).expect("bind control socket");
    let accepted = Arc::new(AtomicUsize::new(0));
    let frames = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::new(Mutex::new(None));
    let counter = Arc::clone(&accepted);
    let sink = Arc::clone(&frames);
    let writer_slot = Arc::clone(&writer);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(s) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
            let write_half = s.try_clone().expect("clone control stream");
            *writer_slot.lock().unwrap() =
                Some(flz_transport::transport::FlezhaStream::Unix(write_half));
            let sink = Arc::clone(&sink);
            thread::spawn(move || {
                let stream = flz_transport::transport::FlezhaStream::Unix(s);
                while let Ok((payload_id, message, _metadata)) =
                    flz_transport::service::payloader::extract_payload(&stream)
                {
                    sink.lock().unwrap().push((payload_id, message));
                }
            });
        }
    });
    (accepted, frames, writer)
}

fn send_command(
    writer: &Arc<Mutex<Option<flz_transport::transport::FlezhaStream>>>,
    reference_id: &str,
    command: flz_core::pb::command_request::Command,
) {
    use std::io::Write;
    let request = flz_core::pb::CommandRequest {
        reference_id: reference_id.to_string(),
        command: Some(command),
    };
    let frame = flz_transport::service::payloader::build_from_binary(
        "cmd",
        &request.encode_to_vec(),
        Default::default(),
    );
    let mut guard = writer.lock().unwrap();
    let stream = guard.as_mut().expect("control connection accepted");
    (&*stream).write_all(&frame).expect("write command frame");
}

fn await_frame<T>(
    frames: &Arc<Mutex<Vec<CapturedFrame>>>,
    what: &str,
    probe: impl Fn(&[CapturedFrame]) -> Option<T>,
) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(found) = probe(&frames.lock().unwrap()) {
            return found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Poll through the ABI, panicking on error; returns (status, decoded).
fn abi_poll(ctx: *mut FlzContext, timeout_ms: u64) -> (i32, Option<schema::Activation>) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe { flz_core_poll(ctx, timeout_ms, &mut buf, &mut len) };
    if status != FLZ_POLL_ACTIVATION {
        return (status, None);
    }
    let bytes = unsafe { std::slice::from_raw_parts(buf, len) }.to_vec();
    unsafe { flz_core_buf_free(buf, len) };
    let act = schema::decode_activation(&bytes).expect("activation decodes");
    (status, Some(act))
}

fn abi_complete(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    let bytes = schema::encode_result(result);
    let status = unsafe { flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        let err = unsafe { flz_core_last_error(ctx) };
        let msg = if err.is_null() {
            "<no error recorded>".to_string()
        } else {
            unsafe { std::ffi::CStr::from_ptr(err) }
                .to_string_lossy()
                .into_owned()
        };
        panic!("complete failed: {msg}");
    }
}

fn ok_effect(consumed_offset: Option<u64>, timers: Vec<schema::TimerOp>) -> schema::ItemEffect {
    schema::ItemEffect {
        verdict: schema::Verdict::Ok,
        changes: vec![],
        outputs: vec![],
        consumed_offset,
        event_time: 0,
        timers,
        checkpoint: false,
        dlq: None,
    }
}

#[test]
fn c_abi_full_scenario() {
    // ---------------------------------------------------------- version pin
    // A shim built against an incompatible schema is refused at creation,
    // with an error naming both versions, before any connection.
    let mismatched = flz_core_context_new(flz_core::SCHEMA_VERSION + 1);
    assert!(mismatched.is_null(), "mismatched schema must refuse");
    let err = unsafe { std::ffi::CStr::from_ptr(flz_core_init_error()) }
        .to_string_lossy()
        .into_owned();
    assert!(
        err.contains(&format!("{}", flz_core::SCHEMA_VERSION))
            && err.contains(&format!("{}", flz_core::SCHEMA_VERSION + 1)),
        "refusal names both versions: {err}"
    );

    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null(), "matching schema creates a context");

    // ------------------------------------------------------ declare + start
    let dir = std::env::temp_dir().join(format!("flz-abi-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "300000");
    let (accepted, frames, writer) = start_fake_controller(&dir);

    let decl = br#"{
        "factory_id": "abi_probe",
        "procedures": [{"name": "abi_pass", "entry_channel": "in"}]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );

    let ns = CString::new("abi_ns").unwrap();
    assert_eq!(unsafe { flz_core_start(ctx, ns.as_ptr()) }, FLZ_OK);
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "one control connect");
    // "Connects": the init frame names the declared factory.
    await_frame(&frames, "init frame", |captured| {
        captured
            .iter()
            .find(|(id, m)| {
                id == "connect_control_channel" && String::from_utf8_lossy(m).contains("abi_probe")
            })
            .map(|_| ())
    });

    // AddOperative through the REAL control path.
    send_command(
        &writer,
        "ref-add-1",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "abi_probe".to_string(),
            factory_id: "abi_probe".to_string(),
            ..Default::default()
        }),
    );
    await_frame(&frames, "AddOperative response", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "cmd"
                && flz_core::pb::CommandResponse::decode(m.as_slice())
                    .is_ok_and(|r| r.reference_id == "ref-add-1" && r.status == "success"))
            .then_some(())
        })
    });

    // -------------------------------------------------- input activations
    // Drive the instance the runner built: route two entry payloads.
    let instance = flz_core::fast::live_unbounded_instance("abi_probe")
        .expect("AddOperative built the instance");
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(flz_transport::transport::FlezhaStream::Unix(sock));
    let entry = instance.topology.channel(0).expect("entry channel");
    for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
        let value: Arc<dyn std::any::Any + Send + Sync> = Arc::new(payload);
        flz_core::routing::route_to_consumers(&instance, entry, &value, &stream, None, false);
    }

    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "queued inputs hand out");
    let act = act.unwrap();
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("expected a work batch");
    };
    let payloads: Vec<Vec<u8>> = items
        .iter()
        .map(|item| match item {
            schema::WorkItem::Input(input) => input.payload.clone(),
            other => panic!("expected inputs, got {other:?}"),
        })
        .collect();
    assert_eq!(payloads, vec![b"alpha".to_vec(), b"beta".to_vec()]);
    assert_eq!(act.target.procedure, 0);

    // Complete FROM A DIFFERENT THREAD (thread-safety pin), registering a
    // processing-time timer 30ms out on the last item.
    let due_at = flz_core::util::wall_now_ms() + 30;
    let result = schema::ActivationResult {
        token: act.token,
        outcome: schema::ActivationOutcome::Work(vec![
            ok_effect(None, vec![]),
            ok_effect(
                None,
                vec![schema::TimerOp::register(
                    schema::TimerKind::ProcessingTime,
                    due_at,
                )],
            ),
        ]),
    };
    let ctx_addr = ctx as usize;
    thread::spawn(move || {
        abi_complete(ctx_addr as *mut FlzContext, &result);
    })
    .join()
    .expect("cross-thread complete");

    // ------------------------------------------------------- timer firing
    let deadline = Instant::now() + Duration::from_secs(5);
    let fired = loop {
        let (status, act) = abi_poll(ctx, 200);
        if status == FLZ_POLL_ACTIVATION {
            break act.unwrap();
        }
        assert!(Instant::now() < deadline, "timer never fired through poll");
    };
    let schema::ActivationKind::Work(items) = &fired.kind else {
        panic!("expected a work batch");
    };
    match &items[..] {
        [schema::WorkItem::Timer(timer)] => {
            assert_eq!(timer.kind, schema::TimerKind::ProcessingTime);
            assert_eq!(timer.scheduled_at, due_at, "fired_at IS the scheduled ts");
        }
        other => panic!("expected exactly the timer firing, got {other:?}"),
    }
    // The firing's removal rides its completion.
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: fired.token,
            outcome: schema::ActivationOutcome::Work(vec![ok_effect(
                None,
                vec![schema::TimerOp::cancel(
                    schema::TimerKind::ProcessingTime,
                    due_at,
                )],
            )]),
        },
    );

    // ------------------------------------------------ Ping never surfaces
    send_command(
        &writer,
        "ref-ping-1",
        flz_core::pb::command_request::Command::Ping(flz_core::pb::Ping::default()),
    );
    await_frame(&frames, "ping response", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "cmd"
                && flz_core::pb::CommandResponse::decode(m.as_slice())
                    .is_ok_and(|r| r.reference_id == "ref-ping-1"))
            .then_some(())
        })
    });
    let (status, _) = abi_poll(ctx, 300);
    assert_eq!(status, FLZ_POLL_TIMEOUT, "Ping generated no activation");

    // -------------------------------------------- poll honors its timeout
    let started = Instant::now();
    let (status, _) = abi_poll(ctx, 100);
    assert_eq!(status, FLZ_POLL_TIMEOUT);
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(90) && elapsed < Duration::from_millis(1_000),
        "timeout bound violated: {elapsed:?}"
    );

    // ------------------------------------- shutdown wakes blocked pollers
    let ctx_addr = ctx as usize;
    let blocked = thread::spawn(move || {
        let started = Instant::now();
        let (status, _) = abi_poll(ctx_addr as *mut FlzContext, 30_000);
        (status, started.elapsed())
    });
    thread::sleep(Duration::from_millis(100));
    unsafe { flz_core_shutdown(ctx) };
    let (status, waited) = blocked.join().expect("blocked poller returns");
    assert_eq!(status, FLZ_POLL_SHUTTING_DOWN);
    assert!(
        waited < Duration::from_secs(5),
        "shutdown must wake promptly, waited {waited:?}"
    );

    unsafe { flz_core_context_free(ctx) };
}
