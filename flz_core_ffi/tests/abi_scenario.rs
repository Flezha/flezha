//! The C-ABI scenario suite (spec `sdk-core-abi`): every capability the
//! crate API has must be reachable through `flz_core_ffi` itself.
//!
//! One sequential `#[test]`: the context, the engine and the fake controller
//! are process-global state, so the phases run in order inside a single
//! test fn (the `runner_lifecycle` pattern).
//!
//! Under `--features napi` the same scenario also proves the Node veneer,
//! with no Node process involved: every activation is mapped through
//! `activation_to_js` and every completion is built in the shape JavaScript
//! answers with and mapped back through `result_from_js`. A veneer that lost
//! or transposed a field would fail the phase that reads it, rather than a
//! separate suite that has to be kept in step with this one.
//!
//! `--features ruby-embed` does the same for the Ruby veneer, through an
//! embedded CRuby VM and with no Ruby process involved, and `--features
//! python` for the Python one, through an embedded CPython interpreter. Every
//! arm hangs off the same two hooks in `abi_poll` / `abi_complete`, so a
//! veneer is proved by the scenario that already covers the capability rather
//! than by a suite of its own that has to be kept in step.

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
    #[cfg(feature = "napi")]
    napi_veneer::assert_activation_maps(&act);
    // The shutdown phase polls from a spawned thread, where an embedded VM
    // has no handle. It answers `SHUTTING_DOWN` and never reaches here, and
    // the guard states that rather than relying on it.
    #[cfg(feature = "ruby-embed")]
    if magnus::Ruby::get().is_ok() {
        ruby_veneer::assert_activation_maps(&act);
    }
    // No thread guard here: unlike an embedded CRuby VM, CPython attaches from
    // any thread once the interpreter is initialised, so the spawned-thread
    // phases exercise the mapping too.
    #[cfg(feature = "python")]
    python_veneer::assert_activation_maps(&act);
    (status, Some(act))
}

/// Map a result through whichever veneers are built in, and assert none of
/// them changed it.
///
/// Separate from [`abi_complete`] because one phase completes from a spawned
/// thread: the ABI accepts a completion from any thread, but an embedded Ruby
/// VM has no handle on a thread it did not create. So the mapping is what
/// stays on the test's own thread, and only the ABI call crosses.
#[allow(clippy::let_and_return)]
fn through_veneers(result: &schema::ActivationResult) -> schema::ActivationResult {
    #[cfg(feature = "napi")]
    let result = &napi_veneer::through_veneer(result);
    #[cfg(feature = "ruby-embed")]
    let result = &ruby_veneer::through_veneer(result);
    #[cfg(feature = "python")]
    let result = &python_veneer::through_veneer(result);
    result.clone()
}

fn abi_complete(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    abi_complete_mapped(ctx, &through_veneers(result));
}

/// Hand an already-mapped result to the ABI. Callable from any thread.
fn abi_complete_mapped(ctx: *mut FlzContext, result: &schema::ActivationResult) {
    let bytes = schema::encode_result(result);
    let status = unsafe { flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
    if status != FLZ_OK {
        panic!("complete failed: {}", last_error(ctx));
    }
}

/// The activation's input payloads, in batch order.
fn work_payloads(act: &schema::Activation) -> Vec<Vec<u8>> {
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("expected a work batch");
    };
    items
        .iter()
        .map(|item| match item {
            schema::WorkItem::Input(input) => input.payload.clone(),
            other => panic!("expected inputs, got {other:?}"),
        })
        .collect()
}

/// The last error the context recorded, or a placeholder.
fn last_error(ctx: *mut FlzContext) -> String {
    let err = unsafe { flz_core_last_error(ctx) };
    if err.is_null() {
        "<no error recorded>".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(err) }
            .to_string_lossy()
            .into_owned()
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
    // The embedded CRuby VM has to be started here rather than lazily: its
    // setup records the current stack frame as the GC's scan root, so it must
    // sit above every frame that touches a Ruby value. Held for the whole
    // test — dropping it runs `ruby_cleanup`.
    // SAFETY: once per process, higher up the stack than any Ruby call below.
    #[cfg(feature = "ruby-embed")]
    let _ruby_vm = unsafe { magnus::embed::init() };

    // CPython is initialised here rather than on first use: initialisation
    // belongs on the main thread, and two phases below poll and complete from
    // spawned ones.
    #[cfg(feature = "python")]
    pyo3::Python::initialize();

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
        "sdk_lang": 1,
        "channels": [{"name": "in", "kind": "entry", "consumers": [0]}],
        "procedures": [{"name": "abi_pass"}]
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
    let instance = flz_core::runner::live_stream_instance("abi_probe")
        .expect("AddOperative built the instance");
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(flz_transport::transport::FlezhaStream::Unix(sock));
    let entry = instance.topology.channel(0).expect("entry channel");
    for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
        // The channel's own entry wrapper — the same one the wire path runs.
        let wrap = entry.wrap_input.as_ref().expect("entry wrapper");
        let value = wrap(payload).expect("bytes passthrough cannot fail");
        flz_core::routing::route_to_consumers(&instance, entry, &value, &stream, None, false, None);
    }

    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "queued inputs hand out");
    let act = act.unwrap();
    assert_eq!(
        work_payloads(&act),
        vec![b"alpha".to_vec(), b"beta".to_vec()]
    );
    assert_eq!(act.target.procedure, 0);

    // ------------------------------------------------------------- abandon
    // The third answer, and the one the ABI has to name because no unwind
    // crosses it: a veneer whose language threw cannot reach `complete`, and
    // per-entity exclusivity would then retire this entity for the life of
    // the process. Abandoning re-hands the batch untouched.
    assert_eq!(unsafe { flz_core_abandon(ctx, act.token) }, FLZ_OK);
    assert_eq!(
        unsafe { flz_core_abandon(ctx, act.token) },
        FLZ_ERR,
        "an already-answered token is refused, not silently accepted"
    );
    assert!(
        last_error(ctx).contains(&format!("{}", act.token)),
        "the refusal names the token: {}",
        last_error(ctx)
    );

    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(
        status, FLZ_POLL_ACTIVATION,
        "an abandoned entity polls again"
    );
    let act = act.unwrap();
    assert_eq!(
        work_payloads(&act),
        vec![b"alpha".to_vec(), b"beta".to_vec()],
        "abandon re-hands the items untouched and in order"
    );

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
    let result = through_veneers(&result);
    thread::spawn(move || {
        abi_complete_mapped(ctx_addr as *mut FlzContext, &result);
    })
    .join()
    .expect("cross-thread complete");

    // ------------------------------------------ entry-input idle signal
    // The C ABI is the only view a non-Rust host has of the last-delivery
    // stamp, so the two reads are pinned here; the wire path that writes it
    // (`routing::dispatch`) is driven by the Rust SDK's `entry_idle` suite.
    let unknown = CString::new("no_such_entry_channel").unwrap();
    assert_eq!(
        unsafe { flz_core_entry_last_delivery_nanos(unknown.as_ptr()) },
        0,
        "a channel nothing has delivered to reads as the never-delivered sentinel"
    );
    entry.observe_delivery();
    let name = CString::new("in").unwrap();
    let stamped = unsafe { flz_core_entry_last_delivery_nanos(name.as_ptr()) };
    assert!(stamped > 0, "the delivery is visible through the ABI");
    assert!(
        stamped <= flz_core_monotonic_nanos(),
        "the stamp and the clock it is read against are the same clock"
    );

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

    // ================================================== restore fold (ABI)
    // A persistent effectively-once host class: the controller's
    // RestoreInitState becomes a RestoreFold activation carrying the naked
    // host-encoded bodies (plain records verbatim, effect records unwrapped),
    // and the restored consumed-offset floor skips already-applied inputs.
    let decl = br#"{
        "factory_id": "abi_eff",
        "persistent": true,
        "effectively_once": true,
        "sdk_lang": 2,
        "change_format_id": 4,
        "channels": [
            {"name": "ein", "kind": "entry", "consumers": [0]},
            {"name": "eout", "kind": "plugin", "producer": 0, "keyed": true}
        ],
        "procedures": [{
            "name": "eff_pass",
            "schema_version": 2,
            "min_readable_schema_version": 1
        }]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    send_command(
        &writer,
        "ref-add-eff",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "eff-1".to_string(),
            factory_id: "abi_eff".to_string(),
            ..Default::default()
        }),
    );
    await_frame(&frames, "AddOperative eff response", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "cmd"
                && flz_core::pb::CommandResponse::decode(m.as_slice())
                    .is_ok_and(|r| r.reference_id == "ref-add-eff" && r.status == "success"))
            .then_some(())
        })
    });

    // On-disk WAL records: seq 1 a plain (host-codec) change, seq 2 an
    // effect record whose consumed offset seeds the floor. The class runs at
    // schema_version 2 reading down to 1, so the snapshot below (stamped 1)
    // crosses tagged for migration while these records (stamped 2) cross to be
    // decoded — both arms of the decision, in one restore.
    let header_for = |format_id: u8, schema_version: u32, body: &[u8]| {
        flz_core::header::HeaderTemplate {
            sdk_lang: 2,
            sdk_version: 0,
            schema_version,
            format_id,
        }
        .stamp(body)
    };
    let record = |seq: u64, format_id: u8, body: &[u8]| {
        let header = header_for(format_id, 2, body);
        let mut framed = Vec::new();
        framed.extend_from_slice(&seq.to_be_bytes());
        framed.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        framed.extend_from_slice(&header);
        framed.extend_from_slice(body);
        let mut crc_input = header.to_vec();
        crc_input.extend_from_slice(body);
        framed.extend_from_slice(&crc32fast::hash(&crc_input).to_be_bytes());
        framed
    };
    let effect_body = postcard::to_stdvec(&flz_core::flusher::EffectRecordBody {
        changes: vec![vec![0xC2]],
        outputs: vec![],
        consumed_offset: Some(7),
        dlq: None,
        event_time: 1_234,
        timers: vec![],
    })
    .unwrap();
    let mut change_log = record(1, 4, &[0xC1]);
    change_log.extend(record(
        2,
        flz_core::header::FORMAT_ID_EFFECT_RECORD,
        &effect_body,
    ));
    let entity_id = flz_core::flusher::encode_entity_identity("eff-1", "eff_pass", "");
    send_command(
        &writer,
        "ref-restore-1",
        flz_core::pb::command_request::Command::RestoreInitState(flz_core::pb::RestoreInitState {
            entity_id: entity_id.clone(),
            snapshot_bytes: vec![0x5A],
            snapshot_header: header_for(4, 1, &[0x5A]).to_vec(),
            change_log_bytes: change_log,
            ..Default::default()
        }),
    );
    let (status, act) = abi_poll(ctx, 5_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "restore fold hands out");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "eff-1");
    let schema::ActivationKind::RestoreFold(fold) = &act.kind else {
        panic!("expected a restore fold, got {:?}", act.kind);
    };
    assert_eq!(
        fold.snapshot,
        Some(flz_core::schema::RestoreBody {
            bytes: vec![0x5A],
            from_version: Some(1),
        }),
        "a snapshot from an older version crosses tagged with that version"
    );
    assert_eq!(
        fold.changes,
        vec![
            flz_core::schema::RestoreBody {
                bytes: vec![0xC1],
                from_version: None,
            },
            flz_core::schema::RestoreBody {
                bytes: vec![0xC2],
                from_version: None,
            },
        ],
        "plain bodies verbatim, effect records unwrapped to their host changes"
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Restored,
        },
    );
    await_frame(&frames, "restore ack (terminal seq 2)", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "cmd"
                && flz_core::pb::CommandResponse::decode(m.as_slice()).is_ok_and(|r| {
                    r.reference_id == "ref-restore-1"
                        && flz_core::pb::RestoreInitStateResponse::decode(r.raw_data.as_slice())
                            .is_ok_and(|ack| ack.terminal_seq == 2)
                }))
            .then_some(())
        })
    });

    // The fold's effect record consumed offset 7: a re-forwarded input at or
    // below the floor is skipped, one above it dispatches.
    let eff_instance = flz_core::runner::live_stream_instance("eff-1").expect("eff instance");
    let eff_entry = eff_instance.topology.channel(0).expect("entry channel");
    let wrap = eff_entry.wrap_input.as_ref().expect("entry wrapper");
    let below = wrap(b"dup".to_vec()).unwrap();
    flz_core::routing::route_to_consumers(
        &eff_instance,
        eff_entry,
        &below,
        &stream,
        Some(7),
        false,
        None,
    );
    let (status, _) = abi_poll(ctx, 300);
    assert_eq!(
        status, FLZ_POLL_TIMEOUT,
        "offset 7 sits at the restored floor"
    );
    let above = wrap(b"new".to_vec()).unwrap();
    flz_core::routing::route_to_consumers(
        &eff_instance,
        eff_entry,
        &above,
        &stream,
        Some(8),
        false,
        None,
    );
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "offset 8 dispatches");
    let act = act.unwrap();
    let schema::ActivationKind::Work(items) = &act.kind else {
        panic!("expected work");
    };
    match &items[..] {
        [schema::WorkItem::Input(input)] => {
            assert_eq!(input.consumed_offset, Some(8));
            assert_eq!(input.payload, b"new".to_vec());
        }
        other => panic!("expected one input, got {other:?}"),
    }
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(vec![ok_effect(Some(8), vec![])]),
        },
    );

    // ==================================================== live read (ABI)
    // EntityState becomes a LiveStateRead activation; the host's JSON answer
    // lands in the command reply under the Decision-10 shape.
    send_command(
        &writer,
        "ref-read-1",
        flz_core::pb::command_request::Command::EntityState(flz_core::pb::EntityState {
            operative_id: "eff-1".to_string(),
            ..Default::default()
        }),
    );
    let (status, act) = abi_poll(ctx, 5_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "live read hands out");
    let act = act.unwrap();
    assert!(
        matches!(act.kind, schema::ActivationKind::LiveStateRead),
        "expected a live read, got {:?}",
        act.kind
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::StateJson {
                json: br#"{"n":3}"#.to_vec(),
            },
        },
    );
    await_frame(
        &frames,
        "entity_state reply carrying the host JSON",
        |captured| {
            captured.iter().find_map(|(id, m)| {
                (id == "cmd"
                    && flz_core::pb::CommandResponse::decode(m.as_slice()).is_ok_and(|r| {
                        r.reference_id == "ref-read-1" && {
                            let body = String::from_utf8_lossy(&r.raw_data);
                            body.contains("eff_pass") && body.contains("\"n\":3")
                        }
                    }))
                .then_some(())
            })
        },
    );

    // ==================== keyed dynamic route via the prepare crossing (ABI)
    let decl = br#"{
        "factory_id": "abi_dyn",
        "sdk_lang": 1,
        "channels": [{"name": "din", "kind": "entry", "consumers": [0]}],
        "procedures": [{"name": "dyn_pass", "entity_mode": "dynamic"}]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    send_command(
        &writer,
        "ref-add-dyn",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "dyn-1".to_string(),
            factory_id: "abi_dyn".to_string(),
            ..Default::default()
        }),
    );
    await_frame(&frames, "AddOperative dyn response", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "cmd"
                && flz_core::pb::CommandResponse::decode(m.as_slice())
                    .is_ok_and(|r| r.reference_id == "ref-add-dyn" && r.status == "success"))
            .then_some(())
        })
    });
    let dyn_instance = flz_core::runner::live_stream_instance("dyn-1").expect("dyn instance");
    let dyn_entry = dyn_instance.topology.channel(0).expect("entry channel");
    // The wire-dispatch staging seam (the same call `OperativeDispatchHandle::
    // dispatch` makes): a keyed consumer holds the payload for the crossing.
    let staged = flz_core::host::maybe_stage_prepare(
        &dyn_instance,
        0,
        dyn_entry,
        b"k1:v".to_vec(),
        None,
        false,
        &stream,
    );
    assert!(staged.is_none(), "keyed consumer stages for the crossing");
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "prepare crossing hands out");
    let act = act.unwrap();
    let schema::ActivationKind::KeyRequest(request) = &act.kind else {
        panic!("expected a key request, got {:?}", act.kind);
    };
    assert_eq!(request.payloads, vec![b"k1:v".to_vec()]);
    assert!(request.want_keys);
    assert!(!request.want_event_time);
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Prepared(vec![Ok(schema::PrepareAnswer {
                keys: vec!["k1".to_string()],
                event_time: None,
            })]),
        },
    );
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "keyed input dispatches");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "dyn-1");
    assert_eq!(
        act.target.entity_key.as_deref(),
        Some("k1"),
        "the prepare answer's key selected the lazily-created entity"
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(vec![ok_effect(None, vec![])]),
        },
    );

    // ============================================ workflow run-sugar (ABI)
    let decl = br#"{
        "factory_id": "abi_sugar",
        "sdk_lang": 1,
        "channels": [],
        "procedures": [],
        "workflow": {}
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    send_command(
        &writer,
        "ref-add-sugar",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "sugar-1".to_string(),
            factory_id: "abi_sugar".to_string(),
            kind: "workflow".to_string(),
            init_values: "seed".to_string(),
            ..Default::default()
        }),
    );
    let (status, act) = abi_poll(ctx, 5_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "workflow seed hands out");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "sugar-1");
    let schema::ActivationKind::WorkflowRun { args } = &act.kind else {
        panic!("expected a workflow run, got {:?}", act.kind);
    };
    assert_eq!(args, b"seed");
    // The session calls a live body uses, through the ABI.
    let instance_c = CString::new("sugar-1").unwrap();
    assert_eq!(
        unsafe { flz_core_workflow_cancelled(ctx, instance_c.as_ptr()) },
        0,
        "a live body is not cancelled"
    );
    {
        let mut buf: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let status =
            unsafe { flz_core_workflow_recv(ctx, instance_c.as_ptr(), 50, &mut buf, &mut len) };
        assert_eq!(status, FLZ_POLL_TIMEOUT, "an empty inbox times out");
    }
    assert_eq!(
        unsafe { flz_core_workflow_emit(ctx, instance_c.as_ptr(), b"mid".as_ptr(), 3) },
        FLZ_OK
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::WorkflowDone {
                completed: true,
                result: b"done".to_vec(),
            },
        },
    );
    await_frame(&frames, "WorkflowCompleted teardown", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "control_command"
                && flz_core::pb::CommandRequest::decode(m.as_slice()).is_ok_and(|r| {
                    matches!(
                        r.command,
                        Some(flz_core::pb::command_request::Command::WorkflowCompleted(ref c))
                            if c.operative_id == "sugar-1"
                                && c.status == "ok"
                                && c.result == b"done".to_vec()
                    )
                }))
            .then_some(())
        })
    });

    // ------------------------------------- shutdown wakes blocked pollers
    let ctx_addr = ctx as usize;
    let blocked = thread::spawn(move || {
        let started = Instant::now();
        let (status, _) = abi_poll(ctx_addr as *mut FlzContext, 30_000);
        (status, started.elapsed())
    });
    // The poller stays blocked for as long as it is watched; shutdown is what
    // frees it.
    let watched = Instant::now();
    while watched.elapsed() < Duration::from_millis(100) {
        assert!(
            !blocked.is_finished(),
            "the poller returned before shutdown"
        );
        thread::yield_now();
    }
    unsafe { flz_core_shutdown(ctx) };
    let (status, waited) = blocked.join().expect("blocked poller returns");
    assert_eq!(status, FLZ_POLL_SHUTTING_DOWN);
    assert!(
        waited < Duration::from_secs(5),
        "shutdown must wake promptly, waited {waited:?}"
    );

    unsafe { flz_core_context_free(ctx) };
}

/// The Node veneer, driven from Rust. Every mapping the addon performs is
/// exercised by the scenario above; these helpers are the two directions.
#[cfg(feature = "napi")]
mod napi_veneer {
    use flz_core::schema;
    use flz_core_ffi::napi_glue::*;
    use napi::bindgen_prelude::{BigInt, Buffer};

    fn big(v: u64) -> BigInt {
        BigInt::from(v)
    }

    fn u64_of(v: &BigInt) -> u64 {
        let (_, value, lossless) = v.get_u64();
        assert!(lossless, "a u64 field lost precision crossing the veneer");
        value
    }

    /// Assert the decoded activation reaches JavaScript intact. Checked
    /// field by field rather than by a round trip, because the veneer has no
    /// inverse in production — nothing ever maps JavaScript back to an
    /// `Activation`, so an inverse written here could agree with a wrong
    /// forward mapping.
    pub fn assert_activation_maps(act: &schema::Activation) {
        let js = activation_to_js(act.clone());
        assert_eq!(u64_of(&js.token), act.token);
        assert_eq!(js.operative, act.target.operative);
        assert_eq!(js.procedure, act.target.procedure);
        assert_eq!(js.entity_key, act.target.entity_key);
        assert_eq!(u64_of(&js.change_seq), act.change_seq);
        assert_eq!(js.factory, act.factory);
        match &act.kind {
            schema::ActivationKind::Work(items) => {
                assert_eq!(js.kind, "work");
                let mapped = js.items.expect("a work activation carries items");
                assert_eq!(mapped.len(), items.len());
                for (js_item, item) in mapped.iter().zip(items) {
                    match item {
                        schema::WorkItem::Input(i) => {
                            assert_eq!(js_item.kind, "input");
                            assert_eq!(js_item.channel, Some(i.channel));
                            assert_eq!(
                                js_item.payload.as_ref().expect("payload").to_vec(),
                                i.payload
                            );
                            let meta: Vec<(String, String)> = js_item
                                .metadata
                                .as_ref()
                                .expect("metadata")
                                .iter()
                                .map(|m| (m.key.clone(), m.value.clone()))
                                .collect();
                            assert_eq!(meta, i.metadata);
                            assert_eq!(
                                js_item.consumed_offset.as_ref().map(u64_of),
                                i.consumed_offset
                            );
                            assert_eq!(js_item.replay, Some(i.replay));
                            assert_eq!(js_item.now.as_ref().map(u64_of), Some(i.now));
                        }
                        schema::WorkItem::Timer(t) => {
                            assert_eq!(js_item.kind, "timer");
                            assert_eq!(js_item.timer_kind.as_deref(), Some(t.kind.as_str()));
                            assert_eq!(
                                js_item.scheduled_at.as_ref().map(u64_of),
                                Some(t.scheduled_at)
                            );
                            // A timer firing's `now` IS its scheduled time,
                            // so a handler reads one field for both kinds.
                            assert_eq!(js_item.now.as_ref().map(u64_of), Some(t.scheduled_at));
                        }
                    }
                }
            }
            schema::ActivationKind::RestoreFold(fold) => {
                assert_eq!(js.kind, "restore_fold");
                assert_eq!(
                    js.snapshot.as_ref().map(|b| b.bytes.to_vec()),
                    fold.snapshot.as_ref().map(|b| b.bytes.clone())
                );
                let changes = js.changes.expect("a restore fold carries changes");
                assert_eq!(changes.len(), fold.changes.len());
                for (js_body, body) in changes.iter().zip(&fold.changes) {
                    assert_eq!(js_body.bytes.to_vec(), body.bytes);
                    assert_eq!(js_body.from_version, body.from_version);
                }
            }
            schema::ActivationKind::SnapshotRequest => assert_eq!(js.kind, "snapshot_request"),
            schema::ActivationKind::LiveStateRead => assert_eq!(js.kind, "live_state_read"),
            schema::ActivationKind::ApplyState { state_json } => {
                assert_eq!(js.kind, "apply_state");
                assert_eq!(js.state_json.expect("state json").to_vec(), *state_json);
            }
            schema::ActivationKind::MigrationSerialize => {
                assert_eq!(js.kind, "migration_serialize")
            }
            schema::ActivationKind::KeyRequest(req) => {
                assert_eq!(js.kind, "key_request");
                let mapped = js.key_request.expect("a key request carries its payloads");
                assert_eq!(mapped.channel, req.channel);
                assert_eq!(mapped.want_keys, req.want_keys);
                assert_eq!(mapped.want_event_time, req.want_event_time);
                let payloads: Vec<Vec<u8>> = mapped.payloads.iter().map(|p| p.to_vec()).collect();
                assert_eq!(payloads, req.payloads);
            }
            schema::ActivationKind::WorkflowRun { args } => {
                assert_eq!(js.kind, "workflow_run");
                assert_eq!(js.args.expect("workflow args").to_vec(), *args);
            }
            schema::ActivationKind::CompensateRun { outputs } => {
                assert_eq!(js.kind, "compensate_run");
                let mapped: Vec<Vec<u8>> = js
                    .outputs
                    .expect("compensate outputs")
                    .iter()
                    .map(|o| o.to_vec())
                    .collect();
                assert_eq!(mapped, *outputs);
            }
        }
    }

    /// Build the result in the shape JavaScript answers with, map it back
    /// through the veneer, and assert nothing changed. The scenario then
    /// completes with the mapped value, so a lossy field fails the phase that
    /// depends on it and not only this equality.
    pub fn through_veneer(result: &schema::ActivationResult) -> schema::ActivationResult {
        let mapped = result_from_js(js_result(result)).expect("the veneer maps the result back");
        assert_eq!(&mapped, result, "the veneer changed the result");
        mapped
    }

    fn js_meta(pairs: &[(String, String)]) -> Vec<JsMeta> {
        pairs
            .iter()
            .map(|(key, value)| JsMeta {
                key: key.clone(),
                value: value.clone(),
            })
            .collect()
    }

    fn js_output(o: &schema::Output) -> JsOutput {
        let (target, channel, consumer, plugin_channel) = match &o.target {
            schema::OutputTarget::Edge { channel, consumer } => {
                ("edge", Some(*channel), Some(*consumer), None)
            }
            schema::OutputTarget::Plugin { channel } => {
                ("plugin", None, None, Some(channel.clone()))
            }
        };
        JsOutput {
            target: target.to_string(),
            channel,
            consumer,
            plugin_channel,
            key: o.key.clone(),
            event_time: o.event_time.map(big),
            metadata: Some(js_meta(&o.metadata)),
            payload: Buffer::from(o.payload.clone()),
        }
    }

    fn js_effect(e: &schema::ItemEffect) -> JsItemEffect {
        let (verdict, message) = match &e.verdict {
            schema::Verdict::Ok => ("ok", None),
            schema::Verdict::RetryableError { message } => ("retryable", Some(message.clone())),
            schema::Verdict::NonRetryableError { message } => {
                ("non_retryable", Some(message.clone()))
            }
        };
        JsItemEffect {
            verdict: verdict.to_string(),
            message,
            changes: Some(e.changes.iter().cloned().map(Buffer::from).collect()),
            outputs: Some(e.outputs.iter().map(js_output).collect()),
            consumed_offset: e.consumed_offset.map(big),
            event_time: big(e.event_time),
            timers: Some(
                e.timers
                    .iter()
                    .map(|t| JsTimerOp {
                        kind: t.kind.as_str().to_string(),
                        scheduled_at: big(t.scheduled_at),
                        register: t.register,
                    })
                    .collect(),
            ),
            checkpoint: Some(e.checkpoint),
            dlq: e.dlq.as_ref().map(|d| JsDlqEntry {
                channel: d.channel.clone(),
                error: d.error.clone(),
                attempts: d.attempts,
                payload: d.payload.clone().map(Buffer::from),
            }),
        }
    }

    fn js_result(r: &schema::ActivationResult) -> JsActivationResult {
        let mut out = JsActivationResult {
            token: big(r.token),
            outcome: String::new(),
            effects: None,
            state: None,
            json: None,
            prepared: None,
            error: None,
            non_retryable: None,
            workflow_completed: None,
            workflow_result: None,
        };
        match &r.outcome {
            schema::ActivationOutcome::Work(effects) => {
                out.outcome = "work".into();
                out.effects = Some(effects.iter().map(js_effect).collect());
            }
            schema::ActivationOutcome::Restored => out.outcome = "restored".into(),
            schema::ActivationOutcome::Snapshot { state } => {
                out.outcome = "snapshot".into();
                out.state = Some(Buffer::from(state.clone()));
            }
            schema::ActivationOutcome::StateJson { json } => {
                out.outcome = "state_json".into();
                out.json = Some(Buffer::from(json.clone()));
            }
            schema::ActivationOutcome::StateApplied => out.outcome = "state_applied".into(),
            schema::ActivationOutcome::MigrationState { json } => {
                out.outcome = "migration_state".into();
                out.json = Some(Buffer::from(json.clone()));
            }
            schema::ActivationOutcome::Prepared(answers) => {
                out.outcome = "prepared".into();
                out.prepared = Some(
                    answers
                        .iter()
                        .map(|a| match a {
                            Ok(answer) => JsPrepareAnswer {
                                keys: Some(answer.keys.clone()),
                                event_time: answer.event_time.map(big),
                                error: None,
                                non_retryable: None,
                            },
                            Err(e) => JsPrepareAnswer {
                                keys: None,
                                event_time: None,
                                error: Some(e.message.clone()),
                                non_retryable: Some(e.non_retryable),
                            },
                        })
                        .collect(),
                );
            }
            schema::ActivationOutcome::Failed(e) => {
                out.outcome = "failed".into();
                out.error = Some(e.message.clone());
                out.non_retryable = Some(e.non_retryable);
            }
            schema::ActivationOutcome::WorkflowDone { completed, result } => {
                out.outcome = "workflow_done".into();
                out.workflow_completed = Some(*completed);
                out.workflow_result = Some(Buffer::from(result.clone()));
            }
        }
        out
    }

    /// The addon exports exactly what the surface table names, every blocking
    /// entry point is a promise, and neither `poll` nor `waitStarvation` is
    /// among them. Asserted here as well as from Node, because this is the
    /// side that can go wrong without anyone installing the package.
    #[test]
    fn the_native_surface_is_the_declared_one() {
        let surface = native_surface();
        let blocking: Vec<&str> = surface
            .iter()
            .filter(|e| e.blocking)
            .map(|e| e.name.as_str())
            .collect();
        assert_eq!(
            blocking,
            [
                "start",
                "complete",
                "workflowRecv",
                "spawnWorkflow",
                "stopHost"
            ]
        );
        for name in ["poll", "waitStarvation"] {
            assert!(
                !surface.iter().any(|e| e.name == name),
                "{name} must not be exported to JavaScript"
            );
        }
        for entry in &surface {
            if let Some(abi) = &entry.abi {
                assert!(
                    abi.starts_with("flz_core_"),
                    "{} names {abi}, which is not a C ABI entry point",
                    entry.name
                );
            }
        }
    }
}

/// The Ruby veneer, proved by the same scenario with no Ruby process
/// involved: an embedded CRuby VM, `activation_to_ruby` on the way out and
/// `result_from_ruby` on the way back.
#[cfg(feature = "ruby-embed")]
mod ruby_veneer {
    use flz_core::schema;
    use flz_core_ffi::ruby_glue::{activation_to_ruby, result_from_ruby, NATIVE_SURFACE};
    use magnus::prelude::*;
    use magnus::{IntoValue, RArray, RHash, RString, Ruby, TryConvert, Value};

    /// The handle of the VM `c_abi_full_scenario` booted. Every call below
    /// runs deeper in that test's stack, which is what makes it valid.
    fn ruby() -> Ruby {
        // SAFETY: the scenario booted the VM on this thread before reaching
        // any code that calls this.
        unsafe { Ruby::get_unchecked() }
    }

    fn get<T: TryConvert>(h: RHash, key: &str) -> Option<T> {
        h.get(ruby().sym_new(key))
            .filter(|v| !v.is_nil())
            .map(|v| T::try_convert(v).expect("a veneer field has the declared type"))
    }

    fn req<T: TryConvert>(h: RHash, key: &str) -> T {
        get(h, key).unwrap_or_else(|| panic!("the veneer dropped `{key}`"))
    }

    fn sym_of(h: RHash, key: &str) -> String {
        req::<magnus::Symbol>(h, key).name().unwrap().into_owned()
    }

    fn bytes_of(h: RHash, key: &str) -> Vec<u8> {
        let s: RString = req(h, key);
        unsafe { s.as_slice() }.to_vec()
    }

    fn rows(h: RHash, key: &str) -> Vec<Value> {
        let a: RArray = req(h, key);
        unsafe { a.as_slice() }.to_vec()
    }

    fn as_hash(v: Value) -> RHash {
        RHash::from_value(v).expect("the veneer emits Hashes")
    }

    fn meta_of(h: RHash, key: &str) -> Vec<(String, String)> {
        rows(h, key)
            .into_iter()
            .map(|pair| {
                let pair = RArray::from_value(pair).expect("a metadata pair is an Array");
                let pair: Vec<String> = pair.to_vec().expect("metadata pairs are Strings");
                (pair[0].clone(), pair[1].clone())
            })
            .collect()
    }

    /// Assert the decoded activation reaches Ruby intact. Checked field by
    /// field rather than by a round trip, for the reason the Node arm gives:
    /// the veneer has no inverse in production, so an inverse written here
    /// could agree with a wrong forward mapping.
    pub fn assert_activation_maps(act: &schema::Activation) {
        let ruby = ruby();
        let h = activation_to_ruby(&ruby, act.clone()).expect("the veneer maps the activation");
        assert_eq!(req::<u64>(h, "token"), act.token);
        assert_eq!(req::<String>(h, "operative"), act.target.operative);
        assert_eq!(req::<u32>(h, "procedure"), act.target.procedure);
        assert_eq!(get::<String>(h, "entity_key"), act.target.entity_key);
        assert_eq!(req::<u64>(h, "change_seq"), act.change_seq);
        assert_eq!(req::<String>(h, "factory"), act.factory);

        match &act.kind {
            schema::ActivationKind::Work(items) => {
                assert_eq!(sym_of(h, "kind"), "work");
                let mapped = rows(h, "items");
                assert_eq!(mapped.len(), items.len());
                for (value, item) in mapped.into_iter().zip(items) {
                    let m = as_hash(value);
                    match item {
                        schema::WorkItem::Input(i) => {
                            assert_eq!(sym_of(m, "kind"), "input");
                            assert_eq!(req::<u32>(m, "channel"), i.channel);
                            assert_eq!(bytes_of(m, "payload"), i.payload);
                            assert_eq!(meta_of(m, "metadata"), i.metadata);
                            assert_eq!(get::<u64>(m, "consumed_offset"), i.consumed_offset);
                            assert_eq!(req::<bool>(m, "replay"), i.replay);
                            assert_eq!(req::<u64>(m, "now"), i.now);
                        }
                        schema::WorkItem::Timer(t) => {
                            assert_eq!(sym_of(m, "kind"), "timer");
                            assert_eq!(sym_of(m, "timer_kind"), t.kind.as_str());
                            assert_eq!(req::<u64>(m, "scheduled_at"), t.scheduled_at);
                            // A timer firing's `now` IS its scheduled time,
                            // so a handler reads one field for both kinds.
                            assert_eq!(req::<u64>(m, "now"), t.scheduled_at);
                        }
                    }
                }
            }
            schema::ActivationKind::RestoreFold(fold) => {
                assert_eq!(sym_of(h, "kind"), "restore_fold");
                match &fold.snapshot {
                    None => assert!(get::<Value>(h, "snapshot").is_none()),
                    Some(body) => {
                        let s = as_hash(req(h, "snapshot"));
                        assert_eq!(bytes_of(s, "bytes"), body.bytes);
                        assert_eq!(get::<u32>(s, "from_version"), body.from_version);
                    }
                }
                let changes = rows(h, "changes");
                assert_eq!(changes.len(), fold.changes.len());
                for (value, body) in changes.into_iter().zip(&fold.changes) {
                    let c = as_hash(value);
                    assert_eq!(bytes_of(c, "bytes"), body.bytes);
                    assert_eq!(get::<u32>(c, "from_version"), body.from_version);
                }
            }
            schema::ActivationKind::SnapshotRequest => {
                assert_eq!(sym_of(h, "kind"), "snapshot_request")
            }
            schema::ActivationKind::LiveStateRead => {
                assert_eq!(sym_of(h, "kind"), "live_state_read")
            }
            schema::ActivationKind::ApplyState { state_json } => {
                assert_eq!(sym_of(h, "kind"), "apply_state");
                assert_eq!(bytes_of(h, "state_json"), *state_json);
            }
            schema::ActivationKind::MigrationSerialize => {
                assert_eq!(sym_of(h, "kind"), "migration_serialize")
            }
            schema::ActivationKind::KeyRequest(request) => {
                assert_eq!(sym_of(h, "kind"), "key_request");
                let k = as_hash(req(h, "key_request"));
                assert_eq!(req::<u32>(k, "channel"), request.channel);
                assert_eq!(req::<bool>(k, "want_keys"), request.want_keys);
                assert_eq!(req::<bool>(k, "want_event_time"), request.want_event_time);
                let payloads: Vec<Vec<u8>> = rows(k, "payloads")
                    .into_iter()
                    .map(|p| {
                        unsafe {
                            RString::from_value(p)
                                .expect("a payload is a String")
                                .as_slice()
                        }
                        .to_vec()
                    })
                    .collect();
                assert_eq!(payloads, request.payloads);
            }
            schema::ActivationKind::WorkflowRun { args } => {
                assert_eq!(sym_of(h, "kind"), "workflow_run");
                assert_eq!(bytes_of(h, "args"), *args);
            }
            schema::ActivationKind::CompensateRun { outputs } => {
                assert_eq!(sym_of(h, "kind"), "compensate_run");
                let mapped: Vec<Vec<u8>> = rows(h, "outputs")
                    .into_iter()
                    .map(|o| {
                        unsafe {
                            RString::from_value(o)
                                .expect("an output is a String")
                                .as_slice()
                        }
                        .to_vec()
                    })
                    .collect();
                assert_eq!(mapped, *outputs);
            }
        }
    }

    /// Build the result in the shape Ruby answers with, map it back through
    /// the veneer, and assert nothing changed. The scenario then completes
    /// with the mapped value, so a lossy field fails the phase that depends on
    /// it and not only this equality.
    pub fn through_veneer(result: &schema::ActivationResult) -> schema::ActivationResult {
        let ruby = ruby();
        let mapped =
            result_from_ruby(&ruby, rb_result(result)).expect("the veneer maps the result back");
        assert_eq!(&mapped, result, "the veneer changed the result");
        mapped
    }

    fn hash(pairs: Vec<(&str, Value)>) -> RHash {
        let ruby = ruby();
        let h = ruby.hash_new();
        for (key, value) in pairs {
            h.aset(ruby.sym_new(key), value).expect("hash insert");
        }
        h
    }

    fn str_value(v: &[u8]) -> Value {
        ruby().str_from_slice(v).as_value()
    }

    fn array(values: Vec<Value>) -> Value {
        let a = ruby().ary_new_capa(values.len());
        for v in values {
            a.push(v).expect("array push");
        }
        a.as_value()
    }

    fn rb_meta(pairs: &[(String, String)]) -> Value {
        array(
            pairs
                .iter()
                .map(|(k, v)| array(vec![str_value(k.as_bytes()), str_value(v.as_bytes())]))
                .collect(),
        )
    }

    fn rb_output(o: &schema::Output) -> Value {
        let ruby = ruby();
        let mut pairs = vec![
            ("key", o.key.clone().into_value_with(&ruby)),
            ("metadata", rb_meta(&o.metadata)),
            ("payload", str_value(&o.payload)),
            ("event_time", o.event_time.into_value_with(&ruby)),
        ];
        match &o.target {
            schema::OutputTarget::Edge { channel, consumer } => {
                pairs.push(("target", str_value(b"edge")));
                pairs.push(("channel", (*channel).into_value_with(&ruby)));
                pairs.push(("consumer", (*consumer).into_value_with(&ruby)));
            }
            schema::OutputTarget::Plugin { channel } => {
                pairs.push(("target", str_value(b"plugin")));
                pairs.push(("plugin_channel", str_value(channel.as_bytes())));
            }
        }
        hash(pairs).as_value()
    }

    fn rb_effect(e: &schema::ItemEffect) -> Value {
        let ruby = ruby();
        let (verdict, message) = match &e.verdict {
            schema::Verdict::Ok => ("ok", None),
            schema::Verdict::RetryableError { message } => ("retryable", Some(message.clone())),
            schema::Verdict::NonRetryableError { message } => {
                ("non_retryable", Some(message.clone()))
            }
        };
        hash(vec![
            ("verdict", str_value(verdict.as_bytes())),
            ("message", message.into_value_with(&ruby)),
            (
                "changes",
                array(e.changes.iter().map(|c| str_value(c)).collect()),
            ),
            ("outputs", array(e.outputs.iter().map(rb_output).collect())),
            ("consumed_offset", e.consumed_offset.into_value_with(&ruby)),
            ("event_time", e.event_time.into_value_with(&ruby)),
            (
                "timers",
                array(
                    e.timers
                        .iter()
                        .map(|t| {
                            hash(vec![
                                ("kind", str_value(t.kind.as_str().as_bytes())),
                                ("scheduled_at", t.scheduled_at.into_value_with(&ruby)),
                                ("register", t.register.into_value_with(&ruby)),
                            ])
                            .as_value()
                        })
                        .collect(),
                ),
            ),
            ("checkpoint", e.checkpoint.into_value_with(&ruby)),
            (
                "dlq",
                match &e.dlq {
                    None => ruby.qnil().as_value(),
                    Some(d) => hash(vec![
                        ("channel", str_value(d.channel.as_bytes())),
                        ("error", str_value(d.error.as_bytes())),
                        ("attempts", d.attempts.into_value_with(&ruby)),
                        (
                            "payload",
                            match &d.payload {
                                None => ruby.qnil().as_value(),
                                Some(p) => str_value(p),
                            },
                        ),
                    ])
                    .as_value(),
                },
            ),
        ])
        .as_value()
    }

    fn rb_result(r: &schema::ActivationResult) -> RHash {
        let ruby = ruby();
        let mut pairs = vec![("token", r.token.into_value_with(&ruby))];
        match &r.outcome {
            schema::ActivationOutcome::Work(effects) => {
                pairs.push(("outcome", str_value(b"work")));
                pairs.push(("effects", array(effects.iter().map(rb_effect).collect())));
            }
            schema::ActivationOutcome::Restored => pairs.push(("outcome", str_value(b"restored"))),
            schema::ActivationOutcome::Snapshot { state } => {
                pairs.push(("outcome", str_value(b"snapshot")));
                pairs.push(("state", str_value(state)));
            }
            schema::ActivationOutcome::StateJson { json } => {
                pairs.push(("outcome", str_value(b"state_json")));
                pairs.push(("json", str_value(json)));
            }
            schema::ActivationOutcome::StateApplied => {
                pairs.push(("outcome", str_value(b"state_applied")))
            }
            schema::ActivationOutcome::MigrationState { json } => {
                pairs.push(("outcome", str_value(b"migration_state")));
                pairs.push(("json", str_value(json)));
            }
            schema::ActivationOutcome::Prepared(answers) => {
                pairs.push(("outcome", str_value(b"prepared")));
                pairs.push((
                    "prepared",
                    array(
                        answers
                            .iter()
                            .map(|a| match a {
                                Ok(answer) => hash(vec![
                                    (
                                        "keys",
                                        array(
                                            answer
                                                .keys
                                                .iter()
                                                .map(|k| str_value(k.as_bytes()))
                                                .collect(),
                                        ),
                                    ),
                                    ("event_time", answer.event_time.into_value_with(&ruby)),
                                ])
                                .as_value(),
                                Err(e) => hash(vec![
                                    ("error", str_value(e.message.as_bytes())),
                                    ("non_retryable", e.non_retryable.into_value_with(&ruby)),
                                ])
                                .as_value(),
                            })
                            .collect(),
                    ),
                ));
            }
            schema::ActivationOutcome::Failed(e) => {
                pairs.push(("outcome", str_value(b"failed")));
                pairs.push(("error", str_value(e.message.as_bytes())));
                pairs.push(("non_retryable", e.non_retryable.into_value_with(&ruby)));
            }
            schema::ActivationOutcome::WorkflowDone { completed, result } => {
                pairs.push(("outcome", str_value(b"workflow_done")));
                pairs.push(("workflow_completed", (*completed).into_value_with(&ruby)));
                pairs.push(("workflow_result", str_value(result)));
            }
        }
        hash(pairs)
    }

    /// The extension exports exactly what the surface table names, every entry
    /// names a C ABI entry point, and every blocking one is a call the veneer
    /// runs with the GVL released. Asserted on this side because it is the
    /// side that can go wrong without anyone installing the gem.
    #[test]
    fn the_native_surface_is_the_declared_one() {
        let blocking: Vec<&str> = NATIVE_SURFACE
            .iter()
            .filter(|(_, _, blocking)| *blocking)
            .map(|(name, _, _)| *name)
            .collect();
        assert_eq!(
            blocking,
            [
                "start",
                "poll",
                "wait_starvation",
                "complete",
                "workflow_recv",
                "spawn_workflow"
            ]
        );
        for (name, abi, _) in NATIVE_SURFACE {
            assert!(
                abi.starts_with("flz_core_"),
                "{name} names {abi}, which is not a C ABI entry point"
            );
        }
    }
}

/// The same scenario through the Python veneer, with no Python process
/// involved: an embedded CPython interpreter, `activation_to_python` on the
/// way out and `result_from_python` on the way back.
#[cfg(feature = "python")]
mod python_veneer {
    use flz_core::schema;
    use flz_core_ffi::python_glue::{
        activation_to_python, result_from_python, Core, PayloadView, NATIVE_SURFACE,
    };
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
    use pyo3::IntoPyObjectExt;

    fn get<'py, T>(h: &Bound<'py, PyDict>, key: &str) -> Option<T>
    where
        T: for<'a> FromPyObject<'a, 'py>,
    {
        h.get_item(key)
            .expect("dict lookup")
            .filter(|v| !v.is_none())
            .map(|v| {
                v.extract()
                    .unwrap_or_else(|_| panic!("`{key}` has the declared type"))
            })
    }

    fn req<'py, T>(h: &Bound<'py, PyDict>, key: &str) -> T
    where
        T: for<'a> FromPyObject<'a, 'py>,
    {
        get(h, key).unwrap_or_else(|| panic!("the veneer dropped `{key}`"))
    }

    /// The bytes behind a payload, however the veneer chose to expose them.
    ///
    /// The hot paths cross as a [`PayloadView`] — a buffer with no
    /// intermediate `bytes` — and the JSON one crosses as `bytes`, so the
    /// reader accepts either and the assertions below are about the bytes
    /// rather than about which wrapper carried them.
    fn bytes_of_value(v: &Bound<'_, PyAny>) -> Vec<u8> {
        if let Ok(view) = v.cast::<PayloadView>() {
            let py = v.py();
            return view
                .call_method0("tobytes")
                .expect("a view copies on request")
                .extract::<Vec<u8>>()
                .unwrap_or_else(|_| panic!("{:?}", py.None()));
        }
        v.extract::<Vec<u8>>().expect("a payload is bytes")
    }

    fn bytes_of(h: &Bound<'_, PyDict>, key: &str) -> Vec<u8> {
        bytes_of_value(
            &h.get_item(key)
                .expect("dict lookup")
                .unwrap_or_else(|| panic!("the veneer dropped `{key}`")),
        )
    }

    fn rows<'py>(h: &Bound<'py, PyDict>, key: &str) -> Vec<Bound<'py, PyAny>> {
        req::<Bound<'py, PyList>>(h, key).iter().collect()
    }

    fn as_dict<'py>(v: &Bound<'py, PyAny>) -> Bound<'py, PyDict> {
        v.cast::<PyDict>().expect("the veneer emits dicts").clone()
    }

    fn meta_of(h: &Bound<'_, PyDict>, key: &str) -> Vec<(String, String)> {
        rows(h, key)
            .into_iter()
            .map(|pair| {
                let pair: Vec<String> = pair.extract().expect("a metadata pair is two strings");
                (pair[0].clone(), pair[1].clone())
            })
            .collect()
    }

    /// Assert the decoded activation reaches Python intact. Checked field by
    /// field rather than by a round trip, for the reason the Node and Ruby
    /// arms give: the veneer has no inverse in production, so an inverse
    /// written here could agree with a wrong forward mapping.
    pub fn assert_activation_maps(act: &schema::Activation) {
        Python::attach(|py| {
            let h = activation_to_python(py, act.clone()).expect("the veneer maps the activation");
            assert_eq!(req::<u64>(&h, "token"), act.token);
            assert_eq!(req::<String>(&h, "operative"), act.target.operative);
            assert_eq!(req::<u32>(&h, "procedure"), act.target.procedure);
            assert_eq!(get::<String>(&h, "entity_key"), act.target.entity_key);
            assert_eq!(req::<u64>(&h, "change_seq"), act.change_seq);
            assert_eq!(req::<String>(&h, "factory"), act.factory);

            match &act.kind {
                schema::ActivationKind::Work(items) => {
                    assert_eq!(req::<String>(&h, "kind"), "work");
                    let mapped = rows(&h, "items");
                    assert_eq!(mapped.len(), items.len());
                    for (value, item) in mapped.into_iter().zip(items) {
                        let m = as_dict(&value);
                        match item {
                            schema::WorkItem::Input(i) => {
                                assert_eq!(req::<String>(&m, "kind"), "input");
                                assert_eq!(req::<u32>(&m, "channel"), i.channel);
                                assert_eq!(bytes_of(&m, "payload"), i.payload);
                                // The user payload is the common decode path,
                                // so it crosses as a buffer and not a `bytes`.
                                assert!(
                                    m.get_item("payload")
                                        .unwrap()
                                        .unwrap()
                                        .cast::<PayloadView>()
                                        .is_ok(),
                                    "the input payload must cross as a borrowed view"
                                );
                                assert_eq!(meta_of(&m, "metadata"), i.metadata);
                                assert_eq!(get::<u64>(&m, "consumed_offset"), i.consumed_offset);
                                assert_eq!(req::<bool>(&m, "replay"), i.replay);
                                assert_eq!(req::<u64>(&m, "now"), i.now);
                            }
                            schema::WorkItem::Timer(t) => {
                                assert_eq!(req::<String>(&m, "kind"), "timer");
                                assert_eq!(req::<String>(&m, "timer_kind"), t.kind.as_str());
                                assert_eq!(req::<u64>(&m, "scheduled_at"), t.scheduled_at);
                                // A timer firing's `now` IS its scheduled
                                // time, so a handler reads one field for both.
                                assert_eq!(req::<u64>(&m, "now"), t.scheduled_at);
                            }
                        }
                    }
                }
                schema::ActivationKind::RestoreFold(fold) => {
                    assert_eq!(req::<String>(&h, "kind"), "restore_fold");
                    match &fold.snapshot {
                        None => assert!(h.get_item("snapshot").unwrap().is_none()),
                        Some(body) => {
                            let s = as_dict(&req::<Bound<'_, PyAny>>(&h, "snapshot"));
                            assert_eq!(bytes_of(&s, "bytes"), body.bytes);
                            assert_eq!(get::<u32>(&s, "from_version"), body.from_version);
                        }
                    }
                    let changes = rows(&h, "changes");
                    assert_eq!(changes.len(), fold.changes.len());
                    for (value, body) in changes.into_iter().zip(&fold.changes) {
                        let c = as_dict(&value);
                        assert_eq!(bytes_of(&c, "bytes"), body.bytes);
                        assert_eq!(get::<u32>(&c, "from_version"), body.from_version);
                    }
                }
                schema::ActivationKind::SnapshotRequest => {
                    assert_eq!(req::<String>(&h, "kind"), "snapshot_request")
                }
                schema::ActivationKind::LiveStateRead => {
                    assert_eq!(req::<String>(&h, "kind"), "live_state_read")
                }
                schema::ActivationKind::ApplyState { state_json } => {
                    assert_eq!(req::<String>(&h, "kind"), "apply_state");
                    assert_eq!(bytes_of(&h, "state_json"), *state_json);
                }
                schema::ActivationKind::MigrationSerialize => {
                    assert_eq!(req::<String>(&h, "kind"), "migration_serialize")
                }
                schema::ActivationKind::KeyRequest(request) => {
                    assert_eq!(req::<String>(&h, "kind"), "key_request");
                    let k = as_dict(&req::<Bound<'_, PyAny>>(&h, "key_request"));
                    assert_eq!(req::<u32>(&k, "channel"), request.channel);
                    assert_eq!(req::<bool>(&k, "want_keys"), request.want_keys);
                    assert_eq!(req::<bool>(&k, "want_event_time"), request.want_event_time);
                    let payloads: Vec<Vec<u8>> =
                        rows(&k, "payloads").iter().map(bytes_of_value).collect();
                    assert_eq!(payloads, request.payloads);
                }
                schema::ActivationKind::WorkflowRun { args } => {
                    assert_eq!(req::<String>(&h, "kind"), "workflow_run");
                    assert_eq!(bytes_of(&h, "args"), *args);
                }
                schema::ActivationKind::CompensateRun { outputs } => {
                    assert_eq!(req::<String>(&h, "kind"), "compensate_run");
                    let mapped: Vec<Vec<u8>> =
                        rows(&h, "outputs").iter().map(bytes_of_value).collect();
                    assert_eq!(mapped, *outputs);
                }
            }
        });
    }

    /// Build the result in the shape Python answers with, map it back through
    /// the veneer, and assert nothing changed. The scenario then completes
    /// with the mapped value, so a lossy field fails the phase that depends on
    /// it and not only this equality.
    pub fn through_veneer(result: &schema::ActivationResult) -> schema::ActivationResult {
        Python::attach(|py| {
            let mapped = result_from_python(&py_result(py, result))
                .expect("the veneer maps the result back");
            assert_eq!(&mapped, result, "the veneer changed the result");
            mapped
        })
    }

    fn dict<'py>(py: Python<'py>, pairs: Vec<(&str, Bound<'py, PyAny>)>) -> Bound<'py, PyDict> {
        let d = PyDict::new(py);
        for (key, value) in pairs {
            d.set_item(key, value).expect("dict insert");
        }
        d
    }

    fn text<'py>(py: Python<'py>, v: &str) -> Bound<'py, PyAny> {
        v.into_bound_py_any(py).expect("str")
    }

    fn raw<'py>(py: Python<'py>, v: &[u8]) -> Bound<'py, PyAny> {
        PyBytes::new(py, v).into_any()
    }

    fn list<'py>(py: Python<'py>, values: Vec<Bound<'py, PyAny>>) -> Bound<'py, PyAny> {
        PyList::new(py, values).expect("list").into_any()
    }

    fn opt<'py, T>(py: Python<'py>, v: &Option<T>) -> Bound<'py, PyAny>
    where
        T: Copy + for<'a> IntoPyObject<'a>,
    {
        match v {
            None => py.None().into_bound(py),
            Some(value) => (*value).into_bound_py_any(py).expect("scalar"),
        }
    }

    fn py_meta<'py>(py: Python<'py>, pairs: &[(String, String)]) -> Bound<'py, PyAny> {
        list(
            py,
            pairs
                .iter()
                .map(|(k, v)| {
                    PyTuple::new(py, [k.as_str(), v.as_str()])
                        .expect("tuple")
                        .into_any()
                })
                .collect(),
        )
    }

    fn py_output<'py>(py: Python<'py>, o: &schema::Output) -> Bound<'py, PyAny> {
        let mut pairs = vec![
            (
                "key",
                match &o.key {
                    None => py.None().into_bound(py),
                    Some(k) => text(py, k),
                },
            ),
            ("metadata", py_meta(py, &o.metadata)),
            ("payload", raw(py, &o.payload)),
            ("event_time", opt(py, &o.event_time)),
        ];
        match &o.target {
            schema::OutputTarget::Edge { channel, consumer } => {
                pairs.push(("target", text(py, "edge")));
                pairs.push((
                    "channel",
                    (*channel).into_bound_py_any(py).expect("channel"),
                ));
                pairs.push((
                    "consumer",
                    (*consumer).into_bound_py_any(py).expect("consumer"),
                ));
            }
            schema::OutputTarget::Plugin { channel } => {
                pairs.push(("target", text(py, "plugin")));
                pairs.push(("plugin_channel", text(py, channel)));
            }
        }
        dict(py, pairs).into_any()
    }

    fn py_effect<'py>(py: Python<'py>, e: &schema::ItemEffect) -> Bound<'py, PyAny> {
        let (verdict, message) = match &e.verdict {
            schema::Verdict::Ok => ("ok", None),
            schema::Verdict::RetryableError { message } => ("retryable", Some(message.clone())),
            schema::Verdict::NonRetryableError { message } => {
                ("non_retryable", Some(message.clone()))
            }
        };
        dict(
            py,
            vec![
                ("verdict", text(py, verdict)),
                (
                    "message",
                    match &message {
                        None => py.None().into_bound(py),
                        Some(m) => text(py, m),
                    },
                ),
                (
                    "changes",
                    list(py, e.changes.iter().map(|c| raw(py, c)).collect()),
                ),
                (
                    "outputs",
                    list(py, e.outputs.iter().map(|o| py_output(py, o)).collect()),
                ),
                ("consumed_offset", opt(py, &e.consumed_offset)),
                (
                    "event_time",
                    e.event_time.into_bound_py_any(py).expect("event time"),
                ),
                (
                    "timers",
                    list(
                        py,
                        e.timers
                            .iter()
                            .map(|t| {
                                dict(
                                    py,
                                    vec![
                                        ("kind", text(py, t.kind.as_str())),
                                        (
                                            "scheduled_at",
                                            t.scheduled_at
                                                .into_bound_py_any(py)
                                                .expect("scheduled at"),
                                        ),
                                        (
                                            "register",
                                            t.register.into_bound_py_any(py).expect("register"),
                                        ),
                                    ],
                                )
                                .into_any()
                            })
                            .collect(),
                    ),
                ),
                (
                    "checkpoint",
                    e.checkpoint.into_bound_py_any(py).expect("checkpoint"),
                ),
                (
                    "dlq",
                    match &e.dlq {
                        None => py.None().into_bound(py),
                        Some(d) => dict(
                            py,
                            vec![
                                ("channel", text(py, &d.channel)),
                                ("error", text(py, &d.error)),
                                (
                                    "attempts",
                                    d.attempts.into_bound_py_any(py).expect("attempts"),
                                ),
                                (
                                    "payload",
                                    match &d.payload {
                                        None => py.None().into_bound(py),
                                        Some(p) => raw(py, p),
                                    },
                                ),
                            ],
                        )
                        .into_any(),
                    },
                ),
            ],
        )
        .into_any()
    }

    fn py_result<'py>(py: Python<'py>, r: &schema::ActivationResult) -> Bound<'py, PyDict> {
        let mut pairs = vec![("token", r.token.into_bound_py_any(py).expect("token"))];
        match &r.outcome {
            schema::ActivationOutcome::Work(effects) => {
                pairs.push(("outcome", text(py, "work")));
                pairs.push((
                    "effects",
                    list(py, effects.iter().map(|e| py_effect(py, e)).collect()),
                ));
            }
            schema::ActivationOutcome::Restored => pairs.push(("outcome", text(py, "restored"))),
            schema::ActivationOutcome::Snapshot { state } => {
                pairs.push(("outcome", text(py, "snapshot")));
                pairs.push(("state", raw(py, state)));
            }
            schema::ActivationOutcome::StateJson { json } => {
                pairs.push(("outcome", text(py, "state_json")));
                pairs.push(("json", raw(py, json)));
            }
            schema::ActivationOutcome::StateApplied => {
                pairs.push(("outcome", text(py, "state_applied")))
            }
            schema::ActivationOutcome::MigrationState { json } => {
                pairs.push(("outcome", text(py, "migration_state")));
                pairs.push(("json", raw(py, json)));
            }
            schema::ActivationOutcome::Prepared(answers) => {
                pairs.push(("outcome", text(py, "prepared")));
                pairs.push((
                    "prepared",
                    list(
                        py,
                        answers
                            .iter()
                            .map(|a| match a {
                                Ok(answer) => dict(
                                    py,
                                    vec![
                                        (
                                            "keys",
                                            list(
                                                py,
                                                answer.keys.iter().map(|k| text(py, k)).collect(),
                                            ),
                                        ),
                                        ("event_time", opt(py, &answer.event_time)),
                                    ],
                                )
                                .into_any(),
                                Err(e) => dict(
                                    py,
                                    vec![
                                        ("error", text(py, &e.message)),
                                        (
                                            "non_retryable",
                                            e.non_retryable
                                                .into_bound_py_any(py)
                                                .expect("non retryable"),
                                        ),
                                    ],
                                )
                                .into_any(),
                            })
                            .collect(),
                    ),
                ));
            }
            schema::ActivationOutcome::Failed(e) => {
                pairs.push(("outcome", text(py, "failed")));
                pairs.push(("error", text(py, &e.message)));
                pairs.push((
                    "non_retryable",
                    e.non_retryable
                        .into_bound_py_any(py)
                        .expect("non retryable"),
                ));
            }
            schema::ActivationOutcome::WorkflowDone { completed, result } => {
                pairs.push(("outcome", text(py, "workflow_done")));
                pairs.push((
                    "workflow_completed",
                    (*completed).into_bound_py_any(py).expect("completed"),
                ));
                pairs.push(("workflow_result", raw(py, result)));
            }
        }
        dict(py, pairs)
    }

    /// The extension exports exactly what the surface table names, every entry
    /// names a C ABI entry point, and every blocking one is a call the veneer
    /// runs with the GIL released. Asserted on this side because it is the
    /// side that can go wrong without anyone installing the wheel.
    #[test]
    fn the_native_surface_is_the_declared_one() {
        let blocking: Vec<&str> = NATIVE_SURFACE
            .iter()
            .filter(|(_, _, blocking)| *blocking)
            .map(|(name, _, _)| *name)
            .collect();
        assert_eq!(
            blocking,
            [
                "Core.start",
                "Core.poll",
                "Core.wait_starvation",
                "Core.complete",
                "Core.workflow_recv",
                "Core.spawn_workflow"
            ]
        );
        for (name, abi, _) in NATIVE_SURFACE {
            assert!(
                abi.starts_with("flz_core_"),
                "{name} names {abi}, which is not a C ABI entry point"
            );
        }

        // Every blocking export is a `py.detach` call site, so the GIL rule is
        // asserted rather than reviewed: a new blocking entry point that
        // forgot to release the lock fails here.
        let source = include_str!("../src/python_glue.rs");
        let released = source.matches("py.detach(").count();
        assert_eq!(
            released,
            blocking.len(),
            "every blocking export releases the GIL exactly once"
        );
    }

    /// No mutable process-wide global holds the binding handle.
    ///
    /// This is the discipline that keeps a sub-interpreter executor a swap
    /// rather than a rewrite, and it is exactly the kind of thing a later
    /// change reintroduces by accident — a `static CONTEXT` is the shortest
    /// path to "one context per process" and is what the Ruby and JNI veneers
    /// use, because their runtime has one interpreter per process and CPython
    /// does not.
    #[test]
    fn no_process_wide_global_holds_the_context() {
        let source = include_str!("../src/python_glue.rs");
        for (index, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("").trim();
            if code.starts_with("static ") || code.starts_with("pub static ") {
                panic!(
                    "src/python_glue.rs:{} declares a static ({code}); the binding handle is a \
                     #[pyclass] so that a sub-interpreter reaches only its own",
                    index + 1
                );
            }
        }
        assert!(
            source.contains("#[pyclass(module = \"flezha.flz_core_ffi\", name = \"Core\""),
            "the context must stay an opaque pyclass"
        );
    }

    /// A context is created, used and freed entirely through the object, and
    /// freeing twice is a no-op rather than a double free.
    #[test]
    fn the_context_object_owns_its_lifetime() {
        Python::attach(|py| {
            // Constructed the way Python constructs it, so the test drives the
            // surface the wheel exposes rather than the Rust constructor.
            let core = py
                .get_type::<Core>()
                .call1((flz_core::SCHEMA_VERSION,))
                .expect("context");
            assert!(core
                .call_method0("last_error")
                .expect("last_error")
                .is_none());
            core.call_method0("free").expect("free");
            core.call_method0("free").expect("free is idempotent");
            // Every entry point refuses once the context is gone, rather than
            // dereferencing a freed pointer.
            assert!(core
                .call_method1("declare_class", (b"{}".as_slice(),))
                .is_err());
        });
    }

    /// A build against another activation schema is refused at context
    /// creation, naming both versions, before any connection is attempted.
    #[test]
    fn a_schema_mismatch_refuses_before_connecting() {
        Python::attach(|py| {
            let err = py
                .get_type::<Core>()
                .call1((flz_core::SCHEMA_VERSION + 1,))
                .expect_err("must refuse");
            let message = err.value(py).to_string();
            assert!(
                message.contains(&flz_core::SCHEMA_VERSION.to_string())
                    && message.contains(&(flz_core::SCHEMA_VERSION + 1).to_string()),
                "the refusal names both versions: {message}"
            );
            assert!(err.is_instance_of::<flz_core_ffi::python_glue::SchemaMismatch>(py));
        });
    }
}
