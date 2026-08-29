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
    shape_veneer::assert_activation_maps(&act);
    (status, Some(act))
}

/// Map a result through the shape and assert it changed nothing.
fn through_veneers(result: &schema::ActivationResult) -> schema::ActivationResult {
    shape_veneer::through_veneer(result)
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
    #[cfg(feature = "ruby-embed")]
    ruby_veneer::primitives_round_trip();

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

/// The shape every binding shares, proved once against a recording pair.
///
/// `veneer::shape` builds an activation into a `serde_json` tree and reads a
/// result back out of one, so which key carries which field is checked here
/// and each binding's own arm proves only that its language builds and reads
/// the primitives. The activation direction is checked field by field rather
/// than by a round trip: the mapping has no inverse in production — nothing
/// ever maps a host value back to an `Activation` — so an inverse written
/// here could agree with a wrong forward mapping.
mod shape_veneer {
    use flz_core::schema;
    use flz_core_ffi::veneer::shape::{self, Key};
    use flz_core_ffi::veneer::Refusal;
    use serde_json::{json, Map, Value};

    /// A language whose containers are `serde_json`'s. A payload is an array
    /// of byte numbers, so it round-trips with no encoding of its own.
    pub struct Recorder;

    impl shape::Sink for Recorder {
        type Value = Value;
        type Map = Map<String, Value>;
        type List = Vec<Value>;
        type Error = Refusal;

        fn map(&self) -> Result<Self::Map, Refusal> {
            Ok(Map::new())
        }

        fn set(&self, map: &mut Self::Map, key: Key, value: Value) -> Result<(), Refusal> {
            map.insert(key.spell(<Self as shape::Sink>::CAMEL).to_string(), value);
            Ok(())
        }

        fn finish_map(&self, map: Self::Map) -> Result<Value, Refusal> {
            Ok(Value::Object(map))
        }

        fn list(&self, capacity: usize) -> Result<Vec<Value>, Refusal> {
            Ok(Vec::with_capacity(capacity))
        }

        fn push(&self, list: &mut Vec<Value>, value: Value) -> Result<(), Refusal> {
            list.push(value);
            Ok(())
        }

        fn finish_list(&self, list: Vec<Value>) -> Result<Value, Refusal> {
            Ok(Value::Array(list))
        }

        fn bytes(&self, v: Vec<u8>) -> Result<Value, Refusal> {
            Ok(raw(&v))
        }

        fn text(&self, v: String) -> Result<Value, Refusal> {
            Ok(Value::String(v))
        }

        fn word(&self, v: &'static str) -> Result<Value, Refusal> {
            Ok(Value::String(v.to_string()))
        }

        fn pair(&self, key: String, value: String) -> Result<Value, Refusal> {
            Ok(json!([key, value]))
        }

        fn u64(&self, v: u64) -> Result<Value, Refusal> {
            Ok(json!(v))
        }

        fn u32(&self, v: u32) -> Result<Value, Refusal> {
            Ok(json!(v))
        }

        fn bool(&self, v: bool) -> Result<Value, Refusal> {
            Ok(Value::Bool(v))
        }
    }

    impl shape::Source for Recorder {
        type Value = Value;
        type Map = Map<String, Value>;
        type Error = Refusal;

        fn get(&self, map: &Self::Map, key: Key) -> Result<Option<Value>, Refusal> {
            Ok(map
                .get(key.spell(<Self as shape::Source>::CAMEL))
                .filter(|v| !v.is_null())
                .cloned())
        }

        fn map(&self, v: &Value, what: &str) -> Result<Self::Map, Refusal> {
            v.as_object()
                .cloned()
                .ok_or_else(|| Refusal::Shape(format!("{what} must be an object")))
        }

        fn text(&self, v: &Value) -> Result<String, Refusal> {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| Refusal::Shape(format!("{v} is not a string")))
        }

        fn bytes(&self, v: &Value, what: &str) -> Result<Vec<u8>, Refusal> {
            let items = v
                .as_array()
                .ok_or_else(|| Refusal::Shape(format!("{what} must be bytes")))?;
            items
                .iter()
                .map(|b| {
                    b.as_u64()
                        .map(|b| b as u8)
                        .ok_or_else(|| Refusal::Shape(format!("{what} must be bytes")))
                })
                .collect()
        }

        fn u64(&self, v: &Value, what: &str) -> Result<u64, Refusal> {
            v.as_u64()
                .ok_or_else(|| Refusal::Shape(format!("{what} must be an integer")))
        }

        fn u32(&self, v: &Value) -> Result<u32, Refusal> {
            Ok(shape::Source::u64(self, v, "a 32-bit field")? as u32)
        }

        fn bool(&self, v: &Value) -> Result<bool, Refusal> {
            v.as_bool()
                .ok_or_else(|| Refusal::Shape(format!("{v} is not a boolean")))
        }

        fn items(&self, v: &Value) -> Result<Vec<Value>, Refusal> {
            v.as_array()
                .cloned()
                .ok_or_else(|| Refusal::Shape(format!("{v} is not a list")))
        }

        fn pair(&self, v: &Value) -> Result<(String, String), Refusal> {
            match v.as_array().map(Vec::as_slice) {
                Some([key, value]) => Ok((
                    shape::Source::text(self, key)?,
                    shape::Source::text(self, value)?,
                )),
                _ => Err(Refusal::Shape(
                    "a metadata entry must be a (key, value) pair".to_string(),
                )),
            }
        }
    }

    /// Build every primitive the shape asks a language for and read it back,
    /// so a container a binding cannot round-trip fails here rather than at a
    /// branch in that SDK. Written once and called by each arm: what differs
    /// between the bindings is only these primitives.
    ///
    /// `word` is not read back. It is write-only in production — a host
    /// answers `outcome`, `verdict` and a timer `kind` as text, never as the
    /// language's word type — so each arm asserts its own spelling instead.
    pub fn primitives_round_trip<S, V, M>(s: &S)
    where
        S: shape::Sink<Value = V, Map = M> + shape::Source<Value = V, Map = M>,
        <S as shape::Sink>::Error: std::fmt::Debug,
        <S as shape::Source>::Error: std::fmt::Debug,
    {
        let mut map = shape::Sink::map(s).expect("a map");
        let put = |map: &mut M, key, value| {
            shape::Sink::set(s, map, key, value).expect("the key is set")
        };
        put(&mut map, shape::TOKEN, shape::Sink::u64(s, u64::MAX).expect("a u64"));
        put(&mut map, shape::PROCEDURE, shape::Sink::u32(s, 7).expect("a u32"));
        put(&mut map, shape::REPLAY, shape::Sink::bool(s, true).expect("a bool"));
        put(&mut map, shape::FACTORY, shape::Sink::text(s, "f".to_string()).expect("text"));
        put(&mut map, shape::PAYLOAD, shape::Sink::bytes(s, vec![0, 255, 7]).expect("bytes"));
        let mut list = shape::Sink::list(s, 1).expect("a list");
        let pair = shape::Sink::pair(s, "k".to_string(), "v".to_string()).expect("a pair");
        shape::Sink::push(s, &mut list, pair).expect("the pair is pushed");
        put(&mut map, shape::METADATA, shape::Sink::finish_list(s, list).expect("a list"));

        let want = |key| {
            shape::Source::get(s, &map, key)
                .expect("the key reads")
                .expect("the key is present")
        };
        assert_eq!(
            shape::Source::u64(s, &want(shape::TOKEN), "token").expect("a u64"),
            u64::MAX
        );
        assert_eq!(shape::Source::u32(s, &want(shape::PROCEDURE)).expect("a u32"), 7);
        assert!(shape::Source::bool(s, &want(shape::REPLAY)).expect("a bool"));
        assert_eq!(
            shape::Source::text(s, &want(shape::FACTORY)).expect("text"),
            "f"
        );
        assert_eq!(
            shape::Source::bytes(s, &want(shape::PAYLOAD), "payload").expect("bytes"),
            vec![0, 255, 7]
        );
        let rows = shape::Source::items(s, &want(shape::METADATA)).expect("a list");
        assert_eq!(
            shape::Source::pair(s, &rows[0]).expect("a pair"),
            ("k".to_string(), "v".to_string())
        );
        // An absent key reads as absent, not as the language's nil.
        assert!(shape::Source::get(s, &map, shape::ENTITY_KEY)
            .expect("an absent key reads")
            .is_none());
    }

    /// The helper every arm calls, proved against the recording pair itself.
    #[test]
    fn the_recording_pair_round_trips_every_primitive() {
        primitives_round_trip(&Recorder);
    }

    fn raw(bytes: &[u8]) -> Value {
        Value::Array(bytes.iter().map(|b| json!(b)).collect())
    }

    // --- reading the recorded activation ----------------------------------

    fn obj(v: &Value) -> &Map<String, Value> {
        v.as_object().expect("the shape emits maps")
    }

    fn at<'a>(h: &'a Map<String, Value>, key: &str) -> &'a Value {
        h.get(key).unwrap_or_else(|| panic!("the shape dropped `{key}`"))
    }

    fn num(h: &Map<String, Value>, key: &str) -> u64 {
        at(h, key).as_u64().expect("an integer field")
    }

    fn text(h: &Map<String, Value>, key: &str) -> String {
        at(h, key).as_str().expect("a text field").to_string()
    }

    fn opt_text(h: &Map<String, Value>, key: &str) -> Option<String> {
        h.get(key).map(|v| v.as_str().expect("a text field").to_string())
    }

    fn bytes_at(h: &Map<String, Value>, key: &str) -> Vec<u8> {
        at(h, key)
            .as_array()
            .expect("a payload field")
            .iter()
            .map(|b| b.as_u64().expect("a byte") as u8)
            .collect()
    }

    fn rows<'a>(h: &'a Map<String, Value>, key: &str) -> &'a [Value] {
        at(h, key).as_array().expect("a list field")
    }

    fn meta(h: &Map<String, Value>, key: &str) -> Vec<(String, String)> {
        rows(h, key)
            .iter()
            .map(|pair| {
                let pair = pair.as_array().expect("a metadata pair is a list");
                (
                    pair[0].as_str().expect("a key").to_string(),
                    pair[1].as_str().expect("a value").to_string(),
                )
            })
            .collect()
    }

    fn assert_body_maps(recorded: &Value, body: &schema::RestoreBody) {
        let h = obj(recorded);
        assert_eq!(bytes_at(h, "bytes"), body.bytes);
        assert_eq!(
            h.get("from_version").map(|v| v.as_u64().expect("a version") as u32),
            body.from_version
        );
    }

    fn assert_item_maps(recorded: &Value, item: &schema::WorkItem) {
        let h = obj(recorded);
        match item {
            schema::WorkItem::Input(i) => {
                assert_eq!(text(h, "kind"), "input");
                assert_eq!(num(h, "channel") as u32, i.channel);
                assert_eq!(bytes_at(h, "payload"), i.payload);
                assert_eq!(meta(h, "metadata"), i.metadata);
                assert_eq!(h.get("consumed_offset").map(|v| v.as_u64().unwrap()), i.consumed_offset);
                assert_eq!(at(h, "replay").as_bool(), Some(i.replay));
                assert_eq!(num(h, "now"), i.now);
            }
            schema::WorkItem::Timer(t) => {
                assert_eq!(text(h, "kind"), "timer");
                assert_eq!(text(h, "timer_kind"), t.kind.as_str());
                assert_eq!(num(h, "scheduled_at"), t.scheduled_at);
                // A timer firing's `now` IS its scheduled time, so a handler
                // reads one field for both kinds.
                assert_eq!(num(h, "now"), t.scheduled_at);
            }
        }
    }

    /// Assert the decoded activation reaches a host intact.
    pub fn assert_activation_maps(act: &schema::Activation) {
        let recorded = shape::activation(&Recorder, act.clone()).expect("the shape builds it");
        let h = obj(&recorded);
        assert_eq!(num(h, "token"), act.token);
        assert_eq!(text(h, "operative"), act.target.operative);
        assert_eq!(num(h, "procedure") as u32, act.target.procedure);
        assert_eq!(opt_text(h, "entity_key"), act.target.entity_key);
        assert_eq!(num(h, "change_seq"), act.change_seq);
        assert_eq!(text(h, "factory"), act.factory);
        assert_kind_maps(h, &act.kind);
    }

    fn assert_kind_maps(h: &Map<String, Value>, kind: &schema::ActivationKind) {
        match kind {
            schema::ActivationKind::Work(items) => {
                assert_eq!(text(h, "kind"), "work");
                let mapped = rows(h, "items");
                assert_eq!(mapped.len(), items.len());
                for (recorded, item) in mapped.iter().zip(items) {
                    assert_item_maps(recorded, item);
                }
            }
            schema::ActivationKind::RestoreFold(fold) => {
                assert_eq!(text(h, "kind"), "restore_fold");
                assert_eq!(h.contains_key("snapshot"), fold.snapshot.is_some());
                if let Some(snapshot) = &fold.snapshot {
                    assert_body_maps(at(h, "snapshot"), snapshot);
                }
                let changes = rows(h, "changes");
                assert_eq!(changes.len(), fold.changes.len());
                for (recorded, body) in changes.iter().zip(&fold.changes) {
                    assert_body_maps(recorded, body);
                }
            }
            schema::ActivationKind::SnapshotRequest => assert_eq!(text(h, "kind"), "snapshot_request"),
            schema::ActivationKind::LiveStateRead => assert_eq!(text(h, "kind"), "live_state_read"),
            schema::ActivationKind::ApplyState { state_json } => {
                assert_eq!(text(h, "kind"), "apply_state");
                assert_eq!(bytes_at(h, "state_json"), *state_json);
            }
            schema::ActivationKind::MigrationSerialize => {
                assert_eq!(text(h, "kind"), "migration_serialize")
            }
            schema::ActivationKind::KeyRequest(request) => {
                assert_eq!(text(h, "kind"), "key_request");
                let mapped = obj(at(h, "key_request"));
                assert_eq!(num(mapped, "channel") as u32, request.channel);
                assert_eq!(at(mapped, "want_keys").as_bool(), Some(request.want_keys));
                assert_eq!(
                    at(mapped, "want_event_time").as_bool(),
                    Some(request.want_event_time)
                );
                let payloads: Vec<Vec<u8>> = rows(mapped, "payloads")
                    .iter()
                    .map(|p| p.as_array().expect("a payload").iter().map(|b| b.as_u64().unwrap() as u8).collect())
                    .collect();
                assert_eq!(payloads, request.payloads);
            }
            schema::ActivationKind::WorkflowRun { args } => {
                assert_eq!(text(h, "kind"), "workflow_run");
                assert_eq!(bytes_at(h, "args"), *args);
            }
            schema::ActivationKind::CompensateRun { outputs } => {
                assert_eq!(text(h, "kind"), "compensate_run");
                let mapped: Vec<Vec<u8>> = rows(h, "outputs")
                    .iter()
                    .map(|o| o.as_array().expect("an output").iter().map(|b| b.as_u64().unwrap() as u8).collect())
                    .collect();
                assert_eq!(mapped, *outputs);
            }
        }
    }

    // --- writing a result the way a host answers --------------------------

    /// Build the result in the shape a host answers with, read it back
    /// through `shape::result`, and assert nothing changed. The scenario then
    /// completes with the mapped value, so a lossy field fails the phase that
    /// depends on it and not only this equality.
    pub fn through_veneer(result: &schema::ActivationResult) -> schema::ActivationResult {
        let mapped = shape::result(&Recorder, &record_result(result)).expect("the shape reads it");
        assert_eq!(&mapped, result, "the shape changed the result");
        mapped
    }

    fn record_result(r: &schema::ActivationResult) -> Map<String, Value> {
        let mut out = Map::new();
        out.insert("token".into(), json!(r.token));
        let mut say = |outcome: &str| out.insert("outcome".into(), json!(outcome));
        match &r.outcome {
            schema::ActivationOutcome::Work(effects) => {
                say("work");
                let effects: Vec<Value> = effects.iter().map(record_effect).collect();
                out.insert("effects".into(), json!(effects));
            }
            schema::ActivationOutcome::Restored => {
                say("restored");
            }
            schema::ActivationOutcome::Snapshot { state } => {
                say("snapshot");
                out.insert("state".into(), raw(state));
            }
            schema::ActivationOutcome::StateJson { json } => {
                say("state_json");
                out.insert("json".into(), raw(json));
            }
            schema::ActivationOutcome::StateApplied => {
                say("state_applied");
            }
            schema::ActivationOutcome::MigrationState { json } => {
                say("migration_state");
                out.insert("json".into(), raw(json));
            }
            schema::ActivationOutcome::Prepared(answers) => {
                say("prepared");
                let answers: Vec<Value> = answers.iter().map(record_prepared).collect();
                out.insert("prepared".into(), json!(answers));
            }
            schema::ActivationOutcome::Failed(e) => {
                say("failed");
                out.insert("error".into(), json!(e.message));
                out.insert("non_retryable".into(), json!(e.non_retryable));
            }
            schema::ActivationOutcome::WorkflowDone { completed, result } => {
                say("workflow_done");
                out.insert("workflow_completed".into(), json!(completed));
                out.insert("workflow_result".into(), raw(result));
            }
        }
        out
    }

    fn record_effect(e: &schema::ItemEffect) -> Value {
        let (verdict, message) = match &e.verdict {
            schema::Verdict::Ok => ("ok", Value::Null),
            schema::Verdict::RetryableError { message } => ("retryable", json!(message)),
            schema::Verdict::NonRetryableError { message } => ("non_retryable", json!(message)),
        };
        json!({
            "verdict": verdict,
            "message": message,
            "changes": e.changes.iter().map(|c| raw(c)).collect::<Vec<_>>(),
            "outputs": e.outputs.iter().map(record_output).collect::<Vec<_>>(),
            "consumed_offset": e.consumed_offset,
            "event_time": e.event_time,
            "timers": e.timers.iter().map(|t| json!({
                "kind": t.kind.as_str(),
                "scheduled_at": t.scheduled_at,
                "register": t.register,
            })).collect::<Vec<_>>(),
            "checkpoint": e.checkpoint,
            "dlq": e.dlq.as_ref().map(|d| json!({
                "channel": d.channel,
                "error": d.error,
                "attempts": d.attempts,
                "payload": d.payload.as_deref().map(raw),
            })),
        })
    }

    fn record_output(o: &schema::Output) -> Value {
        let mut recorded = json!({
            "key": o.key,
            "metadata": o.metadata.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
            "payload": raw(&o.payload),
            "event_time": o.event_time,
        });
        let map = recorded.as_object_mut().expect("an output is an object");
        match &o.target {
            schema::OutputTarget::Edge { channel, consumer } => {
                map.insert("target".into(), json!("edge"));
                map.insert("channel".into(), json!(channel));
                map.insert("consumer".into(), json!(consumer));
            }
            schema::OutputTarget::Plugin { channel } => {
                map.insert("target".into(), json!("plugin"));
                map.insert("plugin_channel".into(), json!(channel));
            }
        }
        recorded
    }

    fn record_prepared(a: &Result<schema::PrepareAnswer, schema::WorkError>) -> Value {
        match a {
            Ok(answer) => json!({ "keys": answer.keys, "event_time": answer.event_time }),
            Err(e) => json!({ "error": e.message, "non_retryable": e.non_retryable }),
        }
    }
}

/// The Node addon's surface. Its half of the shape — how JavaScript builds
/// and reads a value — cannot be driven from here: a Rust test binary has no
/// `Env`, so `Values` is proved by the compile and by the Node suite, and the
/// mapping it feeds is `shape_veneer`'s.
#[cfg(feature = "napi")]
mod napi_veneer {
    use flz_core_ffi::napi_glue::native_surface;
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

/// The Ruby half of the shape, proved with no Ruby process involved: an
/// embedded CRuby VM, and every primitive `veneer::shape` asks a language for
/// built and read back. The mapping itself is `shape_veneer`'s.
#[cfg(feature = "ruby-embed")]
mod ruby_veneer {
    use flz_core_ffi::ruby_glue::{Values, NATIVE_SURFACE};
    use flz_core_ffi::veneer::shape;
    use magnus::{Ruby, Symbol};

    /// Called from inside `c_abi_full_scenario` rather than as a `#[test]` of
    /// its own: `magnus::embed::init` runs once per process and records the
    /// caller's stack frame as the GC scan root, so every Ruby value must sit
    /// deeper in that one test's stack.
    pub fn primitives_round_trip() {
        // SAFETY: the scenario booted the VM on this thread before reaching
        // any code that calls this.
        let ruby = unsafe { Ruby::get_unchecked() };
        let values = Values(&ruby);
        super::shape_veneer::primitives_round_trip(&values);

        // Ruby is the one language whose vocabulary word is not text.
        let word = shape::Sink::word(&values, shape::word::WORK).expect("a word");
        let word = Symbol::from_value(word).expect("a word is a Symbol");
        assert_eq!(word.name().unwrap(), shape::word::WORK);
    }

    /// The extension exports exactly what the surface table names, every entry
    /// names a C ABI entry point, and every blocking one is a call the veneer
    /// runs with the GVL released. Asserted on this side because it is the
    /// side that can go wrong without anyone installing the gem.
    #[test]
    fn the_native_surface_is_the_declared_one() {
        let blocking: Vec<&str> = NATIVE_SURFACE
            .iter()
            .filter(|(_, abi)| flz_core_ffi::BLOCKING_ENTRY_POINTS.contains(abi))
            .map(|(name, _)| *name)
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
        for (name, abi) in NATIVE_SURFACE {
            assert!(
                abi.starts_with("flz_core_"),
                "{name} names {abi}, which is not a C ABI entry point"
            );
        }
    }
}

/// The Python half of the shape, proved with no Python process involved: an
/// embedded CPython interpreter, and every primitive `veneer::shape` asks a
/// language for built and read back. The mapping itself is `shape_veneer`'s.
#[cfg(feature = "python")]
mod python_veneer {
    use flz_core_ffi::python_glue::{Core, PayloadView, Values, NATIVE_SURFACE};
    use flz_core_ffi::veneer::shape;
    use pyo3::prelude::*;

    /// CPython attaches from any thread once the interpreter is initialised,
    /// so this is an ordinary `#[test]` — unlike Ruby's, which must run inside
    /// the scenario that booted the VM.
    #[test]
    fn every_primitive_round_trips() {
        Python::initialize();
        Python::attach(|py| {
            let values = Values(py);
            super::shape_veneer::primitives_round_trip(&values);

            // Python spells a vocabulary word as text, and state JSON as
            // `bytes` rather than as the borrowed view a payload crosses as.
            let word = shape::Sink::word(&values, shape::word::WORK).expect("a word");
            assert_eq!(word.extract::<String>().expect("text"), shape::word::WORK);
            let json = shape::Sink::json(&values, b"{}".to_vec()).expect("state json");
            assert!(json.is_instance_of::<pyo3::types::PyBytes>());
            let payload = shape::Sink::bytes(&values, b"{}".to_vec()).expect("a payload");
            assert!(payload.is_instance_of::<PayloadView>());
            let copied = payload.call_method0("tobytes").expect("a view copies");
            assert_eq!(copied.extract::<Vec<u8>>().expect("bytes"), b"{}");
        });
    }
    /// The extension exports exactly what the surface table names, every entry
    /// names a C ABI entry point, and every blocking one is a call the veneer
    /// runs with the GIL released. Asserted on this side because it is the
    /// side that can go wrong without anyone installing the wheel.
    #[test]
    fn the_native_surface_is_the_declared_one() {
        let blocking: Vec<&str> = NATIVE_SURFACE
            .iter()
            .filter(|(_, abi)| flz_core_ffi::BLOCKING_ENTRY_POINTS.contains(abi))
            .map(|(name, _)| *name)
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
        for (name, abi) in NATIVE_SURFACE {
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
