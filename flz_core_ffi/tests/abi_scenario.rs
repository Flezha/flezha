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
    let instance =
        flz_core::fast::live_stream_instance("abi_probe").expect("AddOperative built the instance");
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(flz_transport::transport::FlezhaStream::Unix(sock));
    let entry = instance.topology.channel(0).expect("entry channel");
    for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
        // The channel's own entry wrapper — the same one the wire path runs.
        let wrap = entry.wrap_input.as_ref().expect("entry wrapper");
        let value = wrap(payload).expect("bytes passthrough cannot fail");
        flz_core::routing::route_to_consumers(&instance, entry, &value, &stream, None, false);
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
    thread::spawn(move || {
        abi_complete(ctx_addr as *mut FlzContext, &result);
    })
    .join()
    .expect("cross-thread complete");

    // ------------------------------------------ entry-input idle signal
    // The C ABI is the only view a non-Rust host has of the last-delivery
    // stamp, so the two reads are pinned here; the wire path that writes it
    // (`fast::dispatch`) is driven by the Rust SDK's `entry_idle` suite.
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
    let eff_instance = flz_core::fast::live_stream_instance("eff-1").expect("eff instance");
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
    let dyn_instance = flz_core::fast::live_stream_instance("dyn-1").expect("dyn instance");
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
