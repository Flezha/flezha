//! One sequential run of every capability the C ABI carries, in the order a
//! host meets them.
//!
//! The context, the engine and the fake controller are process-global, so the
//! fourteen phases run in order inside a single test fn (the
//! `runner_lifecycle` pattern) and each takes the state the phase before it
//! left behind.

use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use flz_core::schema;
use flz_core_ffi::*;
use flz_transport::transport::FlezhaStream;
use prost::Message;

use crate::harness::{
    abi_complete, abi_complete_mapped, abi_poll, last_error, ok_effect, through_veneers,
    work_payloads, Controller, Demanded,
};

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
    crate::veneer_ruby::primitives_round_trip();

    // CPython is initialised here rather than on first use: initialisation
    // belongs on the main thread, and two phases below poll and complete from
    // spawned ones.
    #[cfg(feature = "python")]
    pyo3::Python::initialize();

    a_mismatched_schema_is_refused();
    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null(), "matching schema creates a context");

    a_local_mode_call_on_a_context_with_no_local_run_is_refused(ctx);

    let controller = Controller::start(&socket_dir());
    declares_and_starts(ctx, &controller);

    // The peer half is held for the whole scenario: dropping it would break
    // every routed write.
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
    let stream = Arc::new(FlezhaStream::Unix(sock));

    let act = routes_two_entry_payloads(ctx, &stream);
    let act = abandon_re_hands_the_batch(ctx, act);
    let due_at = completes_from_another_thread(ctx, &act);
    the_timer_fires_through_poll(ctx, due_at);
    a_ping_generates_no_activation(ctx, &controller);
    poll_honors_its_timeout(ctx);
    restores_and_applies_the_offset_floor(ctx, &controller, &stream);
    a_live_read_answers_with_the_host_json(ctx, &controller);
    a_keyed_dynamic_route_crosses_for_its_key(ctx, &controller, &stream);
    a_workflow_tasklet_completes_through_the_abi(ctx, &controller);
    crate::shutdown::shutdown_ships_state_then_wakes_the_poller(ctx, &controller);

    unsafe { flz_core_context_free(ctx) };
}

/// Local mode is an alternative to `flz_core_start`, not an addition to it:
/// a context that holds no local run refuses every local-mode call rather
/// than answering as if it did. Run before the controller connect, because
/// after it this context is a started runner and can never hold one.
fn a_local_mode_call_on_a_context_with_no_local_run_is_refused(ctx: *mut FlzContext) {
    let channel = CString::new("lines").expect("a channel name");
    let status = unsafe { flz_core_local_feed(ctx, channel.as_ptr(), b"x".as_ptr(), 1) };
    assert_eq!(status, FLZ_ERR, "a feed with no local run must refuse");
    assert!(
        last_error(ctx).contains("no local run"),
        "the refusal says what is missing: {}",
        last_error(ctx)
    );
    assert_eq!(
        unsafe { flz_core_local_run_until_idle(ctx, 1) },
        FLZ_ERR,
        "so must an idle wait"
    );
    assert_eq!(
        unsafe { flz_core_local_stop(ctx) },
        FLZ_OK,
        "stopping a run that never started is a no-op, so a host's close path needs no test of its own"
    );
}

/// A shim built against an incompatible schema is refused at creation, with
/// an error naming both versions, before any connection.
fn a_mismatched_schema_is_refused() {
    let mismatched = flz_core_context_new(flz_core::SCHEMA_VERSION + 1);
    assert!(mismatched.is_null(), "mismatched schema must refuse");
    let err = unsafe { CStr::from_ptr(flz_core_init_error()) }
        .to_string_lossy()
        .into_owned();
    assert!(
        err.contains(&format!("{}", flz_core::SCHEMA_VERSION))
            && err.contains(&format!("{}", flz_core::SCHEMA_VERSION + 1)),
        "refusal names both versions: {err}"
    );
}

/// The socket directory, and the environment the context reads at `start`.
pub(crate) fn socket_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("flz-abi-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "300000");
    dir
}

/// Declare the pass-through class, connect, and add the operative through the
/// real control path.
pub(crate) fn declares_and_starts(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_probe",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [{"name": "in", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]}],
        "tasklets": [{"name": "abi_pass"}]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );

    let ns = CString::new("abi_ns").unwrap();
    assert_eq!(unsafe { flz_core_start(ctx, ns.as_ptr()) }, FLZ_OK);
    assert_eq!(controller.accepted(), 1, "one control connect");
    // "Connects": the init frame names the declared factory.
    controller.await_frame("init frame", |captured| {
        captured
            .iter()
            .find(|(id, m)| {
                id == "connect_control_channel" && String::from_utf8_lossy(m).contains("abi_probe")
            })
            .map(|_| ())
    });

    controller.send(
        "ref-add-1",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "abi_probe".to_string(),
            factory_id: "abi_probe".to_string(),
            ..Default::default()
        }),
    );
    controller.await_success("AddOperative response", "ref-add-1");
}

/// Route two entry payloads into the instance the runner built, and take the
/// batch the ABI hands out.
pub(crate) fn routes_two_entry_payloads(
    ctx: *mut FlzContext,
    stream: &Arc<FlezhaStream>,
) -> schema::Activation {
    let instance = flz_core::runner::live_stream_instance("abi_probe")
        .expect("AddOperative built the instance");
    let entry = instance.topology.channel(0).expect("entry channel");
    for payload in [b"alpha".to_vec(), b"beta".to_vec()] {
        // The channel's own entry wrapper — the same one the wire path runs.
        let wrap = entry.wrap_input.as_ref().expect("entry wrapper");
        let value = wrap(payload).expect("bytes passthrough cannot fail");
        flz_core::routing::route_to_consumers(&instance, entry, &value, stream, None, false, None);
    }

    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "queued inputs hand out");
    let act = act.unwrap();
    assert_eq!(
        work_payloads(&act),
        vec![b"alpha".to_vec(), b"beta".to_vec()]
    );
    assert_eq!(act.target.tasklet, 0);
    act
}

/// The third answer, and the one the ABI has to name because no unwind
/// crosses it: a veneer whose language threw cannot reach `complete`, and
/// per-cell exclusivity would then retire this cell for the life of the
/// process. Abandoning re-hands the batch untouched.
fn abandon_re_hands_the_batch(ctx: *mut FlzContext, act: schema::Activation) -> schema::Activation {
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
    assert_eq!(status, FLZ_POLL_ACTIVATION, "an abandoned cell polls again");
    let act = act.unwrap();
    assert_eq!(
        work_payloads(&act),
        vec![b"alpha".to_vec(), b"beta".to_vec()],
        "abandon re-hands the items untouched and in order"
    );
    act
}

/// Complete from a thread that never polled (the thread-safety pin),
/// registering a processing-time timer 30 ms out on the last item.
fn completes_from_another_thread(ctx: *mut FlzContext, act: &schema::Activation) -> u64 {
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
    due_at
}

/// The registered timer surfaces as a work batch of exactly one firing, and
/// its removal rides the completion.
fn the_timer_fires_through_poll(ctx: *mut FlzContext, due_at: u64) {
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
}

fn a_ping_generates_no_activation(ctx: *mut FlzContext, controller: &Controller) {
    controller.send(
        "ref-ping-1",
        flz_core::pb::command_request::Command::Ping(flz_core::pb::Ping::default()),
    );
    controller.await_reply("ping response", "ref-ping-1");
    let (status, _) = abi_poll(ctx, 300);
    assert_eq!(status, FLZ_POLL_TIMEOUT, "Ping generated no activation");
}

fn poll_honors_its_timeout(ctx: *mut FlzContext) {
    let started = Instant::now();
    let (status, _) = abi_poll(ctx, 100);
    assert_eq!(status, FLZ_POLL_TIMEOUT);
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(90) && elapsed < Duration::from_millis(1_000),
        "timeout bound violated: {elapsed:?}"
    );
}

/// A persistent effectively-once host class: the controller's
/// RestoreInitState becomes a RestoreFold activation carrying the naked
/// host-encoded bodies (plain records verbatim, effect records unwrapped),
/// and the restored consumed-offset floor skips already-applied inputs.
fn restores_and_applies_the_offset_floor(
    ctx: *mut FlzContext,
    controller: &Controller,
    stream: &Arc<FlezhaStream>,
) {
    declares_the_effectively_once_class(ctx, controller);
    restores_from_a_snapshot_and_a_wal(ctx, controller);
    the_restored_floor_skips_applied_inputs(ctx, stream);
}

fn declares_the_effectively_once_class(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_eff",
        "persistent": true,
        "effectively_once": true,
        "sdk_lang": 2,"sdk_version":"0.1.0",
        "change_format_id": 4,
        "channels": [
            {"name": "ein", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]},
            {"name": "eout", "kind": "plugin", "producer": 0, "keyed": true,
             "chains": [[{"link": "sink", "name": "eout"}]]}
        ],
        "tasklets": [{
            "name": "eff_pass",
            "schema_version": 2,
            "min_readable_schema_version": 1
        }]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    controller.send(
        "ref-add-eff",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "eff-1".to_string(),
            factory_id: "abi_eff".to_string(),
            ..Default::default()
        }),
    );
    controller.await_success("AddOperative eff response", "ref-add-eff");
}

/// On-disk WAL records: seq 1 a plain (host-codec) change, seq 2 an effect
/// record whose consumed offset seeds the floor. The class runs at
/// schema_version 2 reading down to 1, so the snapshot (stamped 1) crosses
/// tagged for migration while these records (stamped 2) cross to be decoded —
/// both arms of the decision, in one restore.
fn change_log() -> Vec<u8> {
    let effect_body = postcard::to_stdvec(&flz_core::flusher::EffectRecordBody {
        changes: vec![vec![0xC2]],
        outputs: vec![],
        consumed_offset: Some(7),
        dlq: None,
        event_time: 1_234,
        timers: vec![],
    })
    .unwrap();
    let mut log = wal_record(1, 4, &[0xC1]);
    log.extend(wal_record(
        2,
        flz_core::header::FORMAT_ID_EFFECT_RECORD,
        &effect_body,
    ));
    log
}

fn header_for(format_id: u8, schema_version: u32, body: &[u8]) -> [u8; 16] {
    flz_core::header::HeaderTemplate {
        sdk_lang: 2,
        sdk_version: 0,
        schema_version,
        format_id,
    }
    .stamp(body)
}

fn wal_record(seq: u64, format_id: u8, body: &[u8]) -> Vec<u8> {
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
}

fn restores_from_a_snapshot_and_a_wal(ctx: *mut FlzContext, controller: &Controller) {
    let cell_id = flz_core::flusher::encode_cell_identity("eff-1", "eff_pass", "");
    controller.send(
        "ref-restore-1",
        flz_core::pb::command_request::Command::RestoreInitState(flz_core::pb::RestoreInitState {
            cell_id,
            snapshot_bytes: vec![0x5A],
            snapshot_header: header_for(4, 1, &[0x5A]).to_vec(),
            change_log_bytes: change_log(),
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
    assert_restored_bodies(fold);
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Restored,
        },
    );

    let reply = controller.await_reply("restore ack (terminal seq 2)", "ref-restore-1");
    let ack = flz_core::pb::RestoreInitStateResponse::decode(reply.raw_data.as_slice())
        .expect("a restore ack");
    assert_eq!(ack.terminal_seq, 2);
}

fn assert_restored_bodies(fold: &schema::RestoreFold) {
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
}

/// The fold's effect record consumed offset 7: a re-forwarded input at or
/// below the floor is skipped, one above it dispatches.
fn the_restored_floor_skips_applied_inputs(ctx: *mut FlzContext, stream: &Arc<FlezhaStream>) {
    let instance = flz_core::runner::live_stream_instance("eff-1").expect("eff instance");
    let entry = instance.topology.channel(0).expect("entry channel");
    let wrap = entry.wrap_input.as_ref().expect("entry wrapper");

    let below = wrap(b"dup".to_vec()).unwrap();
    flz_core::routing::route_to_consumers(&instance, entry, &below, stream, Some(7), false, None);
    let (status, _) = abi_poll(ctx, 300);
    assert_eq!(
        status, FLZ_POLL_TIMEOUT,
        "offset 7 sits at the restored floor"
    );

    let above = wrap(b"new".to_vec()).unwrap();
    flz_core::routing::route_to_consumers(&instance, entry, &above, stream, Some(8), false, None);
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
}

/// CellState becomes a LiveStateRead activation; the host's JSON answer
/// lands in the command reply under the Decision-10 shape.
fn a_live_read_answers_with_the_host_json(ctx: *mut FlzContext, controller: &Controller) {
    controller.send(
        "ref-read-1",
        flz_core::pb::command_request::Command::CellState(flz_core::pb::CellState {
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

    let reply = controller.await_reply("cell_state reply carrying the host JSON", "ref-read-1");
    let body = String::from_utf8_lossy(&reply.raw_data);
    assert!(
        body.contains("eff_pass") && body.contains("\"n\":3"),
        "the reply carries the host's own JSON: {body}"
    );
}

fn a_keyed_dynamic_route_crosses_for_its_key(
    ctx: *mut FlzContext,
    controller: &Controller,
    stream: &Arc<FlezhaStream>,
) {
    declares_the_dynamic_class(ctx, controller);
    let instance = flz_core::runner::live_stream_instance("dyn-1").expect("dyn instance");
    let entry = instance.topology.channel(0).expect("entry channel");
    // The wire-dispatch staging seam (the same call `OperativeDispatchHandle::
    // dispatch` makes): a keyed consumer holds the payload for the crossing.
    let staged = flz_core::host::maybe_stage_prepare(
        &instance,
        0,
        entry,
        b"k1:v".to_vec(),
        None,
        false,
        stream,
    );
    assert!(staged.is_none(), "keyed consumer stages for the crossing");
    answers_the_prepare_crossing(ctx);
    the_keyed_input_reaches_its_own_cell(ctx);
}

fn answers_the_prepare_crossing(ctx: *mut FlzContext) {
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
}

fn the_keyed_input_reaches_its_own_cell(ctx: *mut FlzContext) {
    let (status, act) = abi_poll(ctx, 2_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "keyed input dispatches");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "dyn-1");
    assert_eq!(
        act.target.cell_key.as_deref(),
        Some("k1"),
        "the prepare answer's key selected the lazily-created cell"
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(vec![ok_effect(None, vec![])]),
        },
    );
}

fn declares_the_dynamic_class(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_dyn",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [{"name": "din", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]}],
        "tasklets": [{"name": "dyn_pass", "cell_mode": "dynamic"}]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    controller.send(
        "ref-add-dyn",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "dyn-1".to_string(),
            factory_id: "abi_dyn".to_string(),
            ..Default::default()
        }),
    );
    controller.await_success("AddOperative dyn response", "ref-add-dyn");
}

fn a_workflow_tasklet_completes_through_the_abi(ctx: *mut FlzContext, controller: &Controller) {
    declares_the_workflow_class(ctx, controller);
    let (status, act) = abi_poll(ctx, 5_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "the workflow's step hands out");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "wf-1");
    assert!(
        matches!(act.kind, schema::ActivationKind::Work(_)),
        "a workflow step is ordinary work, got {:?}",
        act.kind
    );
    abi_complete(
        ctx,
        &schema::ActivationResult {
            token: act.token,
            outcome: schema::ActivationOutcome::Work(vec![schema::ItemEffect {
                complete: Some(b"done".to_vec()),
                ..ok_effect(None, Vec::new())
            }]),
        },
    );

    controller.await_frame("WorkflowCompleted teardown", |captured| {
        captured.iter().find_map(|(id, m)| {
            (id == "control_command"
                && flz_core::pb::CommandRequest::decode(m.as_slice()).is_ok_and(|r| {
                    matches!(
                        r.command,
                        Some(flz_core::pb::command_request::Command::WorkflowCompleted(ref c))
                            if c.operative_id == "wf-1"
                                && c.status == "ok"
                                && c.result == b"done".to_vec()
                    )
                }))
            .then_some(())
        })
    });
}

fn declares_the_workflow_class(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_workflow",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [{"name": "in", "kind": "entry", "chains": [[{"link": "into", "tasklet": 0}]]}],
        "tasklets": [{"name": "wf_step"}],
        "workflow": {}
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    controller.send(
        "ref-add-wf",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "wf-1".to_string(),
            factory_id: "abi_workflow".to_string(),
            kind: "workflow".to_string(),
            init_values: "seed".to_string(),
            ..Default::default()
        }),
    );
}
