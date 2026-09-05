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
    work_payloads, Controller,
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
    the_entry_delivery_stamp_crosses_the_abi();
    the_timer_fires_through_poll(ctx, due_at);
    a_ping_generates_no_activation(ctx, &controller);
    poll_honors_its_timeout(ctx);
    restores_and_applies_the_offset_floor(ctx, &controller, &stream);
    a_live_read_answers_with_the_host_json(ctx, &controller);
    a_keyed_dynamic_route_crosses_for_its_key(ctx, &controller, &stream);
    a_workflow_body_runs_through_the_abi(ctx, &controller);
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
fn socket_dir() -> std::path::PathBuf {
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
fn declares_and_starts(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_probe",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [{"name": "in", "kind": "entry", "chains": [[{"link": "into", "procedure": 0}]]}],
        "procedures": [{"name": "abi_pass"}]
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
fn routes_two_entry_payloads(
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
    assert_eq!(act.target.procedure, 0);
    act
}

/// The third answer, and the one the ABI has to name because no unwind
/// crosses it: a veneer whose language threw cannot reach `complete`, and
/// per-entity exclusivity would then retire this entity for the life of the
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

/// The C ABI is the only view a non-Rust host has of the last-delivery stamp
/// and of the episode latch over it, so the reads and the three watch calls
/// are pinned here; the wire path that writes the stamp (`routing::dispatch`)
/// is driven by the Rust SDK's `entry_idle` suite.
fn the_entry_delivery_stamp_crosses_the_abi() {
    let unknown = CString::new("no_such_entry_channel").unwrap();
    assert_eq!(
        unsafe { flz_core_entry_last_delivery_nanos(unknown.as_ptr()) },
        0,
        "a channel nothing has delivered to reads as the never-delivered sentinel"
    );

    // A watch registered before the first frame, with a zero threshold so the
    // latch fires on the first reading later than a stamp — never a wait.
    let name = CString::new("in").unwrap();
    let watch = unsafe { flz_core_idle_watch_new(name.as_ptr(), 0) };
    assert!(watch > 0, "the watch is registered by id");
    let mut gap = 0u64;
    assert_eq!(
        unsafe { flz_core_idle_watch_poll(watch, &mut gap) },
        FLZ_IDLE_NONE,
        "a cold start is not an episode"
    );

    let instance = flz_core::runner::live_stream_instance("abi_probe").expect("the instance");
    let channel = instance.topology.channel(0).expect("entry channel");
    channel.observe_delivery();
    let stamped = unsafe { flz_core_entry_last_delivery_nanos(name.as_ptr()) };
    assert!(stamped > 0, "the delivery is visible through the ABI");
    assert!(
        stamped <= flz_core_monotonic_nanos(),
        "the stamp and the clock it is read against are the same clock"
    );

    assert_eq!(poll_until(watch, FLZ_IDLE_IDLE), FLZ_IDLE_IDLE);
    assert!(
        gap_of(watch) == 0,
        "latched: the silence continuing is not a second episode"
    );
    channel.observe_delivery();
    assert_eq!(poll_until(watch, FLZ_IDLE_RESUMED), FLZ_IDLE_RESUMED);
    flz_core_idle_watch_free(watch);
    assert_eq!(
        unsafe { flz_core_idle_watch_poll(watch, &mut gap) },
        FLZ_ERR,
        "a freed id answers the ABI error"
    );
}

/// Poll `watch` until it answers `wanted`, giving up after a second — a
/// zero-threshold latch fires within nanoseconds of the stamp, so this spins,
/// never sleeps.
fn poll_until(watch: u64, wanted: i32) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let mut gap = 0u64;
        let status = unsafe { flz_core_idle_watch_poll(watch, &mut gap) };
        if status == wanted {
            assert!(gap > 0, "an episode boundary carries its gap");
            return status;
        }
        assert_eq!(status, FLZ_IDLE_NONE, "no other boundary is expected here");
        assert!(
            Instant::now() < deadline,
            "the latch did not cross to {wanted} within a second"
        );
    }
}

/// One more poll's gap, which is `0` whenever the poll crossed no boundary.
fn gap_of(watch: u64) -> u64 {
    let mut gap = 0u64;
    assert_eq!(
        unsafe { flz_core_idle_watch_poll(watch, &mut gap) },
        FLZ_IDLE_NONE
    );
    gap
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
            {"name": "ein", "kind": "entry", "chains": [[{"link": "into", "procedure": 0}]]},
            {"name": "eout", "kind": "plugin", "producer": 0, "keyed": true,
             "chains": [[{"link": "sink", "name": "eout"}]]}
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
    let entity_id = flz_core::flusher::encode_entity_identity("eff-1", "eff_pass", "");
    controller.send(
        "ref-restore-1",
        flz_core::pb::command_request::Command::RestoreInitState(flz_core::pb::RestoreInitState {
            entity_id,
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

/// EntityState becomes a LiveStateRead activation; the host's JSON answer
/// lands in the command reply under the Decision-10 shape.
fn a_live_read_answers_with_the_host_json(ctx: *mut FlzContext, controller: &Controller) {
    controller.send(
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

    let reply = controller.await_reply("entity_state reply carrying the host JSON", "ref-read-1");
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
    the_keyed_input_reaches_its_own_entity(ctx);
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

fn the_keyed_input_reaches_its_own_entity(ctx: *mut FlzContext) {
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
}

fn declares_the_dynamic_class(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_dyn",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [{"name": "din", "kind": "entry", "chains": [[{"link": "into", "procedure": 0}]]}],
        "procedures": [{"name": "dyn_pass", "entity_mode": "dynamic"}]
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

fn a_workflow_body_runs_through_the_abi(ctx: *mut FlzContext, controller: &Controller) {
    declares_the_workflow_class(ctx, controller);
    let (status, act) = abi_poll(ctx, 5_000);
    assert_eq!(status, FLZ_POLL_ACTIVATION, "workflow seed hands out");
    let act = act.unwrap();
    assert_eq!(act.target.operative, "sugar-1");
    let schema::ActivationKind::WorkflowRun { args } = &act.kind else {
        panic!("expected a workflow run, got {:?}", act.kind);
    };
    assert_eq!(args, b"seed");
    the_workflow_session_calls_answer(ctx);
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

    controller.await_frame("WorkflowCompleted teardown", |captured| {
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
}

fn declares_the_workflow_class(ctx: *mut FlzContext, controller: &Controller) {
    let decl = br#"{
        "factory_id": "abi_sugar",
        "sdk_lang": 1,"sdk_version":"0.1.0",
        "channels": [],
        "procedures": [],
        "workflow": {}
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK
    );
    controller.send(
        "ref-add-sugar",
        flz_core::pb::command_request::Command::AddOperative(flz_core::pb::AddOperative {
            operative_id: "sugar-1".to_string(),
            factory_id: "abi_sugar".to_string(),
            kind: "workflow".to_string(),
            init_values: "seed".to_string(),
            ..Default::default()
        }),
    );
}

/// The session calls a live body uses, through the ABI.
fn the_workflow_session_calls_answer(ctx: *mut FlzContext) {
    let instance = CString::new("sugar-1").unwrap();
    assert_eq!(
        unsafe { flz_core_workflow_cancelled(ctx, instance.as_ptr()) },
        0,
        "a live body is not cancelled"
    );
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let status = unsafe { flz_core_workflow_recv(ctx, instance.as_ptr(), 50, &mut buf, &mut len) };
    assert_eq!(status, FLZ_POLL_TIMEOUT, "an empty inbox times out");
    assert_eq!(
        unsafe { flz_core_workflow_emit(ctx, instance.as_ptr(), b"mid".as_ptr(), 3) },
        FLZ_OK
    );
}
