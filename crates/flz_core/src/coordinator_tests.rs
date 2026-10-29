//! The coordinator's tests: the heartbeat's self-report and the ladder a
//! heartbeat that reaches nothing arms, and the steps output a runner
//! holds while it has no secondary.

use prost::Message;

use super::*;

/// The whole key set, pinned. The controller's gate refuses a key it
/// holds no slot for, which catches an addition; nothing there catches a
/// removal — the mirror simply stops being fed — so the set is written
/// out here, in the crate that builds it.
#[test]
fn the_heartbeat_builds_exactly_the_contracted_keys() {
    let mut keys: Vec<String> = super::heartbeat_metadata().into_keys().collect();
    keys.sort();

    let mut expected = vec![
        "blocked_parks",
        "credit_parked",
        "dispatches",
        "drops_newest",
        "drops_oldest",
        "handouts",
        "inflight",
        "inflight_oldest_ms",
        "parts_dropped_no_stream",
        "parts_dropped_refused",
        "parts_dropped_too_large",
        "parts_sent",
        "pending_push_oldest_ms",
        "pending_pushes",
        "restore_discarded_inputs",
        "timers_armed",
        "timers_fired",
    ];
    expected.sort_unstable();

    assert_eq!(
        keys, expected,
        "the heartbeat's key set is a cross-SDK contract"
    );
}

/// A rename silently blinds the controller, and a counter that reports a
/// stale value blinds it just as thoroughly.
#[test]
fn the_heartbeat_carries_every_overflow_counter() {
    let before = super::heartbeat_metadata();
    for key in [
        "blocked_parks",
        "drops_newest",
        "drops_oldest",
        "pending_pushes",
        "pending_push_oldest_ms",
    ] {
        assert!(before.contains_key(key), "the heartbeat must carry {key}");
    }

    crate::cell::PUSHES_BLOCKED_TOTAL.fetch_add(3, std::sync::atomic::Ordering::Relaxed);
    crate::cell::DROPS_NEWEST_TOTAL.fetch_add(5, std::sync::atomic::Ordering::Relaxed);
    crate::cell::DROPS_OLDEST_TOTAL.fetch_add(7, std::sync::atomic::Ordering::Relaxed);
    let after = super::heartbeat_metadata();

    for (key, bumped) in [
        ("blocked_parks", 3u64),
        ("drops_newest", 5),
        ("drops_oldest", 7),
    ] {
        let read = |m: &HashMap<String, String>| m[key].parse::<u64>().expect("an integer");
        assert_eq!(
            read(&after) - read(&before),
            bumped,
            "{key} must report what the core counted"
        );
    }
}

/// An idle Relay's heartbeat is its only outbound signal; one that
/// reaches no stream must ask for a reconnect, or the stall is silent.
#[test]
fn heartbeat_reaching_no_stream_arms_the_reconnect_ladder() {
    let _g = crate::state_sink::test_guard();
    std::env::set_var("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD", "3");
    std::env::set_var("FLEZHA_BROKEN_PIPE_WINDOW_MS", "60000");
    crate::state_sink::reset_for_test();

    // The signature of a dead secondary: the stream map is empty, so every
    // heartbeat write is a no-op over zero streams.
    let empty: RwLock<HashMap<u32, Arc<FlezhaStream>>> = RwLock::new(HashMap::new());
    heartbeat_tick(&empty);
    heartbeat_tick(&empty);
    heartbeat_tick(&empty);

    assert!(
        crate::state_sink::reconnect_requested_for_test(),
        "heartbeats that reach no stream must arm the reconnect ladder"
    );

    std::env::remove_var("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD");
    std::env::remove_var("FLEZHA_BROKEN_PIPE_WINDOW_MS");
    crate::state_sink::reset_for_test();
}

/// A removed steps relay's Cells leave the flusher with it: an entry holds its
/// Cell's buffer, so one left behind is drained for the life of the process.
#[test]
fn a_stopped_steps_leaves_no_cell_with_the_flusher() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("flusher_stop_{}", std::process::id());
    let class = crate::test_support::host_class::steps_json(&factory, &["in"], 1).replace(
        r#""durable":false"#,
        r#""durable":{"local":{"sleep_after":false}}"#,
    );
    crate::host::register_host_class_json(class.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());
    let id = format!("{factory}_inst");
    let errors = crate::test_support::host_class::bind(&mut node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let flusher = crate::flusher::CellFlusher::instance();
    assert!(flusher.holds_class(&id), "its Cell was registered");

    assert!(node.stop_relay(&id));

    assert!(!flusher.holds_class(&id), "its Cell left with it");
}

/// A removed stream relay's Cells leave the flusher with it, as a steps
/// relay's do.
#[test]
fn a_stopped_stream_leaves_no_cell_with_the_flusher() {
    let _env = crate::runner::env_test_guard();
    let factory = format!("flusher_stream_stop_{}", std::process::id());
    let class = crate::test_support::host_class::steps_json(&factory, &["in"], 1)
        .replace(
            r#""durable":false"#,
            r#""durable":{"local":{"sleep_after":false}}"#,
        )
        .replace(r#""run":"steps""#, r#""run":"flow""#);
    crate::host::register_host_class_json(class.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());
    let id = format!("{factory}_inst");
    let errors = crate::test_support::host_class::bind(&mut node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let flusher = crate::flusher::CellFlusher::instance();
    assert!(flusher.holds_class(&id), "its Cell was registered");

    assert!(node.stop_relay(&id));

    assert!(!flusher.holds_class(&id), "its Cell left with it");
}

/// A removed instance's processed prefix is never reported again: a hold a
/// settle still in flight at the stop releases unshipped must not move it.
#[test]
fn a_stopped_stream_reports_no_prefix_a_late_release_moves() {
    let _env = crate::runner::env_test_guard();
    let _sink = crate::state_sink::test_guard();
    let factory = format!("prefix_stream_stop_{}", std::process::id());
    let class = crate::test_support::host_class::steps_json(&factory, &["in"], 1)
        .replace(
            r#""durable":false"#,
            r#""durable":{"local":{"sleep_after":600000}}"#,
        )
        .replace(r#""run":"steps""#, r#""run":"flow""#);
    crate::host::register_host_class_json(class.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());
    let id = format!("{factory}_inst");
    let errors = crate::test_support::host_class::bind(&mut node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let ledger = node.get_relay(&id).and_then(|i| i.prefix.clone());
    let ledger = ledger.expect("a flow relay keeps a prefix");
    // Two Cells' inputs in flight at the stop: 10 settles after it, 11 not yet.
    let (settles_late, still_running) = (ledger.hold(10), ledger.hold(11));
    let markers = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (seen, identity) = (
        Arc::clone(&markers),
        crate::flusher::encode_cell_identity(&id, "", ""),
    );
    crate::state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        let marker = crate::test_support::split_cell_frame(frame)
            .filter(|_| payload_id == crate::prefix::PROCESSED_PREFIX_PAYLOAD_ID);
        if let Some(marker) = marker.filter(|marker| marker.identity == identity) {
            seen.lock().unwrap().push(marker.seq);
        }
        Ok(())
    }));

    assert!(node.stop_relay(&id));
    drop(settles_late);
    crate::flusher::CellFlusher::instance().drain_now_for_tests();

    crate::state_sink::reset_for_test();
    let markers = markers.lock().unwrap();
    assert!(
        markers.iter().all(|&p| p < 10),
        "a prefix past the unshipped input: {markers:?}"
    );
    drop(still_running);
}

/// A local send reaches only what this runner holds: a relay bound
/// elsewhere, a channel the relay does not declare and one that is not an
/// entry channel are each refused by code, and nothing is dispatched.
#[test]
fn a_local_send_is_refused_for_what_this_runner_cannot_take() {
    let factory = format!("local_send_{}", std::process::id());
    crate::test_support::host_class::instance_of(&factory, &["in"], &[], &[1], false);
    let mut node = RunnerNode::new("test-ns".to_string());
    let id = format!("{factory}-1");
    let errors = crate::test_support::host_class::bind(&mut node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let bound = node.bound();

    assert_eq!(bound.ids(), vec![id.clone()]);
    let code =
        |result: Result<(), crate::Error>| crate::ErrorModel::code(&result.expect_err("refused"));
    assert_eq!(
        code(bound.send("elsewhere", "in", b"x".to_vec())),
        "runtime.relay_not_local"
    );
    assert_eq!(
        code(bound.send(&id, "nope", b"x".to_vec())),
        "runtime.channel_undeclared"
    );
    assert_eq!(
        code(bound.send(&id, "out", b"x".to_vec())),
        "runtime.channel_wrong_kind"
    );

    assert!(node.stop_relay(&id));
    assert!(
        bound.ids().is_empty(),
        "an unbound relay is no longer listed"
    );
}

/// A controller's end of a control session: requests written as the
/// controller frames them, answers read with a deadline as the hang guard.
struct ControllerEnd(std::os::unix::net::UnixStream);

const DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

impl ControllerEnd {
    fn request(&mut self, reference_id: &str, command: Command) {
        use std::io::Write;
        let frame = command::framed_request(reference_id.to_string(), command);
        self.0.write_all(&frame).expect("the request is written");
    }

    /// The answer to `reference_id`, skipping every other frame.
    fn answer(&mut self, reference_id: &str) -> crate::pb::CommandResponse {
        loop {
            let response = self.next_answer();
            if response.reference_id == reference_id {
                return response;
            }
        }
    }

    /// The next answer to any request, skipping every other frame.
    fn next_answer(&mut self) -> crate::pb::CommandResponse {
        loop {
            let (_, frame, _) =
                payloader::extract_payload(&self.0).unwrap_or_else(|e| panic!("no answer: {e}"));
            if let Some(response) = crate::test_support::response_in(&frame) {
                return response;
            }
        }
    }
}

/// `node` behind a control session read and coordinated as in production;
/// the controller's end, past the handshake.
fn coordinated(node: RunnerNode) -> ControllerEnd {
    coordinated_with(node).0
}

/// [`coordinated`], with the coordinator's own channel, so a test can queue a
/// command in a fixed order, and the bridge its timers write through.
fn coordinated_with(
    node: RunnerNode,
) -> (
    ControllerEnd,
    Arc<Mutex<Sender<RunnerMessage>>>,
    Arc<ShutdownBridge>,
) {
    let (sdk_end, ctrl_end) = crate::test_support::stream_pair();
    ctrl_end
        .set_read_timeout(Some(DEADLINE))
        .expect("a deadline");
    let declared = Arc::new(RwLock::new(Vec::new()));
    let (otx, orx) = crossbeam::channel::unbounded();
    let otx_arc = Arc::new(Mutex::new(otx));
    let (_, control_stream) = control_unit::sock_conn(
        "test-ns".to_string(),
        Arc::clone(&declared),
        sdk_end,
        None,
        Arc::clone(&otx_arc),
    );
    let bridge = ShutdownBridge::new();
    bridge.set_control_stream(Some(Arc::clone(&control_stream)));
    let mut coordinator = Coordinator {
        runner: node,
        control_stream,
        namespace: "test-ns".to_string(),
        declared,
        otx_arc: Arc::clone(&otx_arc),
        shutdown_bridge: Arc::clone(&bridge),
    };
    thread::spawn(move || {
        while let Ok(message) = orx.recv() {
            coordinator.handle_message(message);
        }
    });
    payloader::extract_payload(&ctrl_end).expect("the handshake");
    (ControllerEnd(ctrl_end), otx_arc, bridge)
}

/// A bound host Cell that keeps state, with its wire identity.
fn bound_cell(node: &mut RunnerNode) -> (Vec<u8>, Arc<crate::cell::CellSlot>) {
    let factory = format!("credit_compact_{}", std::process::id());
    crate::test_support::host_class::instance_of(&factory, &["in"], &[], &[1], false);
    let id = format!("{factory}-1");
    let errors = crate::test_support::host_class::bind(node, &factory, &id, "", 0);
    assert!(errors.is_empty(), "{errors:?}");
    let cell_id = crate::flusher::encode_cell_identity(&id, "p0", "");
    let (_, slot) = node.find_cell(&cell_id).expect("the Cell is live");
    (cell_id, slot)
}

/// A sender holding `slot`'s buffer as `flush_one`'s caller does, parked in
/// the credit acquire every drained frame makes; signals once it is freed.
fn parked_under_buffer(slot: &Arc<crate::cell::CellSlot>) -> std::sync::mpsc::Receiver<()> {
    let (locked_tx, locked) = std::sync::mpsc::channel();
    let (freed_tx, freed) = std::sync::mpsc::channel();
    let holder = Arc::clone(slot);
    thread::spawn(move || {
        let _buffer = crate::util::LockRecover::lock_recover(&*holder.flush_buffer);
        locked_tx.send(()).expect("the test waits");
        crate::credit::global().acquire(1);
        freed_tx.send(()).expect("the test waits");
    });
    locked.recv_timeout(DEADLINE).expect("the buffer is held");
    freed
}

/// The host a snapshot crossing reaches: answers `slot`'s request with a
/// one-byte state.
fn host_answering_snapshot(slot: &Arc<crate::cell::CellSlot>) {
    let slot = Arc::clone(slot);
    thread::spawn(move || {
        let core = crate::poll_core::global();
        crate::test_support::spin_until("the snapshot request", || {
            let Some(act) = core.take_snapshot_request(&slot) else {
                return false;
            };
            let token = act.token;
            let outcome = crate::schema::ActivationOutcome::Snapshot { state: vec![b's'] };
            let result = crate::schema::ActivationResult { token, outcome };
            crate::host::apply_result(core, act, result).expect("answered");
            true
        });
    });
}

/// A coordinator command can wait on a Cell's buffer lock whose holder is
/// parked on credit, and only a grant frees that holder: the grant must not
/// queue behind the command, or the runner's control channel wedges.
#[test]
fn a_grant_frees_the_sender_a_compaction_waits_behind() {
    const WINDOW: u64 = 4096;
    if crate::test_support::passes_alone(
        "coordinator::tests::a_grant_frees_the_sender_a_compaction_waits_behind",
    )
    .is_some()
    {
        return;
    }
    let _sink = crate::state_sink::test_guard();
    crate::state_sink::install_global_sink(Arc::new(|_: &str, _: &[u8]| Ok(())));
    let mut node = RunnerNode::new("test-ns".to_string());
    let (cell_id, slot) = bound_cell(&mut node);
    let mut controller = coordinated(node);
    let session = "kb550".to_string();
    let init = crate::pb::CreditWindowInit {
        stream_id: session.clone(),
        window_bytes: WINDOW,
    };
    controller.request("ref-init", Command::CreditWindowInit(init));
    assert_eq!(controller.answer("ref-init").status, "success");
    crate::credit::global().acquire(WINDOW as usize);
    let freed = parked_under_buffer(&slot);
    host_answering_snapshot(&slot);

    let compact = crate::pb::CompactCell {
        cell_id: cell_id.clone(),
    };
    controller.request("ref-compact", Command::CompactCell(compact));
    let grant = crate::pb::CreditGrant {
        stream_id: session,
        bytes: WINDOW,
    };
    controller.request("ref-grant", Command::CreditGrant(grant));

    freed
        .recv_timeout(DEADLINE)
        .expect("the grant reaches the sender parked under the Cell's buffer");
    let compacted = controller.answer("ref-compact");
    assert_eq!(compacted.status, "success", "{:?}", compacted.errors);
    let response = crate::pb::CompactCellResponse::decode(compacted.raw_data.as_slice())
        .expect("a CompactCellResponse");
    assert_eq!(
        (response.cell_id, response.snapshot_bytes_size),
        (cell_id, 1)
    );
    crate::credit::global().disarm();
}

/// A compaction waiting out a long activation holds the coordinator; what
/// the liveness timers write meanwhile must still reach the controller, or
/// its silence window closes a healthy session. `payload_id` is the frame
/// awaited: the keepalive on the control stream, or the data heartbeat.
fn a_liveness_frame_leaves_while_a_compaction_waits(test: &str, payload_id: &str) {
    const WITHIN: std::time::Duration = std::time::Duration::from_secs(3);
    if crate::test_support::passes_alone(test).is_some() {
        return;
    }
    // Its own process, so the short intervals reach no other test.
    std::env::set_var("FLEZHA_CONTROL_KEEPALIVE_MS", "50");
    std::env::set_var("FLEZHA_DATA_HEARTBEAT_MS", "50");
    let _sink = crate::state_sink::test_guard();
    crate::state_sink::install_global_sink(Arc::new(|_: &str, _: &[u8]| Ok(())));
    let mut node = RunnerNode::new("test-ns".to_string());
    let (cell_id, slot) = bound_cell(&mut node);
    let (data_stream, data_end) = crate::test_support::stream_pair();
    let io_streams = node.get_io_streams();
    io_streams.write().unwrap().insert(0, data_stream);
    let (mut controller, otx, bridge) = coordinated_with(node);
    // An outstanding activation: the compaction's exclusive gate waits on it.
    slot.drain_active
        .store(true, std::sync::atomic::Ordering::Release);
    host_answering_snapshot(&slot);
    // Queued ahead of the timers' first tick, so the coordinator is held first.
    let request = crate::command::request_bytes(
        "ref-compact".into(),
        Command::CompactCell(crate::pb::CompactCell { cell_id }),
    );
    otx.lock()
        .unwrap()
        .send(RunnerMessage::HandleCommandAction(
            "control_command".to_string(),
            request,
            HashMap::new(),
        ))
        .expect("the coordinator is up");
    let _timers = super::spawn_timers(&bridge, io_streams);

    let end = if payload_id == liveness::CONTROL_PING_PAYLOAD_ID {
        &controller.0
    } else {
        &data_end
    };
    end.set_read_timeout(Some(WITHIN)).expect("a deadline");
    let arrived = loop {
        match payloader::extract_payload(end) {
            Ok((id, _, _)) if id == payload_id => break true,
            Ok(_) => continue,
            Err(_) => break false,
        }
    };
    slot.drain_active
        .store(false, std::sync::atomic::Ordering::Release);
    controller
        .0
        .set_read_timeout(Some(DEADLINE))
        .expect("a deadline");
    assert_eq!(controller.answer("ref-compact").status, "success");
    assert!(
        arrived,
        "no {payload_id} within {WITHIN:?} while a compaction waited on an activation"
    );
    bridge.set_control_stream(None);
}

#[test]
fn a_keepalive_leaves_while_a_compaction_waits_on_an_activation() {
    a_liveness_frame_leaves_while_a_compaction_waits(
        "coordinator::tests::a_keepalive_leaves_while_a_compaction_waits_on_an_activation",
        liveness::CONTROL_PING_PAYLOAD_ID,
    );
}

#[test]
fn a_data_heartbeat_leaves_while_a_compaction_waits_on_an_activation() {
    a_liveness_frame_leaves_while_a_compaction_waits(
        "coordinator::tests::a_data_heartbeat_leaves_while_a_compaction_waits_on_an_activation",
        liveness::DATA_HEARTBEAT_PAYLOAD_ID,
    );
}

/// fails when: a move reads the next change seq before the source relay
/// stops, and the stop's final drain then ships a record at that seq, so the
/// destination's first record collides with it and is dropped.
#[test]
fn a_move_hands_over_a_seq_above_what_the_source_drains_as_it_stops() {
    let _sink = crate::state_sink::test_guard();
    crate::state_sink::install_global_sink(Arc::new(|_: &str, _: &[u8]| Ok(())));
    let mut node = RunnerNode::new("test-ns".to_string());
    let (_cell_id, slot) = bound_cell(&mut node);
    {
        let mut buffer = crate::util::LockRecover::lock_recover(&*slot.flush_buffer);
        let bytes = buffer
            .as_any_mut()
            .downcast_mut::<crate::host::BytesBuffer>()
            .expect("a host buffer");
        bytes.timer_ops.push(crate::schema::TimerOp::register(
            crate::schema::TimerKind::ProcessingTime,
            31_000,
        ));
    }

    let mut errors = Vec::new();
    let serialize = crate::pb::MigrationSerialization {
        relay_id: slot.instance_id.to_string(),
    };
    let reply = crate::command::CommandAction::handle(&serialize, &mut node, "", &mut errors);

    let shipped: serde_json::Value = serde_json::from_slice(&reply.bytes).expect("a JSON state");
    let next_seq = shipped["$cells"][0]["next_seq"]
        .as_u64()
        .expect("the Cell's next seq");
    let drained = slot.change_seq.load(std::sync::atomic::Ordering::Acquire);
    assert!(
        drained >= 1,
        "the stop drained the buffered timer op: next_seq={next_seq} drained={drained}"
    );
    assert!(
        next_seq >= drained,
        "the move hands over seq {next_seq}, but the source's last record took {}",
        drained - 1
    );
}

/// A host that holds its first batch until `release` sends or is dropped
/// (`held` signals it holds it) and answers every batch with one change per
/// input.
struct BusyHost {
    held: std::sync::mpsc::Receiver<()>,
    release: std::sync::mpsc::Sender<()>,
    _host: crate::test_support::FakeHost,
}

impl BusyHost {
    fn serve() -> Self {
        let (held_tx, held) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel::<()>();
        let first = Mutex::new(Some((held_tx, release_rx)));
        let host = crate::test_support::FakeHost::answering(move |payload, _| {
            let crate::poll_core::EnginePayload::Work { items, .. } = payload else {
                return crate::schema::ActivationOutcome::StateApplied;
            };
            if let Some((held, release)) = first.lock().unwrap().take() {
                held.send(()).expect("the test waits");
                let _ = release.recv_timeout(DEADLINE);
            }
            let changed = |_: &crate::poll_core::ItemAttempt| crate::schema::ItemEffect {
                changes: vec![b"change".to_vec()],
                ..crate::host::empty_effect()
            };
            crate::schema::ActivationOutcome::Work(items.iter().map(changed).collect())
        });
        Self {
            held,
            release,
            _host: host,
        }
    }
}

/// Every relay in `ids` bound on `node` as one class of a flow tasklet that
/// keeps its state, the stop under test's coordinator behind `node`, and a
/// dispatch handle onto them.
fn draining_runner(ids: &[&str]) -> (ControllerEnd, RelayDispatchHandle) {
    let factory = format!("drain_stop_{}", std::process::id());
    let class = format!(
        r#"{{"factory_id":"{factory}","sdk_lang":1,"sdk_version":"0.1.0","run":"flow",
             "durable":{{"local":{{"sleep_after":false}}}},
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{{"name":"p0"}}]}}"#
    );
    crate::host::register_host_class_json(class.as_bytes()).expect("class registers");
    let mut node = RunnerNode::new("test-ns".to_string());
    for id in ids {
        let errors = crate::test_support::host_class::bind(&mut node, &factory, id, "", 0);
        assert!(errors.is_empty(), "{errors:?}");
    }
    let handle = node.relay_dispatch_handle();
    let (controller, coordinator, _bridge) = coordinated_with(node);
    crate::runner::install_reconnect_trigger(coordinator.lock().unwrap().clone());
    (controller, handle)
}

/// What reaches the state sink for relay `id`, in order: `record` for each
/// Cell record, `final` for the final marker of its stop.
fn shipped_by(id: &str) -> Arc<Mutex<Vec<&'static str>>> {
    let shipped = Arc::new(Mutex::new(Vec::new()));
    let (seen, cell, instance) = (
        Arc::clone(&shipped),
        crate::flusher::encode_cell_identity(id, "p0", ""),
        crate::flusher::encode_cell_identity(id, "", ""),
    );
    crate::state_sink::install_global_sink(Arc::new(move |payload_id: &str, frame: &[u8]| {
        let Some(split) = crate::test_support::split_cell_frame(frame) else {
            return Ok(());
        };
        match payload_id {
            crate::state_sink::CELL_STATE_CHANGE_PAYLOAD_ID
            | crate::state_sink::CELL_EFFECT_RECORD_PAYLOAD_ID
                if split.identity == cell =>
            {
                seen.lock().unwrap().push("record")
            }
            crate::prefix::PROCESSED_PREFIX_PAYLOAD_ID
                if split.identity == instance && split.body == [1] =>
            {
                seen.lock().unwrap().push("final")
            }
            _ => {}
        }
        Ok(())
    }));
    shipped
}

/// One frame into `id` as its stream reader hands it over.
fn deliver(handle: &RelayDispatchHandle, id: &str, payload: Vec<u8>, key: &str, value: &str) {
    let metadata = HashMap::from([(key.to_string(), value.to_string())]);
    handle.dispatch(id, payload, &metadata, crate::test_support::stream_pair().0);
}

/// `RemRelay` of `id`, given `drain_ms` to drain.
fn stop(id: &str, drain_ms: u64) -> Command {
    Command::RemRelay(crate::pb::RemRelay {
        relay_id: id.to_string(),
        drain_within_ms: drain_ms,
    })
}

/// fails when: a stop drops the relay while its Cell is busy, so the inputs
/// queued behind the running one, sent before the stop, are never applied
/// and no record of them reaches the state sink ahead of the final marker.
#[test]
fn a_stop_ships_the_records_of_every_input_sent_before_it() {
    const INPUTS: usize = 4;
    let _env = crate::runner::env_test_guard();
    let _sink = crate::state_sink::test_guard();
    let id = format!("drain_stop_{}_shipped", std::process::id());
    let shipped = shipped_by(&id);
    let host = BusyHost::serve();
    let (mut controller, handle) = draining_runner(&[&id]);
    for n in 0..INPUTS {
        deliver(&handle, &id, vec![n as u8], "channel", "in");
    }
    host.held.recv_timeout(DEADLINE).expect("the Cell is busy");

    deliver(&handle, &id, Vec::new(), "flz.control", "stop");
    controller.request("ref-stop", stop(&id, DEADLINE.as_millis() as u64));
    crate::test_support::spin_until("the stop begins", || {
        crate::host::pending_leave(&id).is_some()
    });
    host.release.send(()).expect("the host waits");
    let answer = controller.answer("ref-stop");
    drop(host);

    crate::state_sink::reset_for_test();
    assert_eq!(answer.status, "success", "{:?}", answer.errors);
    assert_eq!(
        *shipped.lock().unwrap(),
        [vec!["record"; INPUTS], vec!["final"]].concat(),
        "every input's record ships ahead of the final marker"
    );
    assert!(handle.registry.get(&id).is_none(), "the relay was dropped");
}

/// fails when: a draining stop waits on the coordinator, so another relay's
/// command queues behind a Cell busy in author code, or a Cell that never
/// finishes holds the stop past its limit.
#[test]
fn a_stop_whose_cell_never_finishes_ends_at_its_limit_and_holds_no_other_relay() {
    const LIMIT_MS: u64 = 200;
    let _env = crate::runner::env_test_guard();
    let _sink = crate::state_sink::test_guard();
    let (busy, other) = (
        format!("drain_stop_{}_busy", std::process::id()),
        format!("drain_stop_{}_other", std::process::id()),
    );
    let shipped = shipped_by(&busy);
    let host = BusyHost::serve();
    let (mut controller, handle) = draining_runner(&[&busy, &other]);
    deliver(&handle, &busy, vec![0], "channel", "in");
    host.held.recv_timeout(DEADLINE).expect("the Cell is busy");
    deliver(&handle, &busy, Vec::new(), "flz.control", "stop");

    let started = std::time::Instant::now();
    controller.request("ref-busy", stop(&busy, LIMIT_MS));
    crate::test_support::spin_until("the stop begins", || {
        crate::host::pending_leave(&busy).is_some()
    });
    controller.request("ref-other", stop(&other, 0));

    let first = controller.next_answer();
    let (late, waited) = (controller.next_answer(), started.elapsed());
    drop(host);
    crate::state_sink::reset_for_test();
    assert_eq!(
        first.reference_id, "ref-other",
        "answered behind the busy stop"
    );
    assert_eq!(
        (late.reference_id.as_str(), late.status.as_str()),
        ("ref-busy", "success")
    );
    assert!(
        waited >= std::time::Duration::from_millis(LIMIT_MS),
        "stopped before its limit: {waited:?}"
    );
    assert!(
        shipped.lock().unwrap().is_empty(),
        "drained without its record"
    );
    assert!(
        handle.registry.get(&busy).is_none(),
        "the relay was dropped"
    );
}
