//! Local mode driven end to end, with this test playing the host: a class is
//! declared, a `LocalRun` feeds it, and a poll/complete loop answers every
//! activation the way a host SDK's poll loop does. The core supplies no host
//! loop, deliberately — each SDK plugs its own in unchanged — so a kernel
//! test has to be one.
//!
//! An integration test, not a unit one, because a poll loop takes work off
//! the process-wide `PollCore`: its own binary is what keeps it from
//! draining Entities the crate's unit tests scheduled.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use flz_core::host;
use flz_core::local::{self, LocalRun, SinkOutput};
use flz_core::poll_core::{self, Polled};
use flz_core::schema::{
    Activation, ActivationKind, ActivationOutcome, ItemEffect, Output, OutputTarget, Verdict,
    WorkItem,
};
use flz_transport::transport::FlezhaStream;

/// One run at a time: the poll core, the class registry and the global state
/// sink are all process-wide.
static ONE_RUN: Mutex<()> = Mutex::new(());

fn one_run() -> MutexGuard<'static, ()> {
    ONE_RUN.lock().unwrap_or_else(|p| p.into_inner())
}

/// `entry → normalize → (inter-procedure) → total → sink`, the graph every
/// SDK's local-mode example declares. `total` is dynamic, so the run mints
/// one Entity per key the upstream procedure resolves.
const TWO_STEP: &str = r#"{
    "factory_id": "local_two_step", "sdk_lang": 1, "sdk_version": "0.1.0", "persistent": true,
    "channels": [{"name": "lines",  "kind": "entry",      "chains": [[{"link": "into", "procedure": 0}]]},
                 {"name": "mid",    "kind": "inter_proc", "producer": 0,
                                    "chains": [[{"link": "into", "procedure": 1}]]},
                 {"name": "totals", "kind": "plugin",     "producer": 1, "keyed": true,
                                    "chains": [[{"link": "sink", "name": "totals"}]]}],
    "procedures": [{"name": "normalize"}, {"name": "total", "entity_mode": "dynamic"}]
}"#;

fn declare(json: &str) {
    host::register_host_class_json(json.as_bytes()).expect("the class declares");
}

/// A host: one thread answering activations until it is dropped. `answer`
/// sees the activation and one input's payload, and returns that item's
/// outputs — everything the core does with them is the deployed path.
struct Host {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Host {
    fn spawn(answer: impl Fn(&Activation, &[u8]) -> Vec<Output> + Send + 'static) -> Host {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let core = poll_core::global();
            while !flag.load(Ordering::Relaxed) {
                let Polled::Activation(activation) = core.poll(Duration::from_millis(20)) else {
                    continue;
                };
                let schema_activation = host::to_schema(&activation);
                let ActivationKind::Work(items) = &schema_activation.kind else {
                    panic!(
                        "local mode hands out work, not {:?}",
                        schema_activation.kind
                    );
                };
                let effects = items
                    .iter()
                    .map(|item| {
                        let payload = match item {
                            WorkItem::Input(input) => input.payload.clone(),
                            WorkItem::Timer(_) => Vec::new(),
                        };
                        ItemEffect {
                            verdict: Verdict::Ok,
                            changes: Vec::new(),
                            outputs: answer(&schema_activation, &payload),
                            consumed_offset: None,
                            event_time: 0,
                            timers: Vec::new(),
                            checkpoint: false,
                            dlq: None,
                        }
                    })
                    .collect();
                host::apply_result(
                    core,
                    activation,
                    flz_core::schema::ActivationResult {
                        token: schema_activation.token,
                        outcome: ActivationOutcome::Work(effects),
                    },
                )
                .expect("the completion fits its activation");
            }
        });
        Host {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn edge_output(channel: u32, consumer: u32, key: &str, payload: &[u8]) -> Output {
    Output {
        target: OutputTarget::Edge { channel, consumer },
        key: Some(key.to_string()),
        event_time: None,
        metadata: Vec::new(),
        payload: payload.to_vec(),
    }
}

/// A sink release carrying the identity stamps every erased SDK attaches
/// host-side before it hands the core an output — the core does not invent
/// them, and local mode must not lose them.
fn sink_output(channel: &str, key: &str, payload: &[u8]) -> Output {
    Output {
        target: OutputTarget::Plugin {
            channel: channel.to_string(),
        },
        key: Some(key.to_string()),
        event_time: None,
        metadata: vec![
            ("flz.entity".to_string(), format!("total/{key}")),
            ("flz.seq".to_string(), "0.0".to_string()),
            ("flz.key".to_string(), key.to_string()),
        ],
        payload: payload.to_vec(),
    }
}

/// The two-step answer: `normalize` (procedure 0) forwards its payload onto
/// channel 1 keyed by its first byte; `total` (procedure 1) releases what it
/// received on the `totals` sink.
fn two_step_answer(activation: &Activation, payload: &[u8]) -> Vec<Output> {
    let key = String::from_utf8_lossy(&payload[..1]).to_string();
    match activation.target.procedure {
        0 => vec![edge_output(1, 1, &key, payload)],
        _ => vec![sink_output("totals", &key, payload)],
    }
}

/// Every value the sink released, in release order.
fn recorder() -> (Arc<Mutex<Vec<SinkOutput>>>, local::SinkCallback) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&seen);
    let callback: local::SinkCallback = Arc::new(move |output: SinkOutput| {
        into.lock().unwrap_or_else(|p| p.into_inner()).push(output);
    });
    (seen, callback)
}

fn payloads(seen: &Arc<Mutex<Vec<SinkOutput>>>) -> Vec<String> {
    seen.lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .map(|output| String::from_utf8_lossy(&output.payload).to_string())
        .collect()
}

#[test]
fn a_two_procedure_graph_runs_with_no_controller() {
    let _serialized = one_run();
    declare(TWO_STEP);
    let _host = Host::spawn(two_step_answer);
    let run = LocalRun::start("local_two_step", "two-step-1").expect("the run starts");
    let (seen, callback) = recorder();
    run.on_sink("totals", callback)
        .expect("`totals` is declared");

    run.feed("lines", b"a1".to_vec()).expect("`lines` accepts");
    run.feed("lines", b"a2".to_vec()).expect("`lines` accepts");
    run.feed("lines", b"b1".to_vec()).expect("`lines` accepts");
    run.run_until_idle(Duration::from_secs(10))
        .expect("the graph settles");

    assert_eq!(
        payloads(&seen),
        vec!["a1", "a2", "b1"],
        "both procedures ran and the sink saw every value in release order"
    );
    // Two distinct keys, so `total` minted two Entities and no more: the
    // inter-procedure hop resolved the key through the deployed router.
    let dynamic_entities: usize = run
        .instance()
        .dynamic_registries
        .iter()
        .flatten()
        .map(|registry| registry.entities.len())
        .sum();
    assert_eq!(dynamic_entities, 2, "one Entity per resolved key");
}

#[test]
fn a_filtered_value_never_reaches_the_consumer_and_a_fan_out_reaches_it_twice() {
    let _serialized = one_run();
    declare(TWO_STEP);
    // The transform lives where a host class's transforms live: in the host's
    // answer. `normalize` drops anything starting `x` and fans a `d` out into
    // two edge values.
    let _host = Host::spawn(|activation, payload| {
        if activation.target.procedure != 0 {
            let key = String::from_utf8_lossy(&payload[..1]).to_string();
            return vec![sink_output("totals", &key, payload)];
        }
        match payload.first() {
            Some(b'x') => Vec::new(),
            Some(b'd') => vec![
                edge_output(1, 1, "d", b"d-left"),
                edge_output(1, 1, "d", b"d-right"),
            ],
            _ => vec![edge_output(1, 1, "k", payload)],
        }
    });
    let run = LocalRun::start("local_two_step", "two-step-2").expect("the run starts");
    let (seen, callback) = recorder();
    run.on_sink("totals", callback)
        .expect("`totals` is declared");

    run.feed("lines", b"x-dropped".to_vec()).expect("accepted");
    run.feed("lines", b"kept".to_vec()).expect("accepted");
    run.feed("lines", b"d".to_vec()).expect("accepted");
    run.run_until_idle(Duration::from_secs(10))
        .expect("the graph settles");

    assert_eq!(
        payloads(&seen),
        vec!["kept", "d-left", "d-right"],
        "the filtered value is absent and the fan-out produced two"
    );
}

#[test]
fn a_keyed_sink_value_carries_the_stamps_a_deployed_run_stamps() {
    let _serialized = one_run();
    declare(TWO_STEP);
    let _host = Host::spawn(two_step_answer);
    let run = LocalRun::start("local_two_step", "two-step-3").expect("the run starts");
    let (seen, callback) = recorder();
    run.on_sink("totals", callback)
        .expect("`totals` is declared");

    run.feed("lines", b"z9".to_vec()).expect("accepted");
    run.run_until_idle(Duration::from_secs(10))
        .expect("the graph settles");

    let released = seen.lock().unwrap_or_else(|p| p.into_inner());
    let output = released.first().expect("one value reached the sink");
    assert_eq!(output.channel, "totals");
    assert_eq!(
        output.metadata.get("flz.key").map(String::as_str),
        Some("z")
    );
    assert_eq!(
        output.metadata.get("flz.entity").map(String::as_str),
        Some("total/z")
    );
    assert_eq!(
        output.metadata.get("flz.seq").map(String::as_str),
        Some("0.0")
    );
    assert_eq!(output.payload, b"z9");
}

#[test]
fn a_value_released_on_a_sink_nobody_claimed_is_reported() {
    let _serialized = one_run();
    declare(TWO_STEP);
    let _host = Host::spawn(two_step_answer);
    let run = LocalRun::start("local_two_step", "two-step-4").expect("the run starts");
    let before = local::unhandled_sink_outputs();

    run.feed("lines", b"q1".to_vec()).expect("accepted");
    run.run_until_idle(Duration::from_secs(10))
        .expect("the graph settles");

    assert_eq!(
        local::unhandled_sink_outputs(),
        before + 1,
        "an unclaimed sink value is reported, not discarded in silence"
    );
}

#[test]
fn the_non_durability_notice_is_emitted_on_every_run() {
    let _serialized = one_run();
    declare(TWO_STEP);
    let before = local::notices_emitted();
    let first = LocalRun::start("local_two_step", "notice-1").expect("the run starts");
    let second = LocalRun::start("local_two_step", "notice-2").expect("the run starts");
    assert_eq!(local::notices_emitted(), before + 2, "one notice per run");
    assert!(
        local::NON_DURABILITY_NOTICE.contains("NOTHING is durable"),
        "the notice states the limitation plainly: {}",
        local::NON_DURABILITY_NOTICE
    );
    drop((first, second));
}

#[test]
fn a_channel_the_declaration_does_not_carry_is_refused_by_name() {
    let _serialized = one_run();
    declare(TWO_STEP);
    let run = LocalRun::start("local_two_step", "refusals-1").expect("the run starts");

    let unknown = run.feed("nope", b"x".to_vec()).expect_err("refused");
    assert!(
        unknown.to_string().contains("nope") && unknown.to_string().contains("refusals-1"),
        "the refusal names the channel and the operative: {unknown}"
    );

    let not_entry = run.feed("mid", b"x".to_vec()).expect_err("refused");
    assert!(
        not_entry.to_string().contains("mid") && not_entry.to_string().contains("Entry"),
        "an inter-procedure channel is not an entry: {not_entry}"
    );

    let not_sink = run.on_sink("lines", Arc::new(|_| {})).expect_err("refused");
    assert!(
        not_sink.to_string().contains("lines"),
        "a sink callback on a non-sink channel is refused: {not_sink}"
    );
}

#[test]
fn a_controller_only_declaration_is_refused_at_startup() {
    let _serialized = one_run();
    declare(
        r#"{
        "factory_id": "local_tier", "sdk_lang": 1, "sdk_version": "0.1.0",
        "persistent": true, "effectively_once": true,
        "channels": [{"name": "lines", "kind": "entry", "chains": [[{"link": "into", "procedure": 0}]]}],
        "procedures": [{"name": "only"}]
    }"#,
    );
    let refusal = LocalRun::start("local_tier", "tier-1")
        .err()
        .expect("refused at startup");
    assert!(
        refusal.to_string().contains("effectively_once")
            && refusal.to_string().contains("requires a controller"),
        "the refusal names the capability and what it needs: {refusal}"
    );

    let missing = LocalRun::start("local_never_declared", "x")
        .err()
        .expect("an unregistered class is refused");
    assert!(
        missing.to_string().contains("local_never_declared"),
        "an unregistered class is named: {missing}"
    );
}

#[test]
fn a_local_stream_reads_at_end_of_stream_and_reports_no_descriptor() {
    // What keeps a `Local` stream out of a deployed runner: a reader handed
    // one stops at once rather than running a half-live connection, and it
    // carries no descriptor for anything to poll. It is reachable only
    // through `LocalRun`, which is the only minter of a frame sink; every
    // dialling site builds `Unix` or `Quic` from a connection it opened.
    let seen: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&seen);
    let mut stream = FlezhaStream::Local(Arc::new(move |frame: &[u8]| {
        into.lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(frame.to_vec());
        Ok(())
    }));

    let mut buf = [0u8; 8];
    assert_eq!(stream.read(&mut buf).expect("a read answers"), 0);
    assert!(stream.as_raw_fd().is_none());
    assert!(stream.shutdown(std::net::Shutdown::Both).is_ok());
    assert!(stream.set_nonblocking(true).is_ok());
    assert!(stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .is_ok());
    assert!(stream.flush().is_ok());
    assert!(matches!(
        stream.clone_handle().expect("a handle clones"),
        FlezhaStream::Local(_)
    ));

    // A write is an in-process handoff of the WHOLE frame: `write_all` hands
    // it over once, and every byte is taken.
    stream
        .write_all(b"a framed payload")
        .expect("a write lands");
    assert_eq!(
        seen.lock().unwrap_or_else(|p| p.into_inner()).as_slice(),
        [b"a framed payload".to_vec()]
    );
}

#[test]
fn the_same_declaration_serves_local_mode_and_class_registration() {
    let _serialized = one_run();
    declare(TWO_STEP);
    // No local-mode member exists: the run is started from the factory id the
    // controller registers, and the topology it reads is the registered one.
    let run = LocalRun::start("local_two_step", "shared-decl").expect("the run starts");
    let registered =
        flz_core::operative::registry::get_class("local_two_step").expect("registered");
    assert_eq!(
        run.instance().topology.factory_id,
        registered.factory_id,
        "local mode consumes the class the controller would receive"
    );
    assert_eq!(run.instance().topology.channels.len(), 3);
}
