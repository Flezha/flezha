//! The coordinator: one thread per runner that owns the control stream and
//! every control-plane write — command replies, keepalives, heartbeats,
//! workflow completions and SDK-initiated requests — so those writes never
//! interleave. Data-plane traffic bypasses it entirely: stream readers push
//! straight into the targeted operative's entity mailboxes.

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::service::payloader;
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::{liveness, FlezhaStream};
use prost::Message;
use tracing::warn;

use crate::operative::instance::StreamInstance;
use crate::operative::registry::{get_class as get_operative_class, Operative, OperativeRegistry};
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, WorkflowCompleted as PbWorkflowCompleted};
use crate::routing::OperativeDispatchHandle;
use crate::runner::{ClientResponseMessage, RunnerMessage, RunnerStatus};
use crate::shutdown::ShutdownBridge;
use crate::util::nonce_suffix;
use crate::{command, control_unit};

pub struct RunnerNode {
    runner_id: String,
    _namespace: String,
    operative_registry: Arc<OperativeRegistry>,
    shared_io_streams: Arc<RwLock<HashMap<u32, FlezhaStream>>>,
    quic_client: Option<Arc<QuicClient>>,
    status: RunnerStatus,
}

/// The newest RunnerNode's operative registry — test support for harnesses
/// (the C-ABI scenario) that drive instances the runner built.
#[doc(hidden)]
pub static LIVE_REGISTRY: std::sync::Mutex<Option<Arc<OperativeRegistry>>> =
    std::sync::Mutex::new(None);

/// Look up a live stream instance by id on the newest runner. Test
/// support; `None` when no runner started or the id is unknown.
#[doc(hidden)]
pub fn live_stream_instance(
    instance_id: &str,
) -> Option<Arc<crate::operative::instance::StreamInstance>> {
    LIVE_REGISTRY
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .and_then(|r| r.get_stream(instance_id))
}

impl RunnerNode {
    pub fn new(namespace: String) -> RunnerNode {
        let operative_registry = Arc::new(OperativeRegistry::new());
        *LIVE_REGISTRY.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(Arc::clone(&operative_registry));
        let shared_io_streams = Arc::new(RwLock::new(HashMap::new()));

        RunnerNode {
            runner_id: "undefined".to_string(),
            _namespace: namespace,
            operative_registry,
            shared_io_streams,
            quic_client: None,
            status: RunnerStatus::Starting,
        }
    }

    /// Spawn a StreamOperative instance from a registered StreamOperative class. `factory_id`
    /// is looked up in the global class registry; `instance_id` is what
    /// the wire calls `operative_id`. `partition` is the member's owned
    /// key-group range (`KeyGroupRange::UNPARTITIONED` for an ordinary
    /// operative). Idempotent on duplicate `instance_id`: returns the
    /// existing instance.
    pub fn start_operative(
        &self,
        factory_id: String,
        instance_id: String,
        partition: crate::operative::instance::KeyGroupRange,
    ) -> Option<Arc<StreamInstance>> {
        if let Some(existing) = self.operative_registry.get_stream(&instance_id) {
            return Some(existing);
        }
        let topology = get_operative_class(&factory_id)?;
        let instance = StreamInstance::with_options(
            topology,
            Arc::from(instance_id.as_str()),
            None,
            partition,
        );
        Some(self.operative_registry.register_stream(instance))
    }

    /// Spawn a workflow-kind unit: register it (idempotent on a duplicate id) and
    /// launch its sequential driver on a dedicated thread with `args` from
    /// `init_values`. The driver runs the run-sugar body or the structured
    /// linear chain, observes the lifecycle (inbox, loop, duration, external
    /// stop), and converges on a single `WorkflowCompleted` teardown. Returns
    /// `false` when no workflow class is registered.
    pub fn start_workflow(&self, factory_id: String, instance_id: String, args: String) -> bool {
        self.start_workflow_resumable(factory_id, instance_id, args, 0, 0, None)
    }

    /// Spawn a workflow unit that may be **resuming** after a crash: a durable
    /// structured driver holds its first Procedure until `resume_entity_count`
    /// per-Entity restores have landed (the Controller ships one
    /// `RestoreInitState` per persisted Entity). `0` ⇒ a fresh spawn / a
    /// run-sugar or opted-out class ⇒ run immediately. `proc_journal` is the
    /// instance's committed proc state, which rides the bind because a durable
    /// class with no per-Entity state ships no restore at all. See
    /// [`crate::workflow::instance`].
    pub fn start_workflow_resumable(
        &self,
        factory_id: String,
        instance_id: String,
        args: String,
        resume_entity_count: u32,
        inbox_tail_count: u32,
        proc_journal: Option<crate::workflow::instance::JournalResume>,
    ) -> bool {
        if self.operative_registry.contains(&instance_id) {
            return true;
        }
        let spec = match crate::workflow::registry::get_class(&factory_id) {
            Some(spec) => spec,
            None => return false,
        };

        // Build the Entities (structured; empty for run-sugar) + the instance
        // now — synchronously, before the AddOperative frame is ACKed — and
        // register it so the Controller's RestoreInitState frames reach live
        // slots and the driver holds its first Procedure until the restores land.
        let id: Arc<str> = Arc::from(instance_id.as_str());
        let stage_entities = spec
            .topology
            .as_ref()
            .map(|_| crate::workflow::driver::build_all_stage_entities(&spec, &id))
            .unwrap_or_default();
        let (instance, inbox_rx) = crate::workflow::instance::WorkflowInstance::new(
            Arc::clone(&id),
            Arc::from(factory_id.as_str()),
            stage_entities,
            resume_entity_count as usize,
            inbox_tail_count,
            proc_journal,
        );
        self.operative_registry
            .register_workflow(Arc::clone(&instance));
        crate::workflow::driver::spawn(spec, instance, inbox_rx, args);
        true
    }

    /// The live-instance check + class unregistration behind
    /// [`RunnerMessage::RemoveFactory`]. MUST run on the coordinator loop —
    /// that is what serializes it with `AddOperative` handling and makes the
    /// check race-free. `Err(n)` = `n` live instances exist; the type stays
    /// registered.
    pub fn remove_factory(&self, factory_id: &str, workflow: bool) -> Result<(), usize> {
        let live = self.operative_registry.count_by_factory(factory_id);
        if live > 0 {
            return Err(live);
        }
        if workflow {
            crate::workflow::registry::unregister_class(factory_id);
        } else {
            crate::operative::registry::unregister_class(factory_id);
        }
        Ok(())
    }

    pub fn stop_operative(&self, instance_id: &str) -> bool {
        match self.operative_registry.remove(instance_id) {
            Some(unit) => {
                match &*unit {
                    Operative::Stream(instance) => instance.shutdown_background_threads(),
                    // Hard removal of a workflow: trip its cancellation token so
                    // the driver thread unwinds and exits promptly.
                    Operative::Workflow(instance) => instance.request_cancel(),
                }
                true
            }
            None => false,
        }
    }

    pub fn has_operative(&self, instance_id: &str) -> bool {
        self.operative_registry.contains(instance_id)
    }

    pub fn get_stream(&self, instance_id: &str) -> Option<Arc<StreamInstance>> {
        self.operative_registry.get_stream(instance_id)
    }

    /// Resolve a running workflow instance by id (cooperative `WorkflowStop`).
    pub fn get_workflow(
        &self,
        instance_id: &str,
    ) -> Option<Arc<crate::workflow::instance::WorkflowInstance>> {
        self.operative_registry.get_workflow(instance_id)
    }

    /// Locate a live workflow Entity slot by wire identity, for the inbound
    /// `RestoreInitState` handler.
    pub fn find_workflow_entity(
        &self,
        entity_id: &[u8],
    ) -> Option<(
        Arc<crate::workflow::instance::WorkflowInstance>,
        Arc<crate::entity::EntitySlot>,
    )> {
        self.operative_registry.find_workflow_entity(entity_id)
    }

    pub fn operative_dispatch_handle(&self) -> OperativeDispatchHandle {
        OperativeDispatchHandle {
            registry: Arc::clone(&self.operative_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    /// Snapshot of live StreamOperative instances — the
    /// `FlezhaRunner::operatives()` view.
    pub fn get_operatives(&self) -> HashMap<String, Arc<StreamInstance>> {
        self.operative_registry.snapshot_stream()
    }

    pub fn get_io_streams(&self) -> Arc<RwLock<HashMap<u32, FlezhaStream>>> {
        Arc::clone(&self.shared_io_streams)
    }

    pub fn get_runner_id(&self) -> String {
        self.runner_id.clone()
    }

    pub fn assign_runner_id(&mut self, runner_id: String) {
        self.runner_id = runner_id;
    }

    pub fn set_status(&mut self, status: RunnerStatus) {
        self.status = status;
    }

    pub fn get_quic_client(&self) -> Option<Arc<QuicClient>> {
        self.quic_client.clone()
    }

    pub fn set_quic_client(&mut self, client: Option<Arc<QuicClient>>) {
        self.quic_client = client;
    }
}

/// What the coordinator loop holds between messages: the runner, its live
/// control stream, what a reconnect needs to re-open it, and the per-workflow
/// count of direct outputs a completion carries.
struct Coordinator {
    runner: RunnerNode,
    control_stream: Arc<FlezhaStream>,
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    otx_arc: Arc<Mutex<Sender<RunnerMessage>>>,
    ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>,
    shutdown_bridge: Arc<ShutdownBridge>,
    /// Direct outputs written per workflow instance, in this loop's order:
    /// the count a completion carries so the controller can wait for them.
    direct_outputs: HashMap<String, u32>,
}

pub fn start(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    (otx, orx): (Sender<RunnerMessage>, Receiver<RunnerMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let (mut coordinator, _timers) =
            Coordinator::connect(namespace, declared, otx, ctx, shutdown_bridge);
        // A closed channel means every sender is gone — the owning
        // FlezhaRunner dropped. Exit cleanly so a later runner can start.
        while let Ok(message) = orx.recv() {
            coordinator.handle_message(message);
        }
    })
}

/// Let the shutdown bridge enumerate live StreamOperative instances without
/// going through the coordinator channel. Survives reconnects since the
/// registry Arc is stable.
fn install_snapshot_fn(shutdown_bridge: &ShutdownBridge, registry: Arc<OperativeRegistry>) {
    shutdown_bridge.set_snapshot_fn(Box::new(move || {
        registry
            .snapshot_stream()
            .into_iter()
            .map(|(k, instance)| {
                (
                    k,
                    crate::command::entity_state::serialize_operative_state(&instance),
                )
            })
            .collect()
    }));
}

impl Coordinator {
    /// Open the control channel and build the coordinator around it. The two
    /// jittered timers returned only signal this coordinator (they never write
    /// a stream), so they survive reconnects and every control write stays
    /// single-threaded: the keepalive pings the current control stream, the
    /// heartbeat writes every secondary so an idle-but-healthy operative keeps
    /// the controller's per-channel liveness clock fresh.
    fn connect(
        namespace: String,
        declared: Arc<RwLock<Vec<String>>>,
        otx: Sender<RunnerMessage>,
        ctx: Sender<ClientResponseMessage>,
        shutdown_bridge: Arc<ShutdownBridge>,
    ) -> (Self, [JoinHandle<()>; 2]) {
        let mut runner = RunnerNode::new(namespace.clone());
        install_snapshot_fn(&shutdown_bridge, Arc::clone(&runner.operative_registry));

        let ctx_arc = Arc::new(Mutex::new(ctx));
        // Expose the coordinator channel process-globally so the
        // entity-state sink can drive a reconnect on a broken-pipe
        // flood without a handle threaded through every layer.
        crate::runner::install_reconnect_trigger(otx.clone());
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, control_stream, quic_client) = control_unit::open(
            namespace.clone(),
            Arc::clone(&declared),
            ctx_arc.clone(),
            otx_arc.clone(),
        );
        runner.set_quic_client(quic_client);
        shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));

        let timers = [
            control_unit::spawn_keepalive(otx_arc.clone()),
            control_unit::spawn_data_heartbeat(otx_arc.clone()),
        ];
        let coordinator = Coordinator {
            runner,
            control_stream,
            namespace,
            declared,
            otx_arc,
            ctx_arc,
            shutdown_bridge,
            direct_outputs: HashMap::new(),
        };
        (coordinator, timers)
    }

    fn handle_message(&mut self, message: RunnerMessage) {
        match message {
            RunnerMessage::HandleCommandAction(payload_id, message, _metadata) => {
                command::handle_command(
                    &mut self.runner,
                    &self.control_stream,
                    &payload_id,
                    &message,
                );
            }
            RunnerMessage::ReturnOperatives => {
                return_operatives(&self.runner, self.ctx_arc.clone());
            }
            RunnerMessage::Reconnect(reason) => self.reconnect(reason),
            // A failed keepalive write means the control stream died; the
            // reader loop detects the same and drives reconnect.
            RunnerMessage::SendControlKeepalive => {
                let _ = control_unit::send_keepalive_ping(&self.control_stream);
            }
            RunnerMessage::SendDataHeartbeat => {
                heartbeat_tick(&self.runner.shared_io_streams);
            }
            RunnerMessage::EmitWorkflowOutput {
                operative_id,
                payload,
            } => {
                if write_workflow_output(&self.runner.shared_io_streams, &operative_id, &payload) {
                    *self.direct_outputs.entry(operative_id).or_insert(0) += 1;
                }
            }
            RunnerMessage::EmitWorkflowCompleted {
                operative_id,
                status,
                error,
                result,
            } => self.write_workflow_completed(operative_id, status, error, result),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => self.write_control_request(reference_id, command),
            RunnerMessage::RemoveFactory {
                factory_id,
                workflow,
            } => self.remove_factory(&factory_id, workflow),
        }
    }

    /// Serialized with AddOperative by construction (same loop), so the
    /// live-instance check cannot race a placement.
    fn remove_factory(&self, factory_id: &str, workflow: bool) {
        let result = self.runner.remove_factory(factory_id, workflow);
        let _ = self
            .ctx_arc
            .lock()
            .unwrap()
            .send(ClientResponseMessage::RemoveFactoryResult(result));
    }

    /// A reconnect on a stopped runner is a no-op; otherwise re-open the
    /// control stream and hand the shutdown bridge the new one.
    fn reconnect(&mut self, reason: &'static str) {
        if matches!(self.runner.status, RunnerStatus::Stopped) {
            return;
        }
        handle_reconnect(
            &mut self.runner,
            &mut self.control_stream,
            &self.namespace,
            &self.declared,
            &self.otx_arc,
            reason,
        );
        self.shutdown_bridge
            .set_control_stream(Some(Arc::clone(&self.control_stream)));
    }

    /// The terminal `WorkflowCompleted` frame, carrying how many direct
    /// outputs this loop wrote for the unit.
    fn write_workflow_completed(
        &mut self,
        operative_id: String,
        status: String,
        error: String,
        result: Vec<u8>,
    ) {
        let frame = control_command_frame(
            "workflow-completed",
            Command::WorkflowCompleted(PbWorkflowCompleted {
                operative_id: operative_id.clone(),
                status,
                error,
                result,
                direct_outputs: self.direct_outputs.remove(&operative_id).unwrap_or(0),
            }),
        );
        if let Err(e) = (&*self.control_stream).write_all(&frame) {
            warn!(
                target: "flezha",
                "[flezha] WorkflowCompleted write failed for {}: {}", operative_id, e
            );
        }
    }

    /// An SDK-initiated request under the caller's own reference id. The
    /// reference_id keys the driver's registered waiter, so it is reused
    /// verbatim (no fresh nonce). A failed write is left to the driver's ack
    /// timeout — same posture as a lost frame.
    fn write_control_request(&self, reference_id: String, command: Command) {
        let frame = framed_request(reference_id, command);
        if let Err(e) = (&*self.control_stream).write_all(&frame) {
            warn!(
                target: "flezha",
                "[flezha] SDK-initiated control request write failed: {}", e
            );
        }
    }
}

/// Write a workflow's output payload on a secondary stream, tagged with its
/// `operative_id` so the controller routes it to the unit's bound plugins. The
/// controller keys output fan-out on the payload id, so any one secondary
/// stream suffices; best-effort like the data heartbeat.
/// `true` when the frame reached the stream: only a written output is one
/// the controller can be asked to wait for.
fn write_workflow_output(
    io_streams: &RwLock<HashMap<u32, FlezhaStream>>,
    operative_id: &str,
    payload: &[u8],
) -> bool {
    let frame = payloader::build_from_binary(operative_id, payload, HashMap::new());
    let streams = io_streams.read().unwrap();
    match streams.values().next() {
        Some(stream) => {
            let mut w: &FlezhaStream = stream;
            match w.write_all(&frame) {
                Ok(()) => true,
                Err(e) => {
                    eprintln!(
                        "[flezha] workflow {} output write failed: {}",
                        operative_id, e
                    );
                    false
                }
            }
        }
        None => {
            eprintln!(
                "[flezha] workflow {} output dropped: no secondary stream available",
                operative_id
            );
            false
        }
    }
}

/// Build an SDK -> Controller `control_command` frame for one oneof command,
/// stamping a `<ref_prefix>-<nonce>` reference id.
fn control_command_frame(ref_prefix: &str, command: Command) -> Vec<u8> {
    framed_request(format!("{ref_prefix}-{}", nonce_suffix()), command)
}

/// Frame one SDK-initiated `CommandRequest` under an explicit reference_id
/// (the caller's waiter key when a controller reply is awaited).
fn framed_request(reference_id: String, command: Command) -> Vec<u8> {
    let request = CommandRequest {
        reference_id,
        command: Some(command),
    };
    payloader::build_from_binary("control_command", &request.encode_to_vec(), HashMap::new())
}

/// One heartbeat tick: write, and report a heartbeat that reached nothing so
/// the flood ladder can drive a reconnect. An idle Operative writes no state
/// changes, so this is the only signal that would ever trip the ladder for it.
fn heartbeat_tick(io_streams: &RwLock<HashMap<u32, FlezhaStream>>) {
    if !send_data_heartbeat(io_streams) {
        crate::state_sink::note_transport_failure();
    }
}

/// Write a data-channel heartbeat on every secondary stream. The frame is a
/// bare payload-id with no body, small enough to write atomically.
///
/// Returns whether it reached at least one stream. An empty map or an
/// all-failed write means the controller's per-channel liveness clock is no
/// longer being fed and it will flag `output_stall`; the caller reports that
/// so a runner producing no state changes still drives its own recovery
/// instead of waiting for a `ConnectSocket` that may never come.
fn send_data_heartbeat(io_streams: &RwLock<HashMap<u32, FlezhaStream>>) -> bool {
    let frame = payloader::build_from_binary(
        liveness::DATA_HEARTBEAT_PAYLOAD_ID,
        &[],
        heartbeat_metadata(),
    );
    let streams = io_streams.read().unwrap();
    let mut delivered = false;
    for stream in streams.values() {
        let mut w: &FlezhaStream = stream;
        if w.write_all(&frame).is_ok() {
            delivered = true;
        }
    }
    delivered
}

/// The runner's self-report, as the heartbeat frame carries it. Separate from
/// the write so the key set — a cross-SDK contract the controller mirrors slot
/// for slot — is readable without a socket.
fn heartbeat_metadata() -> HashMap<String, String> {
    let core = crate::poll_core::global();
    // The runner's whole self-report, as one table. The heartbeat thread
    // outlives a wedged dispatch pool, so these counters are what tell "alive
    // but frozen" from "healthy" — and every key here is mirrored slot for slot
    // by the controller (`Flezha.Runner.StreamChannel`), so the set is a
    // cross-SDK contract, not a convenience.
    [
        // Dispatch progress: frozen with the runner alive is a wedged pool.
        ("dispatches", crate::entity::dispatches_completed()),
        // Overflow outcomes. `dispatches` frozen with `blocked_parks` climbing
        // is healthy backpressure; frozen with it flat is a wedge. The two drop
        // counters are the shed an operator would otherwise only find by
        // attaching a debugger to the runner.
        ("blocked_parks", crate::entity::pushes_blocked_total()),
        ("drops_newest", crate::entity::drops_newest_total()),
        ("drops_oldest", crate::entity::drops_oldest_total()),
        ("credit_parked", crate::credit::credit_parked()),
        // A discard is correct only where the controller re-forwards from a
        // durable log, so a climbing value is the operator's one handle on how
        // much a rebind threw away.
        (
            "restore_discarded_inputs",
            crate::entity::restore_discarded_inputs(),
        ),
        // Handout liveness: how many activations are underway and how long the
        // oldest has been out. `dispatches` freezing tells the controller the
        // WHOLE pool stopped; these two are what make ONE wedged Entity visible
        // among thousands that are still completing. Reporting only — the core
        // cannot tell a ten-minute Procedure from a lost handout, so the
        // decision is the controller's and the remedy is a rebind from durable
        // state.
        ("inflight", core.outstanding_count() as u64),
        ("inflight_oldest_ms", core.outstanding_oldest_age_ms()),
        // Backpressure that withholds work instead of holding a thread. A
        // gated producer holds no handout, so `inflight` cannot see it; these
        // two are what keep it from reading as a wedged pool, and a climbing
        // count with `dispatches` frozen is the consumer being behind — the
        // remedy for which is never a rebind of the producer.
        ("pending_pushes", core.pending_push_count() as u64),
        ("pending_push_oldest_ms", core.pending_push_oldest_age_ms()),
        // Wake-list occupancy: a runaway registration shows here long before it
        // is fatal, and a `timers_fired` that stops moving while `timers_armed`
        // climbs is promotion wedged rather than the Operative idle.
        ("timers_armed", crate::timer::timers_armed() as u64),
        ("timers_fired", crate::timer::timers_fired_total()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

fn return_operatives(runner: &RunnerNode, ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>) {
    let operatives = runner.get_operatives();
    // A closed client channel means the owning handle is gone mid-request;
    // dropping the reply is the only sane outcome — never panic the
    // coordinator.
    let _ = ctx_arc
        .lock()
        .unwrap()
        .send(ClientResponseMessage::ReturnOperatives(operatives));
}

/// Re-establish the control stream while preserving the SDK's
/// in-memory state. The previous behaviour cleared
/// `shared_io_streams`, `operative_registry`, and the `runner_id` —
/// that is what caused the May 25 silent freeze (runner sat empty
/// while the controller still believed the binding was live). The
/// `runner-controller-connection-healing` capability now requires:
///
///   * `operative_registry` and `shared_io_streams` are retained across
///     reconnect (in-flight per-Entity mailbox work keeps draining); the
///     existing `runner_id` is kept until the controller assigns a fresh
///     session token via `AssignRunnerId` on the re-opened stream.
///   * A `tracing::warn!` line is emitted with the upstream-derived
///     `reason` so kubectl logs surface every reconnect.
///   * The re-opened control stream re-presents the runner's stable id in
///     its connect handshake, so the controller resolves the reconnect as a
///     binding swap — there is no separate reconnect frame.
fn handle_reconnect(
    runner: &mut RunnerNode,
    control_stream: &mut Arc<FlezhaStream>,
    namespace: &str,
    declared: &Arc<RwLock<Vec<String>>>,
    otx_arc: &Arc<Mutex<Sender<RunnerMessage>>>,
    reason: &str,
) {
    let previous_id = runner.get_runner_id();
    warn!(
        target: "flezha",
        "[flezha] control-channel reconnect reason={} previous_id={}",
        reason,
        previous_id
    );

    runner.set_status(RunnerStatus::Stopped);

    let (_, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), Arc::clone(declared), otx_arc.clone());

    *control_stream = new_stream;
    runner.set_quic_client(new_quic);

    // Resume processing dispatch traffic against the preserved
    // registry — without `set_status(Running)` the per-Entity
    // mailboxes drain into a paused state and the controller sees
    // the SDK as a black hole even though its registry is intact.
    runner.set_status(RunnerStatus::Running);
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;

    /// Every overflow outcome has to reach an operator without a debugger, and
    /// the heartbeat is the only frame a wedged-but-alive runner still sends.
    /// The keys are a cross-SDK contract — `stream_channel.ex` mirrors them
    /// slot for slot — so a rename here silently blinds the controller.
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

        crate::entity::PUSHES_BLOCKED_TOTAL.fetch_add(3, std::sync::atomic::Ordering::Relaxed);
        crate::entity::DROPS_NEWEST_TOTAL.fetch_add(5, std::sync::atomic::Ordering::Relaxed);
        crate::entity::DROPS_OLDEST_TOTAL.fetch_add(7, std::sync::atomic::Ordering::Relaxed);
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

    /// An Operative that writes no state changes trips no flood ladder, so the
    /// 10 s data heartbeat is its only outbound signal. When that heartbeat
    /// reaches no stream the controller flags `output_stall`, while the runner
    /// — which discarded the write error — sees nothing and never asks to
    /// reconnect. That is a permanent, silent stall.
    #[test]
    fn heartbeat_reaching_no_stream_arms_the_reconnect_ladder() {
        let _g = crate::state_sink::SINK_TEST_LOCK.lock().unwrap();
        std::env::set_var("FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD", "3");
        std::env::set_var("FLEZHA_BROKEN_PIPE_WINDOW_MS", "60000");
        crate::state_sink::reset_for_test();

        // The signature of a dead secondary: the stream map is empty, so every
        // heartbeat write is a no-op over zero streams.
        let empty: RwLock<HashMap<u32, FlezhaStream>> = RwLock::new(HashMap::new());
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
}
