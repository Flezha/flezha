//! The coordinator: one thread per runner that owns the control stream and
//! every control-plane write — command replies, keepalives, heartbeats,
//! workflow completions and SDK-initiated requests — so those writes never
//! interleave. Data-plane traffic bypasses it entirely: stream readers push
//! straight into the targeted operative's cell mailboxes.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use crossbeam::channel::{Receiver, Sender};
use flz_transport::service::payloader;
use flz_transport::transport::liveness::{self, ConnectAnswered};
use flz_transport::transport::quic::QuicClient;
use flz_transport::transport::FlezhaStream;
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
    connect_answered: ConnectAnswered,
    status: RunnerStatus,
}

/// The newest RunnerNode's registry, for harnesses that drive its instances.
#[doc(hidden)]
pub static LIVE_REGISTRY: std::sync::Mutex<Option<Arc<OperativeRegistry>>> =
    std::sync::Mutex::new(None);

/// A live stream instance by id on the newest runner, for harnesses.
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
            connect_answered: ConnectAnswered::default(),
            status: RunnerStatus::Starting,
        }
    }

    /// Spawn a stream instance of a registered class; `instance_id` is the
    /// wire's `operative_id`. Idempotent: a duplicate id answers the existing
    /// instance.
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

    /// Spawn a workflow instance and its driver; `false` when no such class
    /// is registered.
    pub fn start_workflow(&self, factory_id: String, instance_id: String, args: String) -> bool {
        self.start_workflow_resumable(factory_id, instance_id, args, 0, 0, None)
    }

    /// Spawn a workflow instance that may be resuming: the driver holds its
    /// first tasklet until `resume_cell_count` restores have landed (`0`
    /// runs at once). `task_journal` rides the bind: a durable class with no
    /// per-Cell state ships no restore at all.
    pub fn start_workflow_resumable(
        &self,
        factory_id: String,
        instance_id: String,
        args: String,
        resume_cell_count: u32,
        inbox_tail_count: u32,
        task_journal: Option<crate::workflow::instance::JournalResume>,
    ) -> bool {
        if self.operative_registry.contains(&instance_id) {
            return true;
        }
        let spec = match crate::workflow::registry::get_class(&factory_id) {
            Some(spec) => spec,
            None => return false,
        };

        // Build the Cells + the instance now, synchronously, before the
        // AddOperative frame is ACKed, and register it so the Controller's
        // RestoreInitState frames reach live slots and the driver holds its
        // first Tasklet until the restores land.
        let id: Arc<str> = Arc::from(instance_id.as_str());
        let stage_cells = crate::workflow::driver::build_all_stage_cells(&spec, &id);
        let (instance, inbox_rx) = crate::workflow::instance::WorkflowInstance::new(
            Arc::clone(&id),
            Arc::from(factory_id.as_str()),
            stage_cells,
            resume_cell_count as usize,
            inbox_tail_count,
            task_journal,
        );
        self.operative_registry
            .register_workflow(Arc::clone(&instance));
        crate::workflow::driver::spawn(spec, instance, inbox_rx, args);
        true
    }

    /// Unregister a class with no live instance; `Err(n)` names how many
    /// exist. MUST run on the coordinator loop, serialized with `AddOperative`.
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

    pub fn get_workflow(
        &self,
        instance_id: &str,
    ) -> Option<Arc<crate::workflow::instance::WorkflowInstance>> {
        self.operative_registry.get_workflow(instance_id)
    }

    /// A live workflow Cell slot by wire identity.
    pub fn find_workflow_cell(
        &self,
        cell_id: &[u8],
    ) -> Option<(
        Arc<crate::workflow::instance::WorkflowInstance>,
        Arc<crate::cell::CellSlot>,
    )> {
        self.operative_registry.find_workflow_cell(cell_id)
    }

    pub fn operative_dispatch_handle(&self) -> OperativeDispatchHandle {
        OperativeDispatchHandle {
            registry: Arc::clone(&self.operative_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

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

    /// The session the next `ConnectSocket` answer belongs to.
    pub fn set_connect_answered(&mut self, answered: ConnectAnswered) {
        self.connect_answered = answered;
    }

    /// Tell the control reader this session's pair set came up.
    pub fn note_connect_answered(&self) {
        self.connect_answered.note();
    }
}

/// What the loop holds between messages.
struct Coordinator {
    runner: RunnerNode,
    control_stream: Arc<FlezhaStream>,
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    otx_arc: Arc<Mutex<Sender<RunnerMessage>>>,
    ctx_arc: Arc<Mutex<Sender<ClientResponseMessage>>>,
    shutdown_bridge: Arc<ShutdownBridge>,
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

/// Let the shutdown bridge enumerate live instances without the coordinator
/// channel; the registry `Arc` is stable across reconnects.
fn install_snapshot_fn(shutdown_bridge: &ShutdownBridge, registry: Arc<OperativeRegistry>) {
    shutdown_bridge.set_snapshot_fn(Box::new(move || {
        registry
            .snapshot_stream()
            .into_iter()
            .map(|(k, instance)| {
                (
                    k,
                    crate::command::cell_state::serialize_operative_state(&instance),
                )
            })
            .collect()
    }));
}

impl Coordinator {
    /// Open the control channel and build the coordinator around it. The two
    /// timers only signal this loop, so every control write stays on one
    /// thread and both survive reconnects.
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
        // cell-state sink can drive a reconnect on a broken-pipe
        // flood without a handle threaded through every layer.
        crate::runner::install_reconnect_trigger(otx.clone());
        let otx_arc = Arc::new(Mutex::new(otx));
        let (answered, control_stream, quic_client) = control_unit::open(
            namespace.clone(),
            Arc::clone(&declared),
            ctx_arc.clone(),
            otx_arc.clone(),
        );
        runner.set_quic_client(quic_client);
        runner.set_connect_answered(answered);
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
                write_workflow_output(&self.runner.shared_io_streams, &operative_id, &payload);
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
            } => self.write_control_request(reference_id, *command),
            RunnerMessage::RemoveFactory {
                factory_id,
                workflow,
            } => self.remove_factory(&factory_id, workflow),
        }
    }

    fn remove_factory(&self, factory_id: &str, workflow: bool) {
        let result = self.runner.remove_factory(factory_id, workflow);
        let _ = self
            .ctx_arc
            .lock()
            .unwrap()
            .send(ClientResponseMessage::RemoveFactoryResult(result));
    }

    /// A no-op on a stopped runner; otherwise re-open the control stream.
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

    /// The terminal `WorkflowCompleted`, carrying the direct-output count.
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
            }),
        );
        if let Err(e) = self.write_control(frame) {
            warn!(
                target: "flezha",
                "[flezha] WorkflowCompleted write failed for {}: {}", operative_id, e
            );
        }
    }

    /// An SDK-initiated request under the caller's reference id, which keys
    /// its waiter; a failed write is left to the waiter's timeout.
    fn write_control_request(&self, reference_id: String, command: Command) {
        let frame = framed_request(reference_id, command);
        if let Err(e) = self.write_control(frame) {
            warn!(
                target: "flezha",
                "[flezha] SDK-initiated control request write failed: {}", e
            );
        }
    }

    /// A control frame on the control stream: refused to its caller and never
    /// held, because a reply re-driven after its waiter timed out is a stale
    /// answer and the reconnect re-issues the request it belonged to.
    fn write_control(&self, frame: Vec<u8>) -> std::io::Result<()> {
        let stream = Some(&*self.control_stream);
        crate::state_sink::write(crate::state_sink::Class::Control, stream, frame).refused()
    }
}

/// A workflow's output on any one secondary stream, tagged with its
/// `operative_id`. An output the wire cannot take is held for the reconnect on
/// the ring every writer on this wire shares, never printed and dropped.
///
/// An absent stream takes the same route as a refused write, and arms the same
/// flood ladder: `heartbeat_tick` already reads an empty map as a transport
/// failure, and a second policy inside one function is how a wire ends up with
/// two answers to one question.
fn write_workflow_output(
    io_streams: &RwLock<HashMap<u32, FlezhaStream>>,
    operative_id: &str,
    payload: &[u8],
) {
    let frame = payloader::build_without_metadata(operative_id, payload);
    let streams = io_streams.read().unwrap();
    // Held framed, not as a record: the ring re-frames a record with empty
    // metadata, which would strip a stamped output of its identity.
    crate::state_sink::write(
        crate::state_sink::Class::Output,
        streams.values().next(),
        frame,
    );
}

/// A `control_command` frame under a `<ref_prefix>-<nonce>` reference id.
fn control_command_frame(ref_prefix: &str, command: Command) -> Vec<u8> {
    framed_request(format!("{ref_prefix}-{}", nonce_suffix()), command)
}

/// A `CommandRequest` frame under the caller's own reference id.
fn framed_request(reference_id: String, command: Command) -> Vec<u8> {
    let request = CommandRequest {
        reference_id,
        command: Some(command),
    };
    payloader::build_without_metadata("control_command", &request.encode_to_vec())
}

/// One heartbeat tick; a heartbeat that reached nothing feeds the flood
/// ladder, the only signal an idle Operative ever gives it.
fn heartbeat_tick(io_streams: &RwLock<HashMap<u32, FlezhaStream>>) {
    if !send_data_heartbeat(io_streams) {
        crate::state_sink::note_transport_failure();
    }
}

/// A body-less heartbeat on every secondary stream; whether it reached one.
/// Reaching none means the controller will flag `output_stall`, so the
/// caller drives its own recovery rather than wait for a `ConnectSocket`.
fn send_data_heartbeat(io_streams: &RwLock<HashMap<u32, FlezhaStream>>) -> bool {
    // A refused frame is a tick that reached nothing, which is the answer this
    // function already has for that; a heartbeat is owed no delivery, so the
    // tick is skipped rather than held.
    let frame = match payloader::build_from_binary(
        liveness::DATA_HEARTBEAT_PAYLOAD_ID,
        &[],
        heartbeat_metadata(),
    ) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(
                target: "flezha::transport",
                %error,
                "self-report could not be framed; heartbeat tick skipped"
            );
            return false;
        }
    };
    let streams = io_streams.read().unwrap();
    let mut delivered = false;
    for stream in streams.values() {
        // A heartbeat is owed no delivery: the class never holds it, and the
        // caller's ladder is what a tick that reached nothing feeds.
        let class = crate::state_sink::Class::Probe;
        if crate::state_sink::write(class, Some(stream), frame.clone())
            .refused()
            .is_ok()
        {
            delivered = true;
        }
    }
    delivered
}

/// The heartbeat's self-report; its keys are a cross-SDK contract, and the
/// controller holds a liveness slot for every one of them.
fn heartbeat_metadata() -> HashMap<String, String> {
    let core = crate::poll_core::global();
    // The runner's whole self-report, as one table. The heartbeat thread
    // outlives a wedged dispatch pool, so these counters are what tell "alive
    // but frozen" from "healthy". The key set is a cross-SDK contract, not a
    // convenience: every SDK sends these names. The controller
    // (`Flezha.Runner.StreamChannel`) mirrors each into an `hb_<key>` liveness
    // slot, so a key added here stays invisible until it gains one — which the
    // cross-SDK gate refuses.
    [
        // Dispatch progress: frozen with the runner alive is a wedged pool.
        ("dispatches", crate::cell::dispatches_completed()),
        // Overflow outcomes. `dispatches` frozen with `blocked_parks` climbing
        // is healthy backpressure; frozen with it flat is a wedge. The two drop
        // counters are the shed an operator would otherwise only find by
        // attaching a debugger to the runner.
        ("blocked_parks", crate::cell::pushes_blocked_total()),
        ("drops_newest", crate::cell::drops_newest_total()),
        ("drops_oldest", crate::cell::drops_oldest_total()),
        ("credit_parked", crate::credit::credit_parked()),
        // A discard is correct only where the controller re-forwards from a
        // durable log, so a climbing value is the operator's one handle on how
        // much a rebind threw away.
        (
            "restore_discarded_inputs",
            crate::cell::restore_discarded_inputs(),
        ),
        // Handout liveness: how many activations are underway and how long the
        // oldest has been out. `dispatches` freezing tells the controller the
        // WHOLE pool stopped; these two are what make ONE wedged Cell visible
        // among thousands that are still completing. Reporting only — the core
        // cannot tell a ten-minute Tasklet from a lost handout, so the
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

/// Re-open the control stream keeping the registry, the data streams and the
/// `runner_id`: in-flight mailbox work keeps draining, and the handshake's
/// stable id makes the controller resolve the reconnect as a binding swap.
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

    let (answered, new_stream, new_quic) =
        control_unit::open_reconnect(namespace.to_string(), Arc::clone(declared), otx_arc.clone());

    *control_stream = new_stream;
    runner.set_quic_client(new_quic);
    runner.set_connect_answered(answered);

    // Resume processing dispatch traffic against the preserved
    // registry — without `set_status(Running)` the per-Cell
    // mailboxes drain into a paused state and the controller sees
    // the SDK as a black hole even though its registry is intact.
    runner.set_status(RunnerStatus::Running);
}

#[cfg(test)]
#[path = "coordinator_tests.rs"]
mod tests;
