//! Per-Entity mailboxes backed by a DashMap registry, served by the poll
//! core's activation handout. The coordinator thread still exists for control-plane
//! traffic (`HandleCommandAction`, `ReturnOperatives`, `Reconnect`) but
//! data-plane traffic bypasses it entirely — stream readers push directly
//! into the targeted StreamOperative instance's entity mailboxes.

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
        self.start_workflow_resumable(factory_id, instance_id, args, 0, None)
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

pub fn start(
    namespace: String,
    declared: Arc<RwLock<Vec<String>>>,
    (otx, orx): (Sender<RunnerMessage>, Receiver<RunnerMessage>),
    ctx: Sender<ClientResponseMessage>,
    shutdown_bridge: Arc<ShutdownBridge>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut runner = RunnerNode::new(namespace.clone());

        // Capture a registry snapshot closure now so the shutdown bridge
        // can enumerate live StreamOperative instances without going through the
        // coordinator channel. Survives reconnects since the registry
        // Arc is stable.
        {
            let registry = Arc::clone(&runner.operative_registry);
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

        let ctx_arc = Arc::new(Mutex::new(ctx));
        // Expose the coordinator channel process-globally so the
        // entity-state sink can drive a reconnect on a broken-pipe
        // flood without a handle threaded through every layer.
        crate::runner::install_reconnect_trigger(otx.clone());
        let otx_arc = Arc::new(Mutex::new(otx));
        let (_control_thread, mut control_stream, quic_client) = control_unit::open(
            namespace.clone(),
            Arc::clone(&declared),
            ctx_arc.clone(),
            otx_arc.clone(),
        );
        runner.set_quic_client(quic_client);
        shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));

        // Jittered control-channel keepalive. The timer only signals this
        // coordinator (it never writes the stream), so it survives reconnects
        // and always pings the current control stream — single-threaded with
        // command replies, so writes never interleave.
        let _keepalive = control_unit::spawn_keepalive(otx_arc.clone());

        // Jittered data-channel heartbeat. Like the keepalive it only signals
        // this coordinator, which writes every secondary stream — so an
        // idle-but-healthy operative keeps the controller's per-channel liveness
        // clock fresh without producing real output.
        let _data_heartbeat = control_unit::spawn_data_heartbeat(otx_arc.clone());

        loop {
            // A closed channel means every sender is gone — the owning
            // FlezhaRunner dropped. Exit cleanly so a later runner can start.
            let Ok(message) = orx.recv() else {
                break;
            };

            match message {
                RunnerMessage::HandleCommandAction(payload_id, message, _metadata) => {
                    command::handle_command(&mut runner, &control_stream, &payload_id, &message);
                }
                RunnerMessage::ReturnOperatives => {
                    return_operatives(&runner, ctx_arc.clone());
                }
                RunnerMessage::Reconnect(reason) => {
                    if matches!(runner.status, RunnerStatus::Stopped) {
                        continue;
                    }
                    handle_reconnect(
                        &mut runner,
                        &mut control_stream,
                        &namespace,
                        &declared,
                        &otx_arc,
                        reason,
                    );
                    shutdown_bridge.set_control_stream(Some(Arc::clone(&control_stream)));
                }
                RunnerMessage::SendControlKeepalive => {
                    // A failed keepalive write means the control stream died;
                    // the reader loop detects the same and drives reconnect, so
                    // just drop this tick.
                    let _ = control_unit::send_keepalive_ping(&control_stream);
                }
                RunnerMessage::SendDataHeartbeat => {
                    heartbeat_tick(&runner.shared_io_streams);
                }
                RunnerMessage::EmitWorkflowOutput {
                    operative_id,
                    payload,
                } => {
                    write_workflow_output(&runner.shared_io_streams, &operative_id, &payload);
                }
                RunnerMessage::EmitWorkflowCompleted {
                    operative_id,
                    status,
                    error,
                    result,
                } => {
                    let frame = control_command_frame(
                        "workflow-completed",
                        Command::WorkflowCompleted(PbWorkflowCompleted {
                            operative_id: operative_id.clone(),
                            status,
                            error,
                            result,
                        }),
                    );
                    if let Err(e) = (&*control_stream).write_all(&frame) {
                        warn!(
                            target: "flezha",
                            "[flezha] WorkflowCompleted write failed for {}: {}", operative_id, e
                        );
                    }
                }
                RunnerMessage::EmitControlRequest {
                    reference_id,
                    command,
                } => {
                    // The reference_id keys the driver's registered waiter, so
                    // it is reused verbatim (no fresh nonce). A failed write is
                    // left to the driver's ack timeout — same posture as a
                    // lost frame.
                    let frame = framed_request(reference_id, command);
                    if let Err(e) = (&*control_stream).write_all(&frame) {
                        warn!(
                            target: "flezha",
                            "[flezha] SDK-initiated control request write failed: {}", e
                        );
                    }
                }
                RunnerMessage::RemoveFactory {
                    factory_id,
                    workflow,
                } => {
                    // Serialized with AddOperative by construction (same loop),
                    // so the live-instance check cannot race a placement.
                    let result = runner.remove_factory(&factory_id, workflow);
                    let _ = ctx_arc
                        .lock()
                        .unwrap()
                        .send(ClientResponseMessage::RemoveFactoryResult(result));
                }
            }
        }
    })
}

/// Write a workflow's output payload on a secondary stream, tagged with its
/// `operative_id` so the controller routes it to the unit's bound plugins. The
/// controller keys output fan-out on the payload id, so any one secondary
/// stream suffices; best-effort like the data heartbeat.
fn write_workflow_output(
    io_streams: &RwLock<HashMap<u32, FlezhaStream>>,
    operative_id: &str,
    payload: &[u8],
) {
    let frame = payloader::build_from_binary(operative_id, payload, HashMap::new());
    let streams = io_streams.read().unwrap();
    match streams.values().next() {
        Some(stream) => {
            let mut w: &FlezhaStream = stream;
            if let Err(e) = w.write_all(&frame) {
                eprintln!(
                    "[flezha] workflow {} output write failed: {}",
                    operative_id, e
                );
            }
        }
        None => eprintln!(
            "[flezha] workflow {} output dropped: no secondary stream available",
            operative_id
        ),
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
    // Dispatch-progress self-report: the heartbeat thread stays alive even
    // when every worker is wedged, so the controller needs these counters to
    // tell "alive but frozen" apart from "healthy".
    let mut metadata = HashMap::new();
    metadata.insert(
        "dispatches".to_string(),
        crate::entity::dispatches_completed().to_string(),
    );
    metadata.insert(
        "blocked_parks".to_string(),
        crate::entity::blocked_park_episodes().to_string(),
    );
    metadata.insert(
        "credit_parked".to_string(),
        crate::credit::credit_parked().to_string(),
    );
    metadata.insert(
        "restore_discarded_inputs".to_string(),
        crate::entity::restore_discarded_inputs().to_string(),
    );
    // Wake-list occupancy: a runaway registration shows here long before it
    // is fatal, and a `timers_fired` that stops moving while `timers_armed`
    // climbs is promotion wedged rather than the Operative idle.
    metadata.insert(
        "timers_armed".to_string(),
        crate::timer::timers_armed().to_string(),
    );
    metadata.insert(
        "timers_fired".to_string(),
        crate::timer::timers_fired_total().to_string(),
    );
    let frame = payloader::build_from_binary(liveness::DATA_HEARTBEAT_PAYLOAD_ID, &[], metadata);
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

/// Stream-reader handle for the StreamOperative model. Cheap to clone; carries a
/// reference to the live StreamOperative-instance registry. Per-Entity drain
/// scheduling goes through the poll core.
#[derive(Clone)]
pub struct OperativeDispatchHandle {
    pub registry: Arc<OperativeRegistry>,
}

impl OperativeDispatchHandle {
    /// Dispatch a frame from the wire. Resolves the entry channel by
    /// `metadata["channel"]` (or the StreamOperative's first declared input
    /// channel as the documented default), wraps the payload bytes via
    /// the channel's `wrap_input`, and pushes the typed
    /// `Arc<dyn Any + Send + Sync>` onto every consumer Procedure's
    /// entity mailbox.
    pub fn dispatch(
        &self,
        instance_id: &str,
        payload: Vec<u8>,
        metadata: &HashMap<String, String>,
        stream: Arc<FlezhaStream>,
    ) {
        let instance = match self.registry.get_operative(instance_id) {
            Some(unit) => match &*unit {
                Operative::Stream(ri) => Arc::clone(ri),
                // Workflow inbox: route post-start input to the running driver's
                // inbox instead of dropping it. Dropped only if the instance is
                // not running / has no live inbox.
                Operative::Workflow(instance) => {
                    // A durable instance's input was journaled to its inbox
                    // before this frame was forwarded; the stamped offset rides
                    // into the iteration commit as the consumed offset.
                    let offset = metadata
                        .get(crate::workflow::INBOX_OFFSET_KEY)
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    if !instance.push_inbox(payload, offset) {
                        eprintln!(
                            "[flezha] inbox frame for workflow unit {} dropped (not running / no inbox)",
                            instance_id
                        );
                    }
                    return;
                }
            },
            None => {
                eprintln!("[flezha] operative instance {} not found", instance_id);
                return;
            }
        };

        // Late-input correction: the end of a replayed slice is a broadcast, not
        // an input — an input routes to one Entity by key, so a marker riding
        // one would strand the slice every *other* Entity buffered. It carries
        // no payload and never reaches a Procedure.
        if metadata.contains_key("flz.replay_end") {
            // Host classes may hold slice members in the prepare crossing;
            // the broadcast defers behind them instead of overtaking them.
            crate::host::deferred_end_replay_slice(&instance);
            return;
        }

        let topology = &instance.topology;

        let channel_id = match metadata.get("channel") {
            Some(name) => match topology.lookup_channel_id(name) {
                Some(id) => id,
                None => {
                    eprintln!(
                        "[flezha] operative {} has no channel `{}`",
                        instance_id, name
                    );
                    return;
                }
            },
            None => {
                match topology
                    .channels
                    .iter()
                    .position(|c| c.kind == crate::channel::ChannelKind::Entry)
                {
                    Some(idx) => idx as u32,
                    None => {
                        eprintln!("[flezha] operative {} has no entry channels", instance_id);
                        return;
                    }
                }
            }
        };

        let channel = match topology.channel(channel_id) {
            Some(c) => c,
            None => return,
        };

        // The input-liveness signal, taken before anything can decline the
        // frame: a payload that fails to parse still proves the feed is alive,
        // and a host class's staged payload never reaches the lines below.
        channel.observe_delivery();

        // Effectively-once tier: the controller stamps the durable input-log
        // offset on the forwarded frame; thread it to the invocation so the
        // atomic effect record carries the same offset as consumed-offset.
        let consumed_offset = metadata
            .get("flz.offset")
            .and_then(|s| s.parse::<u64>().ok());
        let replay = metadata.contains_key("flz.replay");

        // Typed entries (`sdk-graph-authoring`): a failing parse is classified
        // like a failing invocation and rides the channel's retry→DLQ ladder,
        // so keep the wire bytes for the re-attempts and the park record.
        // Passthrough `Vec<u8>`/`String` entries keep the pre-change
        // drop-and-log path (no clone on the hot path).
        // Host classes whose consumers need keys / event times stage the
        // payload for the prepare crossing instead of enqueueing directly.
        let payload = match crate::host::maybe_stage_prepare(
            &instance,
            channel_id,
            channel,
            payload,
            consumed_offset,
            replay,
            &stream,
        ) {
            Some(payload) => payload,
            None => return,
        };

        let retry_bytes = channel.entry_parsed.then(|| payload.clone());
        let typed_payload = match &channel.wrap_input {
            Some(wrap) => match wrap(payload) {
                Ok(p) => p,
                Err(e) => {
                    match retry_bytes {
                        Some(bytes) => crate::routing::spawn_entry_parse_ladder(
                            crate::routing::EntryParseLadder {
                                instance: Arc::clone(&instance),
                                channel_id,
                                bytes,
                                first_error: e,
                                stream,
                                consumed_offset,
                                replay,
                            },
                        ),
                        None => eprintln!(
                            "[flezha] operative {} channel `{}`: wrap_input failed: {}",
                            instance_id, channel.name, e
                        ),
                    }
                    return;
                }
            },
            None => {
                eprintln!(
                    "[flezha] operative {} channel `{}` has no entry wrapper",
                    instance_id, channel.name
                );
                return;
            }
        };

        crate::routing::route_to_consumers(
            &instance,
            channel,
            &typed_payload,
            &stream,
            consumed_offset,
            replay,
        );
    }
}

#[cfg(test)]
mod heartbeat_tests {
    use super::*;

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
