//! The coordinator: one thread per runner that owns the control stream and
//! every control-plane write — command replies, keepalives, heartbeats,
//! steps completions and SDK-initiated requests — so those writes never
//! interleave. Data-plane traffic bypasses it entirely: stream readers push
//! straight into the targeted relay's cell mailboxes.

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

use crate::channel::ChannelKind;
use crate::error::{Code, Error};
use crate::pb::command_request::Command;
use crate::pb::{CommandRequest, RelayCompleted as PbRelayCompleted};
use crate::relay::instance::RelayInstance;
use crate::relay::registry::{get_class as get_relay_class, RelayRegistry};
use crate::routing::RelayDispatchHandle;
use crate::runner::{ClientResponseMessage, RunnerMessage, RunnerStatus};
use crate::shutdown::ShutdownBridge;
use crate::util::nonce_suffix;
use crate::{command, control_unit};

/// Each data pair's secondary by pair key: the stream a relay's outputs ride.
pub type IoStreams = Arc<RwLock<HashMap<u32, Arc<FlezhaStream>>>>;

pub struct RunnerNode {
    runner_id: String,
    _namespace: String,
    relay_registry: Arc<RelayRegistry>,
    shared_io_streams: IoStreams,
    quic_client: Option<Arc<QuicClient>>,
    connect_answered: ConnectAnswered,
    status: RunnerStatus,
}

/// What application code reaches of a runner without its coordinator: the
/// relays bound on it, and a send to one of them that enters where a frame
/// off the wire enters.
#[derive(Clone)]
pub struct BoundRelays {
    registry: Arc<RelayRegistry>,
    io_streams: IoStreams,
}

static LIVE: Mutex<Option<BoundRelays>> = Mutex::new(None);

/// The newest RunnerNode's bound relays; none before any runner is built.
pub fn bound() -> BoundRelays {
    LIVE.lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .unwrap_or_else(|| BoundRelays {
            registry: Arc::new(RelayRegistry::new()),
            io_streams: Arc::default(),
        })
}

/// A live instance by id on the newest runner, for harnesses.
#[doc(hidden)]
pub fn live_instance(instance_id: &str) -> Option<Arc<RelayInstance>> {
    bound().registry.get(instance_id)
}

impl BoundRelays {
    /// The ids of the relay instances bound now, a partition member by its
    /// member id, sorted.
    pub fn ids(&self) -> Vec<String> {
        self.registry.ids()
    }

    /// Hand `payload` to `relay`'s entry channel `channel` in this process,
    /// through the dispatch a stream reader makes, with no frame to the
    /// controller. The input carries no offset, so no input log holds it.
    /// Blocks while the consumer's mailbox is full, as a reader does.
    pub fn send(&self, relay: &str, channel: &str, payload: Vec<u8>) -> Result<(), Error> {
        let not_local = || Error::Routing(Code::RelayNotLocal.with([("relay", relay.into())]));
        let instance = self.registry.get(relay).ok_or_else(not_local)?;
        crate::local::declared_channel(&instance, channel, ChannelKind::Entry)?;
        let stream = self.stream_of(relay).ok_or_else(not_local)?;
        let metadata = HashMap::from([("channel".to_string(), channel.to_string())]);
        self.dispatch_handle()
            .dispatch(relay, payload, &metadata, stream);
        Ok(())
    }

    /// The stream readers' handle onto these relays, for harnesses.
    #[doc(hidden)]
    pub fn dispatch_handle(&self) -> RelayDispatchHandle {
        RelayDispatchHandle {
            registry: Arc::clone(&self.registry),
        }
    }

    /// The secondary of the pair whose index the controller picks for
    /// `relay` (`Flezha.Relay.Lifecycle.attach_channel/2`: the sum of the
    /// id's code points modulo the pair count), so a local input's outputs
    /// usually share a stream with the relay's other outputs. The controller
    /// numbers pairs in arrival order, which matches the runner's keys when they
    /// connect in turn; when they do not, only output order across the two
    /// paths is affected.
    fn stream_of(&self, relay: &str) -> Option<Arc<FlezhaStream>> {
        let streams = self.io_streams.read().unwrap_or_else(|p| p.into_inner());
        let count = u64::try_from(streams.len()).ok().filter(|n| *n > 0)?;
        let sum: u64 = relay.chars().map(u64::from).sum();
        let key = u32::try_from(sum % count).ok()?;
        streams.get(&key).map(Arc::clone)
    }
}

impl RunnerNode {
    pub fn new(namespace: String) -> RunnerNode {
        let relay_registry = Arc::new(RelayRegistry::new());
        let shared_io_streams: IoStreams = Arc::new(RwLock::new(HashMap::new()));
        *LIVE.lock().unwrap_or_else(|p| p.into_inner()) = Some(BoundRelays {
            registry: Arc::clone(&relay_registry),
            io_streams: Arc::clone(&shared_io_streams),
        });

        RunnerNode {
            runner_id: "undefined".to_string(),
            _namespace: namespace,
            relay_registry,
            shared_io_streams,
            quic_client: None,
            connect_answered: ConnectAnswered::default(),
            status: RunnerStatus::Starting,
        }
    }

    /// Spawn an instance of a registered class of either kind; `instance_id`
    /// is the wire's `relay_id`. Idempotent: a duplicate id answers the
    /// existing instance. The instance takes the input tail the bind owes it;
    /// a steps relay also takes the rest of the resume facts and waits for
    /// [`crate::steps::open`] before its first pass, so its `init_values`
    /// land first.
    pub fn start_relay(
        &self,
        factory_id: String,
        instance_id: String,
        partition: crate::relay::instance::KeyGroupRange,
        resume: crate::steps::instance::Resume,
    ) -> Option<Arc<RelayInstance>> {
        if let Some(existing) = self.relay_registry.get(&instance_id) {
            return Some(existing);
        }
        let topology = get_relay_class(&factory_id)?;
        let instance =
            RelayInstance::with_options(topology, Arc::from(instance_id.as_str()), None, partition);
        instance.owe_inbox(resume.owed_inbox);
        if let Some(run) = &instance.steps {
            run.bind(resume);
        }
        Some(self.relay_registry.register(instance))
    }

    /// Unregister a class with no live instance; `Err(n)` names how many
    /// exist. MUST run on the coordinator loop, serialized with `AddRelay`.
    pub fn remove_factory(&self, factory_id: &str) -> Result<(), usize> {
        let live = self.relay_registry.count_by_factory(factory_id);
        if live > 0 {
            return Err(live);
        }
        crate::relay::registry::unregister_class(factory_id);
        Ok(())
    }

    pub fn stop_relay(&self, instance_id: &str) -> bool {
        match self.relay_registry.remove(instance_id) {
            Some(instance) => {
                instance.shutdown_background_threads();
                crate::steps::removed(&instance);
                crate::effect_result::forget(instance_id);
                crate::host::relay_left(&instance);
                true
            }
            None => false,
        }
    }

    pub fn has_relay(&self, instance_id: &str) -> bool {
        self.relay_registry.contains(instance_id)
    }

    pub fn get_relay(&self, instance_id: &str) -> Option<Arc<RelayInstance>> {
        self.relay_registry.get(instance_id)
    }

    /// The Cell a wire identity names, with its instance; a keyed one is
    /// minted on first sight.
    pub fn find_cell(
        &self,
        cell_id: &[u8],
    ) -> Option<(Arc<RelayInstance>, Arc<crate::cell::CellSlot>)> {
        self.relay_registry.find_cell(cell_id)
    }

    /// The live instance a wire identity names.
    pub fn instance_of_cell(&self, cell_id: &[u8]) -> Option<Arc<RelayInstance>> {
        self.relay_registry.instance_of_cell(cell_id)
    }

    pub fn relay_dispatch_handle(&self) -> RelayDispatchHandle {
        RelayDispatchHandle {
            registry: Arc::clone(&self.relay_registry),
        }
    }

    pub fn _get_namespace(&self) -> String {
        self._namespace.clone()
    }

    pub fn get_io_streams(&self) -> IoStreams {
        Arc::clone(&self.shared_io_streams)
    }

    /// This runner's bound relays, as application code reaches them.
    pub fn bound(&self) -> BoundRelays {
        BoundRelays {
            registry: Arc::clone(&self.relay_registry),
            io_streams: Arc::clone(&self.shared_io_streams),
        }
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
        // None: the host stopped waiting before any controller answered.
        let Some((mut coordinator, _timers)) =
            Coordinator::connect(namespace, declared, otx, ctx, shutdown_bridge)
        else {
            return;
        };
        // A closed channel means every sender is gone — the owning
        // FlezhaRunner dropped. Exit cleanly so a later runner can start.
        while let Ok(message) = orx.recv() {
            let abandoned = matches!(message, RunnerMessage::Abandon);
            coordinator.handle_message(message);
            if abandoned {
                return;
            }
        }
    })
}

/// Let the shutdown bridge enumerate live instances without the coordinator
/// channel; the registry `Arc` is stable across reconnects.
fn install_snapshot_fn(shutdown_bridge: &ShutdownBridge, registry: Arc<RelayRegistry>) {
    shutdown_bridge.set_snapshot_fn(Box::new(move || {
        registry
            .snapshot_stream()
            .into_iter()
            .map(|(k, instance)| {
                (
                    k,
                    crate::command::cell_state::serialize_relay_state(&instance),
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
    ) -> Option<(Self, [JoinHandle<()>; 2])> {
        let mut runner = RunnerNode::new(namespace.clone());
        install_snapshot_fn(&shutdown_bridge, Arc::clone(&runner.relay_registry));

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
        )?;
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
        Some((coordinator, timers))
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
            RunnerMessage::Reconnect(reason) => self.reconnect(reason),
            RunnerMessage::ControlLost { stream, reason } => {
                if Arc::ptr_eq(&stream, &self.control_stream) {
                    self.reconnect(reason);
                }
            }
            // A failed keepalive write means the control stream died; the
            // reader loop detects the same and drives reconnect.
            RunnerMessage::SendControlKeepalive => {
                let _ = control_unit::send_keepalive_ping(&self.control_stream);
            }
            RunnerMessage::SendDataHeartbeat => {
                heartbeat_tick(&self.runner.shared_io_streams);
            }
            RunnerMessage::EmitRelayCompleted {
                relay_id,
                status,
                error,
                result,
            } => self.write_relay_completed(relay_id, status, error, result),
            RunnerMessage::EmitControlRequest {
                reference_id,
                command,
            } => self.write_control_request(reference_id, *command),
            RunnerMessage::RemoveFactory { factory_id } => self.remove_factory(&factory_id),
            RunnerMessage::Abandon => self.abandon(),
        }
    }

    fn remove_factory(&self, factory_id: &str) {
        let result = self.runner.remove_factory(factory_id);
        let _ = self
            .ctx_arc
            .lock()
            .unwrap()
            .send(ClientResponseMessage::RemoveFactoryResult(result));
    }

    /// Stop for good: every later reconnect is a no-op, and the session this
    /// coordinator holds is closed.
    fn abandon(&mut self) {
        self.runner.set_status(RunnerStatus::Stopped);
        let _ = self.control_stream.shutdown(std::net::Shutdown::Both);
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

    /// The terminal `RelayCompleted`, carrying the direct-output count.
    fn write_relay_completed(
        &mut self,
        relay_id: String,
        status: String,
        error: String,
        result: Vec<u8>,
    ) {
        let frame = control_command_frame(
            "steps-completed",
            Command::RelayCompleted(PbRelayCompleted {
                relay_id: relay_id.clone(),
                status,
                error,
                result,
            }),
        );
        if let Err(e) = self.write_control(frame) {
            warn!(
                target: "flezha",
                "[flezha] RelayCompleted write failed for {}: {}", relay_id, e
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
/// ladder, the only signal an idle Relay ever gives it.
fn heartbeat_tick(io_streams: &RwLock<HashMap<u32, Arc<FlezhaStream>>>) {
    if !send_data_heartbeat(io_streams) {
        crate::state_sink::note_transport_failure();
    }
}

/// A body-less heartbeat on every secondary stream; whether it reached one.
/// Reaching none means the controller will flag `output_stall`, so the
/// caller drives its own recovery rather than wait for a `ConnectSocket`.
fn send_data_heartbeat(io_streams: &RwLock<HashMap<u32, Arc<FlezhaStream>>>) -> bool {
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
        // counters are the shed a user would otherwise only find by
        // attaching a debugger to the runner.
        ("blocked_parks", crate::cell::pushes_blocked_total()),
        ("drops_newest", crate::cell::drops_newest_total()),
        ("drops_oldest", crate::cell::drops_oldest_total()),
        ("credit_parked", crate::credit::credit_parked()),
        // A discard is correct only where the controller re-forwards from a
        // durable log, so a climbing value is the user's one handle on how
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
        // climbs is promotion wedged rather than the Relay idle.
        ("timers_armed", crate::timer::timers_armed() as u64),
        ("timers_fired", crate::timer::timers_fired_total()),
    ]
    .into_iter()
    // Parts sent and dropped by reason: a lossy preview is a design choice,
    // and these are what make its loss visible.
    .chain(crate::part::counters())
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
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

    let Some((answered, new_stream, new_quic)) =
        control_unit::open_reconnect(namespace.to_string(), Arc::clone(declared), otx_arc.clone())
    else {
        return;
    };

    // Closed, not just dropped: its reader holds the socket open, and a
    // controller that keeps a superseded session lingers on it.
    let retired = std::mem::replace(control_stream, new_stream);
    let _ = retired.shutdown(std::net::Shutdown::Both);
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
