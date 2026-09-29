//! Running a declared operator graph in this process, with no controller.
//!
//! A [`LocalRun`] adds no engine. It builds the `OperatorInstance` a deployed
//! runner builds, feeds it through the [`OperatorDispatchHandle::dispatch`]
//! a stream reader uses, and takes sink output where production frames it —
//! a [`FlezhaStream::Local`] whose writes are parsed in process instead of
//! reaching a socket. Cell lifecycle, key resolution, routing, mailboxes,
//! overflow and the retry ladder are the deployed ones, untouched.
//!
//! What a local run has not got: durability of any kind (a null sink discards
//! every record at the outbound edge, so the record path still runs and still
//! costs what it costs), plugins, placement and migration, the dead-letter
//! queue, observability export, authz and multi-runner scale. Every one of
//! those needs a controller.
//!
//! Timers it has got, and through the core's own path rather than one of its
//! own: a committed `TimerOp` arms the poll core's wake list exactly as it
//! does under a controller, and an event-time deadline fires off the
//! high-water the dispatch path advances. The one thing a local run owes them
//! is patience: [`LocalRun::run_until_idle`] counts an armed timer as work,
//! or it would call a graph settled in the gap between a registration and its
//! firing and the callback would never run.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::channel::ChannelKind;
use crate::error::{Code, Error};
use crate::operator::instance::OperatorInstance;
use crate::operator::registry::OperatorRegistry;
use crate::operator::topology::ChannelLookup;
use crate::poll_core::PollCore;
use crate::routing::OperatorDispatchHandle;
use crate::util::LockRecover;

/// What every local run states before it does any work. Emitted by the core,
/// so a host SDK cannot ship a local mode that looks durable.
pub const NON_DURABILITY_NOTICE: &str = "[flezha] LOCAL MODE: state is held in memory and NOTHING is durable — \
no write-ahead log, no snapshots, no dead-letter queue. Everything this run computes is gone when the process \
exits. Durability, plugins, placement and migration, the dead-letter queue, observability export, authz and \
multi-runner scale all need a controller.";

static NOTICES_EMITTED: AtomicU64 = AtomicU64::new(0);
static UNHANDLED_SINK_OUTPUTS: AtomicU64 = AtomicU64::new(0);

/// Non-durability notices emitted in this process; one per started run.
pub fn notices_emitted() -> u64 {
    NOTICES_EMITTED.load(Ordering::Relaxed)
}

/// Values released on a sink no callback claimed. Each is reported as it
/// happens; the count is what a run can assert on afterwards.
pub fn unhandled_sink_outputs() -> u64 {
    UNHANDLED_SINK_OUTPUTS.load(Ordering::Relaxed)
}

/// One value a graph released on a plugin-bound sink, carrying the identity
/// and metadata a deployed run would have put on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkOutput {
    pub channel: String,
    pub payload: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

/// What a caller registers for one declared sink name.
pub type SinkCallback = Arc<dyn Fn(SinkOutput) + Send + Sync>;

/// The registered callbacks, and the parse a written frame goes through to
/// reach one.
#[derive(Default)]
struct SinkTable {
    by_channel: RwLock<HashMap<String, SinkCallback>>,
}

impl SinkTable {
    fn register(&self, channel: &str, callback: SinkCallback) {
        self.by_channel
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(channel.to_string(), callback);
    }

    /// One framed plugin write, decoded exactly as the controller decodes it
    /// and handed to the callback the sink's declared name registered.
    fn deliver(&self, frame: &[u8]) -> std::io::Result<()> {
        let (payload_id, payload, metadata) = payloader::extract_payload(frame)?;
        let channel = metadata
            .get("channel")
            .cloned()
            .unwrap_or_else(|| payload_id.clone());
        let callback = self
            .by_channel
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&channel)
            .map(Arc::clone);
        match callback {
            Some(callback) => callback(SinkOutput {
                channel,
                payload,
                metadata,
            }),
            None => {
                UNHANDLED_SINK_OUTPUTS.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[flezha] local mode: {} bytes were released on sink `{channel}`, \
                     which has no registered callback",
                    payload.len()
                );
            }
        }
        Ok(())
    }
}

/// One operator graph running in this process.
pub struct LocalRun {
    instance: Arc<OperatorInstance>,
    dispatch: OperatorDispatchHandle,
    stream: Arc<FlezhaStream>,
    sinks: Arc<SinkTable>,
}

impl LocalRun {
    /// Start the class already registered under `factory_id`. Refuses before
    /// building anything when the declaration asks for something only a
    /// controller can answer.
    pub fn start(factory_id: &str, operator_id: &str) -> Result<Self, Error> {
        refuse_controller_only(factory_id)?;
        let instance = OperatorInstance::of_class(factory_id, operator_id)?;
        let sinks = Arc::new(SinkTable::default());
        let frames = Arc::clone(&sinks);
        let stream = Arc::new(FlezhaStream::Local(Arc::new(move |frame: &[u8]| {
            frames.deliver(frame)
        })));
        // Durability is removed by supplying a different sink, never by
        // branching: the change log, the fold and the effect path all still
        // run, and their frames are discarded at the outbound edge.
        crate::state_sink::install_global_sink(Arc::new(|_payload_id: &str, _frame: &[u8]| Ok(())));
        let registry = Arc::new(OperatorRegistry::new());
        registry.register(Arc::clone(&instance));
        NOTICES_EMITTED.fetch_add(1, Ordering::Relaxed);
        eprintln!("{NON_DURABILITY_NOTICE}");
        Ok(LocalRun {
            instance,
            dispatch: OperatorDispatchHandle { registry },
            stream,
            sinks,
        })
    }

    /// Deliver every value released on the sink `channel` to `callback`, in
    /// release order, on the thread that released it.
    pub fn on_sink(&self, channel: &str, callback: SinkCallback) -> Result<(), Error> {
        self.channel_of_kind(channel, ChannelKind::PluginBound)?;
        self.sinks.register(channel, callback);
        Ok(())
    }

    /// Feed one value to a declared entry channel, entering exactly where a
    /// frame off a socket enters. Blocks while the consumer's mailbox is
    /// full: the entry path's own backpressure, unmodified.
    pub fn feed(&self, channel: &str, payload: Vec<u8>) -> Result<(), Error> {
        self.channel_of_kind(channel, ChannelKind::Entry)?;
        let metadata = HashMap::from([("channel".to_string(), channel.to_string())]);
        self.dispatch.dispatch(
            &self.instance.instance_id,
            payload,
            &metadata,
            Arc::clone(&self.stream),
        );
        Ok(())
    }

    /// Block until every Cell has drained and nothing is in flight, or
    /// `budget` elapses — which is reported, never passed off as done.
    pub fn run_until_idle(&self, budget: Duration) -> Result<(), Error> {
        let deadline = Instant::now() + budget;
        let core = crate::poll_core::global();
        // Two consecutive idle observations, because a Cell between its
        // pop and its next push is momentarily idle by every counter.
        let mut idle_seen = 0;
        while Instant::now() < deadline {
            idle_seen = if self.working(core) { 0 } else { idle_seen + 1 };
            if idle_seen == 2 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(Error::Routing(Code::LocalRunBusy.with([
            ("operator", self.instance.instance_id.as_ref().into()),
            ("budget_ms", (budget.as_millis() as u64).into()),
        ])))
    }

    /// The live instance, for a caller that wants to read its Cells.
    pub fn instance(&self) -> &Arc<OperatorInstance> {
        &self.instance
    }

    /// An armed processing-time timer counts as work: it is due to become a
    /// firing on the wake list the core already holds, and a run that called
    /// itself settled before it fired would drop it. So does an entry payload
    /// still crossing to the host for its key or time (a direct activation,
    /// which the outstanding count does not include) and a payload on a retry
    /// ladder: until either is done the payload is in no mailbox.
    fn working(&self, core: &PollCore) -> bool {
        core.outstanding_count() > 0
            || core.pending_push_count() > 0
            || core.timers_armed() > 0
            || self.instance.ladders_running()
            || self.instance.host_prepare.lock_recover().in_flight()
            || self
                .instance
                .all_slots()
                .iter()
                .any(|slot| !slot.mailbox.is_empty() || slot.mailbox.is_inflight())
    }

    /// The declared channel of that kind; the refusal names the channel and
    /// the operator, because a name nothing matches is otherwise a value
    /// that vanishes. The lookup is the topology's own, shared with the
    /// dispatch path so a feed and a frame off the wire cannot disagree about
    /// what a name means.
    fn channel_of_kind(&self, channel: &str, kind: ChannelKind) -> Result<(), Error> {
        let topology = &self.instance.topology;
        match topology.channel_of_kind(channel, kind) {
            ChannelLookup::Found(..) => Ok(()),
            ChannelLookup::WrongKind(declared) => {
                Err(Error::Routing(Code::ChannelWrongKind.with([
                    ("operator", self.instance.instance_id.as_ref().into()),
                    ("channel", channel.into()),
                    ("declared", declared.as_str().into()),
                    ("kind", kind.as_str().into()),
                ])))
            }
            ChannelLookup::Undeclared => Err(Error::Routing(Code::ChannelUndeclared.with([
                ("operator", self.instance.instance_id.as_ref().into()),
                ("channel", channel.into()),
                ("kind", kind.as_str().into()),
                ("channels", topology.declared_names(kind).into()),
            ]))),
        }
    }
}

impl Drop for LocalRun {
    fn drop(&mut self) {
        self.instance.shutdown_background_threads();
    }
}

/// What a declaration can ask for that only a controller answers. Each is
/// refused by name rather than run as a graph whose work would go nowhere.
/// Partitioning is absent from this list because it is absent from the
/// declaration: a member's key-group range arrives with `AddOperator`, so a
/// local run is unpartitioned by construction.
fn refuse_controller_only(factory_id: &str) -> Result<(), Error> {
    let class = crate::operator::registry::get_class(factory_id);
    let bounded = class
        .as_ref()
        .is_some_and(|topology| topology.bounded.is_some());
    let tier = class.is_some_and(|topology| topology.effectively_once);
    // A bounded operator's messages come from the controller's input log and
    // its commit barriers are durable; under the tier an output is released
    // once its effect record is durable. Local mode has neither.
    let capability = if bounded {
        "a bounded operator"
    } else if tier {
        "effectively_once"
    } else {
        // The registry does not have it, or it asks for nothing a controller owns;
        // `of_class` reports an unregistered class by name.
        return Ok(());
    };
    Err(Error::Declaration(Code::LocalNeedsController.with([
        ("factory", factory_id.into()),
        ("capability", capability.into()),
    ])))
}
