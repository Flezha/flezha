//! Addressing, off the lock: values to the Cells they feed, and where a
//! pass's first items come from.

use std::sync::Arc;

use flz_transport::transport::FlezhaStream;

use super::instance::{Carry, Firing, Message, MidResume, PassOrigin, Phase, Release, Value};
use super::resume::{resumed_seed, seed_from_journal, Seed};
use super::run::Run;
use super::spec::Chain;
use super::{Each, StepsError};
use crate::cell::{make_cell_payload, CellItem, CellSlot};
use crate::channel::{ChannelDescriptor, ChannelId, ChannelKind, ConsumerEdge};
use crate::host::{Crossed, HostPayload};
use crate::relay::topology::CellMode;
use crate::routing::{Destination, Entering, Routed};

// ---- addressing, off the lock ------------------------------------------------

impl Run<'_> {
    /// Each value through its edge of `channel`: the transform and
    /// `cells_for` run here, and each item is addressed to its Cell.
    pub(super) fn address<'e>(
        &self,
        channel: &ChannelDescriptor,
        routed: impl IntoIterator<Item = Routed<'e>>,
        stream: &Arc<FlezhaStream>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Result<Vec<Release>, String> {
        let mut items = Vec::new();
        let mut keyless = Vec::new();
        let d = &crate::routing::Delivery {
            stream,
            consumed_offset: None,
            call: None,
            replay: false,
        };
        for (edge, value) in routed {
            if self.asks_host_for_keys(channel, edge, value) {
                if let Some(node) = self.spec.dag.node_of(edge.tasklet) {
                    keyless.push(Release {
                        node,
                        cell: None,
                        item: steps_item(value, stream),
                        producer: producer.map(Arc::clone),
                    });
                }
                continue;
            }
            let to = &mut Destination::Held(&mut items);
            crate::routing::deliver_edge(self.inst, channel, edge, value, d, to)
                .map_err(|e| e.to_string())?;
        }
        let mut releases = self.releases(items, producer);
        releases.extend(keyless);
        Ok(releases)
    }

    /// Whether `value` needs its host to key it for `edge`: a host releases a
    /// steps relay value untransformed and unkeyed, and a keyed consumer has no
    /// Cell until the edge's transforms and key have run. An entry value
    /// crossed on its way in.
    fn asks_host_for_keys(
        &self,
        channel: &ChannelDescriptor,
        edge: &ConsumerEdge,
        value: &Value,
    ) -> bool {
        channel.kind != ChannelKind::Entry
            && edge.key.is_none()
            && self
                .inst
                .topology
                .tasklet(edge.tasklet)
                .is_some_and(|t| t.mode == CellMode::Dynamic)
            && value
                .downcast_ref::<HostPayload>()
                .is_some_and(|p| p.keys.is_empty())
    }

    /// An answered host value through the edge into `node`'s tasklet, to the
    /// Cells its answer named; none when the host named none.
    pub(super) fn address_crossed(
        &self,
        node: usize,
        channel: ChannelId,
        crossed: &Crossed,
        stream: &Arc<FlezhaStream>,
    ) -> Result<Vec<Release>, String> {
        let tasklet = self.spec.dag.order[node];
        let Some(channel) = self.inst.topology.channel(channel) else {
            return Ok(Vec::new());
        };
        let routed = crossed
            .each(channel)
            .filter(|(edge, _)| edge.tasklet == tasklet);
        self.address(channel, routed, stream, None)
    }

    /// A chained loop's leaf value addressed to the roots of the chain the
    /// loop runs, as the next pass's input; nothing for any other loop.
    pub(super) fn carry_of(
        &self,
        value: &Value,
        stream: &Arc<FlezhaStream>,
        producer: &Arc<CellSlot>,
    ) -> Vec<Release> {
        if self.spec.each != Each::Chained {
            return Vec::new();
        }
        let Some(channel) = self.run.sheet().pass.as_ref().map(|pass| pass.channel) else {
            return Vec::new();
        };
        self.to_roots(channel, std::slice::from_ref(value), stream, Some(producer))
    }

    /// Values into every root of `channel`'s chain, with no edge between.
    fn to_roots(
        &self,
        channel: ChannelId,
        values: &[Value],
        stream: &Arc<FlezhaStream>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Vec<Release> {
        let Some(chain) = self.chain(channel) else {
            return Vec::new();
        };
        let mut items = Vec::new();
        for &root in &chain.roots {
            for value in values {
                let tasklet = self.spec.dag.order[root];
                for cell in crate::routing::declared_cells_for(self.inst, tasklet, value) {
                    items.push((cell, steps_item(value, stream)));
                }
            }
        }
        self.releases(items, producer)
    }

    fn releases(
        &self,
        items: Vec<(Arc<CellSlot>, CellItem)>,
        producer: Option<&Arc<CellSlot>>,
    ) -> Vec<Release> {
        items
            .into_iter()
            .filter_map(|(cell, item)| {
                Some(Release {
                    node: self.spec.dag.node_of(cell.tasklet_id)?,
                    cell: Some(cell),
                    item,
                    producer: producer.map(Arc::clone),
                })
            })
            .collect()
    }

    /// Seed a pass off the lock: the items it starts with, and for a pass the
    /// crash interrupted, the journal's committed producers re-seeded into
    /// their children.
    pub(super) fn begin_pass(&self, source: Source) {
        if let Source::Firing(firing) = source {
            // A firing that waited may have been served or cancelled since.
            // A Cell in the middle of an invocation holds its set: that answer
            // waits for the assembly, which drops a firing no longer pending.
            let pending = firing.slot.timers.try_lock().map_or(true, |pending| {
                pending.contains(firing.fire.kind, firing.fire.scheduled_at)
            });
            return self.decide(|d| d.start_firing(firing, pending));
        }
        let mid = {
            let mut sheet = self.run.sheet();
            if sheet.phase != Phase::Starting {
                return;
            }
            sheet.resume_mid.take()
        };
        let seeded = self.seed(source, mid.as_deref());
        self.decide(|d| match seeded {
            Ok(Some((origin, message, seed))) => d.start_pass(origin, message, seed),
            Ok(None) => d.finish_clean(),
            Err(error) => d.compensate_on(error),
        });
    }

    #[allow(clippy::type_complexity)]
    fn seed(
        &self,
        source: Source,
        mid: Option<&MidResume>,
    ) -> Result<Option<(PassOrigin, Message, Seed)>, StepsError> {
        let message = |channel| PassOrigin::Message { channel };
        // Only a message's pass carries its offset and its call.
        let without_input = |channel, roots| Message {
            channel,
            offset: None,
            call: None,
            roots,
        };
        let (origin, mut pass) = match source {
            Source::Message(m) => (message(m.channel), m),
            Source::Carry(carry) => (
                message(carry.channel),
                without_input(carry.channel, carry.items),
            ),
            Source::Firing(_) => return Ok(None),
            Source::Resumed { .. } if mid.is_none() => return Ok(None),
            Source::Resumed { origin, channel } => (origin, without_input(channel, Vec::new())),
            Source::Journal { channel, handoff } => {
                let values = seed_from_journal(&self.inst.topology, &handoff, None)?;
                if values.is_empty() && mid.is_none() {
                    return Ok(None);
                }
                let stream = unwired_stream();
                let roots = self.to_roots(channel, &values, &stream, None);
                (message(channel), without_input(channel, roots))
            }
        };
        let seed = match mid {
            Some(mid) => {
                pass.roots
                    .retain(|r| !mid.committed_nodes.contains(&r.node));
                resumed_seed(self, mid, self.resumed_chain(&origin)?)?
            }
            None => Seed::default(),
        };
        Ok(Some((origin, pass, seed)))
    }

    /// The chain an interrupted pass of `origin` resumes over.
    fn resumed_chain(&self, origin: &PassOrigin) -> Result<&Chain, StepsError> {
        self.origin_chain(origin).ok_or_else(|| {
            StepsError::non_retryable(match origin {
                PassOrigin::Message { channel } => format!(
                    "channel {channel} is not an entry channel of steps {}",
                    self.inst.instance_id
                ),
                PassOrigin::Timer { .. } => format!(
                    "the journaled timer pass of steps {} names no tasklet of its chain",
                    self.inst.instance_id
                ),
            })
        })
    }
}

/// A steps relay Cell's input: no input-log offset (the pass records it) and
/// never a replay member.
pub(crate) fn steps_item(value: &Value, stream: &Arc<FlezhaStream>) -> CellItem {
    CellItem::Input(make_cell_payload(value, stream, None, false))
}

/// The connection a steps relay value that arrived on none rides — a journal
/// re-seed, an undo: the runner's secondary stream, or a detached one before
/// any is connected.
pub fn unwired_stream() -> Arc<FlezhaStream> {
    crate::state_sink::secondary_stream().unwrap_or_else(|| {
        static DETACHED: std::sync::OnceLock<Arc<FlezhaStream>> = std::sync::OnceLock::new();
        Arc::clone(
            DETACHED.get_or_init(|| Arc::new(FlezhaStream::Local(Arc::new(|_: &[u8]| Ok(()))))),
        )
    })
}

/// Where a pass's first items come from.
pub(super) enum Source {
    Message(Message),
    Carry(Carry),
    Firing(Firing),
    /// A timer-fed pass the crash interrupted after its root committed.
    Resumed {
        origin: PassOrigin,
        channel: ChannelId,
    },
    /// The journal's terminal handoff into the roots of `channel`'s chain.
    Journal {
        channel: ChannelId,
        handoff: Vec<(u32, Vec<u8>)>,
    },
}
