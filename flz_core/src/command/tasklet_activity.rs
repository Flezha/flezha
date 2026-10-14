//! `TaskletActivity`: one row per tasklet of an instance, summed from the
//! counters its Cells' mailboxes already keep, and on request one tasklet's
//! most recently admitting Cells. The invariant: the read takes no Cell lock
//! and writes nothing the data path reads, so it never blocks a dispatch. An
//! eviction during the walk can count one Cell twice or miss it; these are
//! monitoring totals, and the next read is right again.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prost::Message;

use super::cell_state::relay_not_found;
use super::{OffReaderAction, OffReaderBuild};
use crate::cell::{CellCounters, CellSlot};
use crate::channel::TaskletId;
use crate::dynamic::DynamicCellCounters;
use crate::error::{Code, Error};
use crate::pb::{
    CellActivityRow, TaskletActivity, TaskletActivityResponse, TaskletActivityRow, TaskletCells,
};
use crate::relay::instance::RelayInstance;
use crate::runner::RunnerNode;

const DEFAULT_CELL_ROWS: usize = 50;
const MAX_CELL_ROWS: usize = 500;

impl OffReaderAction for TaskletActivity {
    /// The walk is O(live Cells), about 30 ms at 100,000, so it runs off the
    /// control reader. The tasklet `cells_of` names is resolved here, so an
    /// unknown one is refused without a walk.
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, Error> {
        let instance = runner
            .get_relay(&self.relay_id)
            .ok_or_else(|| relay_not_found(&self.relay_id))?;
        let cells_of = match self.cells_of.as_deref() {
            Some(name) => Some(cells_of(&instance, &self.relay_id, name, self.limit)?),
            None => None,
        };
        Ok(Box::new(move || {
            activity(&instance, cells_of).encode_to_vec()
        }))
    }
}

/// The tasklet a Cell read names and how many of its Cells to answer.
#[derive(Clone, Copy)]
struct CellsOf {
    tasklet_id: TaskletId,
    limit: usize,
}

fn cells_of(
    instance: &RelayInstance,
    relay_id: &str,
    name: &str,
    limit: Option<u32>,
) -> Result<CellsOf, Error> {
    let tasklet_id = instance
        .topology
        .tasklet_name_to_id
        .get(name)
        .copied()
        .ok_or_else(|| {
            Error::State(
                Code::TaskletNotFound.with([("relay", relay_id.into()), ("tasklet", name.into())]),
            )
        })?;
    let limit = limit.map_or(DEFAULT_CELL_ROWS, |limit| limit as usize);
    Ok(CellsOf {
        tasklet_id,
        limit: limit.min(MAX_CELL_ROWS),
    })
}

fn activity(instance: &RelayInstance, cells_of: Option<CellsOf>) -> TaskletActivityResponse {
    let mut recent = cells_of.map(Recent::new);
    let rows: Vec<_> = (0..instance.topology.tasklets.len() as TaskletId)
        .map(|tasklet_id| {
            let picked = recent.as_mut().filter(|r| r.of.tasklet_id == tasklet_id);
            row(instance, tasklet_id, picked)
        })
        .collect();
    let read_at_ms = crate::util::monotonic_ms() as u64;
    TaskletActivityResponse {
        cells: recent.map(|recent| {
            let of = &rows[recent.of.tasklet_id as usize];
            recent.answer(of.tasklet.clone(), of.cells)
        }),
        rows,
        read_at_ms,
        instance_seq: instance.seq,
    }
}

/// A tasklet's live Cells plus, for a dynamic one, its evicted Cells' totals.
/// `queued` is `Mailbox::len()`, which leaves out items held behind locks.
/// `recent`, when given, is offered every Cell the walk visits.
fn row(
    instance: &RelayInstance,
    tasklet_id: TaskletId,
    mut recent: Option<&mut Recent>,
) -> TaskletActivityRow {
    let sum = CellCounters::default();
    let (mut cells, mut queued, mut queued_max) = (0u64, 0u64, 0u64);
    let registry = instance.dynamic_registry(tasklet_id);
    if let Some(registry) = registry {
        sum.add(&registry.retired);
    }
    instance.for_each_slot(tasklet_id, |slot| {
        let len = slot.mailbox.len() as u64;
        cells += 1;
        queued += len;
        queued_max = queued_max.max(len);
        sum.add(slot.counters());
        if let Some(recent) = recent.as_mut() {
            recent.offer(slot);
        }
    });

    let total = |counter: &AtomicU64| Some(counter.load(Ordering::Relaxed));
    let dynamic = |pick: fn(&DynamicCellCounters) -> &AtomicU64| {
        registry.map(|registry| pick(&registry.counters).load(Ordering::Relaxed))
    };
    TaskletActivityRow {
        tasklet: instance.topology.tasklets[tasklet_id as usize]
            .name
            .to_string(),
        cells,
        queued: Some(queued),
        queued_max: Some(queued_max),
        admitted_total: total(&sum.pushes_total),
        blocked_total: total(&sum.pushes_blocked_total),
        dropped_newest_total: total(&sum.drops_newest_total),
        dropped_oldest_total: total(&sum.drops_oldest_total),
        dropped_capped_total: dynamic(|c| &c.inputs_dropped_capped_total),
        cells_created_total: dynamic(|c| &c.cells_created_total),
        cells_evicted_total: dynamic(|c| &c.cells_evicted_total),
    }
}

/// The `limit` most recently admitting Cells seen so far: a min-heap of
/// `(admission stamp, index into slots)`, so a Cell costs one load and, only
/// when it displaces the oldest kept, one reference count and `log limit`
/// swaps. The stamp is read once, so the order answered is the order kept.
struct Recent {
    of: CellsOf,
    heap: BinaryHeap<Reverse<(i64, usize)>>,
    slots: Vec<Arc<CellSlot>>,
}

impl Recent {
    fn new(of: CellsOf) -> Self {
        Self {
            of,
            heap: BinaryHeap::with_capacity(of.limit),
            slots: Vec::with_capacity(of.limit),
        }
    }

    fn offer(&mut self, slot: &Arc<CellSlot>) {
        let last_admitted_ms = slot.last_activity_ms.load(Ordering::Relaxed);
        if self.slots.len() < self.of.limit {
            self.heap
                .push(Reverse((last_admitted_ms, self.slots.len())));
            self.slots.push(Arc::clone(slot));
        } else if let Some(&Reverse((oldest, index))) = self.heap.peek() {
            if last_admitted_ms > oldest {
                self.heap.pop();
                self.heap.push(Reverse((last_admitted_ms, index)));
                self.slots[index] = Arc::clone(slot);
            }
        }
    }

    /// The kept Cells, newest first.
    fn answer(self, tasklet: String, cells_total: u64) -> TaskletCells {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let rows = self
            .heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse((last_admitted_ms, index))| {
                let slot = &self.slots[index];
                let counters = slot.counters();
                CellActivityRow {
                    key: slot.cell_key.as_deref().map(str::to_string),
                    queued: slot.mailbox.len() as u64,
                    admitted_total: load(&counters.pushes_total),
                    blocked_total: load(&counters.pushes_blocked_total),
                    dropped_newest_total: load(&counters.drops_newest_total),
                    dropped_oldest_total: load(&counters.drops_oldest_total),
                    last_admitted_ms: last_admitted_ms as u64,
                }
            })
            .collect();
        TaskletCells {
            tasklet,
            cells_total,
            rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::admission::{admit_from_reader, try_admit};
    use crate::cell::{CellItem, CellSlot};
    use crate::channel::OverflowPolicy;

    /// `ohlc`: one Cell; `trades`: dynamic, capped at two Cells; `book`: static,
    /// keys `a` to `d`; `symbols`: dynamic, room for the cap and timing tests.
    fn instance(id: &str) -> Arc<RelayInstance> {
        let id = class(id);
        RelayInstance::of_class(&id, &format!("{id}_inst")).expect("class registered")
    }

    fn class(id: &str) -> String {
        let id = format!("{id}_{}", std::process::id());
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
                 "channels":[{{"name":"in","kind":"entry",
                   "chains":[[{{"link":"into","tasklet":0}}],[{{"link":"key"}},{{"link":"into","tasklet":1}}],
                             [{{"link":"into","tasklet":2}}],[{{"link":"key"}},{{"link":"into","tasklet":3}}]]}}],
                 "tasklets":[{{"name":"ohlc","mailbox_capacity":4}},
                             {{"name":"trades","cell_mode":"dynamic","max_cells":2,"mailbox_capacity":16}},
                             {{"name":"book","cell_mode":"static","cell_keys":["a","b","c","d"],
                               "cells_for_overridden":true,"mailbox_capacity":64}},
                             {{"name":"symbols","cell_mode":"dynamic","max_cells":200000,"mailbox_capacity":4}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        id
    }

    /// Holding the drain gate keeps what is admitted queued, with no round
    /// scheduled to take it.
    fn admit(slot: &Arc<CellSlot>, n: usize) {
        let _ = slot.mailbox.claim_inflight();
        for _ in 0..n {
            assert!(try_admit(slot, CellItem::CloseInterval).is_ok(), "room");
        }
    }

    fn row_of(answer: &TaskletActivityResponse, tasklet: &str) -> TaskletActivityRow {
        let row = answer.rows.iter().find(|row| row.tasklet == tasklet);
        row.cloned().expect("a row per tasklet")
    }

    #[test]
    fn a_tasklet_reports_queued_admitted_blocked_and_shed() {
        let instance = instance("activity_single");
        let slot = Arc::clone(&instance.cells[0][0]);
        admit(&slot, 4);
        slot.note_blocked();
        slot.note_blocked();
        admit_from_reader(&slot, CellItem::CloseInterval, OverflowPolicy::DropNewest);
        admit_from_reader(&slot, CellItem::CloseInterval, OverflowPolicy::DropOldest);

        let answer = activity(&instance, None);
        assert_eq!(answer.instance_seq, instance.seq);
        assert_eq!(answer.rows.len(), 4, "one row per tasklet");
        assert_eq!(answer.cells, None, "no Cells asked, none answered");
        assert_eq!(
            row_of(&answer, "ohlc"),
            TaskletActivityRow {
                tasklet: "ohlc".into(),
                cells: 1,
                queued: Some(4),
                queued_max: Some(4),
                admitted_total: Some(5),
                blocked_total: Some(2),
                dropped_newest_total: Some(1),
                dropped_oldest_total: Some(1),
                dropped_capped_total: None,
                cells_created_total: None,
                cells_evicted_total: None,
            }
        );
    }

    #[test]
    fn a_dynamic_tasklet_keeps_its_totals_across_an_eviction() {
        let instance = instance("activity_dynamic");
        let registry = instance.dynamic_registries[1].as_ref().expect("dynamic");
        let a = registry.get_or_create("a").expect("first Cell");
        let b = registry.get_or_create("b").expect("second Cell");
        assert!(registry.get_or_create("c").is_err(), "past the cap");
        admit(&a, 10);
        admit(&b, 2);

        let before = row_of(&activity(&instance, None), "trades");
        assert_eq!(
            (before.cells, before.queued, before.queued_max),
            (2, Some(12), Some(10))
        );
        assert_eq!(before.admitted_total, Some(12));
        assert_eq!(before.dropped_capped_total, Some(1));
        assert_eq!(before.cells_created_total, Some(2));
        assert_eq!(before.cells_evicted_total, Some(0));

        a.mailbox.clear();
        a.mailbox.release_inflight();
        a.last_activity_ms
            .store(crate::util::monotonic_ms() - 60_000, Ordering::Relaxed);
        registry.reap_sweep(Duration::from_millis(1));
        drop(a);

        let after = row_of(&activity(&instance, None), "trades");
        assert_eq!((after.cells, after.queued), (1, Some(2)), "`a` was evicted");
        assert_eq!(after.cells_evicted_total, Some(1));
        assert_eq!(
            after.admitted_total, before.admitted_total,
            "an eviction keeps the evicted Cell's totals"
        );
    }

    #[test]
    fn a_new_instance_answers_with_a_new_seq() {
        let first = activity(&instance("activity_seq_a"), None).instance_seq;
        let second = activity(&instance("activity_seq_b"), None).instance_seq;
        assert_ne!(first, second);
    }

    /// The Cells of `tasklet` as one read answers them.
    fn cells(instance: &RelayInstance, tasklet: &str, limit: Option<u32>) -> TaskletCells {
        let of = cells_of(instance, "r", tasklet, limit).expect("a declared tasklet");
        activity(instance, Some(of)).cells.expect("Cells asked for")
    }

    fn stamp(slot: &CellSlot, last_admitted_ms: i64) {
        slot.last_activity_ms
            .store(last_admitted_ms, Ordering::Relaxed);
    }

    fn keys(cells: &TaskletCells) -> Vec<&str> {
        let keys = cells.rows.iter().map(|row| row.key.as_deref());
        keys.map(|key| key.expect("a keyed Cell")).collect()
    }

    #[test]
    fn a_static_tasklets_cells_are_answered_newest_first() {
        let instance = instance("cells_static");
        for (key, admitted, at) in [
            ("a", 3, 1_000),
            ("b", 0, 500),
            ("c", 5, 3_000),
            ("d", 1, 2_000),
        ] {
            let slots = instance.slots_of(2);
            let slot = slots.iter().find(|s| s.cell_key.as_deref() == Some(key));
            let slot = slot.expect("a declared key");
            admit(slot, admitted);
            stamp(slot, at);
        }

        let answer = cells(&instance, "book", None);
        assert_eq!((answer.tasklet.as_str(), answer.cells_total), ("book", 4));
        assert_eq!(
            keys(&answer),
            ["c", "d", "a", "b"],
            "most recent admission first"
        );
        let figures: Vec<_> = answer
            .rows
            .iter()
            .map(|row| (row.admitted_total, row.queued, row.last_admitted_ms))
            .collect();
        assert_eq!(
            figures,
            [(5, 5, 3_000), (1, 1, 2_000), (3, 3, 1_000), (0, 0, 500)]
        );
    }

    #[test]
    fn a_large_tasklet_answers_its_newest_cells_up_to_the_limit() {
        let instance = instance("cells_cap");
        let registry = instance.dynamic_registries[3].as_ref().expect("dynamic");
        for i in 0..1_000 {
            stamp(&registry.get_or_create(&format!("k{i}")).expect("room"), i);
        }

        let ten = cells(&instance, "symbols", Some(10));
        assert_eq!(ten.cells_total, 1_000, "every live Cell is counted");
        let newest: Vec<_> = (990..1_000).rev().map(|i| format!("k{i}")).collect();
        assert_eq!(keys(&ten), newest);
        assert_eq!(
            cells(&instance, "symbols", None).rows.len(),
            DEFAULT_CELL_ROWS
        );
        assert_eq!(
            cells(&instance, "symbols", Some(10_000)).rows.len(),
            MAX_CELL_ROWS,
            "a limit past the clamp"
        );
    }

    #[test]
    fn an_undeclared_tasklet_is_refused_by_name() {
        let instance = instance("cells_unknown");
        let Err(Error::State(refusal)) = cells_of(&instance, "wire#p0", "nope", None) else {
            panic!("an undeclared tasklet is refused");
        };
        assert_eq!(refusal.code, "state.tasklet_not_found");
        assert_eq!(refusal.details["tasklet"], "nope");
    }

    /// The reply to one `TaskletActivity` frame, as the controller reads it.
    fn ask(
        node: &mut RunnerNode,
        relay_id: &str,
        cells_of: Option<&str>,
    ) -> crate::pb::CommandResponse {
        use std::os::unix::net::UnixStream;

        use crate::pb::command_request::Command;
        let (stream, mut controller): (_, UnixStream) = crate::test_support::stream_pair();
        controller
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout");
        let frame = crate::pb::CommandRequest {
            reference_id: "ref-activity".into(),
            command: Some(Command::TaskletActivity(TaskletActivity {
                relay_id: relay_id.into(),
                cells_of: cells_of.map(str::to_string),
                limit: None,
            })),
        }
        .encode_to_vec();
        crate::command::handle_command(node, &stream, "control_command", &frame);
        let (_, message, _) =
            flz_transport::service::payloader::extract_payload(&mut controller).expect("a reply");
        crate::pb::CommandResponse::decode(message.as_slice()).expect("a CommandResponse")
    }

    #[test]
    fn a_frame_is_answered_or_refused_on_the_control_stream() {
        let mut node = RunnerNode::new("test-ns".to_string());
        let factory = class("activity_wire");
        let refusals =
            crate::test_support::host_class::bind(&mut node, &factory, "wire#p0", "{}", 0);
        assert!(refusals.is_empty(), "{refusals:?}");

        let answered = ask(&mut node, "wire#p0", Some("ohlc"));
        assert_eq!(
            (answered.reference_id.as_str(), answered.status.as_str()),
            ("ref-activity", "success")
        );
        let answer = TaskletActivityResponse::decode(answered.raw_data.as_slice()).expect("rows");
        let names: Vec<_> = answer.rows.iter().map(|row| row.tasklet.as_str()).collect();
        assert_eq!(names, ["ohlc", "trades", "book", "symbols"]);
        let ohlc = answer.cells.expect("Cells asked for");
        assert_eq!(ohlc.cells_total, 1);
        assert_eq!(ohlc.rows[0].key, None, "an unkeyed Cell has no key");

        let refused = ask(&mut node, "nowhere", None);
        assert_eq!(refused.status, "error");
        assert_eq!(refused.errors[0].code, "runtime.relay_not_found");
        assert!(refused.errors[0].details_json.contains("nowhere"));

        let refused = ask(&mut node, "wire#p0", Some("nope"));
        assert_eq!(refused.errors[0].code, "state.tasklet_not_found");
        assert!(refused.errors[0].details_json.contains("nope"));
    }
}
