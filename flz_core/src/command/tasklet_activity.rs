//! `TaskletActivity`: one row per tasklet of an instance, summed from the
//! counters its Cells' mailboxes already keep. The invariant: the read takes
//! no Cell lock and writes nothing the data path reads, so it never blocks a
//! dispatch. An eviction during the walk can count one Cell twice or miss it;
//! these are monitoring totals, and the next read is right again.

use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;

use super::cell_state::relay_not_found;
use super::{OffReaderAction, OffReaderBuild};
use crate::cell::CellCounters;
use crate::channel::TaskletId;
use crate::dynamic::DynamicCellCounters;
use crate::error::Error;
use crate::pb::{TaskletActivity, TaskletActivityResponse, TaskletActivityRow};
use crate::relay::instance::RelayInstance;
use crate::runner::RunnerNode;

impl OffReaderAction for TaskletActivity {
    /// The walk is O(live Cells), about 30 ms at 100,000, so it runs off the
    /// control reader.
    fn prepare(&self, runner: &mut RunnerNode) -> Result<OffReaderBuild, Error> {
        let instance = runner
            .get_relay(&self.relay_id)
            .ok_or_else(|| relay_not_found(&self.relay_id))?;
        Ok(Box::new(move || activity(&instance).encode_to_vec()))
    }
}

fn activity(instance: &RelayInstance) -> TaskletActivityResponse {
    TaskletActivityResponse {
        rows: (0..instance.topology.tasklets.len() as TaskletId)
            .map(|tasklet_id| row(instance, tasklet_id))
            .collect(),
        read_at_ms: crate::util::monotonic_ms() as u64,
        instance_seq: instance.seq,
    }
}

/// A tasklet's live Cells plus, for a dynamic one, its evicted Cells' totals.
/// `queued` is `Mailbox::len()`, which leaves out items held behind locks.
fn row(instance: &RelayInstance, tasklet_id: TaskletId) -> TaskletActivityRow {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::admission::{admit_from_reader, try_admit};
    use crate::cell::{CellItem, CellSlot};
    use crate::channel::OverflowPolicy;

    /// `ohlc`: one Cell; `trades`: dynamic, capped at two Cells.
    fn instance(id: &str) -> Arc<RelayInstance> {
        let id = class(id);
        RelayInstance::of_class(&id, &format!("{id}_inst")).expect("class registered")
    }

    fn class(id: &str) -> String {
        let id = format!("{id}_{}", std::process::id());
        let json = format!(
            r#"{{"factory_id":"{id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
                 "channels":[{{"name":"in","kind":"entry",
                   "chains":[[{{"link":"into","tasklet":0}}],[{{"link":"key"}},{{"link":"into","tasklet":1}}]]}}],
                 "tasklets":[{{"name":"ohlc","mailbox_capacity":4}},
                             {{"name":"trades","cell_mode":"dynamic","max_cells":2,"mailbox_capacity":16}}]}}"#
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

        let answer = activity(&instance);
        assert_eq!(answer.instance_seq, instance.seq);
        assert_eq!(answer.rows.len(), 2, "one row per tasklet");
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

        let before = row_of(&activity(&instance), "trades");
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

        let after = row_of(&activity(&instance), "trades");
        assert_eq!((after.cells, after.queued), (1, Some(2)), "`a` was evicted");
        assert_eq!(after.cells_evicted_total, Some(1));
        assert_eq!(
            after.admitted_total, before.admitted_total,
            "an eviction keeps the evicted Cell's totals"
        );
    }

    #[test]
    fn a_new_instance_answers_with_a_new_seq() {
        let first = activity(&instance("activity_seq_a")).instance_seq;
        let second = activity(&instance("activity_seq_b")).instance_seq;
        assert_ne!(first, second);
    }

    /// The reply to one `TaskletActivity` frame, as the controller reads it.
    fn ask(node: &mut RunnerNode, relay_id: &str) -> crate::pb::CommandResponse {
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

        let answered = ask(&mut node, "wire#p0");
        assert_eq!(
            (answered.reference_id.as_str(), answered.status.as_str()),
            ("ref-activity", "success")
        );
        let answer = TaskletActivityResponse::decode(answered.raw_data.as_slice()).expect("rows");
        let names: Vec<_> = answer.rows.iter().map(|row| row.tasklet.as_str()).collect();
        assert_eq!(names, ["ohlc", "trades"]);

        let refused = ask(&mut node, "nowhere");
        assert_eq!(refused.status, "error");
        assert_eq!(refused.errors[0].code, "runtime.relay_not_found");
        assert!(refused.errors[0].details_json.contains("nowhere"));
    }
}
