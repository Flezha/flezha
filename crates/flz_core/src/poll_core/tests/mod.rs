//! The engine's own tests: handout exclusivity, ordering, the ladder, the
//! deferred-wake list, the cap and the starvation signal.

/// Slot scaffolding shared by the engine's tests and the pool governor's —
/// one definition of how a test builds a Cell from a host declaration.
pub(crate) mod fixtures {
    use std::sync::Arc;

    use flz_transport::transport::FlezhaStream;

    use crate::cell::CellSlot;
    use crate::relay::instance::RelayInstance;

    /// One instance of a one-tasklet host class and its Cell: the class
    /// `factory_id` declares, its entry channel `input` into tasklet `p0`.
    /// `tasklet` and `channel` are extra declaration keys of each.
    pub(crate) fn build_slot_of(
        factory_id: &str,
        tasklet: &str,
        channel: &str,
        instance_id: &str,
    ) -> (Arc<RelayInstance>, Arc<CellSlot>) {
        let json = format!(
            r#"{{"factory_id":"{factory_id}","sdk_lang":1,"sdk_version":"0.1.0","durable":false,"run":"flow",
                 "channels":[{{"name":"input","kind":"entry",{channel}"chains":[[{{"link":"into","tasklet":0}}]]}}],
                 "tasklets":[{{"name":"p0"{tasklet}}}]}}"#
        );
        crate::host::register_host_class_json(json.as_bytes()).expect("class registers");
        let instance = RelayInstance::of_class(factory_id, instance_id).expect("class registered");
        let slot = Arc::clone(&instance.cells[0][0]);
        (instance, slot)
    }

    /// One instance of the plain class and its Cell.
    pub(crate) fn build_slot(instance_id: &str) -> (Arc<RelayInstance>, Arc<CellSlot>) {
        build_slot_of("poll_core_exclusivity_op", "", "", instance_id)
    }

    /// A stream token for an input or firing. The peer is dropped straight
    /// away — the engine only carries the handle, it never writes to it.
    pub(crate) fn unix_stream() -> Arc<FlezhaStream> {
        crate::test_support::stream_pair().0
    }

    /// Occupy the calling thread for `span` — a stand-in for work — without
    /// yielding it to the scheduler the way a sleep would.
    pub(crate) fn hold_for(span: std::time::Duration) {
        let started = std::time::Instant::now();
        while started.elapsed() < span {
            std::hint::spin_loop();
        }
    }

    /// Drive `done` until it holds, failing the test at a 10 s deadline with
    /// `what` naming the condition.
    pub(crate) fn wait_until(what: &str, done: impl FnMut() -> bool) {
        use std::time::Duration;
        assert!(
            crate::test_support::wait_until(
                Duration::from_secs(10),
                Duration::from_millis(10),
                done
            ),
            "timed out waiting for {what}"
        );
    }
}

/// The slot shapes and handout helpers the three test modules below are
/// written against.
mod support {
    use std::sync::Arc;
    use std::time::Duration;

    use flz_transport::transport::FlezhaStream;

    use super::fixtures::{build_slot_of, unix_stream};
    use crate::cell::{CellItem, CellSlot};
    use crate::poll_core::{EngineActivation, PollCore, Polled};
    use crate::relay::instance::RelayInstance;
    use crate::test_support::input_item;
    use crate::timer::{TimerKind, TimerOp};

    /// A slot whose one mailbox slot is taken and whose round is claimed by
    /// nobody: every wake promoted against it meets a full mailbox.
    pub(super) fn full_slot(instance_id: &str) -> (Arc<RelayInstance>, Arc<CellSlot>) {
        let (instance, slot) = build_slot_of(
            "poll_core_tiny_mailbox_op",
            r#","mailbox_capacity":1"#,
            "",
            instance_id,
        );
        assert!(slot.mailbox.try_push(input_item(0, &unix_stream())).is_ok());
        assert!(slot.mailbox.is_full());
        // Claim the round so nothing drains it — the caller decides when room
        // appears.
        assert!(slot.mailbox.claim_inflight());
        (instance, slot)
    }

    /// A backoff long enough to observe the deferral window from the outside:
    /// base 2 s, so attempt 1 comes due in 1–3 s.
    pub(super) fn slow_retry_slot(instance_id: &str) -> (Arc<RelayInstance>, Arc<CellSlot>) {
        build_slot_of(
            "poll_core_slow_retry_op",
            "",
            r#""retry_backoff_ms":2000,"#,
            instance_id,
        )
    }

    /// Arm one already-due processing-time timer and keep its firing on this
    /// core: `admission::admit_from_reader` only schedules a round when it wins the
    /// inflight CAS, and the caller has already taken it.
    pub(super) fn arm_due_timer(core: &PollCore, slot: &Arc<CellSlot>, at: u64) {
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
        assert!(slot.mailbox.claim_inflight());
        core.arm_timer(slot, at, unix_stream());
    }

    /// The one timer item a promotion left on the slot's mailbox.
    pub(super) fn assemble_timer(core: &PollCore, slot: &Arc<CellSlot>) -> (u64, u64) {
        let act = core.try_assemble(slot).expect("a firing to assemble");
        assert_eq!(act.items().len(), 1);
        assert!(matches!(act.items()[0].item, CellItem::Timer(_)));
        let now = act.items()[0]
            .now
            .expect("a firing carries its scheduled at");
        (act.token, now)
    }

    /// The producer protocol followed by one handout: queue `count` inputs
    /// (`0..count`), claim the round on the winning inflight CAS, and take the
    /// activation it earns.
    pub(super) fn queue_and_hand_out(
        core: &PollCore,
        slot: &Arc<CellSlot>,
        stream: &Arc<FlezhaStream>,
        count: usize,
    ) -> EngineActivation {
        for i in 0..count {
            assert!(slot.mailbox.try_push(input_item(i, stream)).is_ok());
        }
        assert!(slot.mailbox.claim_inflight());
        core.schedule(slot);
        match core.poll(Duration::from_millis(100)) {
            Polled::Activation(act) => act,
            _ => panic!("expected an activation"),
        }
    }

    /// The ordinals a batch carries, in order.
    pub(super) fn ordinals(act: &EngineActivation) -> Vec<usize> {
        act.items()
            .iter()
            .map(|w| {
                let CellItem::Input(p) = &w.item else {
                    panic!("unexpected timer item")
                };
                *p.value.downcast_ref::<usize>().unwrap()
            })
            .collect()
    }
}

mod cap_and_starvation;
mod handout;
mod wake_list;
