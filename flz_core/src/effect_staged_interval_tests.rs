use super::*;
use crate::relay::topology::{CheckpointOutputs, CheckpointPolicy};

fn output(payload: &str) -> OutputRecord {
    OutputRecord {
        channel: "out".to_string(),
        key: "k".to_string(),
        metadata: Vec::new(),
        payload: payload.as_bytes().to_vec(),
    }
}

fn invocation(
    changes: Vec<u8>,
    outputs: Vec<OutputRecord>,
    offset: Option<u64>,
) -> InvocationEffects<u8> {
    InvocationEffects {
        changes,
        timers: Vec::new(),
        outputs,
        offset,
        event_time: 7,
        checkpointed: false,
        parked: false,
        edges: Vec::new(),
    }
}

fn at_checkpoint(every: Option<u32>, max_delay_ms: Option<u64>) -> CheckpointPolicy {
    CheckpointPolicy {
        every,
        outputs: CheckpointOutputs::AtCheckpoint,
        max_delay_ms,
    }
}

fn as_produced(every: Option<u32>) -> CheckpointPolicy {
    CheckpointPolicy {
        every,
        outputs: CheckpointOutputs::AsProduced,
        max_delay_ms: None,
    }
}

#[test]
fn per_input_ships_each_effectful_invocation_and_nothing_else() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();

    let shipped = staged
        .fold(None, invocation(vec![1], vec![output("a")], Some(4)), 0)
        .expect("an effectful invocation ships its own record");
    assert_eq!(shipped.changes, vec![1]);
    assert_eq!(shipped.offset, Some(4));
    assert_eq!(shipped.outputs.len(), 1);

    assert!(
        staged
            .fold(None, invocation(Vec::new(), Vec::new(), Some(5)), 0)
            .is_none(),
        "an effect-free invocation contributes nothing durable"
    );
}

#[test]
fn as_produced_flushes_mid_interval_outputs_acknowledging_nothing() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = as_produced(None);

    let shipped = staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(4)),
            0,
        )
        .expect("an as-produced emit still leaves mid-interval");
    assert!(
        shipped.changes.is_empty() && shipped.offset.is_none(),
        "a mid-interval record acknowledges nothing: the state it came from is not durable"
    );
    assert_eq!(shipped.outputs.len(), 1);
    assert_eq!(staged.changes, vec![1], "the change stays staged");
}

#[test]
fn at_checkpoint_holds_every_output_until_the_interval_closes() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(Some(3), Some(50));

    for (n, offset) in [(1u8, 10u64), (2, 11)] {
        assert!(
            staged
                .fold(
                    Some(policy),
                    invocation(vec![n], vec![output("held")], Some(offset)),
                    0
                )
                .is_none(),
            "no output may reach a plugin before the checkpoint"
        );
    }

    let closing = staged
        .fold(
            Some(policy),
            invocation(vec![3], vec![output("last")], Some(12)),
            0,
        )
        .expect("the third input reaches `every` and closes the interval");

    assert_eq!(closing.changes, vec![1, 2, 3]);
    assert_eq!(
        closing
            .outputs
            .iter()
            .map(|o| o.payload.clone())
            .collect::<Vec<_>>(),
        vec![b"held".to_vec(), b"held".to_vec(), b"last".to_vec()],
        "the whole interval's outputs ship as one batch, in production order"
    );
    assert_eq!(
        closing.offset,
        Some(12),
        "the closing record carries the interval's last offset"
    );
    assert!(!staged.is_open(), "the interval closed");
}

#[test]
fn an_explicit_checkpoint_closes_early() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(Some(500), Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            0
        )
        .is_none());

    let mut closing = invocation(vec![2], vec![output("b")], Some(2));
    closing.checkpointed = true;
    let shipped = staged
        .fold(Some(policy), closing, 0)
        .expect("an explicit checkpoint closes the interval well before `every`");
    assert_eq!(shipped.changes, vec![1, 2]);
    assert_eq!(shipped.outputs.len(), 2);
}

#[test]
fn the_delay_closes_a_quiet_entitys_interval() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(None, Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(9)),
            1_000
        )
        .is_none());
    assert_eq!(staged.deadline_ms(policy), Some(1_050));
    assert!(!staged.delay_elapsed(policy, 1_049));
    assert!(staged.delay_elapsed(policy, 1_050));

    let shipped = staged
        .close_on_deadline(policy, 1_050)
        .expect("the deadline ships what the interval accumulated");
    assert_eq!(shipped.outputs.len(), 1);
    assert_eq!(
        shipped.offset,
        Some(9),
        "the close acknowledges the interval's last input"
    );
    assert_eq!(
        shipped.event_time, 7,
        "the closing record is dated by its last invocation"
    );

    assert!(
        staged.close_on_deadline(policy, 1_050).is_none(),
        "a spent deadline entry finds nothing open and does nothing"
    );
}

#[test]
fn a_spent_delay_wake_leaves_the_next_interval_open_until_its_own_deadline() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(None, Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            1_000
        )
        .is_none());
    let mut closing = invocation(vec![2], Vec::new(), Some(2));
    closing.checkpointed = true;
    assert!(staged.fold(Some(policy), closing, 1_010).is_some());

    // The next interval opens before the first one's deadline at 1_050.
    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![3], vec![output("b")], Some(3)),
            1_020
        )
        .is_none());
    assert_eq!(staged.deadline_ms(policy), Some(1_070));

    assert!(
        staged.close_on_deadline(policy, 1_050).is_none(),
        "the first interval's wake closed the next one early"
    );
    assert!(staged.is_open());
    let shipped = staged
        .close_on_deadline(policy, 1_070)
        .expect("its own deadline closes the next interval");
    assert_eq!(shipped.offset, Some(3));
}

#[test]
fn a_close_refused_on_the_delay_leaves_a_deadline_after_the_clock_it_read() {
    // A wake that closes nothing re-arms the open interval's deadline, so a
    // refusal at `now` must leave that deadline after `now`, or the re-armed
    // wake would come due at once and spin.
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(None, Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            1_000
        )
        .is_none());
    // 0 and 999: the wall clock stepped back below the open.
    for now in [0, 999, 1_000, 1_049] {
        assert!(staged.close_on_deadline(policy, now).is_none());
        assert!(staged.deadline_ms(policy).is_some_and(|at| at > now));
    }
    assert!(staged.close_on_deadline(policy, 1_050).is_some());
}

#[test]
fn a_spent_delay_wake_acknowledges_nothing_of_the_next_as_produced_interval() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = CheckpointPolicy {
        max_delay_ms: Some(50),
        ..as_produced(None)
    };

    let flushed = staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            1_000,
        )
        .expect("an as-produced emit leaves at once");
    assert_eq!(flushed.offset, None);
    let mut closing = invocation(vec![2], Vec::new(), Some(2));
    closing.checkpointed = true;
    assert_eq!(
        staged.fold(Some(policy), closing, 1_010).map(|r| r.offset),
        Some(Some(2))
    );

    let flushed = staged
        .fold(
            Some(policy),
            invocation(vec![3], vec![output("b")], Some(3)),
            1_020,
        )
        .expect("an as-produced emit leaves at once");
    assert_eq!(
        flushed.offset, None,
        "a mid-interval flush acknowledges nothing"
    );

    assert!(
        staged.close_on_deadline(policy, 1_050).is_none(),
        "the first interval's wake acknowledged the next one's input early"
    );
    let shipped = staged
        .close_on_deadline(policy, 1_070)
        .expect("its own deadline closes the next interval");
    assert_eq!(shipped.offset, Some(3));
}

#[test]
fn a_park_closes_the_interval_so_the_poison_input_never_replays() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(Some(500), Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            0
        )
        .is_none());

    let mut parking = invocation(Vec::new(), Vec::new(), Some(2));
    parking.parked = true;
    let shipped = staged
        .fold(Some(policy), parking, 0)
        .expect("a park must ship its record atomically with the offset advance");
    assert_eq!(shipped.offset, Some(2));
    assert_eq!(shipped.changes, vec![1]);
}

#[test]
fn a_restore_discards_the_open_interval_entirely() {
    let mut staged: StagedInterval<u8> = StagedInterval::default();
    let policy = at_checkpoint(Some(500), Some(50));

    assert!(staged
        .fold(
            Some(policy),
            invocation(vec![1], vec![output("a")], Some(1)),
            0
        )
        .is_none());
    staged.clear();

    assert!(staged.changes.is_empty());
    assert!(staged.outputs.is_empty());
    assert!(staged.offset.is_none());
    assert!(!staged.is_open());
}
