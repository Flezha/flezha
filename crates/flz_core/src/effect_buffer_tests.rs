use super::*;
use crate::flusher::encode_cell_identity;

/// The host's bodies are already bytes: the drain moves them, the same
/// allocation going in and coming out.
#[test]
fn a_bytes_buffer_drains_its_bodies_by_move() {
    let bodies: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i, i, i]).collect();
    let allocation = bodies.as_ptr();
    let coalesced = RawBodies::coalesce(bodies);
    assert_eq!(coalesced.as_ptr(), allocation, "coalesce is a move");
    let out = RawBodies::into_bodies(coalesced);
    assert_eq!(out.as_ptr(), allocation, "into_bodies is a move");
    assert_eq!(out.len(), 64);
}

/// The generic drain frames exactly as `frame_change_bodies` does.
#[test]
fn a_bytes_buffer_drain_is_frame_change_bodies() {
    let identity = encode_cell_identity("op", "task", "k");
    let header = HeaderTemplate::new(0x010203, 1);
    let bodies: Vec<Vec<u8>> = vec![vec![1, 2], vec![3], vec![4, 5, 6]];
    let ops = vec![TimerOp::register(
        crate::schema::TimerKind::ProcessingTime,
        7,
    )];

    let mut buffer = EffectBuffer::<RawBodies> {
        changes: bodies.clone(),
        timer_ops: ops.clone(),
        ..Default::default()
    };
    let (count, frames) = buffer.drain_to_frames(&identity, &AtomicU64::new(3), &header);
    let expected = frame_change_bodies(bodies, ops, &identity, &AtomicU64::new(3), &header);

    assert_eq!(count, 4);
    assert!(buffer.is_empty());
    assert_eq!(
        frames
            .iter()
            .map(|f| (f.payload_id, &f.bytes))
            .collect::<Vec<_>>(),
        expected
            .iter()
            .map(|f| (f.payload_id, &f.bytes))
            .collect::<Vec<_>>()
    );
}

fn parked() -> DlqEntryBody {
    DlqEntryBody {
        channel: "in".into(),
        error: "poison".into(),
        attempts: 3,
        payload: Some(vec![9]),
    }
}

fn invocation(changes: Vec<Vec<u8>>, offset: Option<u64>) -> InvocationEffects<Vec<u8>> {
    InvocationEffects {
        changes,
        timers: Vec::new(),
        outputs: Vec::new(),
        offset,
        event_time: 0,
        checkpointed: false,
        parked: false,
        edges: Vec::new(),
    }
}

const TIER_OFF: SettlePolicy = SettlePolicy {
    records_effects: false,
    checkpoint: None,
    buffered: true,
};

/// A Cell that consumes no offset outside the tier hands its park back
/// to the caller; inside the tier the park rides the one effect entry.
#[test]
fn settle_effects_keeps_the_tier_off_park_for_the_caller_and_folds_the_tier_on_one() {
    let mut off = EffectBuffer::<RawBodies>::default();
    off.pending_attempt.push(vec![0]);
    let park = settle_effects(
        &mut off,
        TIER_OFF,
        invocation(vec![vec![1]], None),
        Some(parked()),
    );
    assert_eq!(park.as_ref().map(|d| d.attempts), Some(3));
    assert_eq!(
        off.changes,
        vec![vec![0u8], vec![1u8]],
        "the held attempt comes first"
    );
    assert!(off.effects.is_empty() && off.pending_attempt.is_empty());

    let mut on = EffectBuffer::<RawBodies>::default();
    let policy = SettlePolicy {
        records_effects: true,
        ..TIER_OFF
    };
    let effects = invocation(vec![vec![1]], Some(4));
    assert!(settle_effects(&mut on, policy, effects, Some(parked())).is_none());
    assert!(on.changes.is_empty());
    assert_eq!(on.effects.len(), 1);
    assert_eq!(on.effects[0].consumed_offset, Some(4));
    assert_eq!(on.effects[0].dlq.as_ref().map(|d| d.attempts), Some(3));
}

/// Outside the tier a Cell that consumes offsets folds its inputs into one
/// record carrying the highest, a firing between them included; a park
/// closes that record with its own offset.
#[test]
fn a_cell_consuming_offsets_folds_a_drain_into_one_record_with_the_highest_offset() {
    let mut buffer = EffectBuffer::<RawBodies>::default();
    for (offset, change) in [(Some(10), 1u8), (None, 2), (Some(11), 3), (Some(12), 4)] {
        assert!(settle_effects(
            &mut buffer,
            TIER_OFF,
            invocation(vec![vec![change]], offset),
            None
        )
        .is_none());
    }
    assert!(settle_effects(
        &mut buffer,
        TIER_OFF,
        invocation(Vec::new(), Some(13)),
        Some(parked())
    )
    .is_none());
    settle_effects(
        &mut buffer,
        TIER_OFF,
        invocation(vec![vec![5]], Some(14)),
        None,
    );

    let records: Vec<(Vec<Vec<u8>>, Option<u64>, bool)> = buffer
        .effects
        .iter()
        .map(|e| (e.changes.clone(), e.consumed_offset, e.dlq.is_some()))
        .collect();
    assert_eq!(
        records,
        vec![
            (vec![vec![1], vec![2], vec![3], vec![4]], Some(13), true),
            (vec![vec![5]], Some(14), false),
        ]
    );
    assert!(buffer.changes.is_empty());
}

/// An input that changed nothing starts no record of its own.
#[test]
fn an_input_that_changed_nothing_starts_no_record() {
    let mut buffer = EffectBuffer::<RawBodies>::default();
    settle_effects(&mut buffer, TIER_OFF, invocation(Vec::new(), Some(3)), None);
    assert!(buffer.is_empty());
}

/// Outside the tier an input's inter-tasklet values ride the record that
/// carries its offset, so a restore can deliver them again.
#[test]
fn a_cell_consuming_offsets_keeps_its_edge_values_in_the_record() {
    let edge = |payload: u8| EdgeRecord {
        channel: 1,
        consumer: 2,
        origin: Some(3),
        payload: vec![payload],
    };
    let mut buffer = EffectBuffer::<RawBodies>::default();
    let mut only_edges = invocation(Vec::new(), Some(3));
    only_edges.edges = vec![edge(1)];
    settle_effects(&mut buffer, TIER_OFF, only_edges, None);
    let mut more = invocation(vec![vec![9]], Some(4));
    more.edges = vec![edge(2)];
    settle_effects(&mut buffer, TIER_OFF, more, None);

    assert_eq!(buffer.effects.len(), 1);
    assert_eq!(buffer.effects[0].edges, vec![edge(1), edge(2)]);
    assert_eq!(buffer.effects[0].consumed_offset, Some(4));
}

/// A compaction re-states the offset of the state its snapshot holds, and the
/// pending timers with it, as one record.
// fails when: they are two records, and the controller, holding one refused
// record for a pulled baseline, loses the other.
#[test]
fn a_snapshot_re_states_the_offset_it_holds() {
    let identity = encode_cell_identity("op", "task", "k");
    let header = HeaderTemplate::new(0x010203, 1);
    let ops = vec![TimerOp::register(
        crate::schema::TimerKind::ProcessingTime,
        7,
    )];
    let mut buffer = EffectBuffer::<RawBodies>::default();
    assert!(
        !buffer.restate_above_snapshot(Vec::new()),
        "nothing shipped yet"
    );

    settle_effects(
        &mut buffer,
        TIER_OFF,
        invocation(vec![vec![1]], Some(7)),
        None,
    );
    buffer.drain_to_frames(&identity, &AtomicU64::new(0), &header);
    assert!(buffer.restate_above_snapshot(ops.clone()));
    assert!(buffer.timer_ops.is_empty());
    assert_eq!(buffer.effects.len(), 1);
    assert_eq!(buffer.effects[0].consumed_offset, Some(7));
    assert_eq!(buffer.effects[0].timers, ops);
    assert!(buffer.effects[0].changes.is_empty());
    let (count, _frames) = buffer.drain_to_frames(&identity, &AtomicU64::new(1), &header);
    assert_eq!(count, 1);
}
