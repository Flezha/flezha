package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.bounded.RollbackMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Saga compensation planning for the bounded driver — the Java mirror of the
 * Rust SDK's {@code bounded::compensation}.
 *
 * <p>The driver records each <strong>completed</strong> stage (the operation +
 * the segments that ran, with their collected outputs) as it advances. On
 * abnormal termination — an operation fails, or a cooperative stop trips after
 * partial progress — {@link #planCompensation} turns those records into the
 * exact rollback order: completed operations are undone in <strong>reverse</strong>,
 * and within an operation each segment that ran is compensated once. A clean
 * completion produces no records, so nothing is compensated.
 *
 * <p>The planner is <strong>pure</strong> so both rollback modes are testable
 * without driving the executor. v1 runs the structured chain as a single pass,
 * so it always produces exactly one in-flight iteration and zero committed
 * iterations; the {@code committed} parameter and the keep-progress branch are
 * exercised by the unit tests and carry the structured-loop follow-up unchanged.
 */
final class BoundedCompensation {

    private BoundedCompensation() {}

    /**
     * One segment's contribution to a completed stage: which segment ran and the
     * outputs it collected. Only segments that processed input are recorded
     * ("an effect").
     */
    record SegmentRun(int segmentIndex, List<Object> outputs) {}

    /**
     * A completed operation in the chain, recorded so it can be rolled back.
     * {@code stageIdx} indexes both the chain and the driver's per-stage
     * segment table.
     */
    record StageRecord(int stageIdx, List<SegmentRun> segments) {}

    /**
     * Resolve the rollback order for an abnormal termination.
     *
     * <p>{@code inFlight} is the interrupted iteration's completed stages, in
     * completion order; it is <strong>always</strong> undone (reverse order).
     * {@code committed} holds fully finished iterations, in completion order:
     * all-or-nothing undoes everything (in-flight then each committed iteration,
     * all reversed) while keep-progress undoes only the in-flight iteration.
     * Within a single iteration (or a no-loop pass) {@code committed} is empty,
     * so both modes return the in-flight stages reversed.
     */
    static List<StageRecord> planCompensation(
            RollbackMode mode, List<List<StageRecord>> committed, List<StageRecord> inFlight) {
        List<StageRecord> plan = new ArrayList<>();
        for (int i = inFlight.size() - 1; i >= 0; i--) {
            plan.add(inFlight.get(i));
        }
        if (mode == RollbackMode.ALL_OR_NOTHING) {
            for (int it = committed.size() - 1; it >= 0; it--) {
                List<StageRecord> iteration = committed.get(it);
                for (int s = iteration.size() - 1; s >= 0; s--) {
                    plan.add(iteration.get(s));
                }
            }
        }
        return plan;
    }
}
