package com.flezha.sdk.runtime.bounded;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.bounded.RollbackMode;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Pure unit tests for the saga compensation planner (the Java mirror of Rust {@code bounded::compensation::tests}). */
class BoundedCompensationTest {

    private static BoundedCompensation.StageRecord stage(int idx) {
        // The planner orders by stage, never by iteration — 0 keeps these cases
        // reading as the single-iteration passes they are.
        return new BoundedCompensation.StageRecord(idx, 0, List.of());
    }

    private static List<Integer> order(List<BoundedCompensation.StageRecord> plan) {
        return plan.stream().map(BoundedCompensation.StageRecord::stageIdx).toList();
    }

    @Test
    @DisplayName("no progress ⇒ nothing to compensate, in either mode")
    void noProgressCompensatesNothing() {
        assertThat(BoundedCompensation.planCompensation(RollbackMode.ALL_OR_NOTHING, List.of(), List.of()))
                .isEmpty();
        assertThat(BoundedCompensation.planCompensation(RollbackMode.KEEP_PROGRESS, List.of(), List.of()))
                .isEmpty();
    }

    @Test
    @DisplayName("a single no-loop pass unwinds in reverse in both modes")
    void singlePassIsReverseOrderInBothModes() {
        List<BoundedCompensation.StageRecord> inFlight = List.of(stage(0), stage(1), stage(2));
        for (RollbackMode mode : RollbackMode.values()) {
            assertThat(order(BoundedCompensation.planCompensation(mode, List.of(), inFlight)))
                    .as("mode %s", mode)
                    .containsExactly(2, 1, 0);
        }
    }

    @Test
    @DisplayName("all-or-nothing also unwinds committed iterations, back to the start")
    void allOrNothingUndoesCommittedIterations() {
        List<List<BoundedCompensation.StageRecord>> committed =
                List.of(List.of(stage(0), stage(1)), List.of(stage(0), stage(1)));
        List<BoundedCompensation.StageRecord> inFlight = List.of(stage(0));
        assertThat(order(BoundedCompensation.planCompensation(RollbackMode.ALL_OR_NOTHING, committed, inFlight)))
                .containsExactly(0, 1, 0, 1, 0);
    }

    @Test
    @DisplayName("keep-progress keeps committed iterations, undoing only the in-flight one")
    void keepProgressKeepsCommittedIterations() {
        List<List<BoundedCompensation.StageRecord>> committed =
                List.of(List.of(stage(0), stage(1)), List.of(stage(0), stage(1)));
        List<BoundedCompensation.StageRecord> inFlight = List.of(stage(0));
        assertThat(order(BoundedCompensation.planCompensation(RollbackMode.KEEP_PROGRESS, committed, inFlight)))
                .containsExactly(0);
    }
}
