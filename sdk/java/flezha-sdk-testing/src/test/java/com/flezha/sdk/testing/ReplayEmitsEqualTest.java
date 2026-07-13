package com.flezha.sdk.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@code replayEmitsEqual} is the OUTPUT-side determinism gate.
 *
 * <p>{@code replayEquals} alone is not enough: an Operation can rebuild identical
 * state on replay while emitting a different key or timestamp each run — and a
 * keyed sink's idempotence depends on exactly that not happening. This is what
 * catches an Operation reading ambient time or randomness instead of
 * {@code ctx.now()} / {@code ctx.stableId()}.
 */
class ReplayEmitsEqualTest {

    @Test
    @DisplayName("a deterministic Operation replays identical emits")
    void deterministicOperationReplaysEqualEmits() {
        List<String> inputs = List.of("a", "b");
        OperationTestHarness<String, Unit, String, NoChange> harness =
                OperationTestHarness.of(new DeterministicOp()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(inputs);

        assertThat(harness.replayEmitsEqual(inputs)).isEmpty();
    }

    @Test
    @DisplayName("an Operation reading ambient time is caught")
    void ambientTimeOperationIsCaught() {
        List<String> inputs = List.of("a");
        OperationTestHarness<String, Unit, String, NoChange> harness =
                OperationTestHarness.of(new AmbientTimeOp()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(inputs);

        // The replay emits a different nanoTime, so the output is not a function
        // of the input — which is precisely the defect.
        assertThat(harness.replayEmitsEqual(inputs)).isPresent();
    }

    @Test
    @DisplayName("a scripted clock serves one value per invocation")
    void scriptedClockAdvancesPerInvocation() {
        OperationTestHarness<String, Unit, String, NoChange> harness =
                OperationTestHarness.of(new DeterministicOp()).withWallClockScript(1000L, 2000L, 3000L);

        harness.applyInputs(List.of("a", "b", "c"));

        assertThat(harness.recordedEmits()).containsExactly("a@1000", "b@2000", "c@3000");
    }

    @Test
    @DisplayName("a keyed emit records its key")
    void keyedEmitRecordsItsKey() {
        OperationTestHarness<String, Unit, String, NoChange> harness =
                OperationTestHarness.of(new KeyedOp()).withWallClock(1L);

        harness.applyInputs(List.of("BTC"));

        assertThat(harness.emitRecords()).hasSize(1);
        assertThat(harness.emitRecords().get(0).key()).isEqualTo("BTC");
    }

    // --- fixtures -----------------------------------------------------------

    public record Unit() {}

    abstract static class Base implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }
    }

    /** Reads time via ctx.now() — replayable. */
    public static final class DeterministicOp extends Base {
        @Override
        public String name() {
            return "deterministic_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(input + "@" + ctx.now());
        }
    }

    /** Reads ambient time — NOT replayable. */
    public static final class AmbientTimeOp extends Base {
        @Override
        public String name() {
            return "ambient_time_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(input + "@" + System.nanoTime());
        }
    }

    public static final class KeyedOp extends Base {
        @Override
        public String name() {
            return "keyed_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emitKeyed(input, "payload");
        }
    }
}
