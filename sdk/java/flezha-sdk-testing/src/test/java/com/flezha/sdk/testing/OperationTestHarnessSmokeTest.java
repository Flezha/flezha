package com.flezha.sdk.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class OperationTestHarnessSmokeTest {

    public record CounterChange(long delta) implements Change {}

    public static final class CounterState {
        long value;
    }

    /** Deterministic Operation: applyChange = state.value += delta. */
    public static final class DeterministicCounterOperation
            implements Operation<Long, CounterState, Void, CounterChange, WallClock, Void> {
        @Override
        public String name() {
            return "counter";
        }

        @Override
        public Class<Long> inputType() {
            return Long.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public Class<CounterChange> changeType() {
            return CounterChange.class;
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
        public CounterState initState(Optional<String> operationKey, Optional<RestoreInitPayload> payload) {
            return new CounterState();
        }

        @Override
        public void applyChange(CounterState state, CounterChange change) {
            state.value += change.delta();
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(CounterState.class, CounterChange.class);
        }

        @Override
        public void run(Long input, StateHandle<CounterState, CounterChange> state, OperationCtx ctx) {
            state.apply(new CounterChange(input));
        }
    }

    /**
     * Non-deterministic Operation: applyChange ignores the change and increments a
     * field that the run path sets from a global counter (which differs across
     * replays). This drives a {@code replayEquals} mismatch.
     */
    public static final class NonDeterministicCounterOperation
            implements Operation<Long, CounterState, Void, CounterChange, WallClock, Void> {
        private static final AtomicLong GLOBAL = new AtomicLong();

        @Override
        public String name() {
            return "counter_nd";
        }

        @Override
        public Class<Long> inputType() {
            return Long.class;
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public Class<CounterChange> changeType() {
            return CounterChange.class;
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
        public CounterState initState(Optional<String> operationKey, Optional<RestoreInitPayload> payload) {
            return new CounterState();
        }

        @Override
        public void applyChange(CounterState state, CounterChange change) {
            // Pretend the application side-effects on a global counter that
            // diverges from the recorded Change. This is the bug a determinism
            // gate is meant to catch.
            state.value += GLOBAL.incrementAndGet();
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(CounterState.class, CounterChange.class);
        }

        @Override
        public void run(Long input, StateHandle<CounterState, CounterChange> state, OperationCtx ctx)
                throws OperationException {
            state.apply(new CounterChange(input));
        }
    }

    @Test
    void deterministicOperationReplayEqualsPasses() {
        OperationTestHarness<Long, CounterState, Void, CounterChange> h =
                OperationTestHarness.of(new DeterministicCounterOperation()).applyInputs(List.of(1L, 2L, 3L));
        assertThat(h.recordedChanges()).hasSize(3);
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void nonDeterministicOperationReplayEqualsReportsMismatch() {
        OperationTestHarness<Long, CounterState, Void, CounterChange> h =
                OperationTestHarness.of(new NonDeterministicCounterOperation()).applyInputs(List.of(1L, 2L, 3L));
        assertThat(h.replayEquals()).isPresent();
    }
}
