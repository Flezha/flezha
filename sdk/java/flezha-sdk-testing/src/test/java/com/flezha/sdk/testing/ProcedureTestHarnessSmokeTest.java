package com.flezha.sdk.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ProcedureTestHarnessSmokeTest {

    public record CounterChange(long delta) implements Change {}

    public static final class CounterState {
        long value;
    }

    /** Everything three fixtures share; each declares only its name, its output and its run. */
    abstract static class Base<O> implements Procedure<Long, CounterState, O, CounterChange, WallClock, Void> {
        @Override
        public Class<Long> inputType() {
            return Long.class;
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
        public CounterState initState(Optional<String> entityKey, Optional<RestoreInitPayload> payload) {
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
    }

    /** Deterministic Procedure: applyChange = state.value += delta. */
    public static final class DeterministicCounterProcedure extends Base<Void> {
        @Override
        public String name() {
            return "counter";
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public void run(Long input, StateHandle<CounterState, CounterChange> state, ProcedureCtx ctx) {
            state.apply(new CounterChange(input));
        }
    }

    /**
     * Non-deterministic Procedure: applyChange ignores the change and increments a
     * field that the run path sets from a global counter (which differs across
     * replays). This drives a {@code replayEquals} mismatch.
     */
    public static final class NonDeterministicCounterProcedure extends Base<Void> {
        private static final AtomicLong GLOBAL = new AtomicLong();

        @Override
        public String name() {
            return "counter_nd";
        }

        @Override
        public Class<Void> outputType() {
            return Void.class;
        }

        @Override
        public void applyChange(CounterState state, CounterChange change) {
            state.value += GLOBAL.incrementAndGet();
        }

        @Override
        public void run(Long input, StateHandle<CounterState, CounterChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(new CounterChange(input));
        }
    }

    /** Emits on what the harness may declare keyed, without supplying a key of its own. */
    public static final class KeyLessEmitProcedure extends Base<String> {
        @Override
        public String name() {
            return "key_less_emit";
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public void run(Long input, StateHandle<CounterState, CounterChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit("v" + input);
        }
    }

    /**
     * fails when: a key-less emit inside a keyed Entity stops carrying that Entity's own
     * key — the harness would stop showing authors the key their records actually ship
     * under, which is the drift this default exists to remove.
     */
    @Test
    void aKeyedEntityRecordsItsOwnKeyOnAKeyLessEmit() {
        ProcedureTestHarness<Long, CounterState, String, CounterChange> h = ProcedureTestHarness.of(
                        new KeyLessEmitProcedure())
                .withKeyedChannel()
                .withEntityKey("BTC/USD")
                .applyInputs(List.of(1L));

        assertThat(h.emitRecords()).hasSize(1);
        assertThat(h.emitRecords().get(0).key()).isEqualTo("BTC/USD");
        assertThat(h.replayEmitsEqual(List.of(1L))).isEmpty();
    }

    /**
     * fails when: a single-entity Procedure's key-less emit is accepted on a keyed
     * channel — it has no key to default to, so the record would ship keyless.
     */
    @Test
    void aSingleEntityKeyLessEmitOnAKeyedChannelIsRefused() {
        ProcedureTestHarness<Long, CounterState, String, CounterChange> h = ProcedureTestHarness.of(
                        new KeyLessEmitProcedure())
                .withKeyedChannel()
                .applyInputs(List.of(1L));

        assertThat(h.emitRecords()).as("the refusal leaves nothing recorded").isEmpty();
    }

    /** A harness that declares no keyed channel is unchanged: no key, no refusal. */
    @Test
    void aNonKeyedChannelRecordsNoKey() {
        ProcedureTestHarness<Long, CounterState, String, CounterChange> h = ProcedureTestHarness.of(
                        new KeyLessEmitProcedure())
                .withEntityKey("BTC/USD")
                .applyInputs(List.of(1L));

        assertThat(h.emitRecords()).hasSize(1);
        assertThat(h.emitRecords().get(0).key()).isEmpty();
    }

    @Test
    void deterministicProcedureReplayEqualsPasses() {
        ProcedureTestHarness<Long, CounterState, Void, CounterChange> h =
                ProcedureTestHarness.of(new DeterministicCounterProcedure()).applyInputs(List.of(1L, 2L, 3L));
        assertThat(h.recordedChanges()).hasSize(3);
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void nonDeterministicProcedureReplayEqualsReportsMismatch() {
        ProcedureTestHarness<Long, CounterState, Void, CounterChange> h =
                ProcedureTestHarness.of(new NonDeterministicCounterProcedure()).applyInputs(List.of(1L, 2L, 3L));
        assertThat(h.replayEquals()).isPresent();
    }
}
