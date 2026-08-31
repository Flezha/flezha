package com.flezha.sdk.testing;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What {@link ProcedureTestHarness} shows an author, in three parts: the key a
 * record ships under, the determinism gates over state ({@code replayEquals})
 * and over output ({@code replayEmitsEqual}), and the state-mutation bypass
 * only {@code replayEquals} can see.
 *
 * <p>{@code replayEquals} alone is not enough: a Procedure can rebuild
 * identical state on replay while emitting a different key or timestamp each
 * run — and a keyed sink's idempotence depends on exactly that not happening.
 *
 * <p>Java's bypass is structural: {@code state.get()} returns the live State
 * object, so {@code state.get().field = x} compiles, runs, and is NEVER
 * journaled as a Change. Rust forecloses it by not implementing
 * {@code DerefMut}; Java cannot, because {@code get()} must return a readable
 * object, so the opt-in gate is the detection this SDK provides instead.
 */
class HarnessBehaviourTest {

    public record CounterChange(long delta) implements Change {}

    public static final class CounterState {
        long value;
    }

    /** Everything three fixtures share; each declares only its name, its output and its run. */
    abstract static class CounterBase<O> implements Procedure<Long, CounterState, O, CounterChange, WallClock, Void> {
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
    public static final class DeterministicCounterProcedure extends CounterBase<Void> {
        @Override
        public String name() {
            return "counter";
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
    public static final class NonDeterministicCounterProcedure extends CounterBase<Void> {
        private static final AtomicLong GLOBAL = new AtomicLong();

        @Override
        public String name() {
            return "counter_nd";
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
    public static final class KeyLessEmitProcedure extends CounterBase<String> {
        @Override
        public String name() {
            return "key_less_emit";
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

    @Test
    @DisplayName("a deterministic Procedure replays identical emits")
    void deterministicProcedureReplaysEqualEmits() {
        List<String> inputs = List.of("a", "b");
        ProcedureTestHarness<String, Unit, String, NoChange> harness =
                ProcedureTestHarness.of(new DeterministicProc()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(inputs);

        assertThat(harness.replayEmitsEqual(inputs)).isEmpty();
    }

    @Test
    @DisplayName("a Procedure reading ambient time is caught")
    void ambientTimeProcedureIsCaught() {
        List<String> inputs = List.of("a");
        ProcedureTestHarness<String, Unit, String, NoChange> harness =
                ProcedureTestHarness.of(new AmbientTimeProc()).withWallClock(1_700_000_000_000L);

        harness.applyInputs(inputs);

        // The replay emits a different nanoTime, so the output is not a function
        // of the input — which is precisely the defect.
        assertThat(harness.replayEmitsEqual(inputs)).isPresent();
    }

    @Test
    @DisplayName("a scripted clock serves one value per invocation")
    void scriptedClockAdvancesPerInvocation() {
        ProcedureTestHarness<String, Unit, String, NoChange> harness =
                ProcedureTestHarness.of(new DeterministicProc()).withWallClockScript(1000L, 2000L, 3000L);

        harness.applyInputs(List.of("a", "b", "c"));

        assertThat(harness.recordedEmits()).containsExactly("a@1000", "b@2000", "c@3000");
    }

    @Test
    @DisplayName("a keyed emit records its key")
    void keyedEmitRecordsItsKey() {
        ProcedureTestHarness<String, Unit, String, NoChange> harness =
                ProcedureTestHarness.of(new KeyedProc()).withKeyedChannel().withWallClock(1L);

        harness.applyInputs(List.of("BTC"));

        assertThat(harness.emitRecords()).hasSize(1);
        assertThat(harness.emitRecords().get(0).key()).isEqualTo("BTC");
    }

    public record Unit() {}

    abstract static class EmitBase implements Procedure<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public Unit initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }
    }

    /** Reads time via ctx.now() — replayable. */
    public static final class DeterministicProc extends EmitBase {
        @Override
        public String name() {
            return "deterministic_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit(input + "@" + ctx.now());
        }
    }

    /** Reads ambient time — NOT replayable. */
    public static final class AmbientTimeProc extends EmitBase {
        @Override
        public String name() {
            return "ambient_time_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit(input + "@" + System.nanoTime());
        }
    }

    public static final class KeyedProc extends EmitBase {
        @Override
        public String name() {
            return "keyed_op";
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emitKeyed(input, "payload");
        }
    }

    @Test
    @DisplayName("a direct field write bypasses the change log and is caught only by replayEquals()")
    void directMutationIsDroppedAndOnlyReplayEqualsCatchesIt() {
        ProcedureTestHarness<String, Counter, String, Bump> harness =
                ProcedureTestHarness.of(new BypassingProc()).withWallClock(1L);

        harness.applyInputs(List.of("a", "b", "c"));

        // The live object WAS mutated — the bypass "works" in memory.
        assertThat(harness.state().count).isEqualTo(3L);
        // ...yet nothing was journaled: a restore rebuilds from snapshot + change
        // log and would come back at 0. This is the silent data loss.
        assertThat(harness.recordedChanges()).isEmpty();
        // The one thing that catches it: replaying the (empty) change log rebuilds
        // count=0, which does not match the live count=3.
        assertThat(harness.replayEquals()).isPresent();
    }

    @Test
    @DisplayName("the same Procedure using state.apply() is journaled and replay-clean")
    void applyPathIsJournaledAndReplaySafe() {
        ProcedureTestHarness<String, Counter, String, Bump> harness =
                ProcedureTestHarness.of(new JournalingProc()).withWallClock(1L);

        harness.applyInputs(List.of("a", "b", "c"));

        assertThat(harness.state().count).isEqualTo(3L);
        assertThat(harness.recordedChanges()).hasSize(3);
        assertThat(harness.replayEquals()).isEmpty();
    }

    /** Mutable per-Entity state; only {@code applyChange} is meant to write it. */
    public static final class Counter {
        public long count;
    }

    public record Bump() implements Change {}

    abstract static class MutationBase implements Procedure<String, Counter, String, Bump, WallClock, Void> {
        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Bump.class, Counter.class);
        }

        @Override
        public void applyChange(Counter state, Bump change) {
            state.count++;
        }

        @Override
        public Counter initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Counter();
        }
    }

    /** The defect: writes the field directly, bypassing {@code apply}. */
    public static final class BypassingProc extends MutationBase {
        @Override
        public String name() {
            return "bypassing_op";
        }

        @Override
        public void run(String input, StateHandle<Counter, Bump> state, ProcedureCtx ctx) throws ProcedureException {
            state.get().count += 1; // compiles, runs, never journaled
        }
    }

    /** The correct form, for contrast. */
    public static final class JournalingProc extends MutationBase {
        @Override
        public String name() {
            return "journaling_op";
        }

        @Override
        public void run(String input, StateHandle<Counter, Bump> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new Bump());
        }
    }
}
