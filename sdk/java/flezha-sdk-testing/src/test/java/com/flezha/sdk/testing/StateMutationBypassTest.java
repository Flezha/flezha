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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the Java state-mutation asymmetry with Rust: {@code state.get()} returns
 * the live State object, so {@code state.get().field = x} compiles, runs, and is
 * NEVER journaled as a Change. Rust forecloses this at compile time by not
 * implementing {@code DerefMut}; Java cannot, because {@code get()} must return a
 * readable object.
 *
 * <p>The bypass is silent everywhere except one place: {@link
 * ProcedureTestHarness#replayEquals()}, the opt-in determinism gate. That is the
 * detection the Java SDK provides in lieu of structural enforcement.
 */
class StateMutationBypassTest {

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

    // --- fixtures -----------------------------------------------------------

    /** Mutable per-Entity state; only {@code applyChange} is meant to write it. */
    public static final class Counter {
        public long count;
    }

    public record Bump() implements Change {}

    abstract static class Base implements Procedure<String, Counter, String, Bump, WallClock, Void> {
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<Bump> changeType() {
            return Bump.class;
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
    public static final class BypassingProc extends Base {
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
    public static final class JournalingProc extends Base {
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
