package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProcedureDefaultsTest {

    @Test
    void compactAfterBytesDefaultsTo8Mib() {
        StatelessProcedure procedure = new StatelessProcedure();
        // 8 MiB, in lockstep with Rust and the controller's default.
        assertThat(procedure.compactAfterBytes()).isEqualTo(8388608L);
    }

    @Test
    void schemaVersionDefaultsToOne() {
        StatelessProcedure procedure = new StatelessProcedure();
        assertThat(procedure.schemaVersion()).isEqualTo(1);
    }

    @Test
    void kryoTypesDefaultsToEmpty() {
        StatelessProcedure procedure = new StatelessProcedure();
        assertThat(procedure.kryoTypes()).isEmpty();
    }

    @Test
    void fromConfigVoidInvokesNoArgCtor() {
        StatelessProcedure procedure = new StatelessProcedure();
        Procedure<String, String, String, NoChange, WallClock, Void> fresh = procedure.fromConfig(null);
        assertThat(fresh).isNotNull();
        assertThat(fresh).isNotSameAs(procedure);
        assertThat(fresh.name()).isEqualTo("stateless");
    }

    @Test
    void applyChangeWithNoChangeIsNoop() {
        StatelessProcedure procedure = new StatelessProcedure();
        // Cannot construct NoChange (uninhabited); just confirm the call path
        // returns without throwing for the NoChange-typed Procedure. We prove the
        // path with a null cast accepted by the type system because NoChange
        // is uninhabited.
        procedure.applyChange("state", null);
    }

    @Test
    void applyChangeWithUserChangeWithoutOverrideThrows() {
        DeclaredTokens procedure = new DeclaredTokens();
        assertThatThrownBy(() -> procedure.applyChange(0L, new SampleChange(1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not override applyChange");
    }

    /**
     * fails when: the reflective resolution stops reading the implements
     * clause — every hand-written Procedure would then be back to five Class
     * literals restating what the clause already says.
     */
    @Test
    void tokensResolveFromTheImplementsClause() {
        DeclaredTokens procedure = new DeclaredTokens();
        assertThat(procedure.inputType()).isEqualTo(String.class);
        assertThat(procedure.outputType()).isEqualTo(Integer.class);
        assertThat(procedure.changeType()).isEqualTo(SampleChange.class);
        assertThat(procedure.clockType()).isEqualTo(WallClock.class);
        assertThat(procedure.configType()).isEqualTo(Void.class);
    }

    /**
     * fails when: resolution stops substituting type variables across the
     * supertype chain — a generic base class holding the shared body is the
     * shape that most wants the defaults, and it is the one that breaks first.
     */
    @Test
    void tokensResolveThroughAConcreteSubclassOfAGenericBase() {
        StringOneShot procedure = new StringOneShot();
        assertThat(procedure.inputType()).isEqualTo(String.class);
        assertThat(procedure.outputType()).isEqualTo(String.class);
        assertThat(procedure.changeType()).isEqualTo(NoChange.class);
    }

    /**
     * fails when: an unresolvable argument is guessed instead of reported —
     * the wrong guess surfaces as a type mismatch at dispatch, far from the
     * declaration that caused it.
     */
    @Test
    void anUnresolvableTokenFailsNamingTheClassAndParameter() {
        assertThatThrownBy(() -> new StillGeneric<String>().inputType())
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining(StillGeneric.class.getName())
                .hasMessageContaining("Input")
                .hasMessageContaining("inputType");
    }

    /** fails when: a default starts winning over the explicit declaration it exists to spare. */
    @Test
    void anExplicitOverrideWinsOverTheDefault() {
        assertThat(new OverridingStillGeneric<String>().inputType()).isEqualTo(String.class);
        // The parameters it does NOT override still resolve.
        assertThat(new OverridingStillGeneric<String>().outputType()).isEqualTo(String.class);
    }

    /** fails when: the per-Entity codec stops being handed the two types it always needs. */
    @Test
    void kryoTypesDefaultToTheResolvedChangeAndState() {
        assertThat(new DeclaredTokens().kryoTypes()).containsExactly(SampleChange.class, Long.class);
    }

    /** A Procedure declaring every type argument concretely and overriding no token method. */
    public static final class DeclaredTokens
            implements Procedure<String, Long, Integer, SampleChange, WallClock, Void> {
        @Override
        public String name() {
            return "declared";
        }

        @Override
        public Long initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return 0L;
        }

        @Override
        public void run(String input, StateHandle<Long, SampleChange> state, ProcedureCtx ctx) {}
    }

    /** A generic intermediate class: the shared body, one type argument still open. */
    public abstract static class OneShot<I> implements Procedure<I, String, I, NoChange, WallClock, Void> {
        @Override
        public String initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return "";
        }

        @Override
        public void run(I input, StateHandle<String, NoChange> state, ProcedureCtx ctx) {}
    }

    /** The concrete subclass that pins it. */
    public static final class StringOneShot extends OneShot<String> {
        @Override
        public String name() {
            return "string_one_shot";
        }
    }

    /** Concrete class, still-generic Input: nothing to resolve `Input` to. */
    public static class StillGeneric<I> implements Procedure<I, String, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "still_generic";
        }

        @Override
        public String initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return "";
        }

        @Override
        public void run(I input, StateHandle<String, NoChange> state, ProcedureCtx ctx) {}
    }

    /** The same, with the one unresolvable token declared explicitly. */
    public static final class OverridingStillGeneric<I> extends StillGeneric<I> {
        @Override
        @SuppressWarnings("unchecked")
        public Class<I> inputType() {
            return (Class<I>) String.class;
        }
    }

    public static final class StatelessProcedure
            implements Procedure<String, String, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "stateless";
        }

        @Override
        public String initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, NoChange> state, ProcedureCtx ctx) {}
    }

    public record SampleChange(long delta) implements com.flezha.sdk.state.Change {}
}
