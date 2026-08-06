package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        ChangeyProcedureWithoutOverride procedure = new ChangeyProcedureWithoutOverride();
        assertThatThrownBy(() -> procedure.applyChange("s", new SampleChange(1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not override applyChange");
    }

    public static final class StatelessProcedure
            implements Procedure<String, String, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "stateless";
        }

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
        public String initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, NoChange> state, ProcedureCtx ctx) {}
    }

    public record SampleChange(long delta) implements com.flezha.sdk.state.Change {}

    public static final class ChangeyProcedureWithoutOverride
            implements Procedure<String, String, String, SampleChange, WallClock, Void> {
        @Override
        public String name() {
            return "changey";
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<SampleChange> changeType() {
            return SampleChange.class;
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
        public String initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, SampleChange> state, ProcedureCtx ctx) {}
    }
}
