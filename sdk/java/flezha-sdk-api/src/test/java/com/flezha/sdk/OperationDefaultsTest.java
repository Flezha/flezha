package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OperationDefaultsTest {

    @Test
    void compactAfterBytesDefaultsTo8Mib() {
        StatelessOperation operation = new StatelessOperation();
        // 8 MiB, in lockstep with Rust and the controller's default.
        assertThat(operation.compactAfterBytes()).isEqualTo(8388608L);
    }

    @Test
    void schemaVersionDefaultsToOne() {
        StatelessOperation operation = new StatelessOperation();
        assertThat(operation.schemaVersion()).isEqualTo(1);
    }

    @Test
    void kryoTypesDefaultsToEmpty() {
        StatelessOperation operation = new StatelessOperation();
        assertThat(operation.kryoTypes()).isEmpty();
    }

    @Test
    void fromConfigVoidInvokesNoArgCtor() {
        StatelessOperation operation = new StatelessOperation();
        Operation<String, String, String, NoChange, WallClock, Void> fresh = operation.fromConfig(null);
        assertThat(fresh).isNotNull();
        assertThat(fresh).isNotSameAs(operation);
        assertThat(fresh.name()).isEqualTo("stateless");
    }

    @Test
    void applyChangeWithNoChangeIsNoop() {
        StatelessOperation operation = new StatelessOperation();
        // Cannot construct NoChange (uninhabited); just confirm the call path
        // returns without throwing for the NoChange-typed Operation. We prove the
        // path with a null cast accepted by the type system because NoChange
        // is uninhabited.
        operation.applyChange("state", null);
    }

    @Test
    void applyChangeWithUserChangeWithoutOverrideThrows() {
        ChangeyOperationWithoutOverride operation = new ChangeyOperationWithoutOverride();
        assertThatThrownBy(() -> operation.applyChange("s", new SampleChange(1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not override applyChange");
    }

    public static final class StatelessOperation
            implements Operation<String, String, String, NoChange, WallClock, Void> {
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
        public String initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, NoChange> state, OperationCtx ctx) {}
    }

    public record SampleChange(long delta) implements com.flezha.sdk.state.Change {}

    public static final class ChangeyOperationWithoutOverride
            implements Operation<String, String, String, SampleChange, WallClock, Void> {
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
        public String initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, SampleChange> state, OperationCtx ctx) {}
    }
}
