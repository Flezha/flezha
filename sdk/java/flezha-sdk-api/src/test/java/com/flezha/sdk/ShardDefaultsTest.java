package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ShardDefaultsTest {

    @Test
    void compactAfterBytesDefaultsTo64Mib() {
        StatelessShard shard = new StatelessShard();
        assertThat(shard.compactAfterBytes()).isEqualTo(67108864L);
    }

    @Test
    void schemaVersionDefaultsToOne() {
        StatelessShard shard = new StatelessShard();
        assertThat(shard.schemaVersion()).isEqualTo(1);
    }

    @Test
    void kryoTypesDefaultsToEmpty() {
        StatelessShard shard = new StatelessShard();
        assertThat(shard.kryoTypes()).isEmpty();
    }

    @Test
    void fromConfigVoidInvokesNoArgCtor() {
        StatelessShard shard = new StatelessShard();
        Shard<String, String, String, NoChange, WallClock, Void> fresh = shard.fromConfig(null);
        assertThat(fresh).isNotNull();
        assertThat(fresh).isNotSameAs(shard);
        assertThat(fresh.name()).isEqualTo("stateless");
    }

    @Test
    void applyChangeWithNoChangeIsNoop() {
        StatelessShard shard = new StatelessShard();
        // Cannot construct NoChange (uninhabited); just confirm the call path
        // returns without throwing for the NoChange-typed Shard. We prove the
        // path with a null cast accepted by the type system because NoChange
        // is uninhabited.
        shard.applyChange("state", null);
    }

    @Test
    void applyChangeWithUserChangeWithoutOverrideThrows() {
        ChangeyShardWithoutOverride shard = new ChangeyShardWithoutOverride();
        assertThatThrownBy(() -> shard.applyChange("s", new SampleChange(1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not override applyChange");
    }

    public static final class StatelessShard implements Shard<String, String, String, NoChange, WallClock, Void> {
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
        public String initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, NoChange> state, ShardCtx ctx) {}
    }

    public record SampleChange(long delta) implements com.flezha.sdk.state.Change {}

    public static final class ChangeyShardWithoutOverride
            implements Shard<String, String, String, SampleChange, WallClock, Void> {
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
        public String initState(Optional<String> shardKey, Optional<RestoreInitPayload> initPayload) {
            return "init";
        }

        @Override
        public void run(String input, StateHandle<String, SampleChange> state, ShardCtx ctx) {}
    }
}
