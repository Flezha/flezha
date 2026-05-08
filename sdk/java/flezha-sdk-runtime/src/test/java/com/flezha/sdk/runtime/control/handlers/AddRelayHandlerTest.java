package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddRelay;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AddRelayHandlerTest {

    private static final String FACTORY_ID = "idempotent_test_factory";
    private static final String RELAY_ID = "idempotent_p1";

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    @DisplayName("duplicate AddRelay for the same relay_id is a no-op (no panic, no growth)")
    void duplicateAddRelayIsNoOp() {
        RelayRegistry.registerRelay(IdempotentRelay.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddRelay(AddRelay.newBuilder()
                        .setRelayId(RELAY_ID)
                        .setFactoryId(FACTORY_ID)
                        .build())
                .build();

        AddRelayHandler subject = new AddRelayHandler();

        List<String> errorsFirst = new ArrayList<>();
        subject.handle(ctx, request, errorsFirst);
        assertThat(errorsFirst).isEmpty();
        assertThat(ctx.relayInstanceRegistry().contains(RELAY_ID)).isTrue();
        assertThat(ctx.relayInstanceRegistry().size()).isEqualTo(1);

        List<String> errorsSecond = new ArrayList<>();
        subject.handle(ctx, request, errorsSecond);
        assertThat(errorsSecond)
                .as("idempotent path must not produce errors on duplicate ids")
                .isEmpty();
        assertThat(ctx.relayInstanceRegistry().size())
                .as("registry must hold exactly one entry after a duplicate AddRelay")
                .isEqualTo(1);
    }

    public static final class IdempotentRelay implements Relay {
        @Override
        public String factoryId() {
            return FACTORY_ID;
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(IdempotentShard.class);
        }
    }

    public static final class IdempotentShard
            implements Shard<
                    String,
                    IdempotentShard.Empty,
                    String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock,
                    Void> {
        public record Empty() {}

        @Override
        public String name() {
            return "idempotent_shard";
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
        public Class<com.flezha.sdk.state.NoChange> changeType() {
            return com.flezha.sdk.state.NoChange.class;
        }

        @Override
        public Class<com.flezha.sdk.state.WallClock> clockType() {
            return com.flezha.sdk.state.WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Empty initState(
                Optional<String> shardKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<Empty, com.flezha.sdk.state.NoChange> state,
                ShardCtx ctx)
                throws ShardException {}
    }
}
