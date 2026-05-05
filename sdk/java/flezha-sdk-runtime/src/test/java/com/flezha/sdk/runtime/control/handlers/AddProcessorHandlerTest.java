package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ComputeEngineContext;
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

class AddProcessorHandlerTest {

    private static final String FACTORY_ID = "idempotent_test_factory";
    private static final String PROCESSOR_ID = "idempotent_p1";

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    @DisplayName("duplicate AddProcessor for the same processor_id is a no-op (no panic, no growth)")
    void duplicateAddProcessorIsNoOp() {
        RelayRegistry.registerRelay(IdempotentRelay.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddProcessor(AddProcessor.newBuilder()
                        .setProcessorId(PROCESSOR_ID)
                        .setFactoryId(FACTORY_ID)
                        .build())
                .build();

        AddProcessorHandler subject = new AddProcessorHandler();

        List<String> errorsFirst = new ArrayList<>();
        subject.handle(ctx, request, errorsFirst);
        assertThat(errorsFirst).isEmpty();
        assertThat(ctx.relayInstanceRegistry().contains(PROCESSOR_ID)).isTrue();
        assertThat(ctx.relayInstanceRegistry().size()).isEqualTo(1);

        List<String> errorsSecond = new ArrayList<>();
        subject.handle(ctx, request, errorsSecond);
        assertThat(errorsSecond)
                .as("idempotent path must not produce errors on duplicate ids")
                .isEmpty();
        assertThat(ctx.relayInstanceRegistry().size())
                .as("registry must hold exactly one entry after a duplicate AddProcessor")
                .isEqualTo(1);
    }

    public static final class IdempotentRelay implements Relay {
        @Override public String factoryId() { return FACTORY_ID; }

        @Override public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(IdempotentShard.class);
        }
    }

    public static final class IdempotentShard implements Shard<String, IdempotentShard.Empty, String> {
        public record Empty() {}

        @Override public String name() { return "idempotent_shard"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public Class<String> outputType() { return String.class; }
        @Override public Empty initState(Optional<String> shardKey) { return new Empty(); }
        @Override public void run(String input, Empty state, ShardCtx ctx) throws ShardException { }
    }
}
