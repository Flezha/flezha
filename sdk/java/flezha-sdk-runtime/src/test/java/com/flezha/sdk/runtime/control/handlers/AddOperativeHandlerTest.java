package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.operative.ContinuousOperativeRegistry;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AddOperativeHandlerTest {

    private static final String FACTORY_ID = "idempotent_test_factory";
    private static final String OPERATIVE_ID = "idempotent_p1";

    @BeforeEach
    void reset() {
        ContinuousOperativeRegistry.reset();
    }

    @Test
    @DisplayName("duplicate AddOperative for the same operative_id is a no-op (no panic, no growth)")
    void duplicateAddContinuousOperativeIsNoOp() {
        ContinuousOperativeRegistry.registerContinuous(IdempotentContinuousOperative.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ExecutorContext ctx = new ExecutorContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddOperative(AddOperative.newBuilder()
                        .setOperativeId(OPERATIVE_ID)
                        .setFactoryId(FACTORY_ID)
                        .build())
                .build();

        AddOperativeHandler subject = new AddOperativeHandler();

        List<String> errorsFirst = new ArrayList<>();
        subject.handle(ctx, request, errorsFirst);
        assertThat(errorsFirst).isEmpty();
        assertThat(ctx.operativeInstanceRegistry().contains(OPERATIVE_ID)).isTrue();
        assertThat(ctx.operativeInstanceRegistry().size()).isEqualTo(1);

        List<String> errorsSecond = new ArrayList<>();
        subject.handle(ctx, request, errorsSecond);
        assertThat(errorsSecond)
                .as("idempotent path must not produce errors on duplicate ids")
                .isEmpty();
        assertThat(ctx.operativeInstanceRegistry().size())
                .as("registry must hold exactly one entry after a duplicate AddOperative")
                .isEqualTo(1);
    }

    public static final class IdempotentContinuousOperative implements ContinuousOperative {
        @Override
        public String factoryId() {
            return FACTORY_ID;
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(IdempotentOperation.class);
        }
    }

    public static final class IdempotentOperation
            implements Operation<
                    String,
                    IdempotentOperation.Empty,
                    String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock,
                    Void> {
        public record Empty() {}

        @Override
        public String name() {
            return "idempotent_operation";
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
                Optional<String> operationKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<Empty, com.flezha.sdk.state.NoChange> state,
                OperationCtx ctx)
                throws OperationException {}
    }
}
