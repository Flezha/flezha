package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.proto.AddOperative;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
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
        UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("unknown kind literal is rejected with an error and nothing is registered")
    void unknownKindIsRejected() {
        UnboundedOperativeClassRegistry.registerClass(IdempotentUnboundedOperative.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddOperative(AddOperative.newBuilder()
                        .setOperativeId("kind_reject_p1")
                        .setFactoryId(FACTORY_ID)
                        .setKind("continuous")
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        new AddOperativeHandler().handle(ctx, request, errors);

        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).contains("unknown compute-unit kind 'continuous'");
        assertThat(ctx.operativeInstanceRegistry().contains("kind_reject_p1")).isFalse();
    }

    @Test
    @DisplayName("an unbounded class declaring nothing replies with zero bytes (pre-declaration behaviour)")
    void classDeclaringNothingRepliesWithZeroBytes() {
        UnboundedOperativeClassRegistry.registerClass(IdempotentUnboundedOperative.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("ref")
                .setAddOperative(AddOperative.newBuilder()
                        .setOperativeId("declares_nothing_p1")
                        .setFactoryId(FACTORY_ID)
                        .build())
                .build();

        List<String> errors = new ArrayList<>();
        byte[] rawData = new AddOperativeHandler().handle(ctx, request, errors);

        assertThat(errors).isEmpty();
        // The controller reads zero bytes as "declared nothing" — a class that
        // opts into no capability must stay byte-identical to a pre-declaration
        // build, so turning the map on changes nothing for existing classes.
        assertThat(rawData).isEmpty();
    }

    @Test
    @DisplayName("duplicate AddOperative for the same operative_id is a no-op (no panic, no growth)")
    void duplicateAddUnboundedOperativeIsNoOp() {
        UnboundedOperativeClassRegistry.registerClass(IdempotentUnboundedOperative.class);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
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

    public static final class IdempotentUnboundedOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return FACTORY_ID;
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(IdempotentProcedure.class);
        }
    }

    public static final class IdempotentProcedure
            implements Procedure<
                    String,
                    IdempotentProcedure.Empty,
                    String,
                    com.flezha.sdk.state.NoChange,
                    com.flezha.sdk.state.WallClock,
                    Void> {
        public record Empty() {}

        @Override
        public String name() {
            return "idempotent_procedure";
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
                Optional<String> entityKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(
                String input,
                com.flezha.sdk.state.StateHandle<Empty, com.flezha.sdk.state.NoChange> state,
                ProcedureCtx ctx)
                throws ProcedureException {}
    }
}
