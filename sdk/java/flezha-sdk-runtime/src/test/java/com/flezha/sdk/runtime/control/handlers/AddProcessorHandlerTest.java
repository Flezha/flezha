package com.flezha.sdk.runtime.control.handlers;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.InputTransformers;
import com.flezha.sdk.OutputTransformers;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;
import com.flezha.sdk.proto.AddProcessor;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AddProcessorHandlerTest {

    private static final String FACTORY_ID = "idempotent_test_factory";
    private static final String PROCESSOR_ID = "idempotent_p1";

    @BeforeEach
    void reset() {
        FactoryRegistry.reset();
    }

    @Test
    @DisplayName("duplicate AddProcessor for the same processor_id is a no-op (no panic, no growth)")
    void duplicateAddProcessorIsNoOp() {
        ProcessorHandler<Object, String, String> handler = (input, state) -> input;

        ProcessorSettings<Object, String, String> settings = ProcessorSettings
                .<Object, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.returnText())
                .runner(ProcessorRunner.THREAD_SPAWN)
                .build();

        FactoryRegistry.registerFactory(FACTORY_ID, settings, new Object(), () -> handler);

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);

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
        assertThat(ctx.processorRegistry().contains(PROCESSOR_ID)).isTrue();
        assertThat(ctx.processorRegistry().size()).isEqualTo(1);

        List<String> errorsSecond = new ArrayList<>();
        subject.handle(ctx, request, errorsSecond);
        assertThat(errorsSecond)
                .as("idempotent path must not produce errors on duplicate ids")
                .isEmpty();
        assertThat(ctx.processorRegistry().size())
                .as("registry must hold exactly one entry after a duplicate AddProcessor")
                .isEqualTo(1);
    }
}
