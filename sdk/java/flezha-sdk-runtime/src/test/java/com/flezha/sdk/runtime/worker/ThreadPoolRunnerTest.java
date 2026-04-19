package com.flezha.sdk.runtime.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.InputTransformers;
import com.flezha.sdk.OutputTransformers;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ThreadPoolRunnerTest {

    @BeforeEach
    void reset() {
        FactoryRegistry.reset();
    }

    @Test
    @DisplayName("THREAD_POOL preserves per-processor input ordering under back-to-back enqueue")
    void stickyOrdering() throws InterruptedException {
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch done = new CountDownLatch(3);

        ProcessorHandler<Object, String, String> handler = (input, state) -> {
            order.add(input);
            done.countDown();
            return input;
        };

        ProcessorSettings<Object, String, String> settings = ProcessorSettings
                .<Object, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.returnText())
                .runner(ProcessorRunner.THREAD_POOL)
                .build();

        FactoryRegistry.registerFactory("ord", settings, new Object(), () -> handler);
        FactoryRegistry.freeze();

        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "4"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);

        ProcessorInstance<?, ?, ?> pi = ProcessorInstance.create("ord", "p1");
        pi.postCreate();
        ctx.processorRegistry().add(pi);

        DefaultProcessorDispatcher dispatcher = new DefaultProcessorDispatcher(ctx, 4);
        ctx.setProcessorDispatcher(dispatcher);

        dispatcher.enqueue("p1", new com.flezha.sdk.ProcessorInput.Text("a"), 0L);
        dispatcher.enqueue("p1", new com.flezha.sdk.ProcessorInput.Text("b"), 0L);
        dispatcher.enqueue("p1", new com.flezha.sdk.ProcessorInput.Text("c"), 0L);

        assertThat(done.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(order).containsExactly("a", "b", "c");
        dispatcher.shutdown();
    }
}
