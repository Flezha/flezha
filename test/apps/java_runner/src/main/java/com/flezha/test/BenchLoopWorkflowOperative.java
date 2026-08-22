package com.flezha.test;

import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.workflow.LoopMode;
import java.time.Duration;
import java.util.Optional;

/**
 * The chat-loop bench fixture ({@code mix bench.workflow --scenario turns}): a
 * durable {@link LoopMode.PER_INPUT} chain, one iteration per inbox turn, its
 * reply going to {@code bench_sink}. Mirrors the Rust {@code bench_loop_workflow}.
 */
public final class BenchLoopWorkflowOperative implements WorkflowOperative {
    @Override
    public String factoryId() {
        return "bench_loop_workflow";
    }

    @Override
    public LoopMode loopMode() {
        return LoopMode.PER_INPUT;
    }

    @Override
    public boolean selfTerminating() {
        return false;
    }

    @Override
    public Optional<Duration> maxDuration() {
        return Optional.of(Duration.ofHours(24));
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(BenchSteps.Turn.class)
                .named("mid")
                .codec()
                .into(BenchSteps.Reply.class)
                .named("out")
                .sink("bench_sink");
    }
}
