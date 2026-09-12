package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.LoopMode;
import com.flezha.sdk.operative.WorkflowOperative;
import java.time.Duration;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code stateless_loop_workflow}: the same durable
 * PER_INPUT loop as {@link ChatLoopWorkflowOperative} minus its stateful first
 * procedure. Nothing writes an Entity WAL, so the controller ships no
 * {@code RestoreInitState} on a rehydrating bind — the instance's proc-journal
 * reaches the driver only if it rides the bind itself.
 */
public final class StatelessLoopWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "stateless_loop_workflow";
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
        return Optional.of(Duration.ofSeconds(600));
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in").into(ChatSteps.Reply.class).sink("out");
    }
}
