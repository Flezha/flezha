package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.LoopMode;
import com.flezha.sdk.operative.WorkflowOperative;
import java.time.Duration;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code chat_loop_workflow}: the durable-loop +
 * suspension shape. One iteration per inbox item — proc TURN commits the turn to
 * Entity state and emits it, proc REPLY emits the answer to the sink. Between
 * turns the loop finds an empty inbox at a committed boundary and PARKS.
 */
public final class ChatLoopWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "chat_loop_workflow";
    }

    @Override
    public LoopMode loopMode() {
        return LoopMode.PER_INPUT;
    }

    /**
     * A chat session ends on an external stop, not on its own; the duration
     * backstop keeps a stray e2e instance from outliving the test run.
     */
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
        c.input(String.class, "in")
                .into(ChatSteps.Turn.class)
                .named("mid")
                .into(ChatSteps.Reply.class)
                .sink("out");
    }
}
