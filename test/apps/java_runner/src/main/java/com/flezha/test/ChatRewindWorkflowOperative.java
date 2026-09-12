package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.LoopMode;
import com.flezha.sdk.operative.WorkflowOperative;
import java.time.Duration;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code chat_rewind_workflow}: the same chat loop,
 * but its replying procedure can be undone. Unwinding in reverse iteration order
 * retracts the turns in reverse.
 */
public final class ChatRewindWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "chat_rewind_workflow";
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
        c.input(String.class, "in")
                .into(ChatSteps.Turn.class)
                .named("mid")
                .into(ChatSteps.ReplyUndoable.class)
                .named("out")
                .sink("chat_sink");
    }
}
