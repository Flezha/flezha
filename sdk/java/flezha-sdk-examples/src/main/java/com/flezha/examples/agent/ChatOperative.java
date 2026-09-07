package com.flezha.examples.agent;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.LoopMode;
import com.flezha.sdk.operative.WorkflowOperative;
import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Wakes on each message, and once a day regardless. */
public final class ChatOperative implements WorkflowOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "chat_agent";
    }

    /** One iteration per inbound message. */
    @Override
    @Nonnull
    public LoopMode loopMode() {
        return LoopMode.PER_INPUT;
    }

    /** Wake on a timer even with no message, for a nudge or a summary. */
    @Override
    @Nonnull
    public Optional<Duration> wakeAfter() {
        return Optional.of(Duration.ofHours(24));
    }

    @Override
    public void procedures(@Nonnull ChannelBuilder c) {
        c.input(String.class, "message")
                .into(ReplyProcedure.class)
                .named("reply")
                .codec()
                .sink("chat_sink");
    }
}
