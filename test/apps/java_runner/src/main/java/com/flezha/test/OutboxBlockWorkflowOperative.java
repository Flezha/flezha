package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.WorkflowOperative;
import java.nio.charset.StandardCharsets;

/**
 * Mirror of the Rust test app's {@code outbox_block_workflow}: the
 * transactional-outbox fixture. Proc A commits durable state AND emits
 * plugin-bound output on a hybrid channel — STAGED, so it reaches the sink only
 * after the controller has journaled and fsynced A's {@code proc_committed}
 * entry (with the output inside it) and flushed the outbox. Proc B blocks, so a
 * SIGKILL lands with A committed + flushed and B unfinished.
 */
public final class OutboxBlockWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "outbox_block_workflow";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(CommitBlockWorkflowOperative.CommitA.class)
                .named("mid")
                .codec()
                .sink("outbox_sink", s -> ("a-out:" + s).getBytes(StandardCharsets.UTF_8))
                .into(CommitBlockWorkflowOperative.BlockB.class);
    }
}
