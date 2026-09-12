package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.WorkflowOperative;

/**
 * Mirror of the Rust test app's {@code sync_durable_workflow}: the
 * {@link CommitBlockWorkflowOperative} shape declaring SYNCHRONOUS remote
 * durability. The controller must have proc A's journal barrier on the
 * durability backend before it acks the commit — which is what lets the
 * kill-the-disk drill destroy the whole state directory and still resume with
 * proc A committed rather than re-run.
 */
public final class SyncDurableWorkflowOperative implements WorkflowOperative {

    @Override
    public String factoryId() {
        return "sync_durable_workflow";
    }

    @Override
    public boolean syncDurability() {
        return true;
    }

    @Override
    public void procedures(ChannelBuilder c) {
        c.input(String.class, "in")
                .into(CommitBlockWorkflowOperative.CommitA.class)
                .named("mid")
                .into(CommitBlockWorkflowOperative.BlockB.class);
    }
}
