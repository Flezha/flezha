package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;

/**
 * Mirror of the Rust test app's {@code CheckpointBatchedOperative}: the same
 * procedure as {@link CheckpointStoreStreamOperative} declaring
 * {@code outputsAtCheckpoint}, so the interval's outputs ship inside its
 * checkpoint record. No {@code checkpointEvery} — the test drives the close
 * itself, with {@code commit:} for the batch case and the delay alone for the
 * quiet-Entity case.
 */
public final class CheckpointBatchedStoreStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "checkpoint_batched_store";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.effectivelyOnce();
        c.checkpointAck();
        c.outputsAtCheckpoint(1_500);
        c.input(String.class, "input")
                .into(CheckpointStoreProcedure.class)
                .keyed()
                .sink("sink");
    }
}
