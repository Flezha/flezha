package com.flezha.sdk.runtime.bounded;

/**
 * Sink driven by {@link BoundedCollectAdapter} while a structured bounded
 * procedure runs: inter-procedure output is <strong>collected</strong> for
 * handoff to the next stage at the barrier; plugin-bound output is
 * <strong>routed</strong> (already serialized) to the operative's wire. The
 * bounded driver implements this per running entity. Mirrors the Rust SDK's
 * {@code BoundedCollectSink}.
 */
interface BoundedCollectSink {

    /** Collect an inter-procedure output value for the next stage's input batch. */
    void collect(Object value);

    /**
     * Hand an already-serialized plugin-bound output to the sink.
     *
     * <p>A durable structured class <b>stages</b> it: the bytes ride the next
     * {@code ProcCommit}'s outbox, so the output becomes durable in the SAME record
     * as the proc that produced it. An opted-out or transport-less class forwards
     * direct, as before.
     *
     * <p>The channel name is carried because a staged output has to be replayable
     * — the controller flushes the outbox by channel.
     */
    void emitPluginBytes(String channel, byte[] bytes);
}
