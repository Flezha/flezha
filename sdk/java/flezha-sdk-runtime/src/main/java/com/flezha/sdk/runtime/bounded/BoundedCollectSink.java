package com.flezha.sdk.runtime.bounded;

/**
 * Sink driven by {@link BoundedCollectAdapter} while a structured bounded
 * operation runs: inter-operation output is <strong>collected</strong> for
 * handoff to the next stage at the barrier; plugin-bound output is
 * <strong>routed</strong> (already serialized) to the operative's wire. The
 * bounded driver implements this per running segment. Mirrors the Rust SDK's
 * {@code BoundedCollectSink}.
 */
interface BoundedCollectSink {

    /** Collect an inter-operation output value for the next stage's input batch. */
    void collect(Object value);

    /** Forward an already-serialized plugin-bound output to the operative's wire (best-effort, at-least-once). */
    void emitPluginBytes(byte[] bytes);
}
