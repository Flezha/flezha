package com.flezha.sdk.runtime.operative;

/**
 * How a checkpoint-ack Operative closes its interval, and what the open
 * interval does with the outputs its invocations produce.
 *
 * <p>The Java mirror of the core's {@code CheckpointPolicy}: Java declares it,
 * {@code flz_core} enforces it. Nothing here is accumulated on the Java side —
 * the interval's changes, timers and held outputs live in the core's staged
 * buffer.
 *
 * @param every close the interval once this many inputs have folded into it;
 *     {@code 0} leaves {@code ctx.checkpoint()} its only closer
 * @param outputsAtCheckpoint hold the interval's outputs and ship them inside
 *     its checkpoint record, instead of flushing each as it is produced
 * @param maxDelayMs close the interval this long after the input that opened
 *     it, so a quiet Entity never holds an output indefinitely; always set when
 *     {@code outputsAtCheckpoint} is true
 */
public record CheckpointPolicy(int every, boolean outputsAtCheckpoint, long maxDelayMs) {

    /** The declaration a bare {@code checkpointAck()} makes. */
    public static CheckpointPolicy defaults() {
        return new CheckpointPolicy(0, false, 0);
    }

    public CheckpointPolicy withEvery(int inputs) {
        return new CheckpointPolicy(inputs, outputsAtCheckpoint, maxDelayMs);
    }

    public CheckpointPolicy withOutputsAtCheckpoint(long delayMs) {
        return new CheckpointPolicy(every, true, delayMs);
    }
}
