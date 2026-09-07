package com.flezha.sdk.operative;

/** How many times a structured workflow Operative runs its chain. */
public enum LoopMode {

    /** One pass, then done. The default, and byte-identical to the pre-loop behaviour. */
    ONCE,

    /**
     * One iteration per inbox item — a chat turn, a command, a request. The loop
     * waits between iterations and parks when idle, rather than completing.
     */
    PER_INPUT,

    /**
     * Each iteration is seeded by the previous iteration's terminal handoff, so
     * the chain feeds itself until it converges.
     *
     * <p>A durable {@code CHAINED} loop requires a codec on its terminal proc's
     * output channel: a crash-resumed iteration re-seeds FROM that journaled
     * handoff, and without one it would re-seed empty — which the loop would read
     * as "converged" and stop, silently truncating the run.
     */
    CHAINED
}
