package com.flezha.sdk;

/**
 * Choice of how a processor's {@code process(...)} invocations are scheduled.
 * Matches the Rust SDK enum of the same name.
 *
 * <ul>
 *   <li>{@link #THREAD_POOL} — default. A shared bounded executor sized
 *   by {@code FLEZHA_WORKER_POOL_SIZE} (defaulting to
 *   {@code Runtime.availableProcessors()}). The runtime enforces sticky
 *   per-processor ordering: two invocations on the same processor never
 *   overlap.</li>
 *   <li>{@link #THREAD_SPAWN} — a fresh virtual thread per invocation.
 *   Useful for handlers with long I/O waits; does not preserve ordering
 *   across invocations of the same processor.</li>
 * </ul>
 */
public enum ProcessorRunner {
    THREAD_POOL,
    THREAD_SPAWN
}
