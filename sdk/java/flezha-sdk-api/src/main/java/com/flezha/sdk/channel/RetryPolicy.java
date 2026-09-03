package com.flezha.sdk.channel;

/**
 * Per-channel retry policy applied by the consumer-side dispatcher when an
 * procedure invocation fails (thrown {@code ProcedureException} or
 * {@code RuntimeException}). After {@code maxAttempts} total attempts — or
 * immediately on an {@link com.flezha.sdk.procedure.ProcedureException.NonRetryable}
 * — the payload is parked to the controller's dead-letter queue and the
 * entity advances. Declared via {@code .retryMax(...)} /
 * {@code .retryBackoffMs(...)} on the channel builder. Mirrors the Rust
 * SDK's {@code RetryPolicy}.
 *
 * @param maxAttempts   total attempts (first run included); default 3
 * @param backoffBaseMs exponential backoff base in milliseconds: attempt n
 *                      sleeps {@code base * 2^(n-1)} (±50% jitter), capped
 *                      at {@link #RETRY_BACKOFF_MAX_MS}
 */
public record RetryPolicy(int maxAttempts, long backoffBaseMs) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(3, 50L);

    /** Cap on the per-attempt retry backoff, jitter included. */
    public static final long RETRY_BACKOFF_MAX_MS = 5_000L;
}
