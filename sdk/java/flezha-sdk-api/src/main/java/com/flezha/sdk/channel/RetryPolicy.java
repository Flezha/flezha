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

    /**
     * Backoff before {@code attempt} (1-based): {@code base × 2^(attempt-1)} with
     * ±50% jitter, capped at {@link #RETRY_BACKOFF_MAX_MS}.
     *
     * <p>Lives here because BOTH retry ladders — the unbounded dead-letter one and
     * the bounded entity one — resolve onto this policy, so they share one
     * backoff curve rather than two that can drift apart.
     */
    public long backoffFor(int attempt) {
        long exp = Math.min(Math.max(backoffBaseMs, 1) << Math.min(attempt - 1, 16), RETRY_BACKOFF_MAX_MS);
        long jittered =
                exp / 2 + java.util.concurrent.ThreadLocalRandom.current().nextLong(Math.max(exp, 1));
        return Math.min(jittered, RETRY_BACKOFF_MAX_MS);
    }
}
