package com.flezha.sdk.runtime.control;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SDK's single controller-ack rendezvous: an SDK-originated command
 * registers a pending ack under its correlation id, ships its frame, and blocks
 * until the controller's reply is routed back here.
 *
 * <p>Every ack-bearing exchange goes through this — the graceful-shutdown
 * {@code RunnerShutdownAck}, the dead-letter {@code ParkPayloadAck}, and the
 * bounded {@code ProcCommitResponse} / {@code IterCommitResponse} barriers.
 *
 * <p><b>Callers MUST register before sending.</b> That ordering is what makes
 * the registry race-free: a reply can never arrive before its future exists, so
 * there is no early-ack to retain and no entry to leak.
 */
public final class AckRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(AckRegistry.class);

    private static final Map<String, CompletableFuture<String>> PENDING = new ConcurrentHashMap<>();

    private AckRegistry() {}

    /**
     * Register a pending ack. Call this <em>before</em> writing the frame that
     * solicits it. Pair with {@link #forget} on any path that abandons the wait.
     */
    @Nonnull
    public static CompletableFuture<String> register(@Nonnull String correlationId) {
        CompletableFuture<String> future = new CompletableFuture<>();
        PENDING.put(correlationId, future);
        return future;
    }

    /**
     * Complete the wait registered under {@code correlationId}. Routed from
     * {@code CommandRouter} when the controller's reply arrives. An unknown id
     * means the waiter already gave up; it is dropped, not an error.
     */
    public static void complete(@Nonnull String correlationId) {
        complete(correlationId, "");
    }

    /**
     * Complete the wait with what the reply reported beyond "it worked" — the
     * resolved outcome of a {@code SpawnBoundedResponse}. Bare acks carry an
     * empty value.
     */
    public static void complete(@Nonnull String correlationId, @Nonnull String value) {
        CompletableFuture<String> future = take(correlationId);
        if (future != null) {
            future.complete(value);
        }
    }

    /** Complete the wait with the failure the reply reported. */
    public static void fail(@Nonnull String correlationId, @Nonnull Throwable error) {
        CompletableFuture<String> future = take(correlationId);
        if (future != null) {
            future.completeExceptionally(error);
        }
    }

    private static CompletableFuture<String> take(String correlationId) {
        CompletableFuture<String> future = PENDING.remove(correlationId);
        if (future == null) {
            LOG.debug("ack for unknown correlation id {} — the waiter timed out or never registered", correlationId);
        }
        return future;
    }

    /** Abandon a registered wait (write failed, or the caller gave up). */
    public static void forget(@Nonnull String correlationId) {
        PENDING.remove(correlationId);
    }

    /**
     * Block on a registered ack. Returns {@code true} once the controller
     * acked, {@code false} on timeout (the registration is dropped) or if the
     * wait is interrupted (the thread's interrupt flag is restored).
     */
    public static boolean await(
            @Nonnull String correlationId, @Nonnull CompletableFuture<String> future, long timeoutMs) {
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            forget(correlationId);
            return false;
        } catch (Exception e) {
            forget(correlationId);
            LOG.warn("ack wait for {} failed: {}", correlationId, e.toString());
            return false;
        }
    }

    /** Test-only: drop all pending waits so independent tests do not interfere. */
    public static void resetForTests() {
        PENDING.clear();
    }
}
