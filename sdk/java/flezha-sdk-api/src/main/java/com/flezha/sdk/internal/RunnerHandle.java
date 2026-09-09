package com.flezha.sdk.internal;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * SPI the runtime's {@code FlezhaImpl} implements and the public
 * {@code FlezhaRunner} holds — the ONE seam between the api module's concrete
 * runner handle and the runtime's connect/lifecycle machinery (resolved via
 * {@link RuntimeBridge}).
 *
 * <p><b>Internal.</b> Not part of the SDK's public API; user code never calls
 * this.
 */
public interface RunnerHandle {

    /** The controller-assigned per-session runner id (rotates across reconnects). */
    @Nonnull
    String runnerId();

    /** Snapshot of currently-spawned Operative instances keyed by {@code operative_id}. */
    @Nonnull
    Map<String, ?> operatives();

    /** Block until the runner shuts down. */
    void awaitTermination() throws InterruptedException;

    /** Graceful shutdown; idempotent; runs the api layer's slot-release hook. */
    void close();

    /**
     * Remove {@code factoryId}'s class from the runtime class registries —
     * unregister-first, then scan live instances; a live hit re-registers the
     * class and throws {@link com.flezha.sdk.OperativeTypeInUseException}
     * (the transient unregistered window is benign: a failing
     * {@code AddOperative} is re-placed by convergence).
     */
    void removeType(String factoryId);

    /**
     * Declare the runner's FULL current factory set to the core after a
     * post-start define/remove. The set reaches the controller on the next
     * connect/reconnect init frame; the ABI has no entry point that pushes it
     * to a live controller.
     */
    void syncFactories(List<String> factoryIds);

    /**
     * Run one administration verb against the controller's published consumer
     * surface, as the identity this connection proved. Answers the published
     * function's own result as JSON.
     */
    @Nonnull
    String adminCall(String verb, String args);
}
