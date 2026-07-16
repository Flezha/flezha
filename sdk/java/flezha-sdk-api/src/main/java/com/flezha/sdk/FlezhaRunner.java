package com.flezha.sdk;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Handle to this JVM's running Flezha runner, returned by
 * {@link RunnerBuilder#start()}. At most one runner runs per JVM (see
 * {@link RunnerBuilder#start()}'s singleton guard); reach the active one from
 * code that does not control startup ordering via {@link Flezha#currentRunner()}.
 *
 * <p>{@code FlezhaRunner} is {@link AutoCloseable}: {@link #close()} runs the
 * graceful-shutdown sequence (ship-fresh-state-on-sdk-shutdown), shipping each
 * running operative's final state to the controller before tearing down. The
 * same sequence is registered as a JVM shutdown hook so SIGTERM (Kubernetes
 * pre-stop) triggers it without an explicit {@code close()} call.
 *
 * <p>Method names and lifecycle verbs follow
 * {@code docs/sdk_naming_conventions.md} (repository root), mirrored 1:1 with
 * the Rust SDK's {@code FlezhaRunner}.
 */
public interface FlezhaRunner extends AutoCloseable {

    /** Returns the runner id assigned by the controller during handshake. */
    @Nonnull
    String getId();

    /**
     * Returns a snapshot of currently-spawned UnboundedOperative instances keyed by what
     * the wire calls {@code operative_id}.
     */
    @Nonnull
    Map<String, ?> getOperatives();

    /**
     * Block the calling thread until the runner shuts down — via
     * {@link #close()} from another thread, or SIGTERM-triggered graceful
     * shutdown. Replaces every hand-rolled keep-alive loop
     * ({@code Thread.currentThread().join()} and friends); a standalone
     * {@code main()} that wants this behaviour in one call uses
     * {@link RunnerBuilder#run()}.
     */
    void awaitTermination() throws InterruptedException;

    /**
     * Run the graceful-shutdown sequence: serialize each running
     * operative's state and ship it to the controller so the next
     * reconnecting runner picks up the runner's true final state
     * (instead of the up-to-{@code overwatch_state_snapshot_interval_ms}
     * stale periodic snapshot). Per-operative ack wait is bounded at
     * 2 seconds, parallelised across operatives.
     *
     * <p>Releases the JVM-wide runner slot (a new runner may be started
     * afterwards) and wakes every thread blocked in {@link #awaitTermination()}.
     *
     * <p>Idempotent across SIGTERM + explicit close — the second caller
     * observes that shutdown has already started and returns immediately.
     */
    @Override
    void close();
}
