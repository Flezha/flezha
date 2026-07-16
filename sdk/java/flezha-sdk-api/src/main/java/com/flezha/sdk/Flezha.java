package com.flezha.sdk;

import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * Static-only entry point of the SDK: {@link #newRunner(String)} to build and
 * start this JVM's runner, {@link #currentRunner()} to reach it from code
 * that does not control startup ordering.
 *
 * <pre>{@code
 * Flezha.newRunner("my_app")
 *     .defineUnbounded(MyOperative.class)
 *     .run(); // .start() instead to keep the calling thread
 * }</pre>
 *
 * <p>At most one runner runs per JVM. A second {@code start()}/{@code run()}
 * while one is active throws {@link IllegalStateException} naming the running
 * namespace; after {@link FlezhaRunner#close()} a new runner may be started.
 *
 * <p>Method names and lifecycle verbs follow
 * {@code docs/sdk_naming_conventions.md} (repository root), mirrored 1:1 with
 * the Rust SDK's {@code Flezha}.
 */
public final class Flezha {

    /**
     * The one runner slot this JVM has. {@code RUNNING_NAMESPACE} is the
     * singleton guard ({@link RunnerBuilder#start()} claims it via CAS);
     * {@code CURRENT} backs {@link #currentRunner()}.
     */
    static final AtomicReference<String> RUNNING_NAMESPACE = new AtomicReference<>();

    static final AtomicReference<FlezhaRunner> CURRENT = new AtomicReference<>();

    private Flezha() {}

    /**
     * Begin building this JVM's runner for {@code namespace}. Declare every
     * operative class with {@link RunnerBuilder#defineUnbounded(Class)} /
     * {@link RunnerBuilder#defineBounded(Class)}, then
     * {@link RunnerBuilder#start()} (keep the calling thread) or
     * {@link RunnerBuilder#run()} (block until shutdown).
     */
    @Nonnull
    public static RunnerBuilder newRunner(@Nonnull String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        return new RunnerBuilder(namespace);
    }

    /**
     * The JVM's active runner, if one has been started and not yet closed —
     * {@link Optional#empty()} otherwise. Never throws and never starts a
     * runner as a side effect.
     */
    @Nonnull
    public static Optional<FlezhaRunner> currentRunner() {
        return Optional.ofNullable(CURRENT.get());
    }

    /**
     * Release the JVM-wide runner slot: clears the guard and the current
     * handle, and unfreezes the class registries so a later runner may declare
     * new operative types. Runs when the active runner closes (via the
     * {@code onTerminated} hook {@link RunnerBuilder} hands the runtime) and
     * when {@code start()} fails mid-flight.
     */
    static void releaseSlot() {
        CURRENT.set(null);
        RUNNING_NAMESPACE.set(null);
        UnboundedOperativeRegistry.unfreeze();
        BoundedOperativeRegistry.unfreeze();
    }
}
