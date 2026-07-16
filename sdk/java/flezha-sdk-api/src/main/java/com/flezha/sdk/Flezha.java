package com.flezha.sdk;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * SDK entry point. Obtain an instance via {@link #newInstance(String)}
 * after calling {@link #registerUnbounded(Class)} for every UnboundedOperative class the
 * compute engine supports.
 *
 * <p>The concrete implementation lives in {@code flezha-sdk-runtime}; this
 * interface + factory is the public surface user code sees.
 *
 * <p>{@link Flezha} is {@link AutoCloseable}: the {@link #close()}
 * method runs the graceful-shutdown sequence introduced by
 * ship-fresh-state-on-sdk-shutdown, shipping each running operative's
 * final state to the controller before tearing down. The same sequence is
 * registered as a JVM shutdown hook so SIGTERM (Kubernetes pre-stop)
 * triggers it without an explicit {@code close()} call.
 */
public interface Flezha extends AutoCloseable {

    /**
     * Register an UnboundedOperative class. Pass-through to
     * {@link UnboundedOperativeRegistry#registerUnbounded(Class)}.
     */
    static void registerUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        UnboundedOperativeRegistry.registerUnbounded(operativeClass);
    }

    /**
     * Register a {@link BoundedOperative} class. Sibling of {@link #registerUnbounded(Class)};
     * pass-through to {@link BoundedOperativeRegistry#registerBounded(Class)}. Makes the
     * bounded spawnable via {@code flz operative start <factory> --kind bounded}.
     */
    static void registerBounded(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        BoundedOperativeRegistry.registerBounded(boundedClass);
    }

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
     * Run the graceful-shutdown sequence: serialize each running
     * operative's state and ship it to the controller so the next
     * reconnecting runner picks up the runner's true final state
     * (instead of the up-to-{@code overwatch_state_snapshot_interval_ms}
     * stale periodic snapshot). Per-operative ack wait is bounded at
     * 2 seconds, parallelised across operatives.
     *
     * <p>Idempotent across SIGTERM + explicit close — the second caller
     * observes that shutdown has already started and returns immediately.
     */
    @Override
    void close();

    /**
     * Start the compute engine, connect to the controller, and block until the
     * controller sends {@code AssignRunnerId}. Thread-safe; may be called
     * at most once per JVM for a given namespace.
     *
     * @throws IllegalStateException if no UnboundedOperative classes have been registered.
     */
    @Nonnull
    static Flezha newInstance(@Nonnull String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        if (UnboundedOperativeRegistry.isEmpty() && BoundedOperativeRegistry.isEmpty()) {
            throw new IllegalStateException("Cannot start Flezha: no operative or bounded classes registered. "
                    + "Call Flezha.registerUnbounded(MyUnboundedOperative.class) or Flezha.registerBounded(MyBoundedOperative.class) "
                    + "before Flezha.newInstance(...).");
        }

        // The implementation class lives in flezha-sdk-runtime to avoid a
        // native / runtime dependency on the API module. We resolve it by
        // reflection so the api module compiles standalone.
        try {
            Class<?> impl = Class.forName("com.flezha.sdk.runtime.FlezhaImpl");
            var m = impl.getMethod("start", String.class);
            Object instance = m.invoke(null, namespace);
            return (Flezha) instance;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "com.flezha.sdk.runtime.FlezhaImpl not on the classpath. "
                            + "Make sure flezha-sdk-runtime is on the runtime classpath.",
                    e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Failed to start Flezha: " + cause.getMessage(), cause);
        }
    }
}
