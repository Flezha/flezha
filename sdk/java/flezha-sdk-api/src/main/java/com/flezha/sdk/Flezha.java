package com.flezha.sdk;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * SDK entry point. Obtain an instance via {@link #newInstance(String)}
 * after calling
 * {@link FactoryRegistry#registerFactory(String, ProcessorSettings, Object, java.util.function.Supplier)}
 * for every factory the compute engine supports.
 *
 * <p>The concrete implementation lives in {@code flezha-sdk-runtime}; this
 * interface + factory is the public surface user code sees.
 *
 * <p>{@link Flezha} is {@link AutoCloseable}: the {@link #close()}
 * method runs the graceful-shutdown sequence introduced by
 * ship-fresh-state-on-sdk-shutdown, shipping each running processor's
 * final state to the agent before tearing down. The same sequence is
 * registered as a JVM shutdown hook so SIGTERM (Kubernetes pre-stop)
 * triggers it without an explicit {@code close()} call.
 */
public interface Flezha extends AutoCloseable {

    /** Returns the compute-engine id assigned by the agent during handshake. */
    @Nonnull
    String getId();

    /** Returns a snapshot of currently-registered local processors keyed by id. */
    @Nonnull
    Map<String, ?> getProcessors();

    /**
     * Run the graceful-shutdown sequence: serialize each running
     * processor's state and ship it to the agent so the next
     * reconnecting operator picks up the operator's true final state
     * (instead of the up-to-{@code overwatch_state_snapshot_interval_ms}
     * stale periodic snapshot). Per-processor ack wait is bounded at
     * 2 seconds, parallelised across processors.
     *
     * <p>Idempotent across SIGTERM + explicit close — the second caller
     * observes that shutdown has already started and returns immediately.
     */
    @Override
    void close();

    /**
     * Start the compute engine, connect to the agent, and block until the
     * agent sends {@code AssignComputeEngineId}. Thread-safe; may be called
     * at most once per JVM for a given namespace.
     *
     * @throws IllegalStateException if no factories have been registered.
     */
    @Nonnull
    static Flezha newInstance(@Nonnull String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        if (FactoryRegistry.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot start Flezha: no factories registered. "
                            + "Call FactoryRegistry.registerFactory(...) before Flezha.newInstance(...).");
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
                            + "Make sure flezha-sdk-runtime is on the runtime classpath.", e);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IllegalStateException("Failed to start Flezha: " + cause.getMessage(), cause);
        }
    }
}
