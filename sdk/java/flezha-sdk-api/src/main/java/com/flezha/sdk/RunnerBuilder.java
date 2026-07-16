package com.flezha.sdk;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.internal.RuntimeBridge;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import javax.annotation.Nonnull;

/**
 * Builder returned by {@link Flezha#newRunner(String)}. Declares operative
 * types ({@code define*}), then starts the runner ({@link #start()} /
 * {@link #run()}). At most one runner may be started per JVM — a second
 * {@code start()}/{@code run()} while one is active throws
 * {@link IllegalStateException} naming the running namespace, before
 * connecting or mutating any shared state.
 */
public final class RunnerBuilder {

    private final String namespace;

    RunnerBuilder(@Nonnull String namespace) {
        this.namespace = namespace;
    }

    /**
     * Declare an UnboundedOperative class the runtime can spawn instances of later
     * (via the controller's {@code AddOperative}). Calls the class's
     * {@code channels(...)} once and freezes the resulting topology in the
     * class registry. An invalid topology is reported here, at the declaring
     * call site, as {@code UnboundedOperativeConfigurationException}.
     *
     * @throws IllegalStateException if a runner has already started (the registry is frozen).
     * @throws IllegalArgumentException on a duplicate factory id.
     */
    @Nonnull
    public RunnerBuilder defineUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        rejectWhileRunning();
        UnboundedOperativeRegistry.registerUnbounded(operativeClass);
        return this;
    }

    /**
     * Declare a {@link BoundedOperative} class. Sibling of
     * {@link #defineUnbounded(Class)}; invalid authoring is reported here, at
     * the declaring call site.
     *
     * @throws IllegalStateException if a runner has already started (the registry is frozen).
     * @throws IllegalArgumentException on a factory id colliding with any declared class.
     */
    @Nonnull
    public RunnerBuilder defineBounded(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        rejectWhileRunning();
        BoundedOperativeRegistry.registerBounded(boundedClass);
        return this;
    }

    /**
     * A declaration while a runner is active can never take effect (the class
     * registries froze at its {@code start()}), so it fails here with the
     * running namespace — the same diagnosis a later {@code start()} would
     * give — instead of the registries' generic frozen message.
     */
    private static void rejectWhileRunning() {
        String running = Flezha.RUNNING_NAMESPACE.get();
        if (running != null) {
            throw new IllegalStateException("a Flezha runner is already running in this JVM for namespace '" + running
                    + "'; operative types must be declared before it starts");
        }
    }

    /**
     * Connect to the controller and return the JVM's runner handle. Blocks
     * only for the connect handshake (send init, wait for
     * {@code AssignRunnerId} — retried with exponential backoff until the
     * controller is reachable), never for the runner's ongoing lifetime.
     *
     * @throws IllegalStateException if a runner is already running in this JVM
     *     (names the running namespace), or if no operative type was declared.
     */
    @Nonnull
    public FlezhaRunner start() {
        // Claim the JVM-wide slot first: exactly one concurrent start() wins,
        // and the loser must not connect or mutate any shared state.
        if (!Flezha.RUNNING_NAMESPACE.compareAndSet(null, namespace)) {
            String running = Flezha.RUNNING_NAMESPACE.get();
            throw new IllegalStateException("a Flezha runner is already running in this JVM"
                    + (running != null ? " for namespace '" + running + "'" : "")
                    + "; at most one runner may be started per process — close() it before starting another");
        }
        try {
            if (UnboundedOperativeRegistry.isEmpty() && BoundedOperativeRegistry.isEmpty()) {
                throw new IllegalStateException("Cannot start the Flezha runner: no operative type declared. "
                        + "Call .defineUnbounded(MyUnboundedOperative.class) or .defineBounded(MyBoundedOperative.class) "
                        + "before .start().");
            }
            UnboundedOperativeRegistry.freeze();
            BoundedOperativeRegistry.freeze();

            FlezhaRunner runner = startRuntime();
            Flezha.CURRENT.set(runner);
            return runner;
        } catch (RuntimeException | Error e) {
            Flezha.releaseSlot();
            throw e;
        }
    }

    /**
     * Sugar for standalone binaries: {@link #start()} followed by
     * {@link FlezhaRunner#awaitTermination()}. Blocks the calling thread until
     * the runner shuts down — no keep-alive loop needed in {@code main()}.
     */
    public void run() throws InterruptedException {
        start().awaitTermination();
    }

    /**
     * Invoke the runtime implementation via {@link RuntimeBridge}. It lives in
     * {@code flezha-sdk-runtime} so this api module compiles standalone
     * (mirror of how the registries reach their runtime side). The
     * {@code Runnable} is the runtime's release hook for the JVM-wide slot,
     * run exactly once when the runner closes.
     */
    private FlezhaRunner startRuntime() {
        return (FlezhaRunner) RuntimeBridge.invoke(
                "com.flezha.sdk.runtime.FlezhaImpl",
                "start",
                new Class<?>[] {String.class, Runnable.class},
                "Failed to start the Flezha runner",
                namespace,
                (Runnable) Flezha::releaseSlot);
    }
}
