package com.flezha.sdk;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.internal.RegistryCore;
import com.flezha.sdk.internal.RunnerHandle;
import com.flezha.sdk.internal.RuntimeBridge;
import com.flezha.sdk.operative.Computation;
import com.flezha.sdk.operative.UnboundedOperative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * This JVM's runner: create → mutate ({@code define*} / {@code remove*}) →
 * start on one object, returned by {@link Flezha#createRunner(String)}.
 * Operative types may be declared and removed before AND after
 * {@link #start()}; a post-start change ships the runner's full factory set
 * to the controller ({@code UpdateFactories} — the reconnect init frame stays
 * the resync of record).
 *
 * <p>At most one runner is started per JVM (the slot is claimed by CAS inside
 * {@link #start()}); reach the active one from code that does not control
 * startup ordering via {@link Flezha#currentRunner()}.
 *
 * <p>{@code FlezhaRunner} is {@link AutoCloseable}: {@link #close()} on a
 * started runner runs the graceful-shutdown sequence
 * (ship-fresh-state-on-sdk-shutdown), releases the singleton slot, and wakes
 * every thread blocked in {@link #awaitTermination()}; on a never-started
 * runner it is a no-op. The same sequence is registered as a JVM shutdown
 * hook so SIGTERM (Kubernetes pre-stop) triggers it without an explicit
 * {@code close()} call.
 *
 * <p>Method names and lifecycle verbs follow
 * {@code docs/sdk_naming_conventions.md} (repository root), mirrored 1:1 with
 * the Rust SDK's {@code FlezhaRunner}.
 */
public final class FlezhaRunner implements AutoCloseable {

    private final String namespace;

    /**
     * This runner's declared factory sets — per-runner (a later runner starts
     * empty; generation 2 must re-define its types). The runtime-side class
     * registries stay process-global behind these.
     */
    private final RegistryCore<UnboundedOperative> unbounded =
            new RegistryCore<>("com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry", "operative");

    private final RegistryCore<BoundedOperative> bounded =
            new RegistryCore<>("com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry", "bounded");

    private final AtomicReference<RunnerHandle> handle = new AtomicReference<>();

    /** Set once by the first {@link #start()}; a second start() is rejected forever. */
    private final AtomicBoolean everStarted = new AtomicBoolean(false);

    FlezhaRunner(@Nonnull String namespace) {
        this.namespace = namespace;
    }

    /** The namespace this runner was created for — its stable identity. */
    @Nonnull
    public String getNamespace() {
        return namespace;
    }

    /**
     * Declare an UnboundedOperative class the runtime can spawn instances of
     * later (via the controller's {@code AddOperative}). Calls the class's
     * {@code channels(...)} once and records the topology in the class
     * registry. Legal before and after {@link #start()}. An invalid topology
     * is reported here, at the declaring call site, as
     * {@code UnboundedOperativeConfigurationException}.
     *
     * @throws IllegalArgumentException on a duplicate factory id.
     */
    @Nonnull
    public FlezhaRunner defineUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        unbounded.register(operativeClass);
        syncFactoriesIfStarted();
        return this;
    }

    /**
     * Declare a {@link BoundedOperative} class. Sibling of
     * {@link #defineUnbounded(Class)}; invalid authoring is reported here, at
     * the declaring call site.
     *
     * @throws IllegalArgumentException on a factory id colliding with any declared class.
     */
    @Nonnull
    public FlezhaRunner defineBounded(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        bounded.register(boundedClass);
        syncFactoriesIfStarted();
        return this;
    }

    /**
     * Remove a declared UnboundedOperative class from this runner. Legal
     * before and after {@link #start()}.
     *
     * @throws IllegalArgumentException if the class's factory id is not
     *     currently defined on this runner.
     * @throws OperativeTypeInUseException if the runner is started and live
     *     operative instances of the factory exist — the type stays defined.
     */
    @Nonnull
    public FlezhaRunner removeUnbounded(@Nonnull Class<? extends UnboundedOperative> operativeClass) {
        return remove(unbounded, operativeClass);
    }

    /** Remove a declared {@link BoundedOperative} class. Sibling of {@link #removeUnbounded(Class)}. */
    @Nonnull
    public FlezhaRunner removeBounded(@Nonnull Class<? extends BoundedOperative> boundedClass) {
        return remove(bounded, boundedClass);
    }

    private <C extends Computation> FlezhaRunner remove(RegistryCore<C> core, Class<? extends C> cls) {
        String factoryId = core.definedFactoryIdOf(cls);
        RunnerHandle h = handle.get();
        if (h != null) {
            // Started: the runtime owns the race-free live-instance check
            // (unregister-first, re-register + throw on a live hit).
            h.removeType(factoryId);
            core.dropEntry(factoryId);
        } else {
            // Never started: no instance can exist; unregister directly.
            core.remove(factoryId);
        }
        syncFactoriesIfStarted();
        return this;
    }

    /**
     * Connect to the controller and return {@code this} for chaining. Blocks
     * only for the connect handshake (send init, wait for
     * {@code AssignRunnerId} — retried with exponential backoff until the
     * controller is reachable), never for the runner's ongoing lifetime.
     * Claims the JVM-wide singleton slot by compare-and-set; the loser fails
     * WITHOUT connecting, mutating shared state, or spawning a thread.
     * Starting with zero declared operative types is legal — the init frame
     * carries an empty factory list, and types may be defined later.
     *
     * @throws IllegalStateException if a runner is already running in this JVM
     *     (names the running namespace), or on a second {@code start()} of
     *     this runner.
     */
    @Nonnull
    public FlezhaRunner start() {
        if (!everStarted.compareAndSet(false, true)) {
            throw new IllegalStateException("this runner for namespace '" + namespace + "' was already started; "
                    + "a FlezhaRunner starts at most once");
        }
        // Claim the JVM-wide slot: exactly one concurrent start() wins, and
        // the loser must not connect or mutate any shared state.
        if (!Flezha.RUNNING_NAMESPACE.compareAndSet(null, namespace)) {
            everStarted.set(false);
            String running = Flezha.RUNNING_NAMESPACE.get();
            throw new IllegalStateException("a Flezha runner is already running in this JVM"
                    + (running != null ? " for namespace '" + running + "'" : "")
                    + "; at most one runner may be started per process — close() it before starting another");
        }
        try {
            handle.set(startRuntime());
            Flezha.CURRENT.set(this);
            return this;
        } catch (RuntimeException | Error e) {
            handle.set(null);
            everStarted.set(false);
            Flezha.releaseSlot();
            throw e;
        }
    }

    /**
     * Sugar for standalone binaries: {@link #start()} followed by
     * {@link #awaitTermination()}. Blocks the calling thread until the runner
     * shuts down — no keep-alive loop needed in {@code main()}.
     */
    public void run() throws InterruptedException {
        start().awaitTermination();
    }

    /**
     * Returns a snapshot of currently-spawned Operative instances keyed by
     * what the wire calls {@code operative_id}.
     *
     * @throws IllegalStateException on a never-started runner.
     */
    @Nonnull
    public Map<String, ?> getOperatives() {
        return started("getOperatives()").operatives();
    }

    /**
     * Block the calling thread until the runner shuts down — via
     * {@link #close()} from another thread, or SIGTERM-triggered graceful
     * shutdown. Replaces every hand-rolled keep-alive loop; a standalone
     * {@code main()} that wants this behaviour in one call uses {@link #run()}.
     *
     * @throws IllegalStateException on a never-started runner.
     */
    public void awaitTermination() throws InterruptedException {
        started("awaitTermination()").awaitTermination();
    }

    /**
     * Run the graceful-shutdown sequence (ship-fresh-state-on-sdk-shutdown),
     * release the JVM-wide runner slot, and wake every thread blocked in
     * {@link #awaitTermination()}. A no-op on a never-started runner;
     * idempotent across SIGTERM + explicit close.
     */
    @Override
    public void close() {
        RunnerHandle h = handle.get();
        if (h != null) {
            h.close();
        }
    }

    private RunnerHandle started(String what) {
        RunnerHandle h = handle.get();
        if (h == null) {
            throw new IllegalStateException(what + " requires a started runner — runner not started");
        }
        return h;
    }

    /** This runner's full current factory set: unbounded first, then bounded. */
    private List<String> declaredFactoryIds() {
        List<String> ids = new ArrayList<>(unbounded.factoryIds());
        ids.addAll(bounded.factoryIds());
        return ids;
    }

    /**
     * Ship the FULL current factory set to the controller after a post-start
     * define/remove — fire-and-forget; full-set semantics make redelivery
     * idempotent. A no-op on a never-started runner.
     */
    private void syncFactoriesIfStarted() {
        RunnerHandle h = handle.get();
        if (h != null) {
            h.syncFactories(declaredFactoryIds());
        }
    }

    /**
     * Invoke the runtime implementation via {@link RuntimeBridge}. It lives in
     * {@code flezha-sdk-runtime} so this api module compiles standalone. The
     * {@code Supplier} feeds the initial handshake AND every reconnect
     * re-handshake (so post-start mutations resync naturally); the
     * {@code Runnable} is the release hook for the JVM-wide slot, run exactly
     * once when the runner closes.
     */
    private RunnerHandle startRuntime() {
        return (RunnerHandle) RuntimeBridge.invoke(
                "com.flezha.sdk.runtime.FlezhaImpl",
                "start",
                new Class<?>[] {String.class, Supplier.class, Runnable.class},
                "Failed to start the Flezha runner",
                namespace,
                (Supplier<List<String>>) this::declaredFactoryIds,
                (Runnable) Flezha::releaseSlot);
    }
}
