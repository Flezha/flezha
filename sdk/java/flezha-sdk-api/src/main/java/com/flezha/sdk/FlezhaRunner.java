package com.flezha.sdk;

import com.flezha.sdk.internal.RegistryCore;
import com.flezha.sdk.internal.RunnerHandle;
import com.flezha.sdk.internal.RuntimeBridge;
import com.flezha.sdk.operative.Computation;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.workflow.WorkflowOperative;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    /** The runtime class backing the entry-idle surface, resolved reflectively. */
    private static final String WATCHER = "com.flezha.sdk.runtime.EntryIdleWatcher";

    private final String namespace;

    /**
     * This runner's declared factory sets — per-runner (a later runner starts
     * empty; generation 2 must re-define its types). The runtime-side class
     * registries stay process-global behind these.
     */
    private final RegistryCore<StreamOperative> stream =
            new RegistryCore<>("com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry", "operative");

    private final RegistryCore<WorkflowOperative> workflow =
            new RegistryCore<>("com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry", "workflow");

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
     * Declare a StreamOperative class the runtime can spawn instances of
     * later (via the controller's {@code AddOperative}). Calls the class's
     * {@code channels(...)} once and records the topology in the class
     * registry. Legal before and after {@link #start()}. An invalid topology
     * is reported here, at the declaring call site, as
     * {@code StreamOperativeConfigurationException}.
     *
     * @throws IllegalArgumentException on a duplicate factory id.
     */
    @Nonnull
    public FlezhaRunner defineStream(@Nonnull Class<? extends StreamOperative> operativeClass) {
        stream.register(operativeClass);
        syncFactoriesIfStarted();
        return this;
    }

    /**
     * Declare a {@link WorkflowOperative} class. Sibling of
     * {@link #defineStream(Class)}; invalid authoring is reported here, at
     * the declaring call site.
     *
     * @throws IllegalArgumentException on a factory id colliding with any declared class.
     */
    @Nonnull
    public FlezhaRunner defineWorkflow(@Nonnull Class<? extends WorkflowOperative> workflowClass) {
        workflow.register(workflowClass);
        syncFactoriesIfStarted();
        return this;
    }

    /**
     * Remove a declared StreamOperative class from this runner. Legal
     * before and after {@link #start()}.
     *
     * @throws IllegalArgumentException if the class's factory id is not
     *     currently defined on this runner.
     * @throws OperativeTypeInUseException if the runner is started and live
     *     operative instances of the factory exist — the type stays defined.
     */
    @Nonnull
    public FlezhaRunner removeStream(@Nonnull Class<? extends StreamOperative> operativeClass) {
        return remove(stream, operativeClass);
    }

    /** Remove a declared {@link WorkflowOperative} class. Sibling of {@link #removeStream(Class)}. */
    @Nonnull
    public FlezhaRunner removeWorkflow(@Nonnull Class<? extends WorkflowOperative> workflowClass) {
        return remove(workflow, workflowClass);
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
     * When the runner last delivered a frame to entry channel {@code channel},
     * or empty before its first frame (and for a name no declared operative
     * has — both mean "nothing has arrived here"). Reading never blocks
     * delivery.
     *
     * <p>The instant is the <i>signal</i>; whether the silence since then is
     * anomalous is the application's call. Register an observer with
     * {@link #onEntryIdle(String, Duration, IdleObserver)} instead of polling
     * this in a loop.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public Optional<Instant> lastEntryDelivery(@Nonnull String channel) {
        return (Optional<Instant>) RuntimeBridge.invoke(
                WATCHER,
                "lastEntryDelivery",
                new Class<?>[] {String.class},
                "Failed to read the last entry delivery",
                channel);
    }

    /**
     * Observe silence on entry channel {@code channel}: {@code observer} is
     * notified once per idle episode, when the gap since the last delivered
     * frame first exceeds {@code threshold}, and once again when a frame
     * resumes it. Silence before the channel's very first frame is never an
     * episode.
     *
     * <p>Legal before and after {@link #start()}. Evaluation runs on one
     * background tick — never on the delivery path — so an observer that
     * blocks or throws costs the data plane nothing. Several observers may
     * watch one channel; {@link #close()} drops them all.
     *
     * <pre>{@code
     * FlezhaRunner runner = Flezha.createRunner("my_app");
     * runner.onEntryIdle("trades_in", Duration.ofSeconds(90),
     *         gap -> LOG.error("trades_in silent for {}", gap));
     * runner.run();
     * }</pre>
     */
    @Nonnull
    public FlezhaRunner onEntryIdle(
            @Nonnull String channel, @Nonnull Duration threshold, @Nonnull IdleObserver observer) {
        RuntimeBridge.invoke(
                WATCHER,
                "onEntryIdle",
                new Class<?>[] {String.class, Duration.class, IdleObserver.class},
                "Failed to register the entry-idle observer",
                channel,
                threshold,
                observer);
        return this;
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
     * release the JVM-wide runner slot, wake every thread blocked in
     * {@link #awaitTermination()}, and drop this runner's entry-idle
     * observers. A no-op on a never-started runner that registered none;
     * idempotent across SIGTERM + explicit close.
     */
    @Override
    public void close() {
        RunnerHandle h = handle.get();
        if (h != null) {
            h.close();
        }
        // Observers are per-runner: a second runner in this JVM must not
        // inherit the first one's watches (the Rust SDK's watcher dies with
        // its runner core for the same reason).
        RuntimeBridge.invokeIfPresent(WATCHER, "reset");
    }

    private RunnerHandle started(String what) {
        RunnerHandle h = handle.get();
        if (h == null) {
            throw new IllegalStateException(what + " requires a started runner — runner not started");
        }
        return h;
    }

    /** This runner's full current factory set: stream first, then workflow. */
    private List<String> declaredFactoryIds() {
        List<String> ids = new ArrayList<>(stream.factoryIds());
        ids.addAll(workflow.factoryIds());
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
