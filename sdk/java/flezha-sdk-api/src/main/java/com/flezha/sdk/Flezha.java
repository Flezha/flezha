package com.flezha.sdk;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * Static-only entry point of the SDK: {@link #createRunner(String)} to create
 * this JVM's runner, {@link #currentRunner()} to reach it from code that does
 * not control startup ordering.
 *
 * <pre>{@code
 * Flezha.createRunner("my_app")
 *     .defineStream(MyOperative.class)
 *     .run(); // .start() instead to keep the calling thread
 * }</pre>
 *
 * <p>At most one runner is <em>started</em> per JVM. Creating (and mutating)
 * runners is free; the singleton slot is claimed at
 * {@link FlezhaRunner#start()}. A second {@code start()}/{@code run()} while
 * one is active throws {@link IllegalStateException} naming the running
 * namespace; after {@link FlezhaRunner#close()} a new runner may be started.
 *
 * <p>Method names and lifecycle verbs follow
 * {@code docs/sdk/naming_conventions.md} (repository root), mirrored 1:1 with
 * the Rust SDK's {@code Flezha}.
 */
public final class Flezha {

    /**
     * The one runner slot this JVM has. {@code RUNNING_NAMESPACE} is the
     * singleton guard ({@link FlezhaRunner#start()} claims it via CAS);
     * {@code CURRENT} backs {@link #currentRunner()}.
     */
    static final AtomicReference<String> RUNNING_NAMESPACE = new AtomicReference<>();

    static final AtomicReference<FlezhaRunner> CURRENT = new AtomicReference<>();

    /**
     * The sink every SDK diagnostic reaches, replaced by
     * {@link FlezhaRunner#logger(Logger)}. Deliberately outside
     * {@link #releaseSlot()}: a logger is configured once per process, and a
     * runner's most interesting diagnostics are the ones it writes while
     * shutting down.
     */
    static final AtomicReference<Logger> LOGGER = new AtomicReference<>(ConsoleLogger.fromEnv());

    /**
     * Forwards to whatever {@link #LOGGER} holds at the moment of the call, so
     * a class may keep the SDK's logger in a {@code static final} field and
     * still reach a sink the host supplies later.
     */
    private static final Logger ACTIVE = (level, format, args) -> LOGGER.get().log(level, format, args);

    private Flezha() {}

    /**
     * The SDK's diagnostics. The returned logger is stable for the life of the
     * JVM and follows {@link FlezhaRunner#logger(Logger)}; by default it writes
     * to standard error at the level {@code FLEZHA_LOG_LEVEL} names, or
     * {@code warn}.
     */
    @Nonnull
    public static Logger log() {
        return ACTIVE;
    }

    /**
     * Create this JVM's runner for {@code namespace} — one object carries
     * declaration and lifecycle. Declare operative classes with
     * {@link FlezhaRunner#defineStream(Class)} /
     * {@link FlezhaRunner#defineWorkflow(Class)} (legal before AND after
     * start), then {@link FlezhaRunner#start()} (keep the calling thread) or
     * {@link FlezhaRunner#run()} (block until shutdown). Creating a runner
     * never connects, claims the JVM singleton slot, or spawns a thread.
     */
    @Nonnull
    public static FlezhaRunner createRunner(String namespace) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        return new FlezhaRunner(namespace);
    }

    /**
     * The JVM's active runner, if one has been started and not yet closed —
     * {@link Optional#empty()} otherwise. Never throws and never starts a
     * runner as a side effect. Runners that were created but never started
     * never appear here.
     */
    @Nonnull
    public static Optional<FlezhaRunner> currentRunner() {
        return Optional.ofNullable(CURRENT.get());
    }

    /**
     * Release the JVM-wide runner slot: clears the guard and the current
     * handle so a later runner may be started. Runs when the active runner
     * closes (via the {@code onTerminated} hook {@link FlezhaRunner#start()}
     * hands the runtime) and when {@code start()} fails mid-flight.
     */
    static void releaseSlot() {
        CURRENT.set(null);
        RUNNING_NAMESPACE.set(null);
    }
}
