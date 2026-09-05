package com.flezha.sdk.runtime;

import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.core.Activation;
import com.flezha.sdk.runtime.core.ActivationCodec;
import com.flezha.sdk.runtime.core.ActivationResult;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Java side of the poll/complete seam: a small fixed set of platform
 * <em>fetcher</em> threads looping {@code poll → decode → hand off → poll},
 * and one virtual thread per activation that runs the dispatch, encodes the
 * result and calls {@code complete} itself.
 *
 * <p>The core decides what runs next and caps how much may be underway; a
 * host's only job is to bring threads, and this one brings three kinds —
 * fetchers ({@link #fetchLoop}), a virtual thread per activation
 * ({@link #runActivation}) and a supervisor against a wedge
 * ({@link #superviseLoop}). Each carries the rule that shapes it.
 */
final class PollHost implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PollHost.class);

    /** Poll timeout per turn; bounds how fast {@link #close()} joins a fetcher. */
    private static final long POLL_TIMEOUT_MS = 250;

    /** How long {@link #close()} waits for fetchers and for in-flight activations. */
    private static final long DRAIN_TIMEOUT_MS = 5_000;

    /**
     * The supervisor's park length. Only bounds how often it re-arms and how
     * fast {@link #close()} joins it — the core decides when the wait returns.
     */
    private static final long SUPERVISOR_WAIT_MS = 1_000;

    /**
     * The core calls this host makes. A seam, so the fetch/dispatch model and
     * the supervisor are testable without the native library — the production
     * implementation is a set of {@code FlzCore} calls on one context.
     */
    /**
     * The production {@link CoreCalls}: the four native calls over one core
     * handle. One definition, because a deployed runner and a local run drive
     * the same seam and a second copy would be free to drift from it.
     */
    static CoreCalls of(long core) {
        return new CoreCalls() {
            @Override
            @Nullable
            public byte[] poll(long timeoutMs) {
                return FlzCore.poll(core, timeoutMs);
            }

            @Override
            public void complete(@Nonnull byte[] result) {
                FlzCore.complete(core, result);
            }

            @Override
            public int waitStarvation(long timeoutMs) {
                return FlzCore.waitStarvation(core, timeoutMs);
            }

            @Override
            public void abandon(long token) {
                FlzCore.abandon(core, token);
            }
        };
    }

    interface CoreCalls {
        /**
         * @return {@code null} when the poll timed out, a zero-length array
         *     when the core is shutting down, else an encoded Activation.
         */
        @Nullable
        byte[] poll(long timeoutMs);

        void complete(@Nonnull byte[] result);

        /**
         * Park up to {@code timeoutMs} for the core to observe starvation.
         *
         * @return {@link FlzCore#STARVATION_OBSERVED},
         *     {@code STARVATION_TIMEOUT} or {@code STARVATION_SHUTTING_DOWN}.
         */
        int waitStarvation(long timeoutMs);

        /**
         * Re-hand an activation's untouched items and release its entity, as
         * if it had never been polled. The third and last way to answer a
         * handout; no retry-ladder attempt is consumed.
         */
        void abandon(long token);
    }

    private final CoreCalls core;
    private final Function<Activation, ActivationResult> dispatch;

    /**
     * The fetchers currently running. ONE structure, not a list plus a
     * counter: a replacement joins it and a departing thread leaves it, so
     * "the configured count is restored" is a size comparison rather than two
     * things that can disagree, and {@link #close()} joins what is actually
     * alive rather than the threads this host happened to start with.
     */
    private final Set<Thread> live = ConcurrentHashMap.newKeySet();

    private final int configuredFetchers;
    private final int maxFetchers;
    private final AtomicInteger nextFetcherId = new AtomicInteger();
    private final ExecutorService activations = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final Runnable onFetchersDrained;
    private final Thread supervisor = new Thread(this::superviseLoop, "flz-poll-supervisor");

    PollHost(
            @Nonnull CoreCalls core,
            @Nonnull Function<Activation, ActivationResult> dispatch,
            int fetcherCount,
            int fetcherMax,
            @Nonnull Runnable onFetchersDrained) {
        this.core = core;
        this.dispatch = dispatch;
        this.onFetchersDrained = onFetchersDrained;
        this.configuredFetchers = Math.max(1, fetcherCount);
        this.maxFetchers = Math.max(this.configuredFetchers, fetcherMax);
        this.supervisor.setDaemon(true);
        for (int i = 0; i < configuredFetchers; i++) {
            newFetcher();
        }
    }

    /** Start the fetchers. Separate from construction so no thread sees a half-built host. */
    void start() {
        live.forEach(Thread::start);
        supervisor.start();
    }

    /** The configured fetcher count — what a replacement restores the pool to. */
    int fetcherCount() {
        return configuredFetchers;
    }

    /** The ceiling the supervisor may grow the pool to. */
    int maxFetcherCount() {
        return maxFetchers;
    }

    /** Fetchers currently running. Restored by {@link #replaceFetcher}. */
    int liveFetcherCount() {
        return live.size();
    }

    /**
     * The supervisor loop: park in the core's starvation wait, and answer
     * every observation with one more fetcher. A fixed fetcher set is not
     * enough on its own — a virtual thread that parks inside {@code complete},
     * pushing into a consumer nobody is draining, holds its fetcher's handout,
     * and with every fetcher held the runner processes nothing and reports
     * nothing. What one more fetcher does <em>not</em> buy is a thread against
     * user code that never returns; no ceiling outruns that.
     *
     * <p>Growth is self-limiting without a settle here — a new fetcher's own
     * {@code poll} entry restarts the core's dwell, so the next one is at
     * least a dwell away and only if the runner is still stuck. The ceiling
     * refusal is latched rather than logged per park: a sustained wedge would
     * otherwise print the same line every second forever.
     */
    private void superviseLoop() {
        boolean atCeilingLogged = false;
        while (!closing.get()) {
            int outcome;
            try {
                outcome = core.waitStarvation(SUPERVISOR_WAIT_MS);
            } catch (Throwable t) {
                if (closing.get()) {
                    return;
                }
                LOG.error("starvation wait failed: {}", t.toString());
                // Same rule the fetch loop applies: an Exception is one bad
                // turn, an Error is the JVM telling this thread it is
                // finished. A supervisor that stopped on a transient failure
                // would leave the runner with no answer to starvation and no
                // report of it — the wedge this thread exists to close.
                if (t instanceof Error) {
                    return;
                }
                continue;
            }
            if (outcome == FlzCore.STARVATION_SHUTTING_DOWN) {
                return;
            }
            if (outcome != FlzCore.STARVATION_OBSERVED || closing.get()) {
                continue;
            }
            atCeilingLogged = growOneFetcher(atCeilingLogged);
        }
    }

    /**
     * Add one fetcher if the ceiling allows. Returns the new latch state for
     * the ceiling warning. Only the supervisor grows the pool, so the
     * size check needs no lock — a concurrent {@link #replaceFetcher} restores
     * a death rather than adding to the pool.
     */
    private boolean growOneFetcher(boolean atCeilingLogged) {
        if (live.size() >= maxFetchers) {
            if (!atCeilingLogged) {
                LOG.warn(
                        "starvation observed with {} fetchers at the ceiling ({}); "
                                + "raise {} if the work is genuinely blocked rather than wedged",
                        live.size(),
                        maxFetchers,
                        FlezhaEnv.FLEZHA_WORKER_POOL_MAX);
            }
            return true;
        }
        if (!startFetcher("a fetcher for observed starvation")) {
            return atCeilingLogged;
        }
        LOG.info("starvation observed; grew the fetcher set to {} (ceiling {})", live.size(), maxFetchers);
        return false;
    }

    /** A fetcher thread, already counted as live. The caller starts it. */
    private Thread newFetcher() {
        Thread t = new Thread(this::fetchLoop, "flz-fetcher-" + nextFetcherId.incrementAndGet());
        t.setDaemon(true);
        live.add(t);
        return t;
    }

    /**
     * Replace a fetcher that ended on a throwable. Nothing else would bring it
     * back: a runner whose last fetcher died stays connected, heartbeating and
     * processing nothing, which is the wedge this host must not be able to
     * reach on its own.
     */
    private void replaceFetcher(Throwable cause) {
        LOG.error("fetcher thread ended unexpectedly; starting a replacement", cause);
        if (closing.get()) {
            return;
        }
        startFetcher("a replacement");
    }

    /**
     * Start one fetcher and keep {@code live} honest about it. A thread that
     * never ran must not stay counted: a phantom fetcher would keep the set
     * non-empty and the runner would never read as drained — and would also
     * consume a slot under the supervisor's ceiling.
     *
     * @return whether a fetcher actually joined the pool.
     */
    private boolean startFetcher(String why) {
        Thread fetcher = newFetcher();
        try {
            fetcher.start();
            return true;
        } catch (Throwable t) {
            live.remove(fetcher);
            LOG.error("could not start {} fetcher", why, t);
            return false;
        }
    }

    /**
     * One fetcher: never runs user code, so it is always available to take
     * the core's next activation. Per-activation failures are logged, never
     * rethrown — a fetcher must not die on one bad activation.
     *
     * <p><b>Fetchers stay platform threads.</b> {@code FlzCore.poll} blocks in
     * native code, and a native frame pins its carrier — virtual fetchers
     * would pin one carrier each and buy nothing. The short native calls the
     * virtual threads make ({@code complete}, emits) pin only for their
     * duration, which is how JNI and virtual threads are meant to compose.
     */
    private void fetchLoop() {
        try {
            fetchTurns();
        } catch (Throwable t) {
            // Anything that escaped the loop's own per-turn guards. The
            // replacement joins `live` BEFORE this thread leaves it, so the
            // set never empties on a replacement and reads as a drained runner.
            replaceFetcher(t);
        }
        live.remove(Thread.currentThread());
        if (live.isEmpty()) {
            onFetchersDrained.run();
        }
    }

    private void fetchTurns() {
        // The core says when: a zero-length poll result is the core shutting
        // down, which it reports only once the shutdown sequence has shipped
        // every Entity's state through these same fetchers. The flag is what a
        // fetcher spawned late reads.
        while (!closing.get()) {
            byte[] bytes;
            try {
                bytes = core.poll(POLL_TIMEOUT_MS);
            } catch (Throwable t) {
                if (closing.get()) {
                    break;
                }
                LOG.error("core poll failed: {}", t.toString());
                // An Exception is one bad turn — retry it. An Error is the JVM
                // telling this thread it is finished (OOM, StackOverflow, a
                // LinkageError); retrying it forever would spin a fetcher that
                // can never fetch. End the thread and let the replacement try
                // with a fresh stack.
                if (t instanceof Error) {
                    throw (Error) t;
                }
                continue;
            }
            if (bytes == null) {
                continue;
            }
            if (bytes.length == 0) {
                break;
            }
            Activation activation;
            try {
                activation = ActivationCodec.decodeActivation(bytes);
            } catch (Throwable t) {
                LOG.error("activation decode failed: {}", t.toString());
                continue;
            }
            try {
                activations.execute(() -> runActivation(activation));
            } catch (RejectedExecutionException e) {
                // close() shut the executor down between the poll and here.
                break;
            }
        }
    }

    /**
     * Run one activation and answer it exactly once, on a virtual thread of
     * its own: an activation that blocks for a minute inside user code costs a
     * stack and nothing else — the fetchers keep fetching and the core keeps
     * handing out work for other Entities. Concurrency is bounded by the
     * core's jobs-underway limit, never by the fetcher count.
     *
     * <p>Per-Entity ordering needs no help here: exclusivity is the core's
     * handout guarantee (one outstanding activation per Entity), and this
     * thread completes its activation only after the dispatch returns, so the
     * next one for that Entity cannot start early.
     *
     * <p>The {@code finally} is the contract, not politeness: that same
     * exclusivity means an activation that returns without {@code complete}
     * retires its entity for the life of the process. A throwable from the
     * dispatcher, from encoding the result, or from {@code complete} itself
     * therefore abandons; logging it and returning would retire the entity
     * silently.
     */
    private void runActivation(Activation activation) {
        boolean answered = false;
        try {
            ActivationResult result = dispatch.apply(activation);
            byte[] encoded = ActivationCodec.encodeResult(result);
            core.complete(encoded);
            answered = true;
        } catch (Throwable t) {
            LOG.error("activation processing failed: {}", t.toString());
        } finally {
            if (!answered) {
                abandonQuietly(activation.token());
            }
        }
    }

    private void abandonQuietly(long token) {
        try {
            core.abandon(token);
        } catch (Throwable t) {
            // The core refuses an unknown token: `complete` had already
            // landed and only the bookkeeping after it threw. Nothing is
            // wedged, so this is a log line and not an escalation.
            LOG.warn("abandoning activation {} failed: {}", token, t.toString());
        }
    }

    /**
     * Stop fetching without the core's word. A local run ends while the
     * process-wide poll core keeps running — there is no controller session
     * to shut down, and shutting the core would stop every later run in the
     * process — so the flag is raised first and the fetchers leave on their
     * next poll timeout instead.
     */
    void stopFetching() {
        closing.set(true);
        close();
    }

    /**
     * Stop fetching and let in-flight activations finish. Draining the
     * virtual threads is not politeness: each still holds an activation the
     * core is waiting on, and the caller frees the core context next.
     */
    @Override
    public void close() {
        // The supervisor first: it is parked in a bounded wait, and joining it
        // before the fetchers means it cannot grow the pool behind the join
        // that is meant to drain it. Both leave on the core's word; the flag
        // is raised only after, so no fetcher leaves before the shutdown
        // sequence has been served.
        joinQuietly(supervisor);
        for (Thread fetcher : List.copyOf(live)) {
            joinQuietly(fetcher);
        }
        closing.set(true);
        activations.shutdown();
        try {
            if (!activations.awaitTermination(DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("in-flight activations did not finish within {}ms", DRAIN_TIMEOUT_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(Thread thread) {
        try {
            thread.join(DRAIN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
