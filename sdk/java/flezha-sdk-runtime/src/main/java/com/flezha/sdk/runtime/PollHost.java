package com.flezha.sdk.runtime;

import com.flezha.sdk.runtime.core.Activation;
import com.flezha.sdk.runtime.core.ActivationCodec;
import com.flezha.sdk.runtime.core.ActivationResult;
import java.util.ArrayList;
import java.util.List;
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
 * host's only job is to bring threads. Java's answer is virtual threads, so
 * an activation that blocks for a minute inside user code costs a stack and
 * nothing else — the fetchers keep fetching and the core keeps handing out
 * work for other Entities. Concurrency is bounded by the core's
 * jobs-underway limit, never by the fetcher count.
 *
 * <p><b>The fetchers stay platform threads.</b> {@code FlzCore.poll} blocks
 * in native code, and a native frame pins its carrier — virtual fetchers
 * would pin one carrier each and buy nothing. The short native calls the
 * virtual threads make ({@code complete}, emits) pin only for their duration,
 * which is how JNI and virtual threads are meant to compose.
 *
 * <p>Per-Entity ordering needs no help here: exclusivity is the core's
 * handout guarantee (one outstanding activation per Entity), and a virtual
 * thread completes its activation only after the dispatch returns, so the
 * next one for that Entity cannot start early.
 */
final class PollHost implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PollHost.class);

    /** Poll timeout per turn; bounds how fast {@link #close()} joins a fetcher. */
    private static final long POLL_TIMEOUT_MS = 250;

    /** How long {@link #close()} waits for fetchers and for in-flight activations. */
    private static final long DRAIN_TIMEOUT_MS = 5_000;

    /**
     * The two core calls the loop makes. A seam, so the fetch/dispatch model
     * is testable without the native library — the production implementation
     * is a pair of {@code FlzCore} calls on one context.
     */
    interface CoreCalls {
        /**
         * @return {@code null} when the poll timed out, a zero-length array
         *     when the core is shutting down, else an encoded Activation.
         */
        @Nullable
        byte[] poll(long timeoutMs);

        void complete(@Nonnull byte[] result);
    }

    private final CoreCalls core;
    private final Function<Activation, ActivationResult> dispatch;
    private final List<Thread> fetchers = new ArrayList<>();
    private final ExecutorService activations = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger liveFetchers = new AtomicInteger();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final Runnable onFetchersDrained;

    PollHost(
            @Nonnull CoreCalls core,
            @Nonnull Function<Activation, ActivationResult> dispatch,
            int fetcherCount,
            @Nonnull Runnable onFetchersDrained) {
        this.core = core;
        this.dispatch = dispatch;
        this.onFetchersDrained = onFetchersDrained;
        for (int i = 0; i < Math.max(1, fetcherCount); i++) {
            Thread t = new Thread(this::fetchLoop, "flz-fetcher-" + (i + 1));
            t.setDaemon(true);
            fetchers.add(t);
        }
    }

    /** Start the fetchers. Separate from construction so no thread sees a half-built host. */
    void start() {
        liveFetchers.set(fetchers.size());
        fetchers.forEach(Thread::start);
    }

    int fetcherCount() {
        return fetchers.size();
    }

    /**
     * One fetcher: never runs user code, so it is always available to take
     * the core's next activation. Per-activation failures are logged, never
     * rethrown — a fetcher must not die on one bad activation.
     */
    private void fetchLoop() {
        // Either end stops a fetcher: the core's shutdown (a zero-length poll
        // result) or close() here. Depending on the core alone would make
        // close() wait out a full poll timeout per fetcher for nothing.
        while (!closing.get()) {
            byte[] bytes;
            try {
                bytes = core.poll(POLL_TIMEOUT_MS);
            } catch (Throwable t) {
                if (closing.get()) {
                    break;
                }
                LOG.error("core poll failed: {}", t.toString());
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
        if (liveFetchers.decrementAndGet() == 0) {
            onFetchersDrained.run();
        }
    }

    private void runActivation(Activation activation) {
        try {
            ActivationResult result = dispatch.apply(activation);
            core.complete(ActivationCodec.encodeResult(result));
        } catch (Throwable t) {
            LOG.error("activation processing failed: {}", t.toString());
        }
    }

    /**
     * Stop fetching and let in-flight activations finish. Draining the
     * virtual threads is not politeness: each still holds an activation the
     * core is waiting on, and the caller frees the core context next.
     */
    @Override
    public void close() {
        closing.set(true);
        for (Thread fetcher : fetchers) {
            joinQuietly(fetcher);
        }
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
