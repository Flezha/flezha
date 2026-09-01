package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.runtime.core.Activation;
import com.flezha.sdk.runtime.core.ActivationCodec;
import com.flezha.sdk.runtime.core.ActivationKind;
import com.flezha.sdk.runtime.core.ActivationOutcome;
import com.flezha.sdk.runtime.core.ActivationResult;
import com.flezha.sdk.runtime.core.EntityRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The concurrency contract of the Java poll host: fetchers never run user
 * code, so an activation blocked in user code cannot consume one, and a
 * virtual thread completes its activation only after the dispatch returns.
 */
class PollHostTest {

    /** A stand-in core: hands out queued activations and records answers. */
    private static final class FakeCore implements PollHost.CoreCalls {
        private final LinkedBlockingQueue<byte[]> handouts = new LinkedBlockingQueue<>();
        private final AtomicInteger completed = new AtomicInteger();
        private final List<Long> abandoned = new CopyOnWriteArrayList<>();
        /** Set to make every {@code complete} throw, as a refusing core would. */
        private volatile boolean completeThrows = false;
        /** How many {@code poll} calls still throw an Error (killing a fetcher). */
        private final AtomicInteger pollErrors = new AtomicInteger();
        /** How many {@code waitStarvation} calls still report an observation. */
        private final AtomicInteger starvations = new AtomicInteger();
        /** Every {@code waitStarvation} call, observed or not. */
        private final AtomicInteger starvationWaits = new AtomicInteger();
        /** How many {@code waitStarvation} calls still throw an Exception. */
        private final AtomicInteger starvationErrors = new AtomicInteger();
        /** Never released: parking on it is how a quiet core spends its wait. */
        private final CountDownLatch quiet = new CountDownLatch(1);
        /** The thread the supervisor loop runs on, as this core sees it. */
        private volatile Thread supervisor;
        /** What {@code FlzCore.shutdown} leaves behind: every poll and wait answers shutting down. */
        private volatile boolean shuttingDown = false;

        void handOut(Activation activation) {
            handouts.add(ActivationCodec.encodeActivation(activation));
        }

        void shutDown() {
            shuttingDown = true;
            handouts.add(new byte[0]);
            quiet.countDown();
        }

        @Override
        @Nullable
        public byte[] poll(long timeoutMs) {
            // An Error, not an Exception: an Exception is one bad turn the
            // loop retries, an Error ends the thread — which is the death a
            // replacement has to cover.
            if (pollErrors.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new StackOverflowError("fetcher died");
            }
            if (shuttingDown) {
                return new byte[0];
            }
            try {
                return handouts.poll(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        @Override
        public void complete(byte[] result) {
            // Decoding an ActivationResult is the core's side of the ABI; the
            // host contract under test is only that `complete` is called.
            if (completeThrows) {
                throw new IllegalStateException("core refused the completion");
            }
            completed.incrementAndGet();
        }

        @Override
        public int waitStarvation(long timeoutMs) {
            supervisor = Thread.currentThread();
            starvationWaits.incrementAndGet();
            if (starvationErrors.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IllegalStateException("starvation wait failed");
            }
            if (starvations.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                return FlzCore.STARVATION_OBSERVED;
            }
            try {
                quiet.await(Math.min(timeoutMs, 20), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return shuttingDown ? FlzCore.STARVATION_SHUTTING_DOWN : FlzCore.STARVATION_TIMEOUT;
        }

        @Override
        public void abandon(long token) {
            abandoned.add(token);
        }
    }

    private static Activation activation(long token, String entityKey) {
        return new Activation(
                token,
                new EntityRef("op-1", 0, Optional.of(entityKey)),
                new ActivationKind.Work(List.of()),
                0,
                "factory");
    }

    private static ActivationResult ok(Activation activation) {
        return new ActivationResult(activation.token(), new ActivationOutcome.Work(List.of()));
    }

    private static PollHost start(FakeCore core, Function<Activation, ActivationResult> dispatch, int fetchers) {
        return start(core, dispatch, fetchers, fetchers * 10);
    }

    private static PollHost start(
            FakeCore core, Function<Activation, ActivationResult> dispatch, int fetchers, int max) {
        return start(core, dispatch, fetchers, max, () -> {});
    }

    private static PollHost start(
            FakeCore core,
            Function<Activation, ActivationResult> dispatch,
            int fetchers,
            int max,
            Runnable onFetchersDrained) {
        PollHost host = new PollHost(core, dispatch, fetchers, max, onFetchersDrained);
        host.start();
        return host;
    }

    /** What {@code FlezhaImpl.close()} does: the core is told first, and the host follows its word. */
    private static void close(FakeCore core, PollHost host) {
        core.shutDown();
        host.close();
    }

    @Test
    @DisplayName("activations blocked in user code do not consume the fetchers")
    void blockedActivationsDoNotConsumeFetchers() throws Exception {
        FakeCore core = new FakeCore();
        CountDownLatch gate = new CountDownLatch(1);
        CountDownLatch unblockedDispatched = new CountDownLatch(4);
        Function<Activation, ActivationResult> dispatch = activation -> {
            if (activation.target().entityKey().orElseThrow().startsWith("blocked")) {
                try {
                    gate.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                unblockedDispatched.countDown();
            }
            return ok(activation);
        };

        // As many blocked activations as there are fetchers: with dispatch
        // running on the fetcher itself, nothing below would ever be picked
        // up.
        try (PollHost host = start(core, dispatch, 2)) {
            core.handOut(activation(1, "blocked-a"));
            core.handOut(activation(2, "blocked-b"));
            for (long token = 3; token <= 6; token++) {
                core.handOut(activation(token, "healthy-" + token));
            }

            assertThat(unblockedDispatched.await(10, TimeUnit.SECONDS))
                    .as("work for unblocked entities was served while two activations blocked")
                    .isTrue();
            assertThat(gate.getCount())
                    .as("the blocked activations really were still blocked")
                    .isEqualTo(1);
            gate.countDown();
        }
    }

    /**
     * Per-Entity exclusivity is the core's guarantee — it hands out the next
     * activation for an Entity only once the previous one completes. The host
     * keeps that true by completing only after the dispatch returns; a host
     * that completed early would let two activations for one Entity overlap.
     */
    @Test
    @DisplayName("per-entity order holds across successive activations")
    void perEntityOrderHoldsAcrossSuccessiveActivations() throws Exception {
        FakeCore core = new FakeCore();
        List<String> trace = new CopyOnWriteArrayList<>();
        AtomicInteger handedOut = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        CountDownLatch insideFirst = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        Function<Activation, ActivationResult> dispatch = activation -> {
            trace.add("enter-" + activation.token());
            if (activation.token() == 1) {
                insideFirst.countDown();
                await(releaseFirst);
            }
            trace.add("exit-" + activation.token());
            // The core's exclusivity, reproduced: the next activation for this
            // Entity becomes available only now.
            int next = handedOut.incrementAndGet() + 1;
            if (next <= 3) {
                core.handOut(activation(next, "same-entity"));
            } else {
                done.countDown();
            }
            return ok(activation);
        };

        try (PollHost host = start(core, dispatch, 2)) {
            core.handOut(activation(1, "same-entity"));
            assertThat(insideFirst.await(10, TimeUnit.SECONDS))
                    .as("the first activation is held inside the dispatch")
                    .isTrue();
            assertThat(new ArrayList<>(trace))
                    .as("nothing follows an activation that has not returned")
                    .containsExactly("enter-1");
            releaseFirst.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(new ArrayList<>(trace))
                .containsExactly("enter-1", "exit-1", "enter-2", "exit-2", "enter-3", "exit-3");
    }

    /**
     * Per-entity exclusivity IS the handout, so an activation that returns
     * without an answer retires its Entity for the life of the process. Every
     * way the answer can be missed — the dispatch throwing, the result failing
     * to encode, `complete` itself throwing — must abandon instead.
     */
    @Test
    @DisplayName("a throwing dispatch abandons rather than dropping the activation")
    void aThrowingDispatchAbandons() throws Exception {
        FakeCore core = new FakeCore();
        CountDownLatch dispatched = new CountDownLatch(1);
        Function<Activation, ActivationResult> dispatch = activation -> {
            dispatched.countDown();
            throw new IllegalStateException("dispatcher blew up");
        };

        try (PollHost host = start(core, dispatch, 1)) {
            core.handOut(activation(7, "unlucky"));
            assertThat(dispatched.await(10, TimeUnit.SECONDS)).isTrue();
            awaitTrue(() -> core.abandoned.contains(7L), "the activation was abandoned");
        }
        assertThat(core.completed.get()).isZero();
    }

    @Test
    @DisplayName("a throwing complete abandons rather than dropping the activation")
    void aThrowingCompleteAbandons() throws Exception {
        FakeCore core = new FakeCore();
        core.completeThrows = true;

        try (PollHost host = start(core, PollHostTest::ok, 1)) {
            core.handOut(activation(8, "unlucky"));
            awaitTrue(() -> core.abandoned.contains(8L), "the activation was abandoned");
        }
    }

    /**
     * A runner whose last fetcher died stays connected, heartbeating and
     * processing nothing — the wedge this host must not be able to reach on
     * its own.
     */
    @Test
    @DisplayName("a fetcher killed by a throwable is replaced")
    void aDeadFetcherIsReplaced() throws Exception {
        FakeCore core = new FakeCore();
        core.pollErrors.set(1);

        try (PollHost host = start(core, PollHostTest::ok, 2)) {
            awaitTrue(() -> core.pollErrors.get() == 0, "a fetcher died on the poll Error");
            // The configured count is what the runner budgets for, so it must
            // come back — and the replacement must actually fetch.
            awaitTrue(
                    () -> host.liveFetcherCount() == host.fetcherCount(),
                    "the fetcher count returned to its configured size");
            core.handOut(activation(9, "after-the-death"));
            awaitTrue(() -> core.completed.get() == 1, "a replacement fetcher served new work");
        }
    }

    /**
     * The other half of the core's bring-your-own-thread contract. Java ships
     * a fixed fetcher set, so without this a runner whose fetchers are all
     * held processes nothing, recovers from nothing, and reports nothing —
     * measured as 0 of 20 payloads delivered, forever.
     */
    @Test
    @DisplayName("observed starvation grows the fetcher set")
    void starvationGrowsTheFetcherSet() throws Exception {
        FakeCore core = new FakeCore();
        try (PollHost host = start(core, PollHostTest::ok, 1, 3)) {
            core.starvations.set(2);
            awaitTrue(() -> host.liveFetcherCount() == 3, "the pool grew once per observation");
            // A grown fetcher is a real one: it fetches.
            core.handOut(activation(11, "after-the-growth"));
            awaitTrue(() -> core.completed.get() == 1, "a grown fetcher served work");
        }
    }

    @Test
    @DisplayName("growth stops at the ceiling and the supervisor keeps waiting")
    void starvationGrowthStopsAtTheCeiling() throws Exception {
        FakeCore core = new FakeCore();
        try (PollHost host = start(core, PollHostTest::ok, 1, 2)) {
            core.starvations.set(5);
            awaitTrue(() -> host.liveFetcherCount() == 2, "the pool grew to its ceiling");
            awaitTrue(() -> core.starvations.get() == 0, "every observation was consumed");
            assertThat(host.liveFetcherCount()).as("the ceiling holds exactly").isEqualTo(host.maxFetcherCount());
            int waits = core.starvationWaits.get();
            awaitTrue(() -> core.starvationWaits.get() > waits, "the supervisor keeps waiting after refusing to grow");
        }
    }

    /**
     * Nothing in this host may stop on its own. A supervisor that ended on a
     * transient failure would leave the runner with no answer to starvation
     * and no report of it — the exact wedge the thread exists to close.
     */
    @Test
    @DisplayName("the supervisor survives a failing starvation wait")
    void supervisorSurvivesAFailingWait() throws Exception {
        FakeCore core = new FakeCore();
        core.starvationErrors.set(2);
        try (PollHost host = start(core, PollHostTest::ok, 1, 3)) {
            awaitTrue(() -> core.starvationErrors.get() == 0, "both failing waits were made");
            core.starvations.set(1);
            awaitTrue(() -> host.liveFetcherCount() == 2, "the supervisor still grows the pool");
        }
    }

    @Test
    @DisplayName("close() joins the supervisor")
    void closeJoinsTheSupervisor() throws Exception {
        FakeCore core = new FakeCore();
        PollHost host = start(core, PollHostTest::ok, 1, 4);
        awaitTrue(() -> core.starvationWaits.get() > 0, "the supervisor parked at least once");
        close(core, host);
        Thread supervisor = core.supervisor;
        supervisor.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(supervisor.isAlive())
                .as("the supervisor was still parking after close() returned")
                .isFalse();
    }

    private static void awaitTrue(java.util.function.BooleanSupplier done, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!done.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out waiting for %s", what).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /**
     * The activation is released by the fetcher drain itself, so close() is
     * provably still running when the dispatch returns — the overlap is forced
     * rather than sampled from a window.
     */
    @Test
    @DisplayName("close() lets in-flight activations finish and complete")
    void closeCompletesInFlightActivations() throws Exception {
        FakeCore core = new FakeCore();
        CountDownLatch dispatching = new CountDownLatch(1);
        CountDownLatch fetchersDrained = new CountDownLatch(1);
        Function<Activation, ActivationResult> dispatch = activation -> {
            dispatching.countDown();
            await(fetchersDrained);
            return ok(activation);
        };

        PollHost host = start(core, dispatch, 2, 20, fetchersDrained::countDown);
        core.handOut(activation(1, "in-flight"));
        assertThat(dispatching.await(10, TimeUnit.SECONDS)).isTrue();

        close(core, host);
        assertThat(core.completed.get())
                .as("close() returned before the in-flight activation completed")
                .isEqualTo(1);
    }

    /** Wait on a latch the test is about to release; an interrupt is a failure. */
    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS))
                    .as("the test released the dispatch")
                    .isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
