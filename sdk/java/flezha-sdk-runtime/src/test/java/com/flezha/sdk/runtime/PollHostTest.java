package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;

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

    /** A stand-in core: hands out queued activations and records completions. */
    private static final class FakeCore implements PollHost.CoreCalls {
        private final LinkedBlockingQueue<byte[]> handouts = new LinkedBlockingQueue<>();
        private final AtomicInteger completed = new AtomicInteger();

        void handOut(Activation activation) {
            handouts.add(ActivationCodec.encodeActivation(activation));
        }

        @Override
        @Nullable
        public byte[] poll(long timeoutMs) {
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
            completed.incrementAndGet();
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
        PollHost host = new PollHost(core, dispatch, fetchers, () -> {});
        host.start();
        return host;
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
        Function<Activation, ActivationResult> dispatch = activation -> {
            trace.add("enter-" + activation.token());
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(new ArrayList<>(trace))
                .containsExactly("enter-1", "exit-1", "enter-2", "exit-2", "enter-3", "exit-3");
    }

    @Test
    @DisplayName("close() lets in-flight activations finish and complete")
    void closeCompletesInFlightActivations() throws Exception {
        FakeCore core = new FakeCore();
        CountDownLatch dispatching = new CountDownLatch(1);
        Function<Activation, ActivationResult> dispatch = activation -> {
            dispatching.countDown();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ok(activation);
        };

        PollHost host = start(core, dispatch, 2);
        core.handOut(activation(1, "in-flight"));
        assertThat(dispatching.await(10, TimeUnit.SECONDS)).isTrue();

        host.close();
        assertThat(core.completed.get())
                .as("close() returned before the in-flight activation completed")
                .isEqualTo(1);
    }
}
