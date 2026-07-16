package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.bounded.BoundedOperativeRegistry;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeRegistry;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Runner lifecycle + JVM-wide singleton guard
 * (`runner-lifecycle-singleton-api`). The runtime side is the test double
 * {@link com.flezha.sdk.runtime.FlezhaImpl}, so these tests exercise the
 * guard/lifecycle logic without native transport; the real runtime wiring is
 * covered by the cross-language e2e suite.
 */
class RunnerLifecycleTest {

    public static final class OperativeA implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "lifecycle_a";
        }

        @Override
        public void channels(ChannelBuilder c) {}
    }

    public static final class OperativeB implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "lifecycle_b";
        }

        @Override
        public void channels(ChannelBuilder c) {}
    }

    @AfterEach
    void releaseSingletonSlot() {
        Flezha.currentRunner().ifPresent(FlezhaRunner::close);
        UnboundedOperativeRegistry.reset();
        BoundedOperativeRegistry.reset();
    }

    @Test
    @DisplayName(
            "second start() in the same JVM is rejected naming the running namespace, without reaching the runtime")
    void secondStartIsRejected() {
        FlezhaRunner first =
                Flezha.newRunner("ns1").defineUnbounded(OperativeA.class).start();
        int startsBefore = com.flezha.sdk.runtime.FlezhaImpl.STARTS.get();

        assertThatThrownBy(() -> Flezha.newRunner("ns2").start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ns1");
        // A declaration while ns1 runs is rejected with the same diagnosis.
        assertThatThrownBy(() -> Flezha.newRunner("ns2").defineUnbounded(OperativeB.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ns1");

        assertThat(com.flezha.sdk.runtime.FlezhaImpl.STARTS.get())
                .as("a rejected start() must not reach the runtime")
                .isEqualTo(startsBefore);
        assertThat(Flezha.currentRunner()).as("the active runner is untouched").containsSame(first);
        assertThat(first.getId()).isEqualTo("ns1");
    }

    @Test
    @DisplayName("a runner may be started again after the first one closes")
    void restartAfterCloseSucceeds() {
        Flezha.newRunner("ns1").defineUnbounded(OperativeA.class).start().close();
        assertThat(Flezha.currentRunner()).isEmpty();

        FlezhaRunner second =
                Flezha.newRunner("ns2").defineUnbounded(OperativeB.class).start();
        assertThat(second.getId()).isEqualTo("ns2");
        assertThat(Flezha.currentRunner()).containsSame(second);
    }

    @Test
    @DisplayName("try-with-resources starts the runner for the block and closes it at the end")
    void tryWithResourcesWorks() {
        try (FlezhaRunner r =
                Flezha.newRunner("ns_try").defineUnbounded(OperativeA.class).start()) {
            assertThat(r.getId()).isEqualTo("ns_try");
            assertThat(Flezha.currentRunner()).containsSame(r);
        }
        assertThat(Flezha.currentRunner()).isEmpty();
    }

    @Test
    @DisplayName("close() from another thread wakes a thread blocked in awaitTermination()")
    void closeWakesAwaitTermination() throws Exception {
        FlezhaRunner runner =
                Flezha.newRunner("ns_await").defineUnbounded(OperativeA.class).start();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> blocked = pool.submit(() -> {
                runner.awaitTermination();
                return null;
            });
            assertThatThrownBy(() -> blocked.get(150, TimeUnit.MILLISECONDS))
                    .as("awaitTermination must block while the runner is live")
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);

            runner.close();
            blocked.get(5, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("currentRunner() reflects empty -> active -> empty across the lifecycle")
    void currentRunnerReflectsLifecycle() {
        assertThat(Flezha.currentRunner()).isEmpty();
        FlezhaRunner runner =
                Flezha.newRunner("ns_cur").defineUnbounded(OperativeA.class).start();
        assertThat(Flezha.currentRunner()).containsSame(runner);
        runner.close();
        assertThat(Flezha.currentRunner()).isEmpty();
    }

    @Test
    @DisplayName("two concurrent start() calls — exactly one wins, the loser is told the winner's namespace")
    void concurrentStartsExactlyOneWins() throws Exception {
        RunnerBuilder b1 = Flezha.newRunner("ns_one").defineUnbounded(OperativeA.class);
        RunnerBuilder b2 = Flezha.newRunner("ns_two").defineUnbounded(OperativeB.class);

        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Object> f1 = pool.submit(() -> race(barrier, b1));
            Future<Object> f2 = pool.submit(() -> race(barrier, b2));
            Object r1 = f1.get(10, TimeUnit.SECONDS);
            Object r2 = f2.get(10, TimeUnit.SECONDS);

            FlezhaRunner winner;
            IllegalStateException loser;
            if (r1 instanceof FlezhaRunner w) {
                winner = w;
                loser = (IllegalStateException) r2;
            } else {
                winner = (FlezhaRunner) r2;
                loser = (IllegalStateException) r1;
            }
            assertThat(loser)
                    .as("the loser must be told the winner's namespace")
                    .hasMessageContaining(winner.getId());
            assertThat(Flezha.currentRunner()).containsSame(winner);
        } finally {
            pool.shutdownNow();
        }
    }

    private static Object race(CyclicBarrier barrier, RunnerBuilder builder) throws Exception {
        barrier.await();
        try {
            return builder.start();
        } catch (IllegalStateException e) {
            return e;
        }
    }

    @Test
    @DisplayName("start() with no declared operative type fails fast and releases the slot")
    void startWithoutDeclarationFailsFast() {
        assertThatThrownBy(() -> Flezha.newRunner("ns_empty").start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no operative type declared");
        // The failed start must have released the slot: a valid start works.
        FlezhaRunner runner =
                Flezha.newRunner("ns_valid").defineUnbounded(OperativeA.class).start();
        assertThat(runner.getId()).isEqualTo("ns_valid");
    }

    @Test
    @DisplayName("newRunner rejects a null or empty namespace")
    void newRunnerValidatesNamespace() {
        assertThatThrownBy(() -> Flezha.newRunner("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Flezha.newRunner(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
