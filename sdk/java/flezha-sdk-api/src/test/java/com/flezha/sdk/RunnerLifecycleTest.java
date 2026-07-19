package com.flezha.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Runner lifecycle + JVM-wide singleton guard + on-demand define/remove
 * ({@code sdk-runner-entrypoint-redesign}). The runtime side is the test
 * double {@link com.flezha.sdk.runtime.FlezhaImpl} (implementing the
 * {@code RunnerHandle} SPI with fakeable live instances), so these tests
 * exercise the guard/lifecycle/declared-set logic without native transport;
 * the real runtime wiring is covered by the cross-language e2e suite.
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

    public static final class OperativeC implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "lifecycle_c";
        }

        @Override
        public void channels(ChannelBuilder c) {}
    }

    @BeforeEach
    @AfterEach
    void releaseSingletonSlot() {
        Flezha.currentRunner().ifPresent(FlezhaRunner::close);
        com.flezha.sdk.runtime.FlezhaImpl.resetForTests();
        com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName(
            "second start() in the same JVM is rejected naming the running namespace, without reaching the runtime")
    void secondStartIsRejected() {
        FlezhaRunner first =
                Flezha.createRunner("ns1").defineUnbounded(OperativeA.class).start();
        int startsBefore = com.flezha.sdk.runtime.FlezhaImpl.STARTS.get();

        assertThatThrownBy(() -> Flezha.createRunner("ns2").start())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ns1");

        assertThat(com.flezha.sdk.runtime.FlezhaImpl.STARTS.get())
                .as("a rejected start() must not reach the runtime")
                .isEqualTo(startsBefore);
        assertThat(Flezha.currentRunner()).as("the active runner is untouched").containsSame(first);
        assertThat(first.getNamespace()).isEqualTo("ns1");
    }

    @Test
    @DisplayName("creating and mutating runners never claims the slot")
    void creatingRunnersNeverClaimsTheSlot() {
        FlezhaRunner active =
                Flezha.createRunner("ns1").defineUnbounded(OperativeA.class).start();

        assertThatCode(() -> Flezha.createRunner("ns2")
                        .defineUnbounded(OperativeB.class)
                        .removeUnbounded(OperativeB.class))
                .as("mutating an unstarted runner while another is active is legal")
                .doesNotThrowAnyException();

        assertThat(Flezha.currentRunner()).containsSame(active);
    }

    @Test
    @DisplayName("start() twice on the same runner is rejected")
    void doubleStartOnOneRunnerIsRejected() {
        FlezhaRunner runner = Flezha.createRunner("ns_double")
                .defineUnbounded(OperativeA.class)
                .start();
        assertThatThrownBy(runner::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already started");
    }

    @Test
    @DisplayName("zero-operative start is legal; the handshake supplier serves an empty set")
    void zeroOperativeStartIsLegal() {
        FlezhaRunner runner = Flezha.createRunner("ns_zero").start();
        assertThat(runner.getNamespace()).isEqualTo("ns_zero");
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.LAST_DECLARED_SUPPLIER.get())
                .as("the init handshake must carry an empty factory list")
                .isEmpty();
        // Types may be defined later on the live runner.
        runner.defineUnbounded(OperativeA.class);
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.LAST_DECLARED_SUPPLIER.get())
                .containsExactly("lifecycle_a");
    }

    @Test
    @DisplayName("a pre-start remove shapes the declared set the handshake reads")
    void preStartRemoveShapesTheHandshake() {
        FlezhaRunner runner = Flezha.createRunner("ns_shape")
                .defineUnbounded(OperativeA.class)
                .defineUnbounded(OperativeB.class)
                .removeUnbounded(OperativeB.class)
                .start();
        assertThat(runner.getNamespace()).isEqualTo("ns_shape");
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.LAST_DECLARED_SUPPLIER.get())
                .containsExactly("lifecycle_a");
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.SYNCED_SETS)
                .as("pre-start mutations ship no UpdateFactories")
                .isEmpty();
    }

    @Test
    @DisplayName("a post-start define ships the full factory set")
    void postStartDefineShipsTheFullSet() {
        Flezha.createRunner("ns_def").defineUnbounded(OperativeA.class).start().defineUnbounded(OperativeC.class);

        assertThat(com.flezha.sdk.runtime.FlezhaImpl.SYNCED_SETS)
                .containsExactly(List.of("lifecycle_a", "lifecycle_c"));
    }

    @Test
    @DisplayName("a post-start remove (no live instances) ships the remaining full set")
    void postStartRemoveShipsTheRemainingSet() {
        Flezha.createRunner("ns_rem")
                .defineUnbounded(OperativeA.class)
                .defineUnbounded(OperativeC.class)
                .start()
                .removeUnbounded(OperativeC.class);

        assertThat(com.flezha.sdk.runtime.FlezhaImpl.SYNCED_SETS).containsExactly(List.of("lifecycle_a"));
        assertThat(com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry.registeredIds())
                .as("the runtime-side class must be unregistered")
                .doesNotContain("lifecycle_c");
    }

    @Test
    @DisplayName("removing a type with live instances throws OperativeTypeInUseException and keeps it defined")
    void removeWithLiveInstancesRefuses() {
        FlezhaRunner runner =
                Flezha.createRunner("ns_live").defineUnbounded(OperativeA.class).start();
        com.flezha.sdk.runtime.FlezhaImpl.LIVE_INSTANCES.put("lifecycle_a", 1);

        assertThatThrownBy(() -> runner.removeUnbounded(OperativeA.class))
                .isInstanceOf(OperativeTypeInUseException.class)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lifecycle_a");

        assertThat(com.flezha.sdk.runtime.FlezhaImpl.LAST_DECLARED_SUPPLIER.get())
                .as("the type stays defined after a refused removal")
                .containsExactly("lifecycle_a");
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.SYNCED_SETS)
                .as("a refused removal ships nothing")
                .isEmpty();

        // Once the instances stop, the removal succeeds.
        com.flezha.sdk.runtime.FlezhaImpl.LIVE_INSTANCES.remove("lifecycle_a");
        runner.removeUnbounded(OperativeA.class);
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.SYNCED_SETS).containsExactly(List.of());
    }

    @Test
    @DisplayName("removing an undefined type throws IllegalArgumentException naming the factory id")
    void removeUndefinedTypeIsRejected() {
        FlezhaRunner runner = Flezha.createRunner("ns_undef").defineUnbounded(OperativeA.class);
        assertThatThrownBy(() -> runner.removeUnbounded(OperativeB.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lifecycle_b");
        // Already-removed counts as undefined too.
        runner.removeUnbounded(OperativeA.class);
        assertThatThrownBy(() -> runner.removeUnbounded(OperativeA.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lifecycle_a");
    }

    @Test
    @DisplayName("defining a duplicate factory id on one runner is rejected")
    void duplicateFactoryIdIsRejected() {
        FlezhaRunner runner = Flezha.createRunner("ns_dup").defineUnbounded(OperativeA.class);
        assertThatThrownBy(() -> runner.defineUnbounded(OperativeA.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lifecycle_a");
    }

    @Test
    @DisplayName("pre-start awaitTermination()/getOperatives() throw ISE; close() is a no-op")
    void preStartLifecycleCalls() {
        FlezhaRunner runner = Flezha.createRunner("ns_cold");
        assertThatThrownBy(runner::awaitTermination)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not started");
        assertThatThrownBy(runner::getOperatives)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not started");
        assertThatCode(runner::close).doesNotThrowAnyException();
        assertThat(Flezha.currentRunner())
                .as("a never-started runner never appears")
                .isEmpty();
    }

    @Test
    @DisplayName("a runner may be started again after the first one closes")
    void restartAfterCloseSucceeds() {
        Flezha.createRunner("ns1").defineUnbounded(OperativeA.class).start().close();
        assertThat(Flezha.currentRunner()).isEmpty();

        FlezhaRunner second =
                Flezha.createRunner("ns2").defineUnbounded(OperativeB.class).start();
        assertThat(second.getNamespace()).isEqualTo("ns2");
        assertThat(Flezha.currentRunner()).containsSame(second);
        assertThat(com.flezha.sdk.runtime.FlezhaImpl.LAST_DECLARED_SUPPLIER.get())
                .as("generation 2's declared set starts empty and re-defines its own types")
                .containsExactly("lifecycle_b");
    }

    @Test
    @DisplayName("try-with-resources starts the runner for the block and closes it at the end")
    void tryWithResourcesWorks() {
        try (FlezhaRunner r =
                Flezha.createRunner("ns_try").defineUnbounded(OperativeA.class).start()) {
            assertThat(r.getNamespace()).isEqualTo("ns_try");
            assertThat(Flezha.currentRunner()).containsSame(r);
        }
        assertThat(Flezha.currentRunner()).isEmpty();
    }

    @Test
    @DisplayName("close() from another thread wakes a thread blocked in awaitTermination()")
    void closeWakesAwaitTermination() throws Exception {
        FlezhaRunner runner = Flezha.createRunner("ns_await")
                .defineUnbounded(OperativeA.class)
                .start();
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
                Flezha.createRunner("ns_cur").defineUnbounded(OperativeA.class).start();
        assertThat(Flezha.currentRunner()).containsSame(runner);
        runner.close();
        assertThat(Flezha.currentRunner()).isEmpty();
    }

    @Test
    @DisplayName("two concurrent start() calls — exactly one wins, the loser is told the winner's namespace")
    void concurrentStartsExactlyOneWins() throws Exception {
        FlezhaRunner r1 = Flezha.createRunner("ns_one").defineUnbounded(OperativeA.class);
        FlezhaRunner r2 = Flezha.createRunner("ns_two").defineUnbounded(OperativeB.class);

        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Object> f1 = pool.submit(() -> race(barrier, r1));
            Future<Object> f2 = pool.submit(() -> race(barrier, r2));
            Object o1 = f1.get(10, TimeUnit.SECONDS);
            Object o2 = f2.get(10, TimeUnit.SECONDS);

            FlezhaRunner winner;
            IllegalStateException loser;
            if (o1 instanceof FlezhaRunner w) {
                winner = w;
                loser = (IllegalStateException) o2;
            } else {
                winner = (FlezhaRunner) o2;
                loser = (IllegalStateException) o1;
            }
            assertThat(loser)
                    .as("the loser must be told the winner's namespace")
                    .hasMessageContaining(winner.getNamespace());
            assertThat(Flezha.currentRunner()).containsSame(winner);
        } finally {
            pool.shutdownNow();
        }
    }

    private static Object race(CyclicBarrier barrier, FlezhaRunner runner) throws Exception {
        barrier.await();
        try {
            return runner.start();
        } catch (IllegalStateException e) {
            return e;
        }
    }

    @Test
    @DisplayName("createRunner rejects a null or empty namespace")
    void createRunnerValidatesNamespace() {
        assertThatThrownBy(() -> Flezha.createRunner("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Flezha.createRunner(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
