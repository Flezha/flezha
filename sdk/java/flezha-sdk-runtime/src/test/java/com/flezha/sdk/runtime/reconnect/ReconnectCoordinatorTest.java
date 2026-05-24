package com.flezha.sdk.runtime.reconnect;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.ExecutorStatus;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Reconnect machinery behind the Java parity fixes (self-healing-control-plane
 * Phase 8.4/8.5): a lost control channel drives a reconnect (the accept-but-
 * never-ack strand is broken once flz_jni surfaces the timeout as
 * onConnectionLost), an intentional stop does not, and the backoff is jittered.
 */
class ReconnectCoordinatorTest {

    private static ExecutorContext newCtx() {
        return new ExecutorContext("ns", FlezhaEnv.resolve(Map.of()), 0L);
    }

    @Test
    @DisplayName("onConnectionLost drives a reconnect attempt and resets executor id")
    void onConnectionLostDrivesReconnect() throws InterruptedException {
        ExecutorContext ctx = newCtx();
        ctx.setStatus(ExecutorStatus.RUNNING);
        ctx.setExecutorId("e1");

        CountDownLatch reconnected = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        ReconnectCoordinator coord = new ReconnectCoordinator(ctx, () -> {
            attempts.incrementAndGet();
            reconnected.countDown();
            return true;
        });
        coord.start();
        try {
            coord.onConnectionLost();

            assertThat(reconnected.await(3, TimeUnit.SECONDS))
                    .as("a lost control channel must drive a reconnect attempt")
                    .isTrue();
            assertThat(attempts.get()).isGreaterThanOrEqualTo(1);
            assertThat(ctx.executorId()).isEqualTo("undefined");
        } finally {
            coord.shutdown();
        }
    }

    @Test
    @DisplayName("a STOPPED executor does not reconnect")
    void stoppedDoesNotReconnect() throws InterruptedException {
        ExecutorContext ctx = newCtx();
        ctx.setStatus(ExecutorStatus.STOPPED);

        AtomicInteger attempts = new AtomicInteger();
        ReconnectCoordinator coord = new ReconnectCoordinator(ctx, () -> {
            attempts.incrementAndGet();
            return true;
        });
        coord.start();
        try {
            coord.onConnectionLost();
            Thread.sleep(300);
            assertThat(attempts.get()).isZero();
        } finally {
            coord.shutdown();
        }
    }

    @Test
    @DisplayName("backoff jitter stays within [base/2, base]; tiny bases pass through")
    void jitterBounds() {
        for (int i = 0; i < 200; i++) {
            assertThat(ReconnectCoordinator.jitter(1000)).isBetween(500L, 1000L);
        }
        assertThat(ReconnectCoordinator.jitter(1)).isEqualTo(1);
        assertThat(ReconnectCoordinator.jitter(0)).isEqualTo(0);
    }
}
