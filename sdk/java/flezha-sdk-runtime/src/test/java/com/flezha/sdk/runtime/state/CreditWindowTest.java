package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Java mirror of the Rust SDK's credit-window tests
 * ({@code sdk/rust/src/entity/credit.rs}): debit/park/grant semantics,
 * stale-grant discard, reconnect reset, oversized-frame overshoot, and
 * the parked-liveness counter. The window is process-global, so the suite
 * is serialized by JUnit's default same-thread execution and each test
 * starts from a disarmed state.
 */
class CreditWindowTest {

    @BeforeEach
    @AfterEach
    void reset() {
        CreditWindow.disarm();
    }

    @Test
    @DisplayName("legacy session (no init) never debits or parks")
    @Timeout(5)
    void legacySessionNeverDebitsOrParks() {
        CreditWindow.acquire(1_000_000);
        assertThat(CreditWindow.currentSession()).isNull();
    }

    @Test
    @DisplayName("armed window debits; exhaustion parks until a grant arrives; counter advances")
    @Timeout(10)
    void exhaustionParksUntilGrant() throws Exception {
        CreditWindow.arm("s1", 100);
        CreditWindow.acquire(60);
        assertThat(CreditWindow.availableBytes()).isEqualTo(40);

        long parkedBefore = CreditWindow.creditParked();
        CountDownLatch sent = new CountDownLatch(1);
        Thread sender = new Thread(() -> {
            CreditWindow.acquire(60); // only 40 available -> parks
            sent.countDown();
        });
        sender.start();

        assertThat(sent.await(200, TimeUnit.MILLISECONDS))
                .as("sender must park while credit is insufficient")
                .isFalse();
        assertThat(CreditWindow.creditParked())
                .as("park counter must advance while parked")
                .isGreaterThan(parkedBefore);

        CreditWindow.grant("s1", 60);
        assertThat(sent.await(2, TimeUnit.SECONDS))
                .as("grant must unpark the sender")
                .isTrue();
        sender.join(Duration.ofSeconds(2).toMillis());
        assertThat(CreditWindow.availableBytes()).isEqualTo(40);
    }

    @Test
    @DisplayName("stale-session grants are discarded; a re-attach resets to a full window")
    @Timeout(5)
    void staleGrantDiscardedAndReconnectResets() {
        CreditWindow.arm("s1", 100);
        CreditWindow.acquire(100);
        CreditWindow.grant("s-old", 100);
        assertThat(CreditWindow.availableBytes())
                .as("stale-session grant must not apply")
                .isEqualTo(0);

        CreditWindow.arm("s2", 100);
        assertThat(CreditWindow.availableBytes()).isEqualTo(100);
        CreditWindow.grant("s1", 50);
        assertThat(CreditWindow.availableBytes())
                .as("grant for the dead session is void")
                .isEqualTo(100);
    }

    @Test
    @DisplayName("a frame larger than the window sends at full window (single-frame overshoot)")
    @Timeout(10)
    void oversizedFrameSendsOnFullWindow() throws Exception {
        CreditWindow.arm("s1", 100);
        CreditWindow.acquire(250);
        assertThat(CreditWindow.availableBytes()).isEqualTo(-150);

        CountDownLatch sent = new CountDownLatch(1);
        Thread sender = new Thread(() -> {
            CreditWindow.acquire(10);
            sent.countDown();
        });
        sender.start();
        assertThat(sent.await(200, TimeUnit.MILLISECONDS)).isFalse();

        CreditWindow.grant("s1", 250); // caps at window_bytes
        assertThat(sent.await(2, TimeUnit.SECONDS)).isTrue();
        sender.join(Duration.ofSeconds(2).toMillis());
        assertThat(CreditWindow.availableBytes()).isEqualTo(90);
    }

    @Test
    @DisplayName("disarm (reconnect) wakes parked senders into the legacy path")
    @Timeout(10)
    void disarmWakesParkedSenders() throws Exception {
        CreditWindow.arm("s1", 10);
        CreditWindow.acquire(10);

        CountDownLatch sent = new CountDownLatch(1);
        Thread sender = new Thread(() -> {
            CreditWindow.acquire(10);
            sent.countDown();
        });
        sender.start();
        assertThat(sent.await(200, TimeUnit.MILLISECONDS)).isFalse();

        CreditWindow.disarm();
        assertThat(sent.await(2, TimeUnit.SECONDS))
                .as("disarm must release parked senders")
                .isTrue();
        sender.join(Duration.ofSeconds(2).toMillis());
    }
}
