package com.flezha.sdk.runtime.bounded;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Crash-resume gate unit tests for {@link BoundedOperativeInstance} — the Java
 * mirror of the Rust SDK's {@code bounded::runtime} tests. A durable structured
 * bounded driver holds its first Operation on {@code awaitRestore} until the
 * Controller's Segment restores have landed; the count is the authoritative
 * release signal, with a cancel/interrupt short-circuit and a safety-net timeout.
 * Deterministic (a start-latch parks the waiter before the wake; no timing
 * sleeps).
 */
class BoundedResumeGateTest {

    private static final long JOIN_TIMEOUT_MS = 5_000L;

    @Test
    @DisplayName("expected == 0 ⇒ awaitRestore returns immediately")
    void zeroExpectedReturnsImmediately() throws Exception {
        assertReturns(awaitOn(gate(0)));
    }

    @Test
    @DisplayName("recording `expected` restores up front releases awaitRestore (count is the release signal)")
    void recordedRestoresReleaseImmediately() throws Exception {
        BoundedOperativeInstance instance = gate(2);
        instance.noteRestored();
        instance.noteRestored();
        assertReturns(awaitOn(instance));
    }

    @Test
    @DisplayName("cancel() short-circuits the gate even with restores outstanding")
    void cancelShortCircuitsGate() throws Exception {
        BoundedOperativeInstance instance = gate(5);
        instance.cancel();
        assertReturns(awaitOn(instance));
    }

    @Test
    @DisplayName("interrupting the parked driver short-circuits the gate")
    void interruptShortCircuitsGate() throws Exception {
        BoundedOperativeInstance instance = gate(5);
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(
                () -> {
                    started.countDown();
                    instance.awaitRestore();
                },
                "await-restore-interrupt");
        waiter.setDaemon(true);
        waiter.start();
        assertThat(started.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .as("waiter thread started")
                .isTrue();
        waiter.interrupt();
        assertReturns(waiter);
    }

    @Test
    @DisplayName("a parked awaitRestore wakes once the last noteRestored lands")
    void noteRestoredWakesBlockedWaiter() throws Exception {
        BoundedOperativeInstance instance = gate(1);
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(
                () -> {
                    started.countDown();
                    instance.awaitRestore();
                },
                "await-restore-blocked");
        waiter.setDaemon(true);
        waiter.start();
        assertThat(started.await(JOIN_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .as("waiter thread parked")
                .isTrue();
        // The last expected restore releases the parked waiter via notifyAll.
        instance.noteRestored();
        assertReturns(waiter);
    }

    // --- helpers -------------------------------------------------------------

    /** A gate expecting {@code expected} restores, with no real Segments (the gate never reads them). */
    private static BoundedOperativeInstance gate(int expected) {
        BoundedOperativeInstance instance = new BoundedOperativeInstance("resume-inst", "test-factory");
        instance.wireStageSegments(List.of(), expected);
        return instance;
    }

    /** Run {@code awaitRestore} on a daemon thread so a hang fails the join rather than the suite. */
    private static Thread awaitOn(BoundedOperativeInstance instance) {
        Thread t = new Thread(instance::awaitRestore, "await-restore-test");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void assertReturns(Thread t) throws InterruptedException {
        t.join(JOIN_TIMEOUT_MS);
        assertThat(t.isAlive()).as("awaitRestore returns").isFalse();
    }
}
