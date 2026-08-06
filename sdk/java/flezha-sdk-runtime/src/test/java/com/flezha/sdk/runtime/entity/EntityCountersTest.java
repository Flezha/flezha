package com.flezha.sdk.runtime.entity;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.OverflowPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The heartbeat's dispatch-progress counters.
 *
 * <p>Reporting only {@code credit_parked} — as this SDK did — leaves the
 * controller's stall classifier unable to tell a wedged Java runner from a
 * healthy idle one: the heartbeat thread keeps ticking either way. That is a
 * liveness defect, not a cosmetic gap, which is why these exist.
 */
class EntityCountersTest {

    @Test
    @DisplayName("a blocked producer is counted as a park episode")
    void blockedProducerIsCounted() throws Exception {
        long before = EntityCounters.blockedParkEpisodes();

        EntityMailbox<String> mailbox = new EntityMailbox<>(1, OverflowPolicy.BLOCK);
        mailbox.push("first"); // fits — not back-pressure, not a park

        assertThat(EntityCounters.blockedParkEpisodes())
                .as("an offer that fits is not a park")
                .isEqualTo(before);

        // The mailbox is now full. A second producer must park; drain from another
        // thread so it can make progress.
        Thread drainer = new Thread(() -> {
            try {
                Thread.sleep(50L);
                mailbox.pop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        drainer.start();
        mailbox.push("second");
        drainer.join();

        assertThat(EntityCounters.blockedParkEpisodes())
                .as("the full-mailbox push parked")
                .isGreaterThan(before);
    }

    @Test
    @DisplayName("dispatches advance monotonically")
    void dispatchesAdvance() {
        long before = EntityCounters.dispatchesCompleted();
        EntityCounters.noteDispatch();
        assertThat(EntityCounters.dispatchesCompleted()).isEqualTo(before + 1);
    }
}
