package com.flezha.sdk.runtime.partition;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.channel.OverflowPolicy;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java mirror of the Rust SDK's bounded mailbox unit tests
 * ({@code sdk/rust/src/executor/mailbox.rs}). Exercises the three
 * {@link OverflowPolicy} variants in isolation; end-to-end dispatcher
 * behaviour is covered by {@code OverflowPolicyTest}.
 */
class PartitionMailboxTest {

    @Test
    @DisplayName("DROP_NEWEST: a push on a full mailbox is discarded and counted")
    void dropNewestDiscardsAndCounts() {
        PartitionMailbox<Integer> mbox = new PartitionMailbox<>(2, OverflowPolicy.DROP_NEWEST);
        assertThat(mbox.push(1)).isTrue(); // inflight CAS won
        assertThat(mbox.push(2)).isFalse(); // inflight already true
        assertThat(mbox.push(3)).isFalse(); // dropped
        assertThat(mbox.dropsNewestTotal()).isEqualTo(1);
        assertThat(mbox.size()).isEqualTo(2);

        assertThat(mbox.pop()).isEqualTo(1);
        assertThat(mbox.pop()).isEqualTo(2);
        assertThat(mbox.pop()).isNull();
    }

    @Test
    @DisplayName("DROP_OLDEST: a push on a full mailbox evicts the oldest item")
    void dropOldestEvictsAndCounts() {
        PartitionMailbox<Integer> mbox = new PartitionMailbox<>(2, OverflowPolicy.DROP_OLDEST);
        mbox.push(1);
        mbox.push(2);
        // No third push needed yet — capacity hit forces a poll+offer.
        mbox.push(3);
        assertThat(mbox.dropsOldestTotal()).isEqualTo(1);
        // Oldest (1) was evicted; queue holds (2, 3) in FIFO order.
        assertThat(mbox.pop()).isEqualTo(2);
        assertThat(mbox.pop()).isEqualTo(3);
        assertThat(mbox.pop()).isNull();
    }

    @Test
    @DisplayName("BLOCK: producer parks until a consumer drains space")
    void blockParksUntilSpace() throws Exception {
        PartitionMailbox<Integer> mbox = new PartitionMailbox<>(1, OverflowPolicy.BLOCK);
        assertThat(mbox.push(1)).isTrue();

        AtomicBoolean pushed = new AtomicBoolean(false);
        Thread producer = new Thread(() -> {
            mbox.push(2);
            pushed.set(true);
        });
        producer.start();
        // Producer should still be parked — capacity = 1, mailbox full.
        Thread.sleep(50);
        assertThat(pushed).isFalse();

        // Free up a slot; producer's put() returns.
        mbox.pop();
        producer.join(2_000);
        assertThat(pushed).isTrue();
        assertThat(mbox.pop()).isEqualTo(2);
    }

    @Test
    @DisplayName("capacity and policy are stable accessors")
    void capacityAndPolicyAccessors() {
        PartitionMailbox<Integer> mbox = new PartitionMailbox<>(8, OverflowPolicy.DROP_OLDEST);
        assertThat(mbox.capacity()).isEqualTo(8);
        assertThat(mbox.policy()).isEqualTo(OverflowPolicy.DROP_OLDEST);
        mbox.push(1);
        mbox.push(2);
        assertThat(mbox.capacity()).isEqualTo(8);
        assertThat(mbox.size()).isEqualTo(2);
    }

    @Test
    @DisplayName("constructor rejects capacity <= 0")
    void rejectsNonPositiveCapacity() {
        try {
            new PartitionMailbox<>(0, OverflowPolicy.BLOCK);
            assertThat(false).as("expected IllegalArgumentException").isTrue();
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage()).contains("capacity must be > 0");
        }
    }
}
