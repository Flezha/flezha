package com.flezha.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.state.PartitionFlusher;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.testing.ShardTestHarness;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JavaStatePersistenceTest {

    @BeforeEach
    void resetFlusher() {
        PartitionFlusher.resetForTesting();
    }

    @Test
    void counterShardRecordsChangesAndReplaysDeterministically() {
        ShardTestHarness<Long, CounterShard.State, Void, CounterChange> h = ShardTestHarness
                .of(new CounterShard())
                .applyInputs(List.of(1L, 2L, 3L));

        assertThat(h.recordedChanges()).containsExactly(
                new CounterChange(1L), new CounterChange(2L), new CounterChange(3L));
        assertThat(h.state().value).isEqualTo(6L);
        assertThat(h.replayEquals()).isEmpty();
    }

    @Test
    void coalesceCollapsesKeyedChangesToLatest() {
        // Five Changes with two distinct (interval, ts) keys should collapse
        // to two: the latest (interval=1, ts=10) and the latest
        // (interval=2, ts=20).
        TickChange a1 = new TickChange(1, 10, 1);
        TickChange a2 = new TickChange(1, 10, 2);  // collapses with a3
        TickChange a3 = new TickChange(1, 10, 5);  // latest of (1, 10)
        TickChange b1 = new TickChange(2, 20, 7);
        TickChange b2 = new TickChange(2, 20, 9);  // latest of (2, 20)

        var coalesced = PartitionFlusher.coalesce(List.of(a1, a2, a3, b1, b2));
        assertThat(coalesced).containsExactly(a3, b2);
    }

    @Test
    void coalesceKeyHashEquivalence() {
        CoalesceKey k1 = CoalesceKey.fromHash(1, 10L);
        CoalesceKey k2 = CoalesceKey.fromHash(1, 10L);
        assertThat(k1).isEqualTo(k2);
    }

    @Test
    void persistenceOffRelayDoesNotRegisterFlusherForPersistenceCheck() {
        // Indirect check: the toggle is read from Relay.persistenceEnabled().
        assertThat(new PersistenceOffRelay().persistenceEnabled()).isFalse();
        assertThat(new StatefulCounterRelay().persistenceEnabled()).isTrue();
    }
}
