package com.flezha.sdk.runtime.reconnect;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReconnectBackoffTest {

    @Test
    @DisplayName("backoff starts at 200 ms, doubles, caps at 30 s")
    void backoffSchedule() {
        long[] table = ReconnectCoordinator.BACKOFF_MS;
        assertThat(table).isNotEmpty();
        assertThat(table[0]).isEqualTo(200);
        assertThat(table[1]).isEqualTo(400);
        assertThat(table[2]).isEqualTo(800);
        // Eventually caps at 30 seconds.
        assertThat(table[table.length - 1]).isEqualTo(30_000);
        // Monotonic non-decreasing.
        for (int i = 1; i < table.length; i++) {
            assertThat(table[i]).isGreaterThanOrEqualTo(table[i - 1]);
        }
    }
}
