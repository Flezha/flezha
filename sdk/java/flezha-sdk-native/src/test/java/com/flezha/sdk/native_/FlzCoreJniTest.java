package com.flezha.sdk.native_;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * JNI smoke tests for the poll-core bridge. Tagged {@code jni} so they only
 * run when the host-triple {@code flz_core_ffi} cdylib has been staged into
 * {@code flezha-sdk-native/src/main/resources/native/}.
 *
 * <p>These tests do NOT require a live controller.
 */
@Tag("jni")
class FlzCoreJniTest {

    @Test
    @DisplayName("native core speaks the shim's schema version")
    void schemaVersionMatches() {
        assertThat(FlzCore.schemaVersion()).isEqualTo(FlzCore.SCHEMA_VERSION);
    }

    @Test
    @DisplayName("context create → free round-trip")
    void contextLifecycle() {
        long ctx = FlzCore.newContext();
        assertThat(ctx).isNotZero();
        FlzCore.contextFree(ctx);
    }

    @Test
    @DisplayName("schema version mismatch throws, naming both versions")
    void versionMismatchThrows() {
        assertThatThrownBy(() -> FlzCore.contextNew(999))
                .isInstanceOf(FlezhaNativeException.class)
                .hasMessageContaining("999")
                .hasMessageContaining(String.valueOf(FlzCore.SCHEMA_VERSION));
    }

    @Test
    @DisplayName("the monotonic clock advances and never goes backwards")
    void monotonicClockAdvances() {
        long first = FlzCore.monotonicNanos();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        long later = first;
        while (later == first) {
            assertThat(System.nanoTime())
                    .as("the monotonic clock never advanced")
                    .isLessThan(deadline);
            later = FlzCore.monotonicNanos();
            assertThat(later).as("the monotonic clock went backwards").isGreaterThanOrEqualTo(first);
        }
        assertThat(later).isGreaterThan(first);
    }
}
