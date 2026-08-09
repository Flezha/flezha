package com.flezha.sdk.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlezhaEnvTest {

    @Test
    @DisplayName("empty env → worker pool defaults to available processors")
    void defaults() {
        var resolved = FlezhaEnv.resolve(Map.of());
        assertThat(resolved.workerPoolSize()).isPositive();
    }

    @Test
    @DisplayName("FLEZHA_WORKER_POOL_SIZE override parses as int")
    void workerPoolOverride() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "42"));
        assertThat(resolved.workerPoolSize()).isEqualTo(42);
    }

    @Test
    @DisplayName("malformed worker pool size falls back to default")
    void workerPoolMalformedFallback() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "notanumber"));
        assertThat(resolved.workerPoolSize()).isPositive();
    }
}
