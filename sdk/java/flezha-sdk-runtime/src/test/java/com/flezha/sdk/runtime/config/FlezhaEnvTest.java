package com.flezha.sdk.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlezhaEnvTest {

    @Test
    @DisplayName("empty env → a small fixed fetcher set, not one per core")
    void defaults() {
        var resolved = FlezhaEnv.resolve(Map.of());
        assertThat(resolved.fetcherCount()).isEqualTo(FlezhaEnv.DEFAULT_FETCHER_COUNT);
    }

    @Test
    @DisplayName("FLEZHA_WORKER_POOL_SIZE override parses as int")
    void fetcherCountOverride() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "42"));
        assertThat(resolved.fetcherCount()).isEqualTo(42);
    }

    @Test
    @DisplayName("malformed fetcher count falls back to the default")
    void fetcherCountMalformedFallback() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "notanumber"));
        assertThat(resolved.fetcherCount()).isEqualTo(FlezhaEnv.DEFAULT_FETCHER_COUNT);
    }

    @Test
    @DisplayName("the supervisor ceiling defaults to a multiple of the configured count")
    void ceilingDefaultsToAMultiple() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "3"));
        assertThat(resolved.fetcherMax()).isEqualTo(3 * FlezhaEnv.DEFAULT_FETCHER_MAX_MULTIPLE);
    }

    @Test
    @DisplayName("FLEZHA_WORKER_POOL_MAX overrides the ceiling")
    void ceilingOverride() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "2", "FLEZHA_WORKER_POOL_MAX", "5"));
        assertThat(resolved.fetcherMax()).isEqualTo(5);
    }

    @Test
    @DisplayName("a ceiling below the configured count never shrinks the pool")
    void ceilingNeverBelowTheCount() {
        var resolved = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "8", "FLEZHA_WORKER_POOL_MAX", "2"));
        assertThat(resolved.fetcherCount()).isEqualTo(8);
        assertThat(resolved.fetcherMax()).isEqualTo(8);
    }
}
