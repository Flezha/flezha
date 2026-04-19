package com.flezha.sdk.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.native_.FlezhaTransport;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlezhaEnvTest {

    @Test
    @DisplayName("empty env → defaults; transport UDS")
    void defaults() {
        var resolved = FlezhaEnv.resolve(Map.of());
        assertThat(resolved.config().transport()).isEqualTo(FlezhaTransport.UDS);
        assertThat(resolved.config().quicAddress()).isEqualTo("127.0.0.1:4433");
        assertThat(resolved.config().caCertPath()).isEqualTo("app_data/certs/ca.pem");
        assertThat(resolved.config().socketFilesPath()).isEqualTo("app_data/sockets");
        assertThat(resolved.workerPoolSize()).isPositive();
    }

    @Test
    @DisplayName("FLEZHA_TRANSPORT=quic selects QUIC transport")
    void quicTransport() {
        var resolved = FlezhaEnv.resolve(Map.of(
                "FLEZHA_TRANSPORT", "quic",
                "FLEZHA_QUIC_ADDRESS", "10.0.0.1:9999"));
        assertThat(resolved.config().transport()).isEqualTo(FlezhaTransport.QUIC);
        assertThat(resolved.config().quicAddress()).isEqualTo("10.0.0.1:9999");
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
