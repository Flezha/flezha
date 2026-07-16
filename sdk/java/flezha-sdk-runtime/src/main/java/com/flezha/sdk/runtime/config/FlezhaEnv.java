package com.flezha.sdk.runtime.config;

import com.flezha.sdk.native_.FlezhaConfig;
import com.flezha.sdk.native_.FlezhaTransport;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads runtime configuration from {@code FLEZHA_*} environment variables.
 * Defaults mirror {@code docs/sdk_protocol_spec.md} §2 and match the Rust
 * SDK's {@code flz_transport::transport::config} getters.
 */
public final class FlezhaEnv {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaEnv.class);

    public static final String FLEZHA_TRANSPORT = "FLEZHA_TRANSPORT";
    public static final String FLEZHA_QUIC_ADDRESS = "FLEZHA_QUIC_ADDRESS";
    public static final String FLEZHA_CA_CERT_PATH = "FLEZHA_CA_CERT_PATH";
    public static final String FLEZHA_SERVER_CERT_PATH = "FLEZHA_SERVER_CERT_PATH";
    public static final String FLEZHA_SERVER_KEY_PATH = "FLEZHA_SERVER_KEY_PATH";
    public static final String FLEZHA_SOCKET_FILES_PATH = "FLEZHA_SOCKET_FILES_PATH";
    public static final String FLEZHA_WORKER_POOL_SIZE = "FLEZHA_WORKER_POOL_SIZE";

    /** Control-channel liveness knobs — shared with the Rust SDK / flz_transport. */
    public static final String FLEZHA_CONTROL_ACK_TIMEOUT_MS = "FLEZHA_CONTROL_ACK_TIMEOUT_MS";

    public static final String FLEZHA_CONTROL_KEEPALIVE_MS = "FLEZHA_CONTROL_KEEPALIVE_MS";

    public static final String FLEZHA_DATA_HEARTBEAT_MS = "FLEZHA_DATA_HEARTBEAT_MS";

    static final long DEFAULT_CONTROL_ACK_TIMEOUT_MS = 10_000L;
    static final long DEFAULT_CONTROL_KEEPALIVE_MS = 15_000L;
    static final long DEFAULT_DATA_HEARTBEAT_MS = 10_000L;

    private FlezhaEnv() {}

    /**
     * Bound on the wait for the controller's first frame (the assign-runner-id
     * ack). Default 10 s, matching {@code flz_transport::liveness}.
     */
    public static long controlAckTimeoutMs() {
        return envMs(FLEZHA_CONTROL_ACK_TIMEOUT_MS, DEFAULT_CONTROL_ACK_TIMEOUT_MS);
    }

    /** How often the SDK sends a control-channel keepalive ping. Default 15 s. */
    public static long controlKeepaliveMs() {
        return envMs(FLEZHA_CONTROL_KEEPALIVE_MS, DEFAULT_CONTROL_KEEPALIVE_MS);
    }

    /** How often the SDK heartbeats the data channel. Default 10 s, matching the Rust SDK. */
    public static long dataHeartbeatMs() {
        return envMs(FLEZHA_DATA_HEARTBEAT_MS, DEFAULT_DATA_HEARTBEAT_MS);
    }

    private static long envMs(String key, long defaultMs) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return defaultMs;
        }
        try {
            long ms = Long.parseLong(raw.trim());
            return ms > 0 ? ms : defaultMs;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}={}; falling back to {} ms", key, raw, defaultMs);
            return defaultMs;
        }
    }

    /** Build a config from the real process environment. */
    @Nonnull
    public static ResolvedConfig resolveFromEnv() {
        return resolve(System::getenv);
    }

    /** Build a config from a map — useful for tests. */
    @Nonnull
    public static ResolvedConfig resolve(@Nonnull Map<String, String> env) {
        return resolve(env::get);
    }

    @Nonnull
    static ResolvedConfig resolve(@Nonnull Function<String, String> lookup) {
        FlezhaTransport transport = FlezhaTransport.parse(lookup.apply(FLEZHA_TRANSPORT));
        String quicAddress = valueOrDefault(lookup.apply(FLEZHA_QUIC_ADDRESS), FlezhaConfig.DEFAULT_QUIC_ADDRESS);
        String caCertPath = valueOrDefault(lookup.apply(FLEZHA_CA_CERT_PATH), FlezhaConfig.DEFAULT_CA_CERT_PATH);
        String serverCertPath =
                valueOrDefault(lookup.apply(FLEZHA_SERVER_CERT_PATH), FlezhaConfig.DEFAULT_SERVER_CERT_PATH);
        String serverKeyPath =
                valueOrDefault(lookup.apply(FLEZHA_SERVER_KEY_PATH), FlezhaConfig.DEFAULT_SERVER_KEY_PATH);
        String socketFilesPath =
                valueOrDefault(lookup.apply(FLEZHA_SOCKET_FILES_PATH), FlezhaConfig.DEFAULT_SOCKET_FILES_PATH);
        int workerPoolSize = parseWorkerPoolSize(lookup.apply(FLEZHA_WORKER_POOL_SIZE));

        FlezhaConfig cfg = FlezhaConfig.builder()
                .transport(transport)
                .quicAddress(quicAddress)
                .caCertPath(caCertPath)
                .serverCertPath(serverCertPath)
                .serverKeyPath(serverKeyPath)
                .socketFilesPath(socketFilesPath)
                .workerPoolSize(workerPoolSize)
                .build();

        return new ResolvedConfig(cfg, workerPoolSize);
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static int parseWorkerPoolSize(String raw) {
        if (raw == null || raw.isBlank()) {
            return Runtime.getRuntime().availableProcessors();
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            return parsed <= 0 ? Runtime.getRuntime().availableProcessors() : parsed;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}={}; falling back to Runtime.availableProcessors()", FLEZHA_WORKER_POOL_SIZE, raw);
            return Runtime.getRuntime().availableProcessors();
        }
    }

    /** Log the resolved config at INFO, redacting cert paths that look sensitive. */
    public static void logConfig(@Nonnull ResolvedConfig resolved) {
        FlezhaConfig c = resolved.config();
        LOG.info(
                "Flezha SDK config: transport={}, quicAddress={}, socketFilesPath={}, "
                        + "caCertPath={}, serverCertPath={}, serverKeyPath={}, workerPoolSize={}",
                c.transport().nativeName(),
                c.quicAddress(),
                c.socketFilesPath(),
                redact(c.caCertPath()),
                redact(c.serverCertPath()),
                redact(c.serverKeyPath()),
                resolved.workerPoolSize());
    }

    private static String redact(String path) {
        if (path == null) {
            return "<unset>";
        }
        String lower = path.toLowerCase();
        if (lower.contains("secret") || lower.contains("private") || lower.endsWith("key.pem")) {
            return "<redacted>";
        }
        return path;
    }

    public record ResolvedConfig(@Nonnull FlezhaConfig config, int workerPoolSize) {}
}
