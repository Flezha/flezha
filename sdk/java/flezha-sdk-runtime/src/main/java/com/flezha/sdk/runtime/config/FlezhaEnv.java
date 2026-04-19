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

    private FlezhaEnv() {
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
        String quicAddress = valueOrDefault(
                lookup.apply(FLEZHA_QUIC_ADDRESS), FlezhaConfig.DEFAULT_QUIC_ADDRESS);
        String caCertPath = valueOrDefault(
                lookup.apply(FLEZHA_CA_CERT_PATH), FlezhaConfig.DEFAULT_CA_CERT_PATH);
        String serverCertPath = valueOrDefault(
                lookup.apply(FLEZHA_SERVER_CERT_PATH), FlezhaConfig.DEFAULT_SERVER_CERT_PATH);
        String serverKeyPath = valueOrDefault(
                lookup.apply(FLEZHA_SERVER_KEY_PATH), FlezhaConfig.DEFAULT_SERVER_KEY_PATH);
        String socketFilesPath = valueOrDefault(
                lookup.apply(FLEZHA_SOCKET_FILES_PATH), FlezhaConfig.DEFAULT_SOCKET_FILES_PATH);
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
            LOG.warn("Invalid {}={}; falling back to Runtime.availableProcessors()",
                    FLEZHA_WORKER_POOL_SIZE, raw);
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

    public record ResolvedConfig(@Nonnull FlezhaConfig config, int workerPoolSize) {
    }
}
