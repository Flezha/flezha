package com.flezha.sdk.runtime.config;

import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the runtime's host-side configuration from {@code FLEZHA_*}
 * environment variables. Transport configuration (socket paths, QUIC
 * address, certificates) is the poll core's — the core reads the same
 * environment the Rust SDK does; the Java layer only sizes its poller pool.
 */
public final class FlezhaEnv {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaEnv.class);

    public static final String FLEZHA_WORKER_POOL_SIZE = "FLEZHA_WORKER_POOL_SIZE";

    private FlezhaEnv() {}

    /** Resolve from the real process environment. */
    @Nonnull
    public static ResolvedConfig resolveFromEnv() {
        return resolve(System::getenv);
    }

    /** Resolve from a map — useful for tests. */
    @Nonnull
    public static ResolvedConfig resolve(@Nonnull Map<String, String> env) {
        return resolve(env::get);
    }

    @Nonnull
    static ResolvedConfig resolve(@Nonnull Function<String, String> lookup) {
        return new ResolvedConfig(parseWorkerPoolSize(lookup.apply(FLEZHA_WORKER_POOL_SIZE)));
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

    public record ResolvedConfig(int workerPoolSize) {}
}
