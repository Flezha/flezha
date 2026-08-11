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
 * environment the Rust SDK does; the Java layer only sizes its fetcher set.
 */
public final class FlezhaEnv {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaEnv.class);

    public static final String FLEZHA_WORKER_POOL_SIZE = "FLEZHA_WORKER_POOL_SIZE";

    /**
     * Fetchers never run user code — they poll, decode and hand off to a
     * virtual thread — so a small fixed set saturates the core's handout.
     * How much work is underway is the core's number, not this one.
     */
    public static final int DEFAULT_FETCHER_COUNT = 2;

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
        return new ResolvedConfig(parseFetcherCount(lookup.apply(FLEZHA_WORKER_POOL_SIZE)));
    }

    private static int parseFetcherCount(String raw) {
        if (raw == null || raw.isBlank()) {
            return DEFAULT_FETCHER_COUNT;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            return parsed <= 0 ? DEFAULT_FETCHER_COUNT : parsed;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}={}; falling back to {}", FLEZHA_WORKER_POOL_SIZE, raw, DEFAULT_FETCHER_COUNT);
            return DEFAULT_FETCHER_COUNT;
        }
    }

    public record ResolvedConfig(int fetcherCount) {}
}
