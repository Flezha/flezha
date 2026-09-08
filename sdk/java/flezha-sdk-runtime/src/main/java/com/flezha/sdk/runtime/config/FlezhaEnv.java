package com.flezha.sdk.runtime.config;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.Logger;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Reads the runtime's host-side configuration from {@code FLEZHA_*}
 * environment variables. Transport configuration (socket paths, QUIC
 * address, certificates) is the poll core's — the core reads the same
 * environment the Rust SDK does; the Java layer only sizes its fetcher set.
 */
public final class FlezhaEnv {

    private static final Logger LOG = Flezha.log();

    public static final String FLEZHA_WORKER_POOL_SIZE = "FLEZHA_WORKER_POOL_SIZE";

    /** Ceiling the starvation supervisor may grow the fetcher set to. */
    public static final String FLEZHA_WORKER_POOL_MAX = "FLEZHA_WORKER_POOL_MAX";

    /**
     * Fetchers never run user code — they poll, decode and hand off to a
     * virtual thread — so a small fixed set saturates the core's handout.
     * How much work is underway is the core's number, not this one.
     */
    public static final int DEFAULT_FETCHER_COUNT = 2;

    /**
     * Default ceiling, as a multiple of the configured count — the same
     * base x 10 the Rust pool's {@code FLEZHA_WORKER_POOL_MAX} defaults to,
     * so the two hosts grow the same distance under the same signal.
     */
    public static final int DEFAULT_FETCHER_MAX_MULTIPLE = 10;

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
        int count =
                parsePositive(FLEZHA_WORKER_POOL_SIZE, lookup.apply(FLEZHA_WORKER_POOL_SIZE), DEFAULT_FETCHER_COUNT);
        int max = parsePositive(
                FLEZHA_WORKER_POOL_MAX, lookup.apply(FLEZHA_WORKER_POOL_MAX), count * DEFAULT_FETCHER_MAX_MULTIPLE);
        // A ceiling below the configured count would make the host start
        // fewer fetchers than the operator asked for; the count wins.
        return new ResolvedConfig(count, Math.max(count, max));
    }

    /**
     * The one count rule every Flezha runtime shares: unset, blank, zero,
     * negative or unparsable text all read as {@code fallback}, so a knob
     * nobody meant to set never shrinks a pool to nothing.
     */
    public static int parsePositive(String name, String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            return parsed <= 0 ? fallback : parsed;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid {}={}; falling back to {}", name, raw, fallback);
            return fallback;
        }
    }

    public record ResolvedConfig(int fetcherCount, int fetcherMax) {}
}
