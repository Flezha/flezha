package com.flezha.sdk.runtime.relay.builder;

import com.flezha.sdk.relay.RelayConfigurationException;
import javax.annotation.Nonnull;

/**
 * Resolves a Shard's per-Partition mailbox capacity per the Rust SDK's
 * three-tier precedence rule:
 *
 * <ol>
 *   <li>Explicit Shard override ({@code mailboxCapacityOverridden() == true}).</li>
 *   <li>{@code FLEZHA_MAILBOX_CAPACITY} env var (parsed as a positive int).</li>
 *   <li>Built-in default ({@link #DEFAULT_MAILBOX_CAPACITY}).</li>
 * </ol>
 *
 * <p>Out-of-range values fail registration with
 * {@link RelayConfigurationException#mailboxCapacityZero(String)} or
 * {@link RelayConfigurationException#mailboxCapacityTooLarge(String, long)}.
 * Unparseable env values fall through to the default — executors tolerate
 * typos in env values during development. Split into a pure-function core
 * so unit tests don't have to mutate the process-global env var.
 */
public final class MailboxCapacityResolver {

    public static final int DEFAULT_MAILBOX_CAPACITY = 1024;
    public static final int MAX_MAILBOX_CAPACITY = 1_048_576;
    public static final String ENV_MAILBOX_CAPACITY = "FLEZHA_MAILBOX_CAPACITY";

    private MailboxCapacityResolver() {}

    /** Resolve using the live process environment for the env-var tier. */
    public static int resolve(@Nonnull String shardName, int declared, boolean overridden) {
        return resolveInner(shardName, declared, overridden, System.getenv(ENV_MAILBOX_CAPACITY));
    }

    /**
     * Pure-function core. {@code envValue} may be {@code null} (unset) or
     * a string that fails to parse as a positive {@code int}, in which case
     * the default is used.
     *
     * @throws RelayConfigurationException if the resolved value is out of
     *     range {@code [1, 1_048_576]}.
     */
    static int resolveInner(@Nonnull String shardName, int declared, boolean overridden, String envValue) {
        int resolved;
        if (overridden) {
            resolved = declared;
        } else {
            Integer parsed = parsePositive(envValue);
            resolved = parsed != null ? parsed : DEFAULT_MAILBOX_CAPACITY;
        }
        if (resolved <= 0) {
            throw RelayConfigurationException.mailboxCapacityZero(shardName);
        }
        if (resolved > MAX_MAILBOX_CAPACITY) {
            throw RelayConfigurationException.mailboxCapacityTooLarge(shardName, resolved);
        }
        return resolved;
    }

    private static Integer parsePositive(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            int v = Integer.parseInt(raw.trim());
            return v > 0 ? v : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
