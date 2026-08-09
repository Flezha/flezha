package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/**
 * Per-item verdict inside an {@link ItemEffect}. Variant order is the
 * postcard enum discriminant.
 */
public sealed interface Verdict {

    /** Discriminant 0. */
    record Ok() implements Verdict {}

    /** Discriminant 1 — the item failed and may be re-attempted. */
    record RetryableError(@Nonnull String message) implements Verdict {}

    /** Discriminant 2 — the item failed and must not be re-attempted. */
    record NonRetryableError(@Nonnull String message) implements Verdict {}
}
