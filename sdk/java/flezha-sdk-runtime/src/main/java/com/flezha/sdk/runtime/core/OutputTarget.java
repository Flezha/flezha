package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/**
 * Where an {@link Output} goes. Variant order is the postcard enum
 * discriminant.
 */
public sealed interface OutputTarget {

    /** Discriminant 0 — a consumer edge: the value goes to {@code consumer}'s Entity mailbox. */
    record Edge(int channel, int consumer) implements OutputTarget {}

    /** Discriminant 1 — a plugin-bound channel: the value ships on the wire under {@code channel}. */
    record Plugin(@Nonnull String channel) implements OutputTarget {}
}
