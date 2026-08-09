package com.flezha.sdk.runtime.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * One value produced by a {@link WorkItem}, already transformed and keyed for
 * its destination (producer-side evaluation — the core routes on
 * {@code (edge, key)} and never evaluates a host closure). {@code eventTime}
 * is the consumer's extracted event time for an edge value into an
 * event-time-clocked consumer.
 */
public record Output(
        @Nonnull OutputTarget target,
        @Nonnull Optional<String> key,
        @Nonnull OptionalLong eventTime,
        @Nonnull List<Map.Entry<String, String>> metadata,
        @Nonnull byte[] payload) {}
