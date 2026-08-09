package com.flezha.sdk.runtime.core;

import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * One payload's prepare answers, in {@link ActivationKind.KeyRequest} payload
 * order. {@code keys} carries the routing keys when {@code wantKeys} (one for
 * a dynamic consumer, any number for a static fan-out, empty = every declared
 * Entity); {@code eventTime} the extracted event time when
 * {@code wantEventTime}.
 */
public record PrepareAnswer(@Nonnull List<String> keys, @Nonnull OptionalLong eventTime) {}
