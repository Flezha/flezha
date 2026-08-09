package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/** The host's answer to an {@link Activation}, addressed by token. */
public record ActivationResult(long token, @Nonnull ActivationOutcome outcome) {}
