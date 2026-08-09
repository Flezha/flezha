package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/** A host-side failure crossing the activation boundary. */
public record WorkError(@Nonnull String message, boolean nonRetryable) {}
