package com.flezha.sdk.runtime.core;

import javax.annotation.Nonnull;

/**
 * Mirror of Rust's {@code Result<PrepareAnswer, WorkError>} — postcard
 * encodes it as an enum: discriminant 0 + answer, or 1 + error.
 */
public sealed interface PrepareResult {

    record Ok(@Nonnull PrepareAnswer answer) implements PrepareResult {}

    record Err(@Nonnull WorkError error) implements PrepareResult {}
}
