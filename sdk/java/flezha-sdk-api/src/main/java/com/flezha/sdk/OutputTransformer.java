package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * Adapter from a user-chosen Java output type to a protocol-level
 * {@link ProcessorResult}. Typically one of the factory-level
 * {@link OutputTransformers output transformer helpers}, but user code may
 * provide its own (e.g., to attach custom metadata on every output).
 */
@FunctionalInterface
public interface OutputTransformer<State, Output> {
    @Nonnull
    ProcessorResult transform(@Nonnull Output output, @Nonnull State state);
}
