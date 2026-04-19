package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * Adapter from the SDK's {@link ProcessorInput} to a user-chosen Java type.
 * Pair with the factory-level
 * {@link InputTransformers input transformer helpers}.
 */
@FunctionalInterface
public interface InputTransformer<Input> {
    @Nonnull
    Input transform(@Nonnull ProcessorInput input);
}
