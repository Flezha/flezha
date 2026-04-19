package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * A processor handler — the user's business logic. Mirrors the Rust SDK's
 * {@code ProcessorHandler<State, I, O>} trait.
 *
 * <p>See {@code docs/sdk_protocol_spec.md} §8 for the protocol-level
 * semantics that drive {@code process(...)} and {@code postCreate(...)}.
 *
 * @param <State>  handler-owned state, serialized by Jackson for migration.
 * @param <Input>  post-transform input type (e.g., {@code String}, {@code byte[]}, domain DTO).
 * @param <Output> pre-transform output type.
 */
public interface ProcessorHandler<State, Input, Output> {

    /**
     * Process a single input against the processor's state.
     *
     * @throws ProcessorException for recoverable errors. The SDK logs the
     *   exception and keeps the compute engine running.
     */
    @Nonnull
    Output process(@Nonnull Input input, @Nonnull State state) throws ProcessorException;

    /**
     * Optional hook invoked once after the processor is created (either via
     * {@code AddProcessor} or {@code MigrationDeserialization}) and before
     * the processor receives any input.
     */
    default void postCreate(@Nonnull State state) {
    }
}
