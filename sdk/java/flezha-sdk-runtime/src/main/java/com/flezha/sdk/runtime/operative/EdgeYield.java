package com.flezha.sdk.runtime.operative;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * What one consumer edge's transform yields for one input value: nothing (a
 * {@code filter} rejection), the common single reshaped value, or a fan-out
 * ({@code flatMap}). Only the fan-out allocates.
 *
 * <p>Mirror of the Rust SDK's {@code channel::EdgeYield}.
 */
public sealed interface EdgeYield {

    /** The values to deliver, in order — empty for a rejected value. */
    @Nonnull
    List<Object> values();

    /** Nothing survives; a {@code filter} rejected the value. */
    EdgeYield DROP = new Drop();

    @Nonnull
    static EdgeYield one(@Nonnull Object value) {
        return new One(value);
    }

    @Nonnull
    static EdgeYield many(@Nonnull List<Object> values) {
        return new Many(values);
    }

    record Drop() implements EdgeYield {
        @Override
        @Nonnull
        public List<Object> values() {
            return List.of();
        }
    }

    record One(@Nonnull Object value) implements EdgeYield {
        @Override
        @Nonnull
        public List<Object> values() {
            return List.of(value);
        }
    }

    record Many(@Nonnull List<Object> values) implements EdgeYield {}
}
