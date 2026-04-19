package com.flezha.sdk;

import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

/**
 * Built-in {@link InputTransformer} implementations matching the Rust
 * SDK's default adapters.
 */
public final class InputTransformers {
    private InputTransformers() {
    }

    /**
     * Coerce any {@link ProcessorInput} into a UTF-8 {@link String}. Binary
     * inputs are decoded as UTF-8 (invalid bytes are replaced).
     */
    @Nonnull
    public static InputTransformer<String> text() {
        return input -> switch (input) {
            case ProcessorInput.Text t -> t.value();
            case ProcessorInput.Binary b -> new String(b.data(), StandardCharsets.UTF_8);
        };
    }

    /**
     * Coerce any {@link ProcessorInput} into a {@code byte[]}. Text inputs
     * are re-encoded as UTF-8.
     */
    @Nonnull
    public static InputTransformer<byte[]> binary() {
        return input -> switch (input) {
            case ProcessorInput.Binary b -> b.data();
            case ProcessorInput.Text t -> t.value().getBytes(StandardCharsets.UTF_8);
        };
    }

    /**
     * Returns the input unchanged — useful for handlers that want to
     * dispatch on the variant themselves.
     */
    @Nonnull
    public static InputTransformer<ProcessorInput> passthrough() {
        return input -> input;
    }
}
