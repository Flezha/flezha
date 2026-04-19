package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * Input delivered to a processor after the input transformer has run. Maps
 * to the Rust SDK's {@code ProcessorInput} enum and
 * {@code docs/sdk_protocol_spec.md} §8.3.
 *
 * <p>The variant is chosen by the {@code input_type} metadata on the
 * primary-stream frame: {@code "1"} → {@link Binary}, {@code "2"} →
 * {@link Text}, absent → {@link Binary}.
 */
public sealed interface ProcessorInput permits ProcessorInput.Binary, ProcessorInput.Text {

    record Binary(@Nonnull byte[] data) implements ProcessorInput {
        public Binary {
            if (data == null) {
                throw new IllegalArgumentException("Binary data must not be null");
            }
        }
    }

    record Text(@Nonnull String value) implements ProcessorInput {
        public Text {
            if (value == null) {
                throw new IllegalArgumentException("Text value must not be null");
            }
        }
    }
}
