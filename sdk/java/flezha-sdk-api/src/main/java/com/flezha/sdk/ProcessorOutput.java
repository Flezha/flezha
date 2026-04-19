package com.flezha.sdk;

import javax.annotation.Nonnull;

/**
 * Output produced by an output transformer, routed through a
 * {@link ProcessorResult}. Mirrors {@link ProcessorInput} but lives in the
 * reverse direction.
 *
 * <p>See {@code docs/sdk_protocol_spec.md} §8.3.
 */
public sealed interface ProcessorOutput permits ProcessorOutput.Binary, ProcessorOutput.Text {

    record Binary(@Nonnull byte[] data) implements ProcessorOutput {
        public Binary {
            if (data == null) {
                throw new IllegalArgumentException("Binary data must not be null");
            }
        }
    }

    record Text(@Nonnull String value) implements ProcessorOutput {
        public Text {
            if (value == null) {
                throw new IllegalArgumentException("Text value must not be null");
            }
        }
    }
}
