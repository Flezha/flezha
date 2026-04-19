package com.flezha.sdk;

import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Result produced by an output transformer, routed by the SDK's data plane.
 * Matches the Rust SDK's {@code ProcessorResult} enum and
 * {@code docs/sdk_protocol_spec.md} §8.3.
 *
 * <p>Variants:
 * <ul>
 *   <li>{@link None} — drop the result. Nothing is written to the secondary stream.</li>
 *   <li>{@link Return} — a plain value. The SDK does not pipe it anywhere; useful
 *       for user-driven orchestration.</li>
 *   <li>{@link PipeToAgent} — write zero or more outputs (with metadata) to the
 *       secondary stream, keyed by the source processor's id.</li>
 *   <li>{@link PipeToProcessor} — route to another processor either locally
 *       (direct invocation) or via the agent if the target is not local.</li>
 * </ul>
 */
public sealed interface ProcessorResult
        permits ProcessorResult.None,
                ProcessorResult.Return,
                ProcessorResult.PipeToAgent,
                ProcessorResult.PipeToProcessor {

    record None() implements ProcessorResult {}

    record Return(Object value) implements ProcessorResult {}

    record PipeToAgent(@Nonnull List<OutputWithMetadata> outputs) implements ProcessorResult {
        public PipeToAgent {
            if (outputs == null) {
                throw new IllegalArgumentException("outputs must not be null");
            }
            outputs = List.copyOf(outputs);
        }
    }

    record PipeToProcessor(@Nonnull List<Route> routes) implements ProcessorResult {
        public PipeToProcessor {
            if (routes == null) {
                throw new IllegalArgumentException("routes must not be null");
            }
            routes = List.copyOf(routes);
        }
    }

    record OutputWithMetadata(
            @Nonnull ProcessorOutput output,
            @Nonnull Map<String, String> metadata) {
        public OutputWithMetadata {
            if (output == null) {
                throw new IllegalArgumentException("output must not be null");
            }
            metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }
    }

    record Route(
            @Nonnull String processorId,
            @Nonnull ProcessorOutput output,
            @Nonnull Map<String, String> metadata) {
        public Route {
            if (processorId == null || processorId.isEmpty()) {
                throw new IllegalArgumentException("processorId must not be null or empty");
            }
            if (output == null) {
                throw new IllegalArgumentException("output must not be null");
            }
            metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }
    }
}
