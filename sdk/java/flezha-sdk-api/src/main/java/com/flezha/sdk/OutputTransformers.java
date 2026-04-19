package com.flezha.sdk;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Built-in {@link OutputTransformer} implementations matching the Rust
 * SDK's default adapters. Each helper is a pure function: it captures no
 * per-invocation state.
 */
public final class OutputTransformers {
    private OutputTransformers() {
    }

    /**
     * Wrap a {@code String} output into a single-entry
     * {@link ProcessorResult.PipeToAgent} with the string UTF-8-encoded as
     * the binary payload and empty metadata.
     */
    @Nonnull
    public static <State> OutputTransformer<State, String> pipeToAgentText() {
        return (output, state) -> new ProcessorResult.PipeToAgent(
                List.of(new ProcessorResult.OutputWithMetadata(
                        new ProcessorOutput.Binary(output.getBytes(StandardCharsets.UTF_8)),
                        Map.of())));
    }

    /**
     * Wrap a {@code byte[]} output into a single-entry
     * {@link ProcessorResult.PipeToAgent} with empty metadata.
     */
    @Nonnull
    public static <State> OutputTransformer<State, byte[]> pipeToAgentBinary() {
        return (output, state) -> new ProcessorResult.PipeToAgent(
                List.of(new ProcessorResult.OutputWithMetadata(
                        new ProcessorOutput.Binary(output),
                        Map.of())));
    }

    /** Wrap a {@code String} output into a {@link ProcessorResult.Return}. */
    @Nonnull
    public static <State> OutputTransformer<State, String> returnText() {
        return (output, state) -> new ProcessorResult.Return(output);
    }

    /** Wrap a {@code byte[]} output into a {@link ProcessorResult.Return}. */
    @Nonnull
    public static <State> OutputTransformer<State, byte[]> returnBinary() {
        return (output, state) -> new ProcessorResult.Return(output);
    }

    /** Drop the output; emits {@link ProcessorResult.None}. */
    @Nonnull
    public static <State, Output> OutputTransformer<State, Output> none() {
        return (output, state) -> new ProcessorResult.None();
    }
}
