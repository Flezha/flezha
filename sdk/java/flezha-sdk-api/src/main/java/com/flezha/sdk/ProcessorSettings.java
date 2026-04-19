package com.flezha.sdk;

import com.fasterxml.jackson.core.type.TypeReference;
import javax.annotation.Nonnull;

/**
 * Per-factory configuration: input/output transformers, runner strategy,
 * and a Jackson {@link TypeReference} describing the handler's state type
 * (needed for generic-safe JSON de/serialization across
 * {@code MigrationSerialization} / {@code MigrationDeserialization}).
 *
 * <p>Build via {@link #builder()}.
 */
public final class ProcessorSettings<State, Input, Output> {

    private final InputTransformer<Input> inputTransformer;
    private final OutputTransformer<State, Output> outputTransformer;
    private final ProcessorRunner runner;
    private final TypeReference<State> stateType;

    private ProcessorSettings(Builder<State, Input, Output> b) {
        this.inputTransformer = b.inputTransformer;
        this.outputTransformer = b.outputTransformer;
        this.runner = b.runner;
        this.stateType = b.stateType;
    }

    @Nonnull
    public InputTransformer<Input> inputTransformer() {
        return inputTransformer;
    }

    @Nonnull
    public OutputTransformer<State, Output> outputTransformer() {
        return outputTransformer;
    }

    @Nonnull
    public ProcessorRunner runner() {
        return runner;
    }

    /** May be null — only required for state migration. */
    public TypeReference<State> stateType() {
        return stateType;
    }

    public static <State, Input, Output> Builder<State, Input, Output> builder() {
        return new Builder<>();
    }

    public static final class Builder<State, Input, Output> {
        private InputTransformer<Input> inputTransformer;
        private OutputTransformer<State, Output> outputTransformer;
        private ProcessorRunner runner = ProcessorRunner.THREAD_POOL;
        private TypeReference<State> stateType;

        public Builder<State, Input, Output> inputTransformer(@Nonnull InputTransformer<Input> t) {
            this.inputTransformer = t;
            return this;
        }

        public Builder<State, Input, Output> outputTransformer(@Nonnull OutputTransformer<State, Output> t) {
            this.outputTransformer = t;
            return this;
        }

        public Builder<State, Input, Output> runner(@Nonnull ProcessorRunner r) {
            this.runner = r;
            return this;
        }

        public Builder<State, Input, Output> stateType(@Nonnull TypeReference<State> stateType) {
            this.stateType = stateType;
            return this;
        }

        public ProcessorSettings<State, Input, Output> build() {
            if (inputTransformer == null) {
                throw new IllegalStateException("inputTransformer is required");
            }
            if (outputTransformer == null) {
                throw new IllegalStateException("outputTransformer is required");
            }
            return new ProcessorSettings<>(this);
        }
    }
}
