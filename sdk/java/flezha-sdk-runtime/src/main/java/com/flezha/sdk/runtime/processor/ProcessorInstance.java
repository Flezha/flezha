package com.flezha.sdk.runtime.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.InputTransformer;
import com.flezha.sdk.OutputTransformer;
import com.flezha.sdk.ProcessorException;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorInput;
import com.flezha.sdk.ProcessorResult;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;
import javax.annotation.Nonnull;

/**
 * Live instance of a processor, owning its state + handler + settings. Thread
 * safety: a single mutex guards {@link #run(ProcessorInput)}; the runtime's
 * worker-pool runner further enforces non-overlapping invocations.
 */
public final class ProcessorInstance<State, Input, Output> {

    private final String processorId;
    private final String factoryId;
    private final ProcessorSettings<State, Input, Output> settings;
    private final ProcessorHandler<State, Input, Output> handler;
    private State state;

    private ProcessorInstance(
            String processorId,
            String factoryId,
            ProcessorSettings<State, Input, Output> settings,
            ProcessorHandler<State, Input, Output> handler,
            State state) {
        this.processorId = processorId;
        this.factoryId = factoryId;
        this.settings = settings;
        this.handler = handler;
        this.state = state;
    }

    @SuppressWarnings("unchecked")
    public static ProcessorInstance<?, ?, ?> create(@Nonnull String factoryId, @Nonnull String processorId) {
        FactoryRegistry.FactoryEntry<?, ?, ?> raw = FactoryRegistry.get(factoryId);
        if (raw == null) {
            throw new IllegalArgumentException("No factory registered for id: " + factoryId);
        }
        @SuppressWarnings("rawtypes")
        FactoryRegistry.FactoryEntry entry = raw;
        Object initialState = cloneInitialState(entry.initialState(), entry);
        ProcessorHandler<?, ?, ?> handler = (ProcessorHandler<?, ?, ?>) entry.handlerSupplier().get();
        return new ProcessorInstance(
                processorId,
                factoryId,
                entry.settings(),
                handler,
                initialState);
    }

    private static Object cloneInitialState(Object initialState, FactoryRegistry.FactoryEntry<?, ?, ?> entry) {
        TypeReference<?> typeRef = entry.settings().stateType();
        if (typeRef == null) {
            // No Jackson-based clone available; share the initial state object. Processors
            // that mutate state MUST provide a TypeReference in ProcessorSettings.
            return initialState;
        }
        ObjectMapper tmp = new ObjectMapper();
        try {
            byte[] bytes = tmp.writeValueAsBytes(initialState);
            return tmp.readValue(bytes, typeRef);
        } catch (Exception e) {
            return initialState;
        }
    }

    @Nonnull
    public String processorId() {
        return processorId;
    }

    @Nonnull
    public String factoryId() {
        return factoryId;
    }

    @Nonnull
    public ProcessorRunner runner() {
        return settings.runner();
    }

    @SuppressWarnings("unchecked")
    public synchronized ProcessorResult run(@Nonnull ProcessorInput rawInput) throws ProcessorException {
        InputTransformer<Input> it = (InputTransformer<Input>) settings.inputTransformer();
        Input input = it.transform(rawInput);
        Output output = ((ProcessorHandler<State, Input, Output>) handler).process(input, state);
        OutputTransformer<State, Output> ot = (OutputTransformer<State, Output>) settings.outputTransformer();
        return ot.transform(output, state);
    }

    public synchronized void postCreate() {
        @SuppressWarnings("unchecked")
        ProcessorHandler<State, Input, Output> casted = (ProcessorHandler<State, Input, Output>) handler;
        casted.postCreate(state);
    }

    @SuppressWarnings("unchecked")
    public synchronized byte[] handleMessage(@Nonnull byte[] payload) throws Exception {
        ProcessorHandler<State, Input, Output> casted =
                (ProcessorHandler<State, Input, Output>) handler;
        return casted.handleMessage(payload, state);
    }

    public synchronized byte[] serializeState(@Nonnull ObjectMapper mapper) throws Exception {
        return mapper.writeValueAsBytes(state);
    }

    @SuppressWarnings("unchecked")
    public synchronized void deserializeState(@Nonnull byte[] bytes, @Nonnull ObjectMapper mapper) throws Exception {
        TypeReference<?> typeRef = settings.stateType();
        if (typeRef == null) {
            throw new IllegalStateException(
                    "Cannot deserialize state for factory '" + factoryId
                            + "' without a TypeReference<State> on ProcessorSettings");
        }
        state = (State) mapper.readValue(bytes, typeRef);
    }

    synchronized State stateForTest() {
        return state;
    }
}
