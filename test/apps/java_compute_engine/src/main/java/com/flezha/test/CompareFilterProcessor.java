package com.flezha.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flezha.sdk.InputTransformers;
import com.flezha.sdk.OutputTransformers;
import com.flezha.sdk.ProcessorException;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;

public final class CompareFilterProcessor
        implements ProcessorHandler<CompareFilterProcessor.State, String, String> {

    @Override
    public String process(String input, State state) throws ProcessorException {
        if (state.comparable().equals(input)) {
            return input;
        }
        return "Not matched";
    }

    public static ProcessorSettings<State, String, String> settings() {
        return ProcessorSettings.<State, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.pipeToAgentText())
                .runner(ProcessorRunner.THREAD_POOL)
                .stateType(new TypeReference<State>() {})
                .build();
    }

    public record State(String comparable, boolean inverted) {
        public State() {
            this("default", false);
        }
    }
}
