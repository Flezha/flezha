package com.flezha.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flezha.sdk.InputTransformers;
import com.flezha.sdk.OutputTransformers;
import com.flezha.sdk.ProcessorException;
import com.flezha.sdk.ProcessorHandler;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.ProcessorSettings;

/**
 * Test factory that overrides {@code handleMessage} to echo the payload
 * verbatim, used by the {@code processor-message-handlers} integration
 * matrix on the Java side.
 */
public final class EchoMessageProcessor
        implements ProcessorHandler<EchoMessageProcessor.State, String, String> {

    @Override
    public String process(String input, State state) throws ProcessorException {
        return input;
    }

    @Override
    public byte[] handleMessage(byte[] payload, State state) {
        return payload;
    }

    public static ProcessorSettings<State, String, String> settings() {
        return ProcessorSettings.<State, String, String>builder()
                .inputTransformer(InputTransformers.text())
                .outputTransformer(OutputTransformers.pipeToAgentText())
                .runner(ProcessorRunner.THREAD_POOL)
                .stateType(new TypeReference<State>() {})
                .build();
    }

    public static final class State {
    }
}
