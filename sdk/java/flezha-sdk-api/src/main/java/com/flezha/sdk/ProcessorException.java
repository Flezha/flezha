package com.flezha.sdk;

/**
 * Recoverable error surfaced by a processor's {@code process(...)} call.
 * The SDK logs the message and skips output for the input, but keeps the
 * compute engine running and continues consuming subsequent inputs.
 *
 * <p>See {@code docs/sdk_protocol_spec.md} §8.
 */
public class ProcessorException extends Exception {
    private static final long serialVersionUID = 1L;

    public ProcessorException(String message) {
        super(message);
    }

    public ProcessorException(String message, Throwable cause) {
        super(message, cause);
    }
}
