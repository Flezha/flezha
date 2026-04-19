package com.flezha.sdk.runtime.framing;

import javax.annotation.Nonnull;

/**
 * Routes decoded {@link Frame}s by {@code payload_id} to the appropriate
 * downstream handler:
 * <ul>
 *   <li>{@code "control_command"} → control-plane command router
 *       ({@link #onControlCommand(Frame)}).</li>
 *   <li>{@code "connect_control_channel"} → agent handshake response
 *       ({@link #onHandshake(Frame)}). In normal operation this never
 *       arrives on an SDK-side stream, but the parser surfaces it so a
 *       future server mode can plug in.</li>
 *   <li>Anything else → data-plane processor lookup by id
 *       ({@link #onProcessorInput(Frame)}).</li>
 * </ul>
 *
 * <p>Implementations are expected to be thread-safe; the dispatcher is
 * called from the transport reader thread.
 */
public interface FrameDispatcher {

    String CONTROL_COMMAND = "control_command";
    String CONNECT_CONTROL_CHANNEL = "connect_control_channel";

    default void dispatch(@Nonnull Frame frame) {
        switch (frame.payloadId()) {
            case CONTROL_COMMAND -> onControlCommand(frame);
            case CONNECT_CONTROL_CHANNEL -> onHandshake(frame);
            default -> onProcessorInput(frame);
        }
    }

    void onControlCommand(@Nonnull Frame frame);

    void onHandshake(@Nonnull Frame frame);

    void onProcessorInput(@Nonnull Frame frame);
}
