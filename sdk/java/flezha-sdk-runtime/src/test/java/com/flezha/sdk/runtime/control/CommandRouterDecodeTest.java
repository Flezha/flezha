package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.Frame;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Java decode-edge parity (self-healing-control-plane Phase 8.6): a malformed
 * or empty-command control frame must surface a typed error and be dropped, not
 * crash the routing thread. These cases return before any native write, so they
 * run without the JNI library.
 */
class CommandRouterDecodeTest {

    private static CommandRouter newRouter() {
        ExecutorContext ctx = new ExecutorContext("ns", FlezhaEnv.resolve(Map.of()), 0L);
        return new CommandRouter(ctx, 1L);
    }

    @Test
    @DisplayName("undecodable CommandRequest bytes are dropped, not crashed")
    void garbageBytesDoNotCrash() {
        // field 1 as a varint, but reference_id is a length-delimited string →
        // a wire-type mismatch that fails prost/protobuf decode.
        Frame frame = new Frame("control_command", new byte[] {0x08, 0x01, 0x12}, Map.of());
        assertThatCode(() -> newRouter().route(frame)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("a CommandRequest with no command oneof is dropped, not crashed")
    void emptyCommandDoesNotCrash() {
        byte[] noCommand =
                CommandRequest.newBuilder().setReferenceId("x").build().toByteArray();
        Frame frame = new Frame("control_command", noCommand, Map.of());
        assertThatCode(() -> newRouter().route(frame)).doesNotThrowAnyException();
    }
}
