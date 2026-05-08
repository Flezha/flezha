package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the control stream handle and sends the handshake init JSON on
 * connect. Incoming bytes are decoded by the shared {@link com.flezha.sdk.runtime.framing.FrameParser}
 * driven from the JNI {@link com.flezha.sdk.native_.NativeEventDispatcher},
 * and frames with payload id {@code control_command} are handed to
 * {@link CommandRouter}.
 */
public final class ControlChannel {

    private static final Logger LOG = LoggerFactory.getLogger(ControlChannel.class);

    public static final String HANDSHAKE_PAYLOAD_ID = "connect_control_channel";

    private final ExecutorContext ctx;
    private final long controlStreamId;

    public ControlChannel(@Nonnull ExecutorContext ctx, long controlStreamId) {
        this.ctx = ctx;
        this.controlStreamId = controlStreamId;
    }

    public long controlStreamId() {
        return controlStreamId;
    }

    public void sendHandshake(@Nonnull String namespace) throws Exception {
        HandshakeMessage msg = HandshakeMessage.from(namespace);
        String json = ctx.objectMapper().writeValueAsString(msg);
        byte[] frame = FrameEncoder.build(
                HANDSHAKE_PAYLOAD_ID,
                json.getBytes(StandardCharsets.UTF_8),
                Map.of());
        int status = NativeTransport.streamWrite(ctx.nativeHandle(), controlStreamId, frame);
        if (status != 0) {
            throw new IllegalStateException("Failed to send handshake (status=" + status + ")");
        }
        LOG.debug("Handshake sent for namespace={} (control stream {})", namespace, controlStreamId);
    }
}
