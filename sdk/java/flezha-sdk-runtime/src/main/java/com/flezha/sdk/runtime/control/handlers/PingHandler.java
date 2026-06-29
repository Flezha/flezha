package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.Ping;
import com.flezha.sdk.proto.PingResponse;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import java.util.List;

/**
 * Handler for the Controller-issued {@link Ping} command (see {@code
 * executor-controller-connection-healing} capability). The Controller issues
 * a ping immediately before {@code AddOperative} during the Overwatch's cold-
 * restart startup sequence; a successful round-trip proves the control
 * channel is bound and the SDK's reply path is intact, so {@code AddOperative}
 * is safe to issue. The handler echoes the request {@code nonce} so the
 * caller can correlate replies if multiple pings are in flight.
 */
public final class PingHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        Ping ping = request.getPing();
        PingResponse response =
                PingResponse.newBuilder().setNonce(ping.getNonce()).build();
        return response.toByteArray();
    }
}
