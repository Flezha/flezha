package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.runtime.stream.StreamRegistry;
import java.util.List;
import java.util.Map;

public final class ConnectSocketHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        int count = request.getConnectSocket().getSocketsCount();
        long nativeHandle = ctx.nativeHandle();
        StreamRegistry streams = ctx.streamRegistry();

        for (int i = 0; i < count; i++) {
            long primary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_PRIMARY, i);
            long secondary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_SECONDARY, i);
            streams.register(i, primary, secondary);
            ctx.primaryReaders().put(primary, new PrimaryStreamReader(ctx, primary));
        }

        // Install the per-Entity flusher's send sink over a secondary stream.
        // Resolves the live stream on each send so a reconnect's fresh stream is
        // used without re-registering entities; the sink replays any frames
        // buffered while it was absent.
        EntityStateSink.installGlobalSink((payloadId, frame) -> {
            long secondary = streams.anySecondary();
            if (secondary == 0L) {
                return false;
            }
            byte[] envelope = FrameEncoder.build(payloadId, frame, Map.of());
            return NativeTransport.streamWrite(nativeHandle, secondary, envelope) == 0;
        });
        return new byte[0];
    }
}
