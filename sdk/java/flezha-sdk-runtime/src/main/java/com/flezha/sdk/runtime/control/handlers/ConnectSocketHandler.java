package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.stream.StreamRegistry;
import java.util.List;

public final class ConnectSocketHandler implements CommandHandler {

    @Override
    public byte[] handle(ComputeEngineContext ctx, CommandRequest request, List<String> errors) {
        int count = request.getConnectSocket().getSocketsCount();
        long nativeHandle = ctx.nativeHandle();
        StreamRegistry streams = ctx.streamRegistry();

        for (int i = 0; i < count; i++) {
            long primary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_PRIMARY, i);
            long secondary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_SECONDARY, i);
            streams.register(i, primary, secondary);
        }
        return new byte[0];
    }
}
