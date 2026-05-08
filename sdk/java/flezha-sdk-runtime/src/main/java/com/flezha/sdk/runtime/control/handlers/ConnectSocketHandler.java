package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.runtime.stream.StreamRegistry;
import java.util.List;

public final class ConnectSocketHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        int count = request.getConnectSocket().getSocketsCount();
        long nativeHandle = ctx.nativeHandle();
        StreamRegistry streams = ctx.streamRegistry();

        for (int i = 0; i < count; i++) {
            long primary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_PRIMARY, i);
            long secondary = NativeTransport.openStream(nativeHandle, NativeTransport.STREAM_TYPE_SECONDARY, i);
            streams.register(i, primary, secondary);
            ctx.primaryReaders().put(primary, new PrimaryStreamReader(ctx, primary));
        }
        return new byte[0];
    }
}
