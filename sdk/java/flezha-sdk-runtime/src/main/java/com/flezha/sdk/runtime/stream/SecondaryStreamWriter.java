package com.flezha.sdk.runtime.stream;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes writes onto a secondary stream. The JNI layer already enforces
 * a per-stream write lock (see {@code flz_jni/src/context.rs}); this class
 * adds Java-side write batching + logging on top. Writes are blocking.
 */
public final class SecondaryStreamWriter {

    private static final Logger LOG = LoggerFactory.getLogger(SecondaryStreamWriter.class);

    private final long nativeHandle;
    private final long streamId;

    public SecondaryStreamWriter(long nativeHandle, long streamId) {
        this.nativeHandle = nativeHandle;
        this.streamId = streamId;
    }

    public long streamId() {
        return streamId;
    }

    public void writeFrame(
            @Nonnull String payloadId,
            @Nonnull byte[] message,
            @Nonnull Map<String, String> metadata) {
        byte[] frame = FrameEncoder.build(payloadId, message, metadata);
        int status = NativeTransport.streamWrite(nativeHandle, streamId, frame);
        if (status != 0) {
            LOG.warn("secondary stream {} write failed (status={})", streamId, status);
        }
    }
}
