package com.flezha.sdk.native_;

import javax.annotation.Nonnull;

/**
 * Java-side receiver for native transport events. The JNI reader thread for
 * each open stream invokes these methods as bytes arrive or as the stream
 * transitions to a closed / lost state.
 *
 * <p><strong>Threading:</strong> callbacks arrive on native-owned reader
 * threads (one per open read-side stream) that have been temporarily
 * attached to the JVM via {@code AttachCurrentThread}. Implementations MUST
 * be thread-safe and should keep per-callback work short (enqueue to a
 * worker queue rather than processing inline).
 *
 * <p><strong>Buffer ownership:</strong> every {@code byte[]} passed to
 * {@link #onData(long, byte[])} is a freshly-allocated JVM array — the
 * native buffer has already been freed before the callback returns to Rust.
 */
public interface NativeEventDispatcher {
    /**
     * Invoked when bytes arrive on a read-side stream. {@code data} is a
     * fresh JVM-owned {@code byte[]}.
     */
    void onData(long streamId, @Nonnull byte[] data);

    /**
     * Invoked once when a stream is orderly closed by the peer.
     */
    void onStreamClosed(long streamId);

    /**
     * Invoked once when the underlying connection is lost (reset, peer
     * gone, transport error). After this point the stream id is invalid.
     */
    void onConnectionLost(long streamId);
}
