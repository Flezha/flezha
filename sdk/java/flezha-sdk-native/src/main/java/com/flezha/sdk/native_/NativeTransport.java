package com.flezha.sdk.native_;

import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single Java-facing surface for the {@code flz_transport} shared library.
 *
 * <p>All memory-ownership rules are documented per-method: every
 * {@code byte[]} handed to Java is a fresh JVM allocation; every
 * {@code byte[]} accepted from Java is copied before the method returns.
 *
 * <p>The class extracts and loads {@code libflz_jni} exactly once per
 * JVM via {@link NativeLibraryLoader} in its static initializer. Accessing
 * any static member triggers the load.
 *
 * <p>Native method names are the <em>short</em> JNI form
 * ({@code Java_com_flezha_sdk_native_1_NativeTransport_<method>}), so the
 * Rust exports in {@code flz_jni/src/} match 1:1.
 */
public final class NativeTransport {

    private static final Logger LOG = LoggerFactory.getLogger(NativeTransport.class);

    static {
        NativeLibraryLoader.loadNativeLibrary();
        verifyVersionCompatibility();
    }

    private NativeTransport() {}

    // ------------------------------------------------------------------
    // Public convenience wrappers
    // ------------------------------------------------------------------

    /**
     * Create a new native transport context from a {@link FlezhaConfig}.
     * Delegates to the raw {@link #contextNew(String, String, String, String)}.
     */
    public static long contextNew(@Nonnull FlezhaConfig config) {
        return contextNew(
                config.transport().nativeName(), config.quicAddress(), config.caCertPath(), config.socketFilesPath());
    }

    /**
     * Install a default log callback that forwards native log events to
     * SLF4J under the {@code com.flezha.sdk.native.transport} logger.
     */
    public static void installDefaultLogCallback(long handle) {
        Logger nativeLog = LoggerFactory.getLogger("com.flezha.sdk.native.transport");
        setLogCallback(handle, (level, message) -> {
            switch (level) {
                case FlezhaLogCallback.LEVEL_ERROR -> nativeLog.error(message);
                case FlezhaLogCallback.LEVEL_WARN -> nativeLog.warn(message);
                case FlezhaLogCallback.LEVEL_INFO -> nativeLog.info(message);
                case FlezhaLogCallback.LEVEL_DEBUG -> nativeLog.debug(message);
                default -> nativeLog.trace(message);
            }
        });
    }

    private static void verifyVersionCompatibility() {
        String nativeVersion;
        try {
            nativeVersion = version();
        } catch (Throwable t) {
            LOG.warn("Unable to query native flz_transport version: {}", t.toString());
            return;
        }
        if (!VersionInfo.VERSION.equals(nativeVersion)) {
            LOG.warn(
                    "Flezha SDK version mismatch: Java SDK is {} but bundled flz_transport is {}",
                    VersionInfo.VERSION,
                    nativeVersion);
        }
    }

    // ------------------------------------------------------------------
    // Native methods (each maps to a Java_..._NativeTransport_<name> export
    // in flz_jni).
    // ------------------------------------------------------------------

    /**
     * Create a new native transport context.
     *
     * @param transport        {@code "uds"} or {@code "quic"}.
     * @param quicAddress      QUIC server address (e.g., {@code 127.0.0.1:4433}).
     * @param caCertPath       Path to the CA certificate PEM file.
     * @param socketBasePath   UDS socket base directory.
     * @return opaque context handle (non-zero on success).
     */
    public static native long contextNew(
            String transport, String quicAddress, String caCertPath, String socketBasePath);

    /** Destroys a context. Double-destroy returns {@link FlezhaStatus#INVALID_STATE} without crashing. */
    public static native int contextDestroy(long handle);

    /** Returns the {@code flezha_version()} string baked into the loaded shared library. */
    @Nonnull
    public static native String version();

    /** Register a {@link NativeEventDispatcher}. Latest registration wins. */
    public static native void setDispatcher(long handle, NativeEventDispatcher dispatcher);

    /** Register a log callback. See {@link #installDefaultLogCallback(long)} for SLF4J routing. */
    public static native void setLogCallback(long handle, FlezhaLogCallback callback);

    /** Update the native-side compute engine id used to build UDS stream socket paths. */
    public static native void setRunnerId(long handle, String id);

    /** Open the control channel. Returns a stream id. */
    public static native long openControlStream(long handle);

    /**
     * Open a data stream. {@code streamType} is {@code 0x01} for primary
     * (read-side, controller → SDK) and {@code 0x02} for secondary
     * (write-side, SDK → controller). {@code index} is the socket slot index.
     */
    public static native long openStream(long handle, int streamType, int index);

    /** Write bytes to a stream. Returns 0 (OK) or a non-zero {@link FlezhaStatus} code. */
    public static native int streamWrite(long handle, long streamId, byte[] data);

    /** Close a stream by id. Idempotent. */
    public static native void streamClose(long handle, long streamId);

    /**
     * Build a protocol frame via the shared library. Returns a fresh
     * {@code byte[]} copy of the native buffer; the native buffer is
     * freed before this method returns.
     */
    @Nonnull
    public static native byte[] buildFrame(String payloadId, byte[] message, String metadataJson);

    /** Stream type constant for the primary stream (controller → SDK). */
    public static final int STREAM_TYPE_PRIMARY = 0x01;
    /** Stream type constant for the secondary stream (SDK → controller). */
    public static final int STREAM_TYPE_SECONDARY = 0x02;

    /**
     * Send a per-Entity state-change frame on the controller's control channel.
     * The {@code frame} bytes are the umbrella-spec wire format (instance id,
     * procedure name, entity key, seq, header, change bytes, crc32) already
     * built by the SDK's {@code EntityFlusher}. Returns 0 on OK, non-zero
     * {@link FlezhaStatus} otherwise.
     *
     * <p>Wraps the existing generic {@link #streamWrite(long, long, byte[])}
     * with the {@code entity_state_change} payload-id constant from
     * {@code operative-state-persistence}'s wire spec.
     */
    public static int sendEntityStateChange(long handle, long streamId, @Nonnull byte[] frame) {
        return streamWrite(handle, streamId, frame);
    }
}
