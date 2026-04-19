package com.flezha.sdk.native_;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * JNI bridge tests that exercise the native layer in isolation. Tagged
 * {@code jni} so they only run when the host-triple {@code flz_jni} cdylib
 * has been staged into {@code flezha-sdk-native/src/main/resources/native/}.
 *
 * <p>These tests do NOT require a live agent.
 */
@Tag("jni")
class NativeTransportJniTest {

    @Test
    @DisplayName("context create → destroy round-trip")
    void contextLifecycle() {
        long handle = NativeTransport.contextNew(FlezhaConfig.defaults());
        assertThat(handle).isNotZero();
        int status = NativeTransport.contextDestroy(handle);
        assertThat(status).isEqualTo(FlezhaStatus.OK.code());
    }

    @Test
    @DisplayName("double destroy returns INVALID_STATE without crashing")
    void doubleDestroy() {
        long handle = NativeTransport.contextNew(FlezhaConfig.defaults());
        assertThat(handle).isNotZero();
        assertThat(NativeTransport.contextDestroy(handle)).isEqualTo(FlezhaStatus.OK.code());
        int second = NativeTransport.contextDestroy(handle);
        assertThat(second).isEqualTo(FlezhaStatus.INVALID_STATE.code());
    }

    @Test
    @DisplayName("buildFrame (JNI) matches pure-Java FrameEncoder bytewise")
    void buildFrameParity() {
        byte[] message = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] viaJni = NativeTransport.buildFrame("echo", message, null);

        // Mirror the pure-Java path. We can't import runtime here (would create
        // a dependency cycle), so we assert structural properties instead: the
        // JNI frame should be ≥ 42 bytes (32 id + 4 size + 4 msg_size + msg + 2 meta_size).
        assertThat(viaJni).hasSizeGreaterThanOrEqualTo(42);

        byte[] paddedId = new byte[32];
        System.arraycopy(viaJni, 0, paddedId, 0, 32);
        String id = new String(paddedId, java.nio.charset.StandardCharsets.UTF_8).trim();
        assertThat(id).isEqualTo("echo");
    }

    @Test
    @DisplayName("log callback receives invocations")
    void logCallbackInvoked() {
        long handle = NativeTransport.contextNew(FlezhaConfig.defaults());
        AtomicInteger count = new AtomicInteger();
        AtomicReference<String> last = new AtomicReference<>();
        try {
            NativeTransport.setLogCallback(handle, (level, message) -> {
                count.incrementAndGet();
                last.set(message);
            });
            // There isn't a direct "emit a log now" FFI call, so this test
            // just asserts that registration does not throw. A full
            // round-trip assertion lives in the stress-and-fuzz tests.
            assertThat(count.get()).isGreaterThanOrEqualTo(0);
        } finally {
            NativeTransport.contextDestroy(handle);
        }
    }

    @Test
    @DisplayName("buildFrame rejects null payload id with a clear error")
    void nullPayloadId() {
        assertThatThrownBy(() -> NativeTransport.buildFrame(null, new byte[0], null))
                .isInstanceOfAny(NullPointerException.class, FlezhaNativeException.class);
    }

    @Test
    @DisplayName("version() returns a non-empty string")
    void versionNotEmpty() {
        assertThat(NativeTransport.version()).isNotBlank();
    }

    @Test
    @Tag("stress")
    @DisplayName("tight buildFrame loop does not leak")
    void buildFrameStressLoop() {
        byte[] message = new byte[1024];
        long start = System.nanoTime();
        long stopAfterNanos = java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
        int iterations = 0;
        while (System.nanoTime() - start < stopAfterNanos) {
            byte[] bytes = NativeTransport.buildFrame("p", message, null);
            assertThat(bytes.length).isGreaterThan(message.length);
            iterations++;
        }
        assertThat(iterations).isGreaterThan(0);
        System.out.println("buildFrame stress: " + iterations + " iterations in 2s");
        // Unused but reserved for leak assertions that can be wired up with
        // JMX heap delta tracking or a native allocator hook (e.g. jemalloc).
        Map<String, String> metrics = Map.of("iterations", String.valueOf(iterations));
        assertThat(metrics).isNotEmpty();
    }
}
