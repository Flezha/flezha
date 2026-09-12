package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rules that reusing one encode buffer per thread puts on this codec:
 * the bytes are the bytes the allocating implementation produced, a returned
 * array never aliases the buffer, a nested encode is refused rather than
 * corrupting the encode it sits inside, and an outsized value does not pin its
 * capacity on the thread that encoded it.
 */
class KryoCodecTest {

    /** The codec a nested encode re-enters; set by the reentrancy test alone. */
    private static final AtomicReference<KryoCodec> NESTING_TARGET = new AtomicReference<>();

    /**
     * A payload the nested decode can really read, encoded by the codec under
     * test. A hand-written byte vector is not one: Kryo writes even a
     * single-character String with a length prefix, so a guessed literal
     * throws at the top level and the test would prove only that garbage
     * fails to decode.
     */
    private static final AtomicReference<byte[]> NESTED_PAYLOAD = new AtomicReference<>();

    @Test
    @DisplayName("two encodes on one thread return byte arrays that do not alias each other")
    void twoEncodesOnOneThreadDoNotAlias() {
        KryoCodec codec = new KryoCodec(List.of(), false);

        byte[] first = codec.serialize("first value");
        byte[] firstAsTaken = first.clone();
        byte[] second = codec.serialize("a second value, longer than the first");

        assertThat(first).isNotSameAs(second);
        assertThat(first).containsExactly(firstAsTaken);
        assertThat(codec.deserialize(first, String.class)).isEqualTo("first value");
        assertThat(codec.deserialize(second, String.class)).isEqualTo("a second value, longer than the first");
    }

    @Test
    @DisplayName("the bytes are the ones the allocating implementation produced")
    void bytesMatchTheAllocatingImplementation() {
        // Recorded by running the pre-change `serialize` (a fresh
        // ByteArrayOutputStream and Output per call) against the same values,
        // so the format is pinned independently of how the buffer is managed.
        for (KryoCodec codec : List.of(new KryoCodec(List.of()), new KryoCodec(List.of(), false))) {
            assertThat(codec.serialize("flezha")).containsExactly(3, 102, 108, 101, 122, 104, -31);
            assertThat(codec.serialize(-1L)).containsExactly(9, 1);
            assertThat(codec.serialize(3.5d)).containsExactly(10, 0, 0, 0, 0, 0, 0, 12, 64);
            assertThat(codec.serialize(new ArrayList<>(List.of("a", "b"))))
                    .containsExactly(19, -125, 3, 0, -126, 97, -126, 98);
            assertThat(codec.serialize(Instant.ofEpochSecond(1_700_000_000L, 123)))
                    .containsExactly(30, -128, -30, -49, -86, 6, 123);
        }
    }

    @Test
    @DisplayName("an encode nested inside an encode is refused and names the nested type")
    void nestedEncodeIsRefused() {
        KryoCodec codec = new KryoCodec(List.of(Nesting.class));
        NESTING_TARGET.set(codec);
        try {
            assertThatThrownBy(() -> codec.serialize(new Nesting()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("KryoCodec.serialize re-entered on one thread")
                    .hasMessageContaining(Nesting.class.getName() + "$Nested");
        } finally {
            NESTING_TARGET.set(null);
        }

        // The refused encode left the buffer usable rather than half-written.
        assertThat(codec.deserialize(codec.serialize("after"), String.class)).isEqualTo("after");
    }

    @Test
    @DisplayName("a decode nested inside a decode is refused and names the requested type")
    void nestedDecodeIsRefused() {
        KryoCodec codec = new KryoCodec(List.of(Reading.class));
        byte[] bytes = codec.serialize(new Reading());
        // The nested decode is handed a payload this codec really wrote, so the
        // only thing that can refuse it is the nesting.
        NESTED_PAYLOAD.set(codec.serialize("nested"));
        assertThat(codec.deserialize(NESTED_PAYLOAD.get(), String.class)).isEqualTo("nested");
        NESTING_TARGET.set(codec);
        try {
            assertThatThrownBy(() -> codec.deserialize(bytes, Reading.class))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("KryoCodec.deserialize re-entered on one thread")
                    .hasMessageContaining(String.class.getName());
        } finally {
            NESTING_TARGET.set(null);
            NESTED_PAYLOAD.set(null);
        }

        assertThat(codec.deserialize(codec.serialize("after"), String.class)).isEqualTo("after");
    }

    @Test
    @DisplayName("a decode nested inside an encode keeps working: they are different buffers")
    void decodeNestedInsideAnEncodeIsAllowed() {
        KryoCodec codec = new KryoCodec(List.of(Writing.class));
        NESTED_PAYLOAD.set(codec.serialize("read-me"));
        NESTING_TARGET.set(codec);
        try {
            byte[] bytes = codec.serialize(new Writing());
            assertThat(codec.deserialize(bytes, Writing.class).saw).isEqualTo("read-me");
        } finally {
            NESTING_TARGET.set(null);
            NESTED_PAYLOAD.set(null);
        }
    }

    @Test
    @DisplayName("threads encoding different values through one codec each get their own bytes")
    void concurrentEncodesDoNotShareABuffer() throws Exception {
        KryoCodec codec = new KryoCodec(List.of(), false);
        int threads = 8;
        int rounds = 500;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<AssertionError> failures = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threads; t++) {
            int id = t;
            Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    for (int r = 0; r < rounds; r++) {
                        String value = "thread-" + id + "-round-" + r + "-".repeat(id * 7);
                        byte[] bytes = codec.serialize(value);
                        if (!value.equals(codec.deserialize(bytes, String.class))) {
                            failures.add(new AssertionError("thread " + id + " round " + r));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    failures.add(new AssertionError(e));
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(failures).isEmpty();
    }

    @Test
    @DisplayName("an outsized value does not pin its capacity on the thread that encoded it")
    void anOutsizedValueDoesNotPinTheBuffer() throws Exception {
        KryoCodec codec = new KryoCodec(List.of(), false);
        codec.serialize("small");
        int afterSmall = outputCapacity(codec);

        byte[] big = codec.serialize("x".repeat(200_000));
        assertThat(big.length).isGreaterThan(200_000);
        assertThat(outputCapacity(codec))
                .as("the buffer a 200 KB value grew is handed back once the encode is done")
                .isEqualTo(afterSmall);

        // Handing it back did not cost the codec the ability to grow again.
        assertThat(codec.deserialize(codec.serialize("y".repeat(200_000)), String.class))
                .hasSize(200_000);
    }

    @Test
    @DisplayName("a finished decode holds no reference to the caller's payload")
    void aFinishedDecodeReleasesThePayload() throws Exception {
        KryoCodec codec = new KryoCodec(List.of(), false);
        byte[] payload = codec.serialize("x".repeat(100_000));

        assertThat(codec.deserialize(payload, String.class)).hasSize(100_000);

        assertThat(inputBuffer(codec))
                .as("the reader points at nothing, so the last payload decoded is collectable")
                .isEmpty();
    }

    // --- the internals the two bound assertions read -------------------------
    //
    // The buffer's size is the property under test and it has no surface of its
    // own: giving it one would be an accessor nothing but a test calls.

    private static Object locals(KryoCodec codec) throws Exception {
        Field field = KryoCodec.class.getDeclaredField("locals");
        field.setAccessible(true);
        return ((ThreadLocal<?>) field.get(codec)).get();
    }

    private static int outputCapacity(KryoCodec codec) throws Exception {
        Object mine = locals(codec);
        Field field = mine.getClass().getDeclaredField("output");
        field.setAccessible(true);
        return ((Output) field.get(mine)).getBuffer().length;
    }

    private static byte[] inputBuffer(KryoCodec codec) throws Exception {
        Object mine = locals(codec);
        Field field = mine.getClass().getDeclaredField("input");
        field.setAccessible(true);
        return ((Input) field.get(mine)).getBuffer();
    }

    // --- the two types that re-enter the codec from inside it ----------------
    //
    // `KryoSerializable` is the one seam a value has into its own encode, so
    // re-entering from there is exactly the hazard the guard exists for.

    /** Encodes a value of its own through the codec that is encoding it. */
    static final class Nesting implements KryoSerializable {
        static final class Nested {}

        @Override
        public void write(Kryo kryo, Output output) {
            NESTING_TARGET.get().serialize(new Nested());
        }

        @Override
        public void read(Kryo kryo, Input input) {}
    }

    /** Decodes through the codec that is decoding it. */
    static final class Reading implements KryoSerializable {
        @Override
        public void write(Kryo kryo, Output output) {
            output.writeByte(0);
        }

        @Override
        public void read(Kryo kryo, Input input) {
            input.readByte();
            NESTING_TARGET.get().deserialize(NESTED_PAYLOAD.get(), String.class);
        }
    }

    /** Decodes through the codec that is ENCODING it: a different buffer. */
    static final class Writing implements KryoSerializable {
        private String saw;

        @Override
        public void write(Kryo kryo, Output output) {
            output.writeString(NESTING_TARGET.get().deserialize(NESTED_PAYLOAD.get(), String.class));
        }

        @Override
        public void read(Kryo kryo, Input input) {
            saw = input.readString();
        }
    }
}
