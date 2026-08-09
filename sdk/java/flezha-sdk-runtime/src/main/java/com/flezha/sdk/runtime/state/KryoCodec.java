package com.flezha.sdk.runtime.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

/**
 * Kryo-backed serializer for per-Entity state and per-Entity Change
 * payloads. Constructed once per {@code UnboundedOperativeInstance}; the {@code userTypes}
 * list aggregates the {@code Procedure.kryoTypes()} returns across every Procedure in
 * the UnboundedOperative class.
 *
 * <p>Stable type IDs are essential for cross-version compatibility under
 * additive Change evolution: a new Change variant added in v2 ships at id
 * N+1 without disturbing existing IDs (umbrella Decision 9).
 */
public final class KryoCodec {

    private final ThreadLocal<Kryo> kryoLocal;
    private final List<Class<?>> registeredTypes;

    public KryoCodec(@Nonnull List<Class<?>> userTypes) {
        this(userTypes, true);
    }

    /**
     * {@code requireRegistration = false} builds a class-name-tagged codec for
     * transient in-process values (edge payloads crossing the poll core), where
     * arbitrary user types must encode without a declared id table.
     */
    public KryoCodec(@Nonnull List<Class<?>> userTypes, boolean requireRegistration) {
        this.registeredTypes = List.copyOf(userTypes);
        this.kryoLocal = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(requireRegistration);
            registerBuiltins(kryo);
            for (Class<?> c : registeredTypes) {
                kryo.register(c);
            }
            return kryo;
        });
    }

    /**
     * Built-in registrations. Stable IDs occupy the low IDs (0-N); user types
     * land at N+1, N+2, ... in the order returned by {@code Procedure.kryoTypes()}.
     */
    private static void registerBuiltins(Kryo kryo) {
        // Common containers and primitives. Kryo's defaults handle primitives
        // and arrays via its default serializers; these registrations stamp
        // stable IDs across processes.
        kryo.register(NoChange.class);
        kryo.register(RestoreInitPayload.class);
        kryo.register(CoalesceKey.class);
        kryo.register(byte[].class);
        kryo.register(int[].class);
        kryo.register(long[].class);
        kryo.register(double[].class);
        kryo.register(String.class);
        kryo.register(Long.class);
        kryo.register(Integer.class);
        kryo.register(Double.class);
        kryo.register(Float.class);
        kryo.register(Boolean.class);
        kryo.register(Optional.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.LinkedList.class);
        kryo.register(HashMap.class);
        kryo.register(LinkedHashMap.class);
        kryo.register(TreeMap.class);
        kryo.register(HashSet.class);
        kryo.register(LinkedHashSet.class);
        kryo.register(TreeSet.class);
        kryo.register(List.class);
        kryo.register(Map.class);
        kryo.register(Set.class);
        kryo.register(java.time.Instant.class);
        // Appended at the tail so existing IDs stay stable. `ArrayDeque` backs
        // `@Journal(MAP_DEQUE)` map-of-deque state.
        kryo.register(java.util.ArrayDeque.class);
        kryo.register(java.util.Deque.class);
    }

    /** Serialize an arbitrary registered object. */
    @Nonnull
    public byte[] serialize(@Nonnull Object obj) {
        Kryo kryo = kryoLocal.get();
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                Output output = new Output(bos)) {
            kryo.writeClassAndObject(output, obj);
            output.flush();
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("Kryo serialize failed: " + e.getMessage(), e);
        }
    }

    /** Deserialize bytes to the requested type. */
    @Nonnull
    public <T> T deserialize(@Nonnull byte[] bytes, @Nonnull Class<T> type) {
        Kryo kryo = kryoLocal.get();
        try (Input input = new Input(bytes)) {
            return type.cast(kryo.readClassAndObject(input));
        }
    }

    /**
     * Serialize {@code obj} preceded by the supplied 16-byte header. The
     * header is written verbatim; the payload follows.
     */
    @Nonnull
    public byte[] serializeWithHeader(@Nonnull Object obj, @Nonnull byte[] header) {
        byte[] payload = serialize(obj);
        byte[] full = new byte[header.length + payload.length];
        System.arraycopy(header, 0, full, 0, header.length);
        System.arraycopy(payload, 0, full, header.length, payload.length);
        return full;
    }

    /**
     * Decode a header-prefixed payload. Returns the 16-byte header slice and
     * the decoded object, leaving header parsing to {@code HeaderTemplate}.
     */
    @Nonnull
    public DecodedFrame deserializeWithHeader(@Nonnull byte[] bytes) {
        if (bytes.length < 16) {
            throw new IllegalArgumentException("frame is shorter than 16 bytes; cannot extract header");
        }
        byte[] header = Arrays.copyOfRange(bytes, 0, 16);
        byte[] payload = Arrays.copyOfRange(bytes, 16, bytes.length);
        Kryo kryo = kryoLocal.get();
        try (Input input = new Input(payload)) {
            Object obj = kryo.readClassAndObject(input);
            return new DecodedFrame(header, obj);
        }
    }

    /** Aggregated registered user-type list (copy). */
    @Nonnull
    public List<Class<?>> registeredTypes() {
        return registeredTypes;
    }

    /** Pair returned by {@link #deserializeWithHeader(byte[])}. */
    public record DecodedFrame(byte[] header, Object value) {}
}
