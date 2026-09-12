package com.flezha.sdk.runtime.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
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
 * payloads. Constructed once per {@code StreamOperativeInstance}; the {@code userTypes}
 * list aggregates the {@code Procedure.kryoTypes()} returns across every Procedure in
 * the StreamOperative class.
 *
 * <p>Stable type IDs are essential for cross-version compatibility under
 * additive Change evolution: a new Change variant added in v2 ships at id
 * N+1 without disturbing existing IDs (umbrella Decision 9).
 *
 * <p>The encode buffer is reused per thread rather than allocated per value,
 * which makes it shared mutable state for the duration of one encode. Two
 * rules follow and neither is visible at a call site: the finished bytes are
 * copied out, because the next encode on this thread overwrites the buffer;
 * and an encode entered while another encode is in progress on this thread is
 * refused rather than allowed to write into the buffer it is nested inside.
 */
public final class KryoCodec {

    /** The buffer a thread starts with, and returns to after an outsized value. */
    private static final int INITIAL_CAPACITY = 4096;

    /**
     * The capacity a thread may keep between encodes. Above it the buffer is
     * handed back, so one outsized value cannot pin its size on that thread for
     * the life of the process.
     */
    private static final int TRIM_ABOVE = 64 * 1024;

    /** What a finished decode points its reader at, so it holds no payload. */
    private static final byte[] NO_BYTES = new byte[0];

    private final ThreadLocal<Locals> locals;
    private final List<Class<?>> registeredTypes;

    /**
     * Everything one thread owns: its Kryo, its encode buffer, its reader, and
     * the flags that keep one encode out of another's buffer. One holder rather
     * than one per part, so a call costs one thread-local lookup.
     *
     * <p>The unbounded growth is what the discarded {@code Output(bos)} had: a
     * 4 KB window flushing into a stream that accepted any size. {@code Input}
     * starts uninitialized; every decode sets its buffer before reading.
     */
    private static final class Locals {
        private final Kryo kryo;
        private final Output output = new Output(INITIAL_CAPACITY, -1);
        private final Input input = new Input();
        private boolean encoding;
        private boolean decoding;

        private Locals(Kryo kryo) {
            this.kryo = kryo;
        }
    }

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
        this.locals = ThreadLocal.withInitial(() -> {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(requireRegistration);
            registerBuiltins(kryo);
            for (Class<?> c : registeredTypes) {
                kryo.register(c);
            }
            return new Locals(kryo);
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

    /**
     * Serialize an arbitrary registered object into this thread's buffer and
     * return a copy of what was written.
     *
     * <p>The copy is what transfers ownership: the caller keeps the array while
     * the next encode on this thread overwrites the buffer behind it.
     */
    @Nonnull
    public byte[] serialize(@Nonnull Object obj) {
        Locals mine = locals.get();
        if (mine.encoding) {
            throw new IllegalStateException(
                    reentered("serialize", obj.getClass().getName()));
        }
        mine.encoding = true;
        try {
            mine.output.reset();
            mine.kryo.writeClassAndObject(mine.output, obj);
            return mine.output.toBytes();
        } finally {
            mine.encoding = false;
            if (mine.output.getBuffer().length > TRIM_ABOVE) {
                mine.output.setBuffer(new byte[INITIAL_CAPACITY], -1);
            }
        }
    }

    /** Deserialize bytes to the requested type. */
    @Nonnull
    public <T> T deserialize(@Nonnull byte[] bytes, @Nonnull Class<T> type) {
        Locals mine = locals.get();
        if (mine.decoding) {
            throw new IllegalStateException(reentered("deserialize", type.getName()));
        }
        mine.decoding = true;
        try {
            mine.input.setBuffer(bytes);
            return type.cast(mine.kryo.readClassAndObject(mine.input));
        } finally {
            mine.decoding = false;
            mine.input.setBuffer(NO_BYTES);
        }
    }

    private static String reentered(String method, String type) {
        return "KryoCodec."
                + method
                + " re-entered on one thread for "
                + type
                + ": this codec reuses one buffer per thread, so a serializer that calls back "
                + "into it would write into the buffer it is nested inside. Give the nested "
                + "value its own codec instead.";
    }

    /** Aggregated registered user-type list (copy). */
    @Nonnull
    public List<Class<?>> registeredTypes() {
        return registeredTypes;
    }
}
