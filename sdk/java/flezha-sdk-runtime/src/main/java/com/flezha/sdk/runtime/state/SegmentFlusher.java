package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-JVM flusher for per-Segment state-Change buffers. Lazy-started on the
 * first non-{@code NoChange} Operation registration; runs a single daemon platform
 * thread named {@code flezha-flusher} that wakes every {@code linger_ms} and
 * drains every registered Segment.
 *
 * <p>Linger-ms resolution: system property {@code flezha.flush.linger.ms} &gt;
 * env var {@code FLEZHA_FLUSH_LINGER_MS} &gt; default 5.
 */
public final class SegmentFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentFlusher.class);
    private static final AtomicReference<SegmentFlusher> INSTANCE = new AtomicReference<>();

    private final ConcurrentHashMap<String, FlusherEntry> segments = new ConcurrentHashMap<>();
    private final long lingerMs;
    private final Thread thread;
    private volatile boolean shutdown = false;

    private SegmentFlusher(long lingerMs) {
        this.lingerMs = lingerMs;
        this.thread = new Thread(this::loop, "flezha-flusher");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /** Lazy-start (or fetch) the singleton. */
    @Nonnull
    public static SegmentFlusher getOrStart() {
        SegmentFlusher instance = INSTANCE.get();
        if (instance != null) {
            return instance;
        }
        long linger = resolveLingerMs();
        SegmentFlusher fresh = new SegmentFlusher(linger);
        if (INSTANCE.compareAndSet(null, fresh)) {
            return fresh;
        }
        fresh.shutdown = true;
        fresh.thread.interrupt();
        return INSTANCE.get();
    }

    /** Test-only reset. Drains all Segments, stops the thread, clears the singleton. */
    public static void resetForTesting() {
        SegmentFlusher current = INSTANCE.getAndSet(null);
        if (current != null) {
            current.shutdown = true;
            current.thread.interrupt();
            try {
                current.thread.join(1000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Register a Segment. {@code Change == NoChange.class} is a no-op:
     * {@code NoChange} Operations skip flusher registration entirely so their
     * empty buffers don't pay tick overhead. Returns {@code true} if a new
     * registration landed; {@code false} if the entry already existed or was
     * skipped.
     */
    public boolean register(
            @Nonnull SegmentId segmentId, @Nonnull FlusherEntry entry, @Nonnull Class<? extends Change> changeType) {
        if (changeType == NoChange.class) {
            return false;
        }
        // Overwrite any prior entry under this identity: after a reconnect the
        // operative registry is cleared and AddOperative rebuilds fresh slots under the
        // same operative_id, so the new slot's buffer/changeSeq must replace the
        // dead one rather than be skipped.
        return segments.put(segmentId.encoded(), entry) == null;
    }

    /** Number of registered Segments; test-visible. */
    public int registeredCount() {
        return segments.size();
    }

    /** Force a single drain pass; test-visible. */
    public void drainOnce() {
        for (FlusherEntry entry : segments.values()) {
            flushEntry(entry);
        }
    }

    private void loop() {
        while (!shutdown) {
            try {
                Thread.sleep(lingerMs);
            } catch (InterruptedException e) {
                if (shutdown) {
                    return;
                }
                Thread.currentThread().interrupt();
                return;
            }
            for (FlusherEntry entry : segments.values()) {
                try {
                    flushEntry(entry);
                } catch (RuntimeException e) {
                    LOG.error(
                            "flusher drain failed for segment `{}`: {}",
                            entry.segmentId().encoded(),
                            e.toString());
                }
            }
        }
    }

    /**
     * Flush one entry's pending Changes to the WAL: acquire the bufferLock, copy
     * out and release, then (when persistence is enabled) coalesce, serialize,
     * and ship the frame. Static — needs no running flusher instance — so the
     * bounded driver calls it directly for its one-shot per-segment runs (never
     * registering with the background flusher, so nothing leaks per job); the
     * background loop uses it too.
     */
    public static void flushEntry(FlusherEntry entry) {
        List<Change> snapshot;
        synchronized (entry.bufferLock()) {
            if (entry.buffer().isEmpty()) {
                return;
            }
            snapshot = new ArrayList<>(entry.buffer());
            entry.buffer().clear();
        }

        if (!entry.persistenceEnabled()) {
            return;
        }

        List<Change> coalesced = coalesce(snapshot);
        for (Change change : coalesced) {
            // 0-based per-Segment seq: the controller treats the first valid
            // seq as 0 (last_persisted_seq starts at -1). getAndIncrement on an
            // AtomicLong based at 0 yields 0 first, matching the Rust flusher's
            // fetch_add from AtomicU64::new(0).
            long seq = entry.changeSeq().getAndIncrement();
            byte[] payload = entry.codec().serialize(change);
            byte[] header = entry.headerTemplate().stamp(entry.schemaVersion(), payload);
            byte[] frame = buildSegmentStateChangeFrame(entry.segmentId(), seq, header, payload);
            try {
                entry.sendFrame().apply(frame);
            } catch (RuntimeException e) {
                LOG.error(
                        "flusher sendFrame failed for segment `{}`: {}",
                        entry.segmentId().encoded(),
                        e.toString());
            }
        }
    }

    /**
     * Two-pass coalescing per {@code operative-state-persistence-rust/design.md}
     * Decision 3: walk the buffer from end to start tracking the latest
     * occurrence of each {@link CoalesceKey}; emit in original order skipping
     * entries that are not the latest for their key. Changes without a key
     * are emitted as-is.
     */
    public static List<Change> coalesce(List<Change> buffer) {
        if (buffer.isEmpty()) {
            return buffer;
        }
        Map<CoalesceKey, Integer> latestIdx = new HashMap<>();
        for (int i = 0; i < buffer.size(); i++) {
            Optional<CoalesceKey> key = buffer.get(i).coalesceKey();
            if (key.isPresent()) {
                latestIdx.put(key.get(), i);
            }
        }
        if (latestIdx.isEmpty()) {
            return buffer;
        }
        List<Change> result = new ArrayList<>(buffer.size());
        Set<CoalesceKey> seenKeys = new HashSet<>();
        for (int i = 0; i < buffer.size(); i++) {
            Change change = buffer.get(i);
            Optional<CoalesceKey> key = change.coalesceKey();
            if (key.isEmpty()) {
                result.add(change);
                continue;
            }
            CoalesceKey k = key.get();
            int latest = latestIdx.get(k);
            if (i == latest && seenKeys.add(k)) {
                result.add(change);
            }
        }
        return result;
    }

    /**
     * Wire-frame layout, byte-identical to the controller's
     * {@code Flezha.State.Wire.decode_frame/1}:
     * <pre>
     *   [segment_id_len: u16 BE]
     *   [segment_id_bytes]          // SegmentId.encodedBytes(): [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
     *   [seq: u64 BE]
     *   [header: 16 bytes]
     *   [change_bytes_len: u32 BE]
     *   [change_bytes: opaque]
     *   [crc32: u32 BE]               // over change_bytes only
     * </pre>
     */
    public static byte[] buildSegmentStateChangeFrame(SegmentId pid, long seq, byte[] header, byte[] changeBytes) {
        byte[] idBytes = pid.encodedBytes();
        if (idBytes.length > 0xFFFF) {
            throw new IllegalStateException("segment identity exceeds 65535 bytes");
        }
        CRC32 crc = new CRC32();
        crc.update(changeBytes);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeShort(idBytes.length); // u16 BE
            out.write(idBytes);
            out.writeLong(seq); // u64 BE
            out.write(header); // 16 bytes
            out.writeInt(changeBytes.length); // u32 BE
            out.write(changeBytes);
            out.writeInt((int) crc.getValue()); // u32 BE over change_bytes
            return bos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("frame build failed", e);
        }
    }

    private static long resolveLingerMs() {
        String prop = System.getProperty("flezha.flush.linger.ms");
        if (prop != null) {
            try {
                return Long.parseLong(prop);
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        String envVar = System.getenv("FLEZHA_FLUSH_LINGER_MS");
        if (envVar != null) {
            try {
                return Long.parseLong(envVar);
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        return 5L;
    }
}
