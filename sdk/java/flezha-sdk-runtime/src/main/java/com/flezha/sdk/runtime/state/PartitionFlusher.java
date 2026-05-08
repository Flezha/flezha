package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-JVM flusher for per-Partition state-Change buffers. Lazy-started on the
 * first non-{@code NoChange} Shard registration; runs a single daemon platform
 * thread named {@code flezha-flusher} that wakes every {@code linger_ms} and
 * drains every registered Partition.
 *
 * <p>Linger-ms resolution: system property {@code flezha.flush.linger.ms} &gt;
 * env var {@code FLEZHA_FLUSH_LINGER_MS} &gt; default 5.
 */
public final class PartitionFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFlusher.class);
    private static final AtomicReference<PartitionFlusher> INSTANCE = new AtomicReference<>();

    private final ConcurrentHashMap<String, FlusherEntry> partitions = new ConcurrentHashMap<>();
    private final long lingerMs;
    private final Thread thread;
    private volatile boolean shutdown = false;

    private PartitionFlusher(long lingerMs) {
        this.lingerMs = lingerMs;
        this.thread = new Thread(this::loop, "flezha-flusher");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /** Lazy-start (or fetch) the singleton. */
    @Nonnull
    public static PartitionFlusher getOrStart() {
        PartitionFlusher instance = INSTANCE.get();
        if (instance != null) {
            return instance;
        }
        long linger = resolveLingerMs();
        PartitionFlusher fresh = new PartitionFlusher(linger);
        if (INSTANCE.compareAndSet(null, fresh)) {
            return fresh;
        }
        fresh.shutdown = true;
        fresh.thread.interrupt();
        return INSTANCE.get();
    }

    /** Test-only reset. Drains all Partitions, stops the thread, clears the singleton. */
    public static void resetForTesting() {
        PartitionFlusher current = INSTANCE.getAndSet(null);
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
     * Register a Partition. {@code Change == NoChange.class} is a no-op:
     * {@code NoChange} Shards skip flusher registration entirely so their
     * empty buffers don't pay tick overhead. Returns {@code true} if a new
     * registration landed; {@code false} if the entry already existed or was
     * skipped.
     */
    public boolean register(
            @Nonnull PartitionId partitionId,
            @Nonnull FlusherEntry entry,
            @Nonnull Class<? extends Change> changeType) {
        if (changeType == NoChange.class) {
            return false;
        }
        return partitions.putIfAbsent(partitionId.encoded(), entry) == null;
    }

    /** Number of registered Partitions; test-visible. */
    public int registeredCount() {
        return partitions.size();
    }

    /** Force a single drain pass; test-visible. */
    public void drainOnce() {
        for (FlusherEntry entry : partitions.values()) {
            drain(entry);
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
            for (FlusherEntry entry : partitions.values()) {
                try {
                    drain(entry);
                } catch (RuntimeException e) {
                    LOG.error(
                            "flusher drain failed for partition `{}`: {}",
                            entry.partitionId().encoded(),
                            e.toString());
                }
            }
        }
    }

    /**
     * Drain a Partition's buffer once. Acquires the entry's bufferLock, copies
     * out the pending Changes, releases the lock, then (when persistence is
     * enabled) coalesces, serializes, and ships the frame.
     */
    void drain(FlusherEntry entry) {
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
            long seq = entry.changeSeq().incrementAndGet();
            byte[] payload = entry.codec().serialize(change);
            byte[] header = entry.headerTemplate().stamp(entry.schemaVersion(), payload);
            byte[] frame = buildPartitionStateChangeFrame(entry.partitionId(), seq, header, payload);
            try {
                entry.sendFrame().apply(frame);
            } catch (RuntimeException e) {
                LOG.error(
                        "flusher sendFrame failed for partition `{}`: {}",
                        entry.partitionId().encoded(),
                        e.toString());
            }
        }
    }

    /**
     * Two-pass coalescing per {@code relay-state-persistence-rust/design.md}
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
     * Wire-frame layout (umbrella spec):
     * <pre>
     *   [partition_id_len: u32 BE]
     *   [partition_id_bytes: utf8]
     *   [seq: u64 BE]
     *   [header: 16 bytes]
     *   [change_bytes_len: u32 BE]
     *   [change_bytes: opaque]
     *   [crc32: u32 BE]   // over the entire preceding frame
     * </pre>
     */
    public static byte[] buildPartitionStateChangeFrame(PartitionId pid, long seq, byte[] header, byte[] changeBytes) {
        byte[] idBytes = pid.encoded().getBytes(StandardCharsets.UTF_8);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(idBytes.length);
            out.write(idBytes);
            out.writeLong(seq);
            out.write(header);
            out.writeInt(changeBytes.length);
            out.write(changeBytes);
            byte[] body = bos.toByteArray();
            java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(body);
            try (ByteArrayOutputStream framed = new ByteArrayOutputStream();
                    DataOutputStream framedOut = new DataOutputStream(framed)) {
                framedOut.write(body);
                framedOut.writeInt((int) crc.getValue());
                return framed.toByteArray();
            }
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
