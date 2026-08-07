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
 * Per-JVM flusher for per-Entity state-Change buffers. Lazy-started on the
 * first non-{@code NoChange} Procedure registration; runs a single daemon platform
 * thread named {@code flezha-flusher} that wakes every {@code linger_ms} and
 * drains every registered Entity.
 *
 * <p>Linger-ms resolution: system property {@code flezha.flush.linger.ms} &gt;
 * env var {@code FLEZHA_FLUSH_LINGER_MS} &gt; default 5.
 */
public final class EntityFlusher {

    private static final Logger LOG = LoggerFactory.getLogger(EntityFlusher.class);
    private static final AtomicReference<EntityFlusher> INSTANCE = new AtomicReference<>();

    private final ConcurrentHashMap<String, FlusherEntry> entities = new ConcurrentHashMap<>();
    private final long lingerMs;
    private final Thread thread;
    private volatile boolean shutdown = false;

    private EntityFlusher(long lingerMs) {
        this.lingerMs = lingerMs;
        this.thread = new Thread(this::loop, "flezha-flusher");
        this.thread.setDaemon(true);
        this.thread.start();
    }

    /** Lazy-start (or fetch) the singleton. */
    @Nonnull
    public static EntityFlusher getOrStart() {
        EntityFlusher instance = INSTANCE.get();
        if (instance != null) {
            return instance;
        }
        long linger = resolveLingerMs();
        EntityFlusher fresh = new EntityFlusher(linger);
        if (INSTANCE.compareAndSet(null, fresh)) {
            return fresh;
        }
        fresh.shutdown = true;
        fresh.thread.interrupt();
        return INSTANCE.get();
    }

    /** Test-only reset. Drains all Entities, stops the thread, clears the singleton. */
    public static void resetForTesting() {
        EntityFlusher current = INSTANCE.getAndSet(null);
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
     * Register an Entity. {@code Change == NoChange.class} is a no-op:
     * {@code NoChange} Procedures skip flusher registration entirely so their
     * empty buffers don't pay tick overhead. Returns {@code true} if a new
     * registration landed; {@code false} if the entry already existed or was
     * skipped.
     */
    public boolean register(
            @Nonnull EntityId entityId, @Nonnull FlusherEntry entry, @Nonnull Class<? extends Change> changeType) {
        if (changeType == NoChange.class) {
            return false;
        }
        // Overwrite any prior entry under this identity: after a reconnect the
        // operative registry is cleared and AddOperative rebuilds fresh slots under the
        // same operative_id, so the new slot's buffer/changeSeq must replace the
        // dead one rather than be skipped.
        return entities.put(entityId.encoded(), entry) == null;
    }

    /** Number of registered Entities; test-visible. */
    public int registeredCount() {
        return entities.size();
    }

    /** Force a single drain pass; test-visible. */
    public void drainOnce() {
        for (FlusherEntry entry : entities.values()) {
            flushEntry(entry);
        }
    }

    /**
     * Drain one registered Entity's pending Changes now. Compaction calls this
     * (under the slot's {@code stateLock}, so no run can refill the buffer)
     * before capturing the snapshot, so the serialized state equals the WAL the
     * controller holds and the snapshot's seq cannot tear against it. No-op if
     * the Entity is unregistered ({@code NoChange}, or torn down).
     */
    public void flushEntity(@Nonnull EntityId entityId) {
        FlusherEntry entry = entities.get(entityId.encoded());
        if (entry != null) {
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
            for (FlusherEntry entry : entities.values()) {
                try {
                    flushEntry(entry);
                } catch (RuntimeException e) {
                    LOG.error(
                            "flusher drain failed for entity `{}`: {}",
                            entry.entityId().encoded(),
                            e.toString());
                }
            }
        }
    }

    /**
     * Flush one entry's pending Changes to the WAL: under the bufferLock, drain
     * the buffer then (when persistence is enabled) coalesce, serialize, and
     * ship the frames. The sends stay inside the lock (mirroring the Rust
     * flusher) so the credit debit-on-send can park here with producers
     * blocking on the lock — the emit-path park — instead of growing the
     * buffer unboundedly while the window is exhausted. Static — needs no
     * running flusher instance — so the bounded driver calls it directly for
     * its one-shot per-entity runs (never registering with the background
     * flusher, so nothing leaks per job); the background loop uses it too.
     */
    public static void flushEntry(FlusherEntry entry) {
        synchronized (entry.bufferLock()) {
            flushTimerOps(entry);
            if (!entry.effects().isEmpty()) {
                flushEffects(entry);
                return;
            }
            if (entry.buffer().isEmpty()) {
                return;
            }
            List<Change> snapshot = new ArrayList<>(entry.buffer());
            entry.buffer().clear();

            if (!entry.persistenceEnabled()) {
                return;
            }

            List<Change> coalesced = coalesce(snapshot);
            for (Change change : coalesced) {
                // 0-based per-Entity seq: the controller treats the first valid
                // seq as 0 (last_persisted_seq starts at -1). getAndIncrement on an
                // AtomicLong based at 0 yields 0 first, matching the Rust flusher's
                // fetch_add from AtomicU64::new(0).
                long seq = entry.changeSeq().getAndIncrement();
                byte[] payload = entry.codec().serialize(change);
                byte[] header = entry.headerTemplate().stamp(entry.schemaVersion(), payload);
                byte[] frame = buildEntityStateChangeFrame(entry.entityId(), seq, header, payload);
                ship(entry, EntityStateSink.PAYLOAD_ID_CHANGE, frame);
            }
        }
    }

    /**
     * Drain the effectively-once buffer: one {@code entity_effect_record} frame
     * per invocation, in emission order.
     *
     * <p><b>Never coalesced.</b> Coalescing collapses same-key changes to the
     * latest — which for an effect record would silently discard the collapsed
     * record's plugin outputs AND its consumed offset. The tier's whole promise
     * is that those three travel together in one WAL record; dropping any of
     * them here would break it in the least visible way possible. (Rust pins the
     * same rule in {@code effect_records_are_never_coalesced_at_drain}.)
     *
     * <p>Caller holds {@code bufferLock}.
     */
    private static void flushEffects(FlusherEntry entry) {
        List<EffectRecord> snapshot = new ArrayList<>(entry.effects());
        entry.effects().clear();

        if (!entry.persistenceEnabled()) {
            return;
        }

        for (EffectRecord record : snapshot) {
            long seq = entry.changeSeq().getAndIncrement();
            byte[] payload = record.encode();
            byte[] header = entry.headerTemplate()
                    .stamp(entry.schemaVersion(), HeaderTemplate.FORMAT_ID_EFFECT_RECORD, payload);
            byte[] frame = buildEntityStateChangeFrame(entry.entityId(), seq, header, payload);
            ship(entry, EntityStateSink.PAYLOAD_ID_EFFECT_RECORD, frame);
        }
    }

    /**
     * Drain the tier-off timer ops as ONE {@code format_id = 3} record. The
     * controller stores and truncates it exactly as it does a change record and
     * never decodes it — the pending set is SDK-owned state, like the change
     * bytes inside an effect record.
     *
     * <p>Caller holds {@code bufferLock}.
     */
    private static void flushTimerOps(FlusherEntry entry) {
        if (entry.timerOps().isEmpty()) {
            return;
        }
        List<TimerOp> ops = new ArrayList<>(entry.timerOps());
        entry.timerOps().clear();
        if (!entry.persistenceEnabled()) {
            return;
        }
        long seq = entry.changeSeq().getAndIncrement();
        byte[] payload = TimerOp.encodeList(ops);
        byte[] header =
                entry.headerTemplate().stamp(entry.schemaVersion(), HeaderTemplate.FORMAT_ID_TIMER_OPS, payload);
        ship(
                entry,
                EntityStateSink.PAYLOAD_ID_CHANGE,
                buildEntityStateChangeFrame(entry.entityId(), seq, header, payload));
    }

    /** Debit the credit window, then send. Caller holds {@code bufferLock}. */
    private static void ship(FlusherEntry entry, String payloadId, byte[] frame) {
        CreditWindow.acquire(frame.length);
        try {
            entry.sendFrame().apply(payloadId, frame);
        } catch (RuntimeException e) {
            LOG.error(
                    "flusher sendFrame failed for entity `{}`: {}",
                    entry.entityId().encoded(),
                    e.toString());
        }
    }

    /**
     * Two-pass coalescing per {@code operative-state-persistence/design.md}
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
     *   [entity_id_len: u16 BE]
     *   [entity_id_bytes]          // EntityId.encodedBytes(): [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
     *   [seq: u64 BE]
     *   [header: 16 bytes]
     *   [change_bytes_len: u32 BE]
     *   [change_bytes: opaque]
     *   [crc32: u32 BE]               // over change_bytes only
     * </pre>
     */
    public static byte[] buildEntityStateChangeFrame(EntityId pid, long seq, byte[] header, byte[] changeBytes) {
        byte[] idBytes = pid.encodedBytes();
        if (idBytes.length > 0xFFFF) {
            throw new IllegalStateException("entity identity exceeds 65535 bytes");
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
