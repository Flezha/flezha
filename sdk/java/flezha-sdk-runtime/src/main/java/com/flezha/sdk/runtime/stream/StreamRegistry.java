package com.flezha.sdk.runtime.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-index registry of (primary, secondary) stream id pairs opened via
 * {@code ConnectSocket}. Mirrors the Rust SDK's {@code shared_io_streams}
 * field, where the key is the 0-based socket index.
 */
public final class StreamRegistry {

    private final Map<Integer, StreamPair> byIndex = new ConcurrentHashMap<>();
    private volatile long controlStreamId;

    public void setControlStreamId(long controlStreamId) {
        this.controlStreamId = controlStreamId;
    }

    public long controlStreamId() {
        return controlStreamId;
    }

    public void register(int index, long primary, long secondary) {
        byIndex.put(index, new StreamPair(primary, secondary));
    }

    public StreamPair get(int index) {
        return byIndex.get(index);
    }

    /** Returns a secondary stream id suitable for SDK→controller writes. Round-robin over indices. */
    public long anySecondary() {
        for (StreamPair pair : byIndex.values()) {
            return pair.secondary();
        }
        return 0L;
    }

    /** Returns every registered secondary stream id — one heartbeat is written per stream. */
    public List<Long> secondaries() {
        List<Long> out = new ArrayList<>(byIndex.size());
        for (StreamPair pair : byIndex.values()) {
            out.add(pair.secondary());
        }
        return out;
    }

    /** Returns the secondary stream id associated with a particular primary stream, if any. */
    public long secondaryFor(long primaryStreamId) {
        for (StreamPair pair : byIndex.values()) {
            if (pair.primary() == primaryStreamId) {
                return pair.secondary();
            }
        }
        return anySecondary();
    }

    public int size() {
        return byIndex.size();
    }

    public void clear() {
        byIndex.clear();
    }

    public record StreamPair(long primary, long secondary) {}
}
