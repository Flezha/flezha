package com.flezha.sdk.runtime.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Stateless;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * The state crossing, in both directions: restore bodies folded onto a cell,
 * the JSON shape the controller reads and writes, and the change buffer one
 * invocation produced. Every method is handed the cell it works on — the cell
 * map itself is {@link OperativeInstance}'s and stays there.
 */
final class StateExchange {

    /** Sorted-key, non-pretty JSON — the Decision-10 per-Entity state shape. */
    private static final ObjectMapper JSON = JsonMapper.builder()
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private StateExchange() {}

    /** Snapshot then changes, in order, onto a cell the caller has just made. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void foldInto(EntityCell cell, ActivationKind.RestoreFold fold) {
        if (fold.snapshot().isPresent()) {
            ActivationKind.RestoreBody body = fold.snapshot().get();
            cell.state = read(body, cell, Object.class, cell.procedure::migrateState);
        }
        for (ActivationKind.RestoreBody body : fold.changes()) {
            Change change = read(body, cell, Change.class, cell.procedure::migrateChange);
            cell.procedure.applyChange(cell.state, change);
        }
    }

    /**
     * One restore body: decoded with this version's codec, or handed to
     * the migration the core tagged it for. The core has already refused
     * everything unreadable, so there is no third case.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> T read(
            ActivationKind.RestoreBody body, EntityCell cell, Class<T> type, BiFunction<Integer, byte[], ?> migrate) {
        return body.fromVersion().isPresent()
                ? (T) migrate.apply(body.fromVersion().getAsInt(), body.bytes())
                : cell.codec.deserialize(body.bytes(), type);
    }

    static byte[] stateJson(EntityCell cell) {
        // A stateless procedure has nothing to ship. Jackson refuses an
        // empty bean, and that refusal is RIGHT for a user state class with
        // no readable properties — an author mistake worth surfacing — but
        // `Stateless` is the SDK's own marker for "there is no state", so it
        // ships `{}` and reads back through `applyState`'s no-arg
        // constructor. Without this, every stateless Java operative failed
        // its migration serialize and its shutdown state ship.
        if (cell.state instanceof Stateless) {
            return "{}".getBytes(StandardCharsets.UTF_8);
        }
        try {
            return JSON.writeValueAsBytes(cell.state);
        } catch (Exception e) {
            throw new IllegalStateException("state serialization failed: " + e.getMessage(), e);
        }
    }

    static void applyState(EntityCell cell, byte[] json) {
        try {
            Class<?> targetClass = cell.state == null ? Object.class : cell.state.getClass();
            cell.state = JSON.readValue(json, targetClass);
        } catch (Exception e) {
            throw new IllegalStateException("state apply failed: " + e.getMessage(), e);
        }
    }

    static List<byte[]> serializeChanges(EntityCell cell, List<Change> buffer) {
        List<byte[]> changeBytes = new ArrayList<>(buffer.size());
        for (Change change : buffer) {
            changeBytes.add(cell.codec.serialize(change));
        }
        return changeBytes;
    }
}
