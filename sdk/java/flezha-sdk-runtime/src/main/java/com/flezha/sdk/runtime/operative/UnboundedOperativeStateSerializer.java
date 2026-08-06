package com.flezha.sdk.runtime.operative;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.flezha.sdk.runtime.entity.EntitySlot;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * Serializes a {@link UnboundedOperativeInstance}'s per-Procedure / per-Entity state to a
 * canonical JSON blob per {@code operative-mvp-rust/design.md} Decision 10:
 *
 * <pre>
 *   single-mode procedure:   {"procedureName": &lt;state&gt;}
 *   static-mode procedure:   {"procedureName": {"entityKey": &lt;state&gt;, ...}}
 *   dynamic-mode procedure:  {"procedureName": {"liveKey": &lt;state&gt;, ...}}
 * </pre>
 *
 * <p>Cross-language byte-equality with the Rust SDK is the eventual gate
 * (verified by an integration test under {@code --only java_sdk}). Jackson
 * is configured to sort keys and avoid pretty-printing so the byte-shape
 * is stable.
 *
 * <p>A {@link Selector} narrows the walk to one Procedure or one Entity so a
 * live read costs what it asked for; the selector-less call is the
 * migration/handover shape and covers every Procedure.
 */
public final class UnboundedOperativeStateSerializer {

    /** Canonical refusal tokens, mirroring the Rust SDK's. */
    public static final String ENTITY_KEY_WITHOUT_PROCEDURE = "entity_key_without_procedure";

    public static final String PROCEDURE_NOT_FOUND = "procedure_not_found";
    public static final String ENTITY_NOT_FOUND = "entity_not_found";
    public static final String ENTITY_EVICTED = "entity_evicted";

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .configure(com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .build();

    private UnboundedOperativeStateSerializer() {}

    /**
     * Which Entities a read covers. Both fields empty is the whole-Operative
     * (migration/handover) walk.
     */
    public record Selector(@Nonnull Optional<String> procedure, @Nonnull Optional<String> entityKey) {

        /** Every Procedure of the Operative. */
        public static Selector all() {
            return new Selector(Optional.empty(), Optional.empty());
        }

        /** Wire form: an empty string is an absent selector field. */
        public static Selector of(String procedure, String entityKey) {
            return new Selector(nonEmpty(procedure), nonEmpty(entityKey));
        }

        private static Optional<String> nonEmpty(String s) {
            return s == null || s.isEmpty() ? Optional.empty() : Optional.of(s);
        }
    }

    /**
     * A selected read: the JSON body, and the {@code change_seq} it was
     * serialized at when the selector named exactly one Entity.
     */
    public record Selection(@Nonnull byte[] json, @Nonnull OptionalLong changeSeq) {}

    /** Refused read. The message starts with one of the canonical tokens above. */
    public static final class RefusedException extends Exception {
        public RefusedException(String message) {
            super(message);
        }
    }

    /**
     * Build the per-Procedure / per-Entity JSON blob for the whole Operative —
     * the migration/handover shape. Returns an empty {@code byte[]} on a
     * serialization failure (logged by the caller).
     */
    @Nonnull
    public static byte[] serialize(@Nonnull UnboundedOperativeInstance instance) throws Exception {
        return select(instance, Selector.all()).json();
    }

    /**
     * Build the JSON blob narrowed by {@code selector}, together with the seq
     * of the single selected Entity (absent when the selector covers more than
     * one).
     *
     * <p>Each Entity is serialized under its {@code stateLock} WITHOUT draining
     * pending Changes — the returned value therefore includes changes not yet
     * flushed to the WAL, and the seq says which version it is. Compaction must
     * drain because its seq becomes a truncation point; a read has no such
     * obligation.
     */
    @Nonnull
    public static Selection select(@Nonnull UnboundedOperativeInstance instance, @Nonnull Selector selector)
            throws Exception {
        if (selector.entityKey().isPresent() && selector.procedure().isEmpty()) {
            throw new RefusedException(ENTITY_KEY_WITHOUT_PROCEDURE
                    + ": an Entity key is unique only within its Procedure — name the Procedure as well");
        }

        Map<String, Object> root = new LinkedHashMap<>();
        List<Long> seqs = new ArrayList<>();
        boolean matchedProcedure = false;

        for (int procedureId = 0; procedureId < instance.topology().procedures().size(); procedureId++) {
            ProcedureDescriptor desc = instance.topology().procedures().get(procedureId);
            if (selector.procedure().isPresent() && !selector.procedure().get().equals(desc.name())) {
                continue;
            }
            matchedProcedure = true;

            // Keyed Procedures — static (declared keys) and dynamic (the live
            // key index IS the keyset) — differ only in where the keyset comes
            // from, so both take the same lookup-one-or-walk-all shape below.
            // Single-mode has no keyset and sits under the Procedure's own name.
            Keyset keyed = Keyset.of(instance, procedureId);
            if (keyed == null) {
                if (selector.entityKey().isPresent()) {
                    throw missingEntity(false, desc.name(), selector.entityKey().get());
                }
                seqs.add(collect(
                        root, desc.name(), instance.entities().get(procedureId).get(0)));
                continue;
            }

            Map<String, Object> perKey = new LinkedHashMap<>();
            if (selector.entityKey().isPresent()) {
                // A narrowed read must cost one lookup, not one walk — that is
                // what the selector is for.
                String key = selector.entityKey().get();
                EntitySlot slot = keyed.get(key);
                if (slot == null) {
                    throw missingEntity(instance.recentlyEvicted(desc.name(), key), desc.name(), key);
                }
                seqs.add(collect(perKey, key, slot));
            } else {
                keyed.forEach((key, slot) -> seqs.add(collect(perKey, key, slot)));
            }
            root.put(desc.name(), perKey);
        }

        if (selector.procedure().isPresent() && !matchedProcedure) {
            throw new RefusedException(PROCEDURE_NOT_FOUND + ": operative `" + instance.instanceId()
                    + "` has no Procedure named `" + selector.procedure().get() + "`");
        }

        OptionalLong seq = seqs.size() == 1 ? OptionalLong.of(seqs.get(0)) : OptionalLong.empty();
        return new Selection(MAPPER.writeValueAsBytes(root), seq);
    }

    /**
     * A keyed Procedure's live Entities: the declared keys for static mode, the
     * live key index for dynamic mode (which is where an evicted Entity
     * disappears from — its slot stays in the list). Both answer a single-key
     * lookup and a walk-all, so a caller expresses one algorithm over either
     * keyset, and a narrowed read never materializes the keys it did not ask
     * for.
     */
    private record Keyset(Map<String, Integer> index, List<String> declared, List<EntitySlot> slots) {

        /** {@code null} for single mode, which has no keyset. */
        static Keyset of(UnboundedOperativeInstance instance, int procedureId) {
            ProcedureDescriptor desc = instance.topology().procedures().get(procedureId);
            List<EntitySlot> slots = instance.entities().get(procedureId);
            if (desc.dynamic()) {
                return new Keyset(instance.entityKeyToIdx().get(procedureId), null, slots);
            }
            if (desc.entityKeys().isEmpty()) {
                return null;
            }
            return new Keyset(null, desc.entityKeys(), slots);
        }

        EntitySlot get(String key) {
            int idx = index != null ? index.getOrDefault(key, -1) : declared.indexOf(key);
            return idx < 0 || idx >= slots.size() ? null : slots.get(idx);
        }

        void forEach(java.util.function.BiConsumer<String, EntitySlot> action) {
            if (index != null) {
                index.forEach((key, idx) -> action.accept(key, slots.get(idx)));
            } else {
                for (int idx = 0; idx < declared.size(); idx++) {
                    action.accept(declared.get(idx), slots.get(idx));
                }
            }
        }
    }

    /** Serialize one Entity into {@code into} under {@code key}, returning the seq it was read at. */
    private static long collect(Map<String, Object> into, String key, EntitySlot slot) {
        slot.stateLock().lock();
        try {
            into.put(key, slot.state().get());
            return slot.changeSeq().get();
        } finally {
            slot.stateLock().unlock();
        }
    }

    /**
     * Eviction is destructive (the recreate cold-starts), so an evicted key has
     * no durable value either — the two refusals differ in diagnosis, not in
     * what could have been served.
     */
    private static RefusedException missingEntity(boolean evicted, String procedureName, String key) {
        return evicted
                ? new RefusedException(ENTITY_EVICTED + ": Entity `" + key + "` of procedure `" + procedureName
                        + "` was idle-evicted; eviction is destructive, so there is no durable value to serve")
                : new RefusedException(
                        ENTITY_NOT_FOUND + ": procedure `" + procedureName + "` holds no Entity keyed `" + key + "`");
    }

    /**
     * Apply a previously-serialized state blob to a freshly-constructed
     * {@link UnboundedOperativeInstance}. Entities absent from the blob retain the
     * state set by their {@code initState} call (forward-compat with Procedures
     * added after the blob was captured).
     */
    public static void deserialize(@Nonnull UnboundedOperativeInstance instance, @Nonnull byte[] blob)
            throws Exception {
        if (blob.length == 0) {
            return;
        }
        // Jackson erases the Map's value type at deserialize; the JSON blob is authored
        // by the matching serialize() above so the cast is round-trip safe.
        @SuppressWarnings("unchecked")
        Map<String, Object> root = MAPPER.readValue(blob, Map.class);
        for (int procedureId = 0; procedureId < instance.topology().procedures().size(); procedureId++) {
            ProcedureDescriptor desc = instance.topology().procedures().get(procedureId);
            Object procedureEntry = root.get(desc.name());
            if (procedureEntry == null) {
                continue;
            }
            Keyset keyed = Keyset.of(instance, procedureId);
            if (keyed == null) {
                EntitySlot slot = instance.entities().get(procedureId).get(0);
                slot.state().set(decodeState(procedureEntry, slot.state().get()));
                continue;
            }
            if (!(procedureEntry instanceof Map<?, ?> perKey)) {
                continue;
            }
            // Entities absent from the blob keep their `initState` value; keys
            // the blob carries that no longer exist (an evicted dynamic one)
            // have nothing to seed.
            for (Map.Entry<?, ?> e : perKey.entrySet()) {
                EntitySlot slot = keyed.get(String.valueOf(e.getKey()));
                if (slot != null) {
                    slot.state().set(decodeState(e.getValue(), slot.state().get()));
                }
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object decodeState(Object encoded, Object existing) throws Exception {
        if (existing == null) {
            return encoded;
        }
        Class<?> targetClass = existing.getClass();
        if (targetClass.isInstance(encoded)) {
            return encoded;
        }
        // Re-route through Jackson with the existing state's class as target;
        // covers records, POJOs, lists, maps.
        byte[] bytes = MAPPER.writeValueAsBytes(encoded);
        return MAPPER.readValue(bytes, targetClass);
    }
}
