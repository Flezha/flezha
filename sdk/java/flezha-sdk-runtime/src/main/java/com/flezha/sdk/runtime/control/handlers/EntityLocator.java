package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.entity.EntitySlot;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.state.EntityId;
import java.util.Arrays;
import java.util.List;

/**
 * Locates a live unbounded Entity by its wire {@code entity_id}. Shared by the
 * control handlers that address one Entity ({@link RestoreInitStateHandler},
 * {@link CompactEntityHandler}): the identity is built the same way the flusher
 * frames {@code entity_id}, so a byte-equal match is exact.
 */
final class EntityLocator {

    private EntityLocator() {}

    /** A located unbounded Entity: its owning instance, slot, and wire identity. */
    record Located(UnboundedOperativeInstance instance, EntitySlot slot, EntityId entityId) {}

    /**
     * Scan every live unbounded operative's entities for the slot whose binary
     * identity equals {@code entityId}; {@code null} if none matches.
     */
    static Located findUnbounded(RunnerContext ctx, byte[] entityId) {
        for (UnboundedOperativeInstance instance :
                ctx.operativeInstanceRegistry().operativeSnapshot().values()) {
            List<List<EntitySlot>> entities = instance.entities();
            for (List<EntitySlot> procedureEntities : entities) {
                for (EntitySlot slot : procedureEntities) {
                    String procedureName = instance.topology()
                            .procedures()
                            .get(slot.procedureId())
                            .name();
                    EntityId identity = EntityId.of(instance.instanceId(), procedureName, slot.entityKey());
                    if (Arrays.equals(identity.encodedBytes(), entityId)) {
                        return new Located(instance, slot, identity);
                    }
                }
            }
        }
        return null;
    }
}
