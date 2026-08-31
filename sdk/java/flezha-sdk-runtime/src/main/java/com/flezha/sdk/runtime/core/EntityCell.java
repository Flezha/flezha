package com.flezha.sdk.runtime.core;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.state.EntityId;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.TimerPolicy;

/** One live Entity: its own Procedure instance, state, and codec. */
final class EntityCell {
    final int procedureId;
    final String entityKey;

    @SuppressWarnings("rawtypes")
    final Procedure procedure;

    Object state;
    final KryoCodec codec;
    final TimerPolicy timerPolicy;
    final EntityId id;

    @SuppressWarnings("rawtypes")
    EntityCell(
            int procedureId,
            String entityKey,
            Procedure procedure,
            Object state,
            KryoCodec codec,
            TimerPolicy timerPolicy,
            EntityId id) {
        this.procedureId = procedureId;
        this.entityKey = entityKey;
        this.procedure = procedure;
        this.state = state;
        this.codec = codec;
        this.timerPolicy = timerPolicy;
        this.id = id;
    }
}
