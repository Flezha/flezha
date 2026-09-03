package com.flezha.sdk.runtime.core;

import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.EdgeYield;
import java.util.List;

/**
 * Applying one consumer edge's fused transform closure to one value: the
 * values it yields, or the value itself on an edge that declares no transform.
 *
 * <p>Three callers evaluate this rule — {@link EntityRouting#valuesForEntity}
 * at delivery, {@link EntityRouting#prepare} when the core asks a payload for
 * its keys or its event time, and {@link EmitContext} producer-side. The value
 * a handler sees has crossed the chain exactly once: a stream producer
 * evaluates the edge as its activation completes, and delivery evaluates it
 * only for the payloads no producer transformed. The prepare crossing answers
 * a question about a payload and hands no value on.
 */
final class EdgeTransforms {

    private EdgeTransforms() {}

    static List<Object> valuesOf(ConsumerEdge edge, Object value) {
        EdgeYield yielded = edge.transformValue(value);
        return yielded == null ? List.of(value) : yielded.values();
    }
}
