package com.flezha.sdk.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * What an operative start takes, which is what {@code flz operative start}
 * takes: an id, the factory that serves it, and optionally its kind, the
 * values it is seeded with and the start options.
 *
 * <p>Instances are immutable; each setter answers a new spec, so a spec is
 * safe to keep and reuse.
 */
public final class OperativeSpec {

    private final String id;
    private final String factoryId;
    private final String kind;
    private final JsonNode initValues;
    private final JsonNode opts;

    private OperativeSpec(
            String id,
            String factoryId,
            @Nullable String kind,
            @Nullable JsonNode initValues,
            @Nullable JsonNode opts) {
        this.id = id;
        this.factoryId = factoryId;
        this.kind = kind;
        this.initValues = initValues;
        this.opts = opts;
    }

    /** A stream operative {@code id} served by {@code factoryId}. */
    @Nonnull
    public static OperativeSpec of(String id, String factoryId) {
        return new OperativeSpec(id, factoryId, null, null, null);
    }

    /** The operative kind: {@code "stream"} (the default) or {@code "workflow"}. */
    @Nonnull
    public OperativeSpec kind(String kind) {
        return new OperativeSpec(id, factoryId, kind, initValues, opts);
    }

    /** The values the factory is seeded with, keyed by procedure. */
    @Nonnull
    public OperativeSpec initValues(JsonNode values) {
        return new OperativeSpec(id, factoryId, kind, values, opts);
    }

    /** The start options, as {@code flz operative start} takes them. */
    @Nonnull
    public OperativeSpec opts(JsonNode opts) {
        return new OperativeSpec(id, factoryId, kind, initValues, opts);
    }

    /** The named parameters the controller builds the operative from. */
    @Nonnull
    ObjectNode fields() {
        ObjectNode fields = JsonNodeFactory.instance.objectNode();
        fields.put("id", id);
        fields.put("factory_id", factoryId);
        if (kind != null) {
            fields.put("kind", kind);
        }
        if (initValues != null) {
            fields.set("init_values", initValues);
        }
        if (opts != null) {
            fields.set("opts", opts);
        }
        return fields;
    }
}
