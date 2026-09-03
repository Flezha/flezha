package com.flezha.sdk.runtime.core;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.state.EventTimeFrom;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nullable;

/**
 * Where a channel's payload goes. Two questions with one set of rules: which
 * values an Entity actually sees when an input is delivered to it
 * ({@link #valuesForEntity}), and which keys and which event time a payload
 * carries before the core has routed it at all ({@link #prepare}). Both decode
 * for the channel's kind and evaluate the consumer edge.
 *
 * <p>Nothing here outlives the call it is made in, which is why one instance
 * per operative is built at materialization and reused for every activation.
 */
final class EntityRouting {

    private final StreamOperativeTopology topology;

    /** Per-Procedure routing instances — {@code entitiesFor} / {@code eventTime} only. */
    @SuppressWarnings("rawtypes")
    private final List<Procedure> routers;

    private final KryoCodec edgeCodec;

    /** Whether this class's producers release their values untransformed. */
    private final boolean structuredWorkflow;

    @SuppressWarnings("rawtypes")
    EntityRouting(
            StreamOperativeTopology topology,
            List<Procedure> routers,
            KryoCodec edgeCodec,
            boolean structuredWorkflow) {
        this.topology = topology;
        this.routers = routers;
        this.edgeCodec = edgeCodec;
        this.structuredWorkflow = structuredWorkflow;
    }

    /**
     * Decode an input payload and keep only the values that route to this
     * Entity, applying the consumer edge's transform chain when — and only
     * when — the payload reached here untransformed: an entry payload, whose
     * producer is the wire, or a workflow stage's released value. A stream
     * producer evaluated the edge when its activation completed, so applying
     * it again would compose the chain with itself.
     */
    @SuppressWarnings("unchecked")
    List<Object> valuesForEntity(EntityCell cell, int channelId, byte[] payload) {
        ChannelDescriptor channel = topology.channel(channelId);
        if (channel == null) {
            throw new IllegalStateException("input names unknown channel id " + channelId);
        }
        Object base = decodePayload(channel, payload);
        ConsumerEdge edge = edgeInto(channel, cell.procedureId);
        if (edge == null) {
            return List.of(base);
        }
        List<Object> values = channel.kind() == ChannelKind.ENTRY || structuredWorkflow
                ? EdgeTransforms.valuesOf(edge, base)
                : List.of(base);
        if (values.isEmpty()) {
            return values;
        }
        ProcedureDescriptor desc = topology.procedure(cell.procedureId);
        if (edge.key().isPresent()) {
            return values.stream()
                    .filter(v -> cell.entityKey.equals(edge.keyOf(v)))
                    .toList();
        }
        if (!desc.entityKeys().isEmpty()) {
            @SuppressWarnings("rawtypes")
            Procedure router = routers.get(cell.procedureId);
            return values.stream()
                    .filter(v -> ((List<String>) router.entitiesFor(v)).contains(cell.entityKey))
                    .toList();
        }
        return values;
    }

    /**
     * The core's pre-routing question, asked per payload: which Entity keys
     * does this payload reach, and what event time does its consumer read off
     * it. A refusal is per payload — one bad value does not fail the batch.
     */
    List<PrepareResult> prepare(ActivationKind.KeyRequest request) {
        ChannelDescriptor channel = topology.channel(request.channel());
        if (channel == null) {
            throw new IllegalStateException("key request names unknown channel id " + request.channel());
        }
        List<PrepareResult> results = new ArrayList<>(request.payloads().size());
        for (byte[] payload : request.payloads()) {
            try {
                Object base = decodePayload(channel, payload);
                List<String> keys = List.of();
                OptionalLong eventTime = OptionalLong.empty();
                if (request.wantKeys()) {
                    keys = prepareKeys(channel, base);
                }
                if (request.wantEventTime()) {
                    eventTime = prepareEventTime(channel, base);
                }
                results.add(new PrepareResult.Ok(new PrepareAnswer(keys, eventTime)));
            } catch (Throwable t) {
                results.add(new PrepareResult.Err(
                        new WorkError(t.toString(), t instanceof ProcedureException.NonRetryable)));
            }
        }
        return results;
    }

    private Object decodePayload(ChannelDescriptor channel, byte[] payload) {
        if (channel.kind() == ChannelKind.ENTRY) {
            return channel.entryParser().map(parser -> parser.apply(payload)).orElse(payload);
        }
        return edgeCodec.deserialize(payload, Object.class);
    }

    @Nullable
    private static ConsumerEdge edgeInto(ChannelDescriptor channel, int procedureId) {
        for (ConsumerEdge edge : channel.consumers()) {
            if (edge.procedure() == procedureId) {
                return edge;
            }
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<String> prepareKeys(ChannelDescriptor channel, Object base) {
        for (ConsumerEdge edge : channel.consumers()) {
            ProcedureDescriptor desc = topology.procedure(edge.procedure());
            if (desc == null) {
                continue;
            }
            if (desc.dynamic() && edge.key().isPresent()) {
                LinkedHashSet<String> keys = new LinkedHashSet<>();
                for (Object value : EdgeTransforms.valuesOf(edge, base)) {
                    keys.add(edge.keyOf(value));
                }
                return List.copyOf(keys);
            }
            if (!desc.dynamic() && desc.entitiesForOverridden()) {
                LinkedHashSet<String> keys = new LinkedHashSet<>();
                Procedure router = routers.get(edge.procedure());
                for (Object value : EdgeTransforms.valuesOf(edge, base)) {
                    keys.addAll(router.entitiesFor(value));
                }
                return List.copyOf(keys);
            }
        }
        return List.of();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OptionalLong prepareEventTime(ChannelDescriptor channel, Object base) {
        for (ConsumerEdge edge : channel.consumers()) {
            ProcedureDescriptor desc = topology.procedure(edge.procedure());
            if (desc == null || desc.clockType() != EventTimeFrom.class) {
                continue;
            }
            List<Object> values = EdgeTransforms.valuesOf(edge, base);
            if (!values.isEmpty()) {
                Procedure router = routers.get(edge.procedure());
                return OptionalLong.of(router.eventTime(values.get(0)));
            }
        }
        return OptionalLong.empty();
    }
}
