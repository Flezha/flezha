package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.runtime.core.DerivedDeclaration;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ChannelEntry;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.PendingEdge;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ProcedureEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Turns the DERIVED declaration the core answered, plus the closures the
 * builder held aside, into the frozen descriptors a
 * {@link StreamOperativeTopology} is made of. The channel names, kinds and
 * consumer edges are the core's answer, never the builder's claim; this class
 * decides nothing and refuses nothing.
 */
final class TopologyMaterializer {

    private TopologyMaterializer() {}

    /** A class that declared no topology at all: the workflow run-sugar shape. */
    static StreamOperativeTopology empty(String factoryId, Map<String, Object> declaration) {
        return new StreamOperativeTopology(factoryId, List.of(), Map.of(), List.of(), Map.of(), false, declaration);
    }

    static StreamOperativeTopology materialize(
            String factoryId,
            List<ChannelEntry> channels,
            List<ProcedureEntry> procedures,
            int[] capacities,
            Map<String, Object> declaration,
            DerivedDeclaration derived) {
        List<ChannelDescriptor> descriptors = channelDescriptors(channels, derived);
        Map<String, Integer> nameToId = new LinkedHashMap<>();
        Map<String, Integer> procedureNameToId = new LinkedHashMap<>();
        for (int id = 0; id < descriptors.size(); id++) {
            nameToId.put(descriptors.get(id).name(), id);
        }
        for (int id = 0; id < procedures.size(); id++) {
            procedureNameToId.put(procedures.get(id).name, id);
        }
        return new StreamOperativeTopology(
                factoryId,
                descriptors,
                nameToId,
                procedureDescriptors(procedures, descriptors, capacities),
                procedureNameToId,
                Boolean.TRUE.equals(declaration.get("effectively_once")),
                declaration);
    }

    private static List<ChannelDescriptor> channelDescriptors(List<ChannelEntry> channels, DerivedDeclaration derived) {
        List<ChannelDescriptor> descriptors = new ArrayList<>(channels.size());
        for (int index = 0; index < channels.size(); index++) {
            ChannelEntry entry = channels.get(index);
            DerivedDeclaration.Channel answer = derived.channel(index);
            Optional<PluginSink> pluginSink = answer.sink() == null
                    ? Optional.empty()
                    : Optional.of(new PluginSink(answer.sink(), entry.pluginSinkSerializer));
            descriptors.add(new ChannelDescriptor(
                    answer.name(),
                    kindOf(answer.kind()),
                    answer.producer() == null ? OptionalInt.empty() : OptionalInt.of(answer.producer()),
                    edges(entry, answer),
                    entry.payloadType,
                    Optional.ofNullable(entry.entryParser),
                    entry.entryParsed,
                    pluginSink,
                    entry.overflowPolicy,
                    new RetryPolicy(entry.retryMaxAttempts, entry.retryBackoffBaseMs),
                    entry.keyed,
                    entry.interProcEncoder == null
                            ? Optional.empty()
                            : Optional.of(new ChannelDescriptor.InterProcCodec(
                                    entry.interProcEncoder, entry.interProcDecoder))));
        }
        return descriptors;
    }

    /**
     * One edge per consumer the core derived, carrying the closures the
     * builder held aside for the chain the core says produced it. Which chain
     * survives a re-declaration is the core's answer; the index is the only
     * thing read here.
     */
    private static List<ConsumerEdge> edges(ChannelEntry entry, DerivedDeclaration.Channel answer) {
        List<ConsumerEdge> out = new ArrayList<>(answer.consumers().size());
        for (DerivedDeclaration.Edge edge : answer.consumers()) {
            PendingEdge chain = entry.chains.get(edge.chain());
            out.add(new ConsumerEdge(
                    edge.procedure(), Optional.ofNullable(chain.transform()), Optional.ofNullable(chain.key())));
        }
        return out;
    }

    private static ChannelKind kindOf(String derived) {
        return switch (derived) {
            case "entry" -> ChannelKind.ENTRY;
            case "plugin" -> ChannelKind.PLUGIN_BOUND;
            default -> ChannelKind.INTER_PROCEDURE;
        };
    }

    private static List<ProcedureDescriptor> procedureDescriptors(
            List<ProcedureEntry> procedures, List<ChannelDescriptor> channels, int[] capacities) {
        List<ProcedureDescriptor> descriptors = new ArrayList<>(procedures.size());
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry entry = procedures.get(i);
            descriptors.add(new ProcedureDescriptor(
                    entry.name,
                    entry.procedureClass,
                    entry.entityKeys,
                    entry.entitiesForOverridden,
                    entry.dynamic,
                    inputChannel(channels, i),
                    entry.outputChannelId == null ? OptionalInt.empty() : OptionalInt.of(entry.outputChannelId),
                    entry.inputType,
                    entry.outputType,
                    entry.changeType,
                    entry.clockType,
                    entry.configType,
                    entry.kryoTypes,
                    entry.schemaVersion,
                    entry.minReadableSchemaVersion,
                    entry.compactAfterBytes,
                    capacities[i],
                    entry.mailboxCapacityOverridden,
                    entry.compensateOverridden,
                    entry.entityIdleTtl,
                    entry.maxEntities));
        }
        return descriptors;
    }

    /**
     * A fan-in procedure keeps its lowest-id input channel here; every
     * incoming channel stays reachable via the channel descriptors, and the
     * dispatcher routes via the channel-side mapping.
     */
    private static OptionalInt inputChannel(List<ChannelDescriptor> channels, int procedure) {
        for (int id = 0; id < channels.size(); id++) {
            if (channels.get(id).consumerIds().contains(procedure)) {
                return OptionalInt.of(id);
            }
        }
        return OptionalInt.empty();
    }
}
