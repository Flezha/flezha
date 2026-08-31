package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.CheckpointPolicy;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ChannelEntry;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ProcedureEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Turns the builder's staging entries into the frozen descriptors a
 * {@link StreamOperativeTopology} is made of. It runs only after
 * {@link TopologyRules} found nothing to refuse, so it decides nothing and
 * refuses nothing.
 */
final class TopologyMaterializer {

    private TopologyMaterializer() {}

    static StreamOperativeTopology materialize(
            String factoryId,
            List<ChannelEntry> channels,
            List<ProcedureEntry> procedures,
            TopologyRules.Checked checked,
            Map<String, Integer> procedureNameToId,
            boolean effectivelyOnce,
            CheckpointPolicy checkpoint,
            long correctionWindowMs,
            BeyondWindowPolicy beyondWindowPolicy) {
        return new StreamOperativeTopology(
                factoryId,
                channelDescriptors(channels),
                Map.copyOf(checked.nameToId()),
                procedureDescriptors(procedures, checked.capacities()),
                Map.copyOf(procedureNameToId),
                effectivelyOnce,
                checkpoint,
                correctionWindowMs,
                beyondWindowPolicy);
    }

    private static List<ChannelDescriptor> channelDescriptors(List<ChannelEntry> channels) {
        List<ChannelDescriptor> descriptors = new ArrayList<>(channels.size());
        for (ChannelEntry entry : channels) {
            Optional<PluginSink> pluginSink = entry.pluginSinkName == null
                    ? Optional.empty()
                    : Optional.of(new PluginSink(entry.pluginSinkName, entry.pluginSinkSerializer));
            // Terminality is derived from the wiring, not from which method the
            // author called: a sink with nothing consuming behind it IS the
            // plugin-bound terminal. Deriving it here is what keeps
            // `sink(...)` one method and makes a sink declared alongside
            // consumers hybrid by construction rather than by ordering.
            ChannelKind kind =
                    entry.pluginSinkName != null && entry.consumers.isEmpty() ? ChannelKind.PLUGIN_BOUND : entry.kind;
            descriptors.add(new ChannelDescriptor(
                    entry.name,
                    kind,
                    entry.producer == null ? OptionalInt.empty() : OptionalInt.of(entry.producer),
                    List.copyOf(entry.consumers),
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

    private static List<ProcedureDescriptor> procedureDescriptors(List<ProcedureEntry> procedures, int[] capacities) {
        List<ProcedureDescriptor> descriptors = new ArrayList<>(procedures.size());
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry entry = procedures.get(i);
            descriptors.add(new ProcedureDescriptor(
                    entry.name,
                    entry.procedureClass,
                    entry.entityKeys,
                    entry.entitiesForOverridden,
                    entry.dynamic,
                    entry.inputChannelId == null ? OptionalInt.empty() : OptionalInt.of(entry.inputChannelId),
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
}
