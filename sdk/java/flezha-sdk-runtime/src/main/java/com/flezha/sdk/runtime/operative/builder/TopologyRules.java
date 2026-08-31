package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.CheckpointPolicy;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ChannelEntry;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.PendingEdge;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ProcedureEntry;
import com.flezha.sdk.state.EventTimeFrom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Every rule a stream topology is refused by, in the order
 * {@code StreamOperativeTopologyBuilder.build(String)} runs them. Each rule
 * appends to the builder's error list, which already carries whatever the
 * declaration calls accumulated; {@code build} throws on the whole list before
 * anything is materialized, so a refusal belongs here and nowhere later.
 */
final class TopologyRules {

    /**
     * What the rules resolve on the way: the per-operative channel-name index
     * and each procedure's mailbox capacity, both of which the materialization
     * needs and neither of which is worth deriving twice.
     */
    record Checked(List<String> errors, Map<String, Integer> nameToId, int[] capacities) {}

    private final List<ChannelEntry> channels;
    private final List<ProcedureEntry> procedures;
    private final List<PendingEdge> pending;
    private final boolean effectivelyOnce;
    private final CheckpointPolicy checkpoint;
    private final long correctionWindowMs;
    private final List<String> errors;

    TopologyRules(
            List<ChannelEntry> channels,
            List<ProcedureEntry> procedures,
            List<PendingEdge> pending,
            boolean effectivelyOnce,
            CheckpointPolicy checkpoint,
            long correctionWindowMs,
            List<String> errors) {
        this.channels = channels;
        this.procedures = procedures;
        this.pending = pending;
        this.effectivelyOnce = effectivelyOnce;
        this.checkpoint = checkpoint;
        this.correctionWindowMs = correctionWindowMs;
        this.errors = errors;
    }

    Checked check() {
        Map<String, Integer> nameToId = uniqueChannelNames();
        graphShape();
        danglingTransformChains();
        entityModeAndKeying();
        staticModeDeclarations();
        int[] capacities = mailboxCapacities();
        correctionWindowAndCheckpoint();
        channelWiring();
        return new Checked(errors, nameToId, capacities);
    }

    /**
     * One per-operative channel namespace: derived names (producer node
     * identity), {@code .named(...)} overrides, and entry names all collide
     * here. Checked at build, not declare — a {@code .named} override frees the
     * derived name a later declaration may claim.
     */
    private Map<String, Integer> uniqueChannelNames() {
        Map<String, Integer> nameToId = new LinkedHashMap<>();
        for (int id = 0; id < channels.size(); id++) {
            if (nameToId.put(channels.get(id).name, id) != null) {
                errors.add("channel `" + channels.get(id).name + "` declared more than once on this operative");
            }
        }
        return nameToId;
    }

    /**
     * Shared graph-shape rule ({@code sdk-graph-authoring}): one OR MORE entry
     * channels feeding the graph, every node reachable from some entry, no
     * cycles. A single operative may ingest several external sources (e.g. a
     * market pipeline reading a trade feed and an order-book feed on distinct
     * channels); the controller routes each frame to its entry by
     * {@code metadata["channel"]}, falling back to the first entry when unset.
     *
     * <p>The walk is {@link GraphShape} — one mechanism for this validation and
     * the workflow execution order. A leftover Kahn node is on a cycle;
     * unreachable is reported only when acyclic (a cycle already explains its
     * own unreachable nodes) and is defensive, since the stream-handle builder
     * only creates nodes by wiring from the entry.
     */
    private void graphShape() {
        if (procedures.isEmpty()) {
            return;
        }
        List<int[]> edges = new ArrayList<>();
        List<Integer> roots = new ArrayList<>();
        for (ChannelEntry ch : channels) {
            if (ch.kind == ChannelKind.ENTRY) {
                ch.consumers.forEach(e -> roots.add(e.procedure()));
            } else if (ch.producer != null) {
                for (ConsumerEdge consumer : ch.consumers) {
                    edges.add(new int[] {ch.producer, consumer.procedure()});
                }
            }
        }
        GraphShape.Order shape = GraphShape.order(procedures.size(), edges, roots);
        if (shape.onCycle() >= 0) {
            errors.add(StreamOperativeConfigurationException.cycle(procedures.get(shape.onCycle()).name)
                    .getMessage());
        } else if (shape.unreachable() >= 0) {
            errors.add(
                    StreamOperativeConfigurationException.unreachableProcedure(procedures.get(shape.unreachable()).name)
                            .getMessage());
        }
    }

    /**
     * A transform chain is stored on the consumer edge it terminates in, so one
     * that no {@code .into(...)} ever closed would silently do nothing. Only a
     * chain's tip can dangle — every earlier link was superseded.
     */
    private void danglingTransformChains() {
        for (PendingEdge chain : pending) {
            if (!chain.superseded && !chain.consumed) {
                errors.add(
                        StreamOperativeConfigurationException.danglingTransformChain(channels.get(chain.channel).name)
                                .getMessage());
            }
        }
    }

    /**
     * Entity mode is read off the WIRING: a procedure reached by an edge
     * carrying a key function is dynamic. Static keying routes by
     * {@code entitiesFor} and may name several Entities, edge keying names
     * exactly one, so declaring both is a contradiction — and because the key is
     * total, a dynamic procedure has no keyless path, which means every edge
     * into one must carry a key.
     */
    private void entityModeAndKeying() {
        for (ChannelEntry channel : channels) {
            for (ConsumerEdge edge : channel.consumers) {
                if (edge.key().isPresent()) {
                    procedures.get(edge.procedure()).dynamic = true;
                }
            }
        }
        for (ChannelEntry channel : channels) {
            for (ConsumerEdge edge : channel.consumers) {
                ProcedureEntry target = procedures.get(edge.procedure());
                if (edge.key().isPresent() && !target.entityKeys.isEmpty()) {
                    errors.add(StreamOperativeConfigurationException.keyByOnUnkeyableTarget(channel.name, target.name)
                            .getMessage());
                }
                if (edge.key().isEmpty() && target.dynamic) {
                    errors.add(StreamOperativeConfigurationException.unkeyedEdgeIntoDynamicProcedure(
                                    channel.name, target.name)
                            .getMessage());
                }
            }
        }
    }

    /** Validate each Procedure's static-mode declaration. */
    private void staticModeDeclarations() {
        for (ProcedureEntry procedure : procedures) {
            if (!procedure.entityKeys.isEmpty() && !procedure.entitiesForOverridden) {
                errors.add("procedure `" + procedure.name
                        + "` declares non-empty entityKeys() but does not override entitiesFor "
                        + "(set entitiesForOverridden() to return true)");
            }
            if (procedure.entitiesForOverridden && procedure.entityKeys.isEmpty()) {
                errors.add("procedure `" + procedure.name
                        + "` overrode entitiesFor — single-mode procedures have one implicit entity");
            }
        }
    }

    /**
     * Resolve mailbox capacity for each Procedure (Procedure override → env var
     * → default), accumulating any range errors into the errors list.
     */
    private int[] mailboxCapacities() {
        int[] resolved = new int[procedures.size()];
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry procedure = procedures.get(i);
            try {
                resolved[i] = MailboxCapacityResolver.resolve(
                        procedure.name, procedure.mailboxCapacityDeclared, procedure.mailboxCapacityOverridden);
            } catch (StreamOperativeConfigurationException e) {
                errors.add(e.getMessage());
                resolved[i] = MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY;
            }
        }
        return resolved;
    }

    /** What a correction window and a checkpoint granularity each require of the rest of the declaration. */
    private void correctionWindowAndCheckpoint() {
        if (correctionWindowMs > 0) {
            if (!effectivelyOnce) {
                errors.add(StreamOperativeConfigurationException.correctionWindowRequires("`effectivelyOnce()`")
                        .getMessage());
            }
            boolean anyEventTime = procedures.stream().anyMatch(o -> o.clockType == EventTimeFrom.class);
            if (!anyEventTime) {
                errors.add(StreamOperativeConfigurationException.correctionWindowRequires(
                                "an `EventTimeFrom` clock on at least one procedure")
                        .getMessage());
            }
            boolean allKeyed =
                    channels.stream().filter(c -> c.pluginSinkName != null).allMatch(c -> c.keyed);
            if (!allKeyed) {
                errors.add(StreamOperativeConfigurationException.correctionWindowRequires(
                                "every plugin-bound channel to be `.keyed()`")
                        .getMessage());
            }
        }

        if (checkpoint != null && !effectivelyOnce) {
            errors.add(StreamOperativeConfigurationException.checkpointAckWithoutTier()
                    .getMessage());
        }
        if (checkpoint != null && checkpoint.every() < 0) {
            errors.add(
                    StreamOperativeConfigurationException.checkpointEveryZero().getMessage());
        }
        if (checkpoint != null && checkpoint.outputsAtCheckpoint() && checkpoint.maxDelayMs() <= 0) {
            errors.add(
                    StreamOperativeConfigurationException.checkpointDelayZero().getMessage());
        }
    }

    /** The per-channel plugin / keyed / tier rules. */
    private void channelWiring() {
        for (ChannelEntry entry : channels) {
            boolean pluginWired = entry.pluginSinkName != null;
            // A key rides as a `flz.key` stamp on a plugin frame; a pure
            // inter-procedure channel has nowhere to carry it.
            if (entry.keyed && !pluginWired) {
                errors.add(StreamOperativeConfigurationException.keyedChannelWithoutPluginWire(entry.name)
                        .getMessage());
            }
            // The tier's replay re-emits outputs; only a keyed sink can absorb
            // that as an idempotent upsert rather than a duplicate row.
            if (effectivelyOnce && pluginWired && !entry.keyed) {
                errors.add(StreamOperativeConfigurationException.tierChannelNotKeyed(entry.name)
                        .getMessage());
            }
            // Promising effectively-once and asking to shed is a contradiction,
            // not a trade-off: a dropped input's offset is advanced past by a
            // later item's watermark. `correctionWindow` requires the tier
            // (checked above), so this arm covers it too.
            if (effectivelyOnce && entry.overflowPolicy != OverflowPolicy.DROP_NOTHING) {
                errors.add(
                        StreamOperativeConfigurationException.tierChannelSheds(entry.name, entry.overflowPolicy.name())
                                .getMessage());
            }
        }
    }
}
