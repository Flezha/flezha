package com.flezha.sdk.runtime.operative.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.Channel;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.EntryChannel;
import com.flezha.sdk.channel.Merge;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Implements the producer-first graph {@link ChannelBuilder} (and the
 * {@link EntryChannel} / {@link Stream} handles). Tracks declared channels
 * and node registrations, validates the graph at {@link #build(String)}
 * time, and produces a frozen {@link UnboundedOperativeTopology}.
 *
 * <p>Mirror of the Rust SDK's {@code channel/builder.rs} +
 * {@code operative/builder.rs}:
 * <ul>
 *   <li>A node is identified by its Procedure class unless an explicit node
 *       name is given at wiring time; two unnamed mentions of one class are
 *       the same node (fan-in), a named mention is its own node, and an
 *       identity claimed by two distinct nodes is a collision.</li>
 *   <li>A node's output channel is created lazily on first use of its stream
 *       handle and named by the node's identity; {@code .named(...)}
 *       overrides. Derived, overridden, and entry names share one namespace
 *       whose duplicate check runs at build time.</li>
 *   <li>Graph shape: one or more entry channels feeding the graph, every node
 *       reachable from some entry, no cycles. Fan-out and fan-in are both
 *       legal.</li>
 *   <li>Typed entries decode at the entry edge (passthrough {@code byte[]},
 *       UTF-8 {@code String}, Jackson JSON otherwise, or a custom parser); a
 *       failing parse on a parsed entry rides the channel's retry→DLQ
 *       ladder.</li>
 * </ul>
 *
 * <p>All failures accumulate and throw {@link UnboundedOperativeConfigurationException}
 * at build time. After {@link #build(String)} the topology is frozen: a
 * retained handle used afterwards throws {@link IllegalStateException}
 * naming the operative.
 */
public final class UnboundedOperativeTopologyBuilder implements ChannelBuilder {

    /** One shared Jackson mapper for every default codec (thread-safe once configured). */
    private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final List<ProcedureEntry> procedures = new ArrayList<>();
    private final Map<String, Integer> procedureNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();
    private boolean effectivelyOnce;
    private boolean checkpointAck;
    private long correctionWindowMs;
    private com.flezha.sdk.channel.BeyondWindowPolicy beyondWindowPolicy =
            com.flezha.sdk.channel.BeyondWindowPolicy.PROCESS_AS_ARRIVAL;

    /** Set at {@link #build(String)}: every mutating path refuses afterwards. */
    private boolean frozen;

    private String frozenFactoryId = "<unbuilt>";

    /**
     * Run the supplied {@link UnboundedOperative#channels(ChannelBuilder)} once, validate,
     * and freeze the result into a {@link UnboundedOperativeTopology}.
     *
     * @throws UnboundedOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static UnboundedOperativeTopology build(@Nonnull UnboundedOperative operative) {
        return build(operative.factoryId(), operative::channels);
    }

    /**
     * Run an authoring lambda against a fresh builder, validate, and freeze the
     * result into a {@link UnboundedOperativeTopology}. Shared by unbounded
     * {@link UnboundedOperative#channels} authoring and bounded
     * {@code procedures(...)} authoring — both declare Procedures / Channels /
     * Entities on the same {@link ChannelBuilder}.
     *
     * @throws UnboundedOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static UnboundedOperativeTopology build(
            @Nonnull String factoryId, @Nonnull java.util.function.Consumer<ChannelBuilder> authoring) {
        UnboundedOperativeTopologyBuilder b = new UnboundedOperativeTopologyBuilder();
        authoring.accept(b);
        return b.build(factoryId);
    }

    @Override
    @Nonnull
    public <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name) {
        checkMutable();
        boolean parsed = type != byte[].class && type != String.class;
        return declareEntry(type, name, defaultEntryParser(type), parsed);
    }

    @Override
    @Nonnull
    public <T> EntryChannel<T> input(
            @Nonnull Class<T> type, @Nonnull String name, @Nonnull Function<byte[], T> parser) {
        checkMutable();
        return declareEntry(type, name, parser::apply, true);
    }

    private <T> EntryChannel<T> declareEntry(
            Class<T> type, String name, Function<byte[], Object> parser, boolean parsed) {
        int id = declareChannel(type, name, ChannelKind.ENTRY);
        ChannelEntry entry = channels.get(id);
        entry.entryParser = parser;
        entry.entryParsed = parsed;
        return new EntryChannelImpl<>(id, name);
    }

    @Override
    @Nonnull
    public <I> Merge<I> merge(@Nonnull Class<I> type) {
        checkMutable();
        return new MergeImpl<>(type);
    }

    @Override
    public void effectivelyOnce() {
        checkMutable();
        this.effectivelyOnce = true;
    }

    @Override
    public void checkpointAck() {
        checkMutable();
        this.checkpointAck = true;
    }

    @Override
    public void correctionWindow(long windowMs) {
        checkMutable();
        this.correctionWindowMs = windowMs;
    }

    @Override
    public void beyondWindowPolicy(@Nonnull com.flezha.sdk.channel.BeyondWindowPolicy policy) {
        checkMutable();
        this.beyondWindowPolicy = policy;
    }

    @Nonnull
    public UnboundedOperativeTopology build(@Nonnull String factoryId) {
        // Freeze first: any retained handle used from here on throws naming
        // the operative, whatever build's own outcome.
        frozenFactoryId = factoryId;
        frozen = true;

        // One per-operative channel namespace: derived names (producer node
        // identity), `.named(...)` overrides, and entry names all collide here.
        // Checked at build, not declare — a `.named` override frees the
        // derived name a later declaration may claim.
        Map<String, Integer> nameToId = new LinkedHashMap<>();
        for (int id = 0; id < channels.size(); id++) {
            if (nameToId.put(channels.get(id).name, id) != null) {
                errors.add("channel `" + channels.get(id).name + "` declared more than once on this operative");
            }
        }

        // Shared graph-shape rule (`sdk-graph-authoring`): one or more entry
        // channels feeding the graph, every node reachable from some entry, no
        // cycles. Runs in the VALIDATION phase — the accumulated errors throw
        // before the descriptor-materialization loop, so a refusal added after
        // that point would never fire.
        if (!procedures.isEmpty()) {
            validateGraphShape();
        }

        // Validate each Procedure's static-mode / dynamic-mode declaration.
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

        // Resolve mailbox capacity for each Procedure (Procedure override → env var →
        // default), accumulating any range errors into the errors list.
        int[] resolvedCapacities = new int[procedures.size()];
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry procedure = procedures.get(i);
            try {
                resolvedCapacities[i] = MailboxCapacityResolver.resolve(
                        procedure.name, procedure.mailboxCapacityDeclared, procedure.mailboxCapacityOverridden);
            } catch (UnboundedOperativeConfigurationException e) {
                errors.add(e.getMessage());
                resolvedCapacities[i] = MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY;
            }
        }

        if (correctionWindowMs > 0) {
            if (!effectivelyOnce) {
                errors.add(UnboundedOperativeConfigurationException.correctionWindowRequires("`effectivelyOnce()`")
                        .getMessage());
            }
            boolean anyEventTime =
                    procedures.stream().anyMatch(o -> o.clockType == com.flezha.sdk.state.EventTimeFrom.class);
            if (!anyEventTime) {
                errors.add(UnboundedOperativeConfigurationException.correctionWindowRequires(
                                "an `EventTimeFrom` clock on at least one procedure")
                        .getMessage());
            }
            boolean allKeyed = channels.stream()
                    .filter(c -> c.pluginName != null || c.pluginSinkName != null)
                    .allMatch(c -> c.keyed);
            if (!allKeyed) {
                errors.add(UnboundedOperativeConfigurationException.correctionWindowRequires(
                                "every plugin-bound channel to be `.keyed()`")
                        .getMessage());
            }
        }

        if (checkpointAck && !effectivelyOnce) {
            errors.add(UnboundedOperativeConfigurationException.checkpointAckWithoutTier()
                    .getMessage());
        }

        for (ChannelEntry entry : channels) {
            boolean pluginWired = entry.pluginName != null || entry.pluginSinkName != null;
            // A key rides as a `flz.key` stamp on a plugin frame; a pure
            // inter-procedure channel has nowhere to carry it.
            if (entry.keyed && !pluginWired) {
                errors.add(UnboundedOperativeConfigurationException.keyedChannelWithoutPluginWire(entry.name)
                        .getMessage());
            }
            // The tier's replay re-emits outputs; only a keyed sink can absorb
            // that as an idempotent upsert rather than a duplicate row.
            if (effectivelyOnce && pluginWired && !entry.keyed) {
                errors.add(UnboundedOperativeConfigurationException.tierChannelNotKeyed(entry.name)
                        .getMessage());
            }
        }

        if (!errors.isEmpty()) {
            throw new UnboundedOperativeConfigurationException(List.copyOf(errors));
        }

        // Materialize ChannelDescriptors.
        List<ChannelDescriptor> channelDescriptors = new ArrayList<>(channels.size());
        for (ChannelEntry entry : channels) {
            Optional<PluginSink> pluginSink = entry.pluginSinkName == null
                    ? Optional.empty()
                    : Optional.of(new PluginSink(entry.pluginSinkName, entry.pluginSinkSerializer));
            channelDescriptors.add(new ChannelDescriptor(
                    entry.name,
                    entry.kind,
                    entry.producer == null ? OptionalInt.empty() : OptionalInt.of(entry.producer),
                    List.copyOf(entry.consumers),
                    Optional.ofNullable(entry.pluginName),
                    entry.payloadType,
                    Optional.ofNullable(entry.entryParser),
                    entry.entryParsed,
                    Optional.ofNullable(entry.pluginSerializer),
                    pluginSink,
                    entry.overflowPolicy,
                    new com.flezha.sdk.channel.RetryPolicy(entry.retryMaxAttempts, entry.retryBackoffBaseMs),
                    entry.keyed,
                    entry.interProcEncoder == null
                            ? Optional.empty()
                            : Optional.of(new ChannelDescriptor.InterProcCodec(
                                    entry.interProcEncoder, entry.interProcDecoder))));
        }

        // Materialize ProcedureDescriptors.
        List<ProcedureDescriptor> procedureDescriptors = new ArrayList<>(procedures.size());
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry entry = procedures.get(i);
            procedureDescriptors.add(new ProcedureDescriptor(
                    entry.name,
                    entry.procedureClass,
                    entry.entityKeys,
                    entry.entitiesForOverridden,
                    entry.entityKeyOverridden,
                    entry.inputChannelId == null ? OptionalInt.empty() : OptionalInt.of(entry.inputChannelId),
                    entry.outputChannelId == null ? OptionalInt.empty() : OptionalInt.of(entry.outputChannelId),
                    entry.inputType,
                    entry.outputType,
                    entry.changeType,
                    entry.clockType,
                    entry.configType,
                    entry.kryoTypes,
                    entry.schemaVersion,
                    entry.compactAfterBytes,
                    resolvedCapacities[i],
                    entry.mailboxCapacityOverridden,
                    entry.compensateOverridden,
                    entry.entityIdleTtl));
        }

        return new UnboundedOperativeTopology(
                factoryId,
                channelDescriptors,
                Map.copyOf(nameToId),
                procedureDescriptors,
                Map.copyOf(procedureNameToId),
                effectivelyOnce,
                checkpointAck,
                correctionWindowMs,
                beyondWindowPolicy);
    }

    // ---- Graph-shape validation ---------------------------------------------

    private void validateGraphShape() {
        // One OR MORE entry channels feed the graph — a single operative may
        // ingest several external sources (e.g. a market pipeline reading a
        // trade feed and an order-book feed on distinct channels); the
        // controller routes each frame to its entry by metadata["channel"],
        // falling back to the first entry when unset. Every node must be
        // reachable from some entry, and the graph must be acyclic.
        //
        // The shared graph-shape walk (GraphShape, one mechanism for this
        // validation and the bounded execution order): a leftover Kahn node
        // is on a cycle; unreachable is reported only when acyclic (a cycle
        // already explains its own unreachable nodes) and is defensive — the
        // stream-handle builder only creates nodes by wiring from the entry.
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
            errors.add(UnboundedOperativeConfigurationException.cycle(procedures.get(shape.onCycle()).name)
                    .getMessage());
        } else if (shape.unreachable() >= 0) {
            errors.add(UnboundedOperativeConfigurationException.unreachableProcedure(
                            procedures.get(shape.unreachable()).name)
                    .getMessage());
        }
    }

    // ---- Internals ----------------------------------------------------------

    private void checkMutable() {
        if (frozen) {
            throw new IllegalStateException("operative `" + frozenFactoryId
                    + "`: the topology is frozen; stream handles are only usable inside channels()/procedures()");
        }
    }

    private int declareChannel(Class<?> payloadType, String name, ChannelKind kind) {
        int id = channels.size();
        ChannelEntry entry = new ChannelEntry();
        entry.name = name;
        entry.kind = kind;
        entry.payloadType = payloadType;
        entry.overflowPolicy = defaultPolicyForKind(kind);
        channels.add(entry);
        return id;
    }

    /**
     * Resolve or create the node for {@code procedureClass} under the
     * node-identity rule: two unnamed mentions of one class are the same
     * node; an explicit node name creates/refers to a distinct node whose
     * identity string replaces the procedure's {@code name()}. An identity
     * claimed by a <i>different</i> declaration is a collision.
     */
    private int registerNode(Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass, String nodeName) {
        Procedure<?, ?, ?, ?, ?, ?> probe = instantiate(procedureClass);
        String identity = nodeName != null ? nodeName : probe.name();
        boolean named = nodeName != null;
        Integer existing = procedureNameToId.get(identity);
        if (existing != null) {
            ProcedureEntry existingEntry = procedures.get(existing);
            if (!existingEntry.procedureClass.equals(procedureClass) || existingEntry.named != named) {
                errors.add(UnboundedOperativeConfigurationException.nodeIdentityCollision(identity)
                        .getMessage());
            }
            return existing;
        }
        int id = procedures.size();
        ProcedureEntry entry = new ProcedureEntry();
        entry.procedureClass = procedureClass;
        entry.name = identity;
        entry.named = named;
        entry.entityKeys = List.copyOf(probe.entityKeys());
        entry.entitiesForOverridden = probe.entitiesForOverridden();
        entry.entityKeyOverridden = probe.entityKeyOverridden();
        entry.inputType = probe.inputType();
        entry.outputType = probe.outputType();
        entry.changeType = probe.changeType();
        entry.clockType = probe.clockType();
        entry.configType = probe.configType();
        entry.kryoTypes = List.copyOf(probe.kryoTypes());
        entry.schemaVersion = probe.schemaVersion();
        entry.compactAfterBytes = probe.compactAfterBytes();
        entry.mailboxCapacityDeclared = probe.mailboxCapacity();
        entry.mailboxCapacityOverridden = probe.mailboxCapacityOverridden();
        entry.compensateOverridden = probe.compensateOverridden();
        entry.entityIdleTtl = probe.entityIdleTtl();
        // Every clock in the sealed hierarchy is wired. What Rust rejects at
        // compile time (a `TimeExtractor<Input>` bound, an unservable clock) is
        // rejected here instead — erasure precludes the compile-time form, so
        // the same defect is caught at registration.
        if (entry.clockType == com.flezha.sdk.state.EventTimeFrom.class && !probe.eventTimeOverridden()) {
            errors.add(UnboundedOperativeConfigurationException.eventTimeExtractorMissing(identity)
                    .getMessage());
        }
        if (entry.clockType == com.flezha.sdk.state.LogicalClock.class
                && entry.changeType == com.flezha.sdk.state.NoChange.class) {
            errors.add(UnboundedOperativeConfigurationException.unservableClock(identity)
                    .getMessage());
        }
        procedures.add(entry);
        procedureNameToId.put(identity, id);
        return id;
    }

    private Procedure<?, ?, ?, ?, ?, ?> instantiate(Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass) {
        try {
            return procedureClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            errors.add("procedure class `" + procedureClass.getName() + "` must have a public no-arg constructor: "
                    + e.toString());
            // Return a placeholder so the rest of the build can collect more
            // errors before failing; the exception throws at build() time.
            return UNINSTANTIABLE;
        }
    }

    private static final Procedure<
                    Object, Object, Object, com.flezha.sdk.state.NoChange, com.flezha.sdk.state.WallClock, Void>
            UNINSTANTIABLE = new Procedure<>() {
                @Override
                public String name() {
                    return "<uninstantiable>";
                }

                @Override
                public Class<Object> inputType() {
                    return Object.class;
                }

                @Override
                public Class<Object> outputType() {
                    return Object.class;
                }

                @Override
                public Class<com.flezha.sdk.state.NoChange> changeType() {
                    return com.flezha.sdk.state.NoChange.class;
                }

                @Override
                public Class<com.flezha.sdk.state.WallClock> clockType() {
                    return com.flezha.sdk.state.WallClock.class;
                }

                @Override
                public Class<Void> configType() {
                    return Void.class;
                }

                @Override
                public Object initState(
                        Optional<String> entityKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
                    throw new UnsupportedOperationException("uninstantiable procedure");
                }

                @Override
                public void run(
                        Object input,
                        com.flezha.sdk.state.StateHandle<Object, com.flezha.sdk.state.NoChange> state,
                        com.flezha.sdk.procedure.ProcedureCtx ctx) {
                    throw new UnsupportedOperationException("uninstantiable procedure");
                }
            };

    /**
     * Wire {@code channelId} into a consumer node (created/resolved by
     * identity). Returns the consumer's node id so its handle can continue
     * the chain.
     */
    private int wireConsumer(
            int channelId, Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass, String nodeName) {
        return wireConsumer(channelId, procedureClass, nodeName, null, null);
    }

    /**
     * Wire {@code channelId} into a consumer node. A merge subscription passes
     * the declared merged input type and the mapping that lifts this channel's
     * payload into it; what Rust checks at the {@code .into::<Proc>()} call site
     * with an {@code Input = I} bound is checked here instead — erasure
     * precludes the compile-time form, so the same defect is caught at
     * registration.
     */
    private int wireConsumer(
            int channelId,
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            String nodeName,
            Class<?> mergeInputType,
            Function<Object, Object> map) {
        int procedureId = registerNode(procedureClass, nodeName);
        ProcedureEntry procedure = procedures.get(procedureId);
        ChannelEntry channel = channels.get(channelId);
        // A merged subscription's payload is the mapping's result, not the
        // channel's — that is exactly what the merge exists to allow.
        Class<?> deliveredType = mergeInputType != null ? mergeInputType : channel.payloadType;
        if (!deliveredType.equals(procedure.inputType)) {
            String producedBy = mergeInputType != null
                    ? "merge maps into"
                    : channel.producer == null
                            ? "payload type"
                            : "producer procedure `" + procedures.get(channel.producer).name + "` outputType() =";
            errors.add("channel `" + channel.name + "`: " + producedBy + " "
                    + describe(deliveredType)
                    + " does not match consumer procedure `" + procedure.name
                    + "` inputType() = " + describe(procedure.inputType));
        }
        // Re-declaring an existing mapping-free edge is the fan-in / repeated
        // `.into` case and stays a no-op. Re-declaring one where either side
        // carries a merge mapping is refused rather than silently dropped — the
        // edge would otherwise keep whichever mapping was declared first and
        // deliver the wrong value forever.
        Optional<ConsumerEdge> existing = channel.consumers.stream()
                .filter(e -> e.procedure() == procedureId)
                .findFirst();
        if (existing.isEmpty()) {
            channel.consumers.add(new ConsumerEdge(procedureId, Optional.ofNullable(map)));
        } else if (existing.get().map().isPresent() || map != null) {
            errors.add(UnboundedOperativeConfigurationException.mergeSourceAlreadyWired(channel.name, procedure.name)
                    .getMessage());
            return procedureId;
        }
        // Cache input channel id on the procedure. A fan-in procedure keeps its
        // first input channel here; every incoming channel stays reachable via
        // the channel descriptors, and the dispatcher routes via the
        // channel-side mapping.
        if (procedure.inputChannelId == null) {
            procedure.inputChannelId = channelId;
        }
        return procedureId;
    }

    /**
     * The node's output channel, created lazily on first use so a leaf node
     * never carries a phantom channel. Named by the node's identity until a
     * {@code .named(...)} override; payload type = the node's declared
     * {@code outputType()}.
     */
    private int ensureOutputChannel(int procedureId) {
        ProcedureEntry procedure = procedures.get(procedureId);
        if (procedure.outputChannelId != null) {
            return procedure.outputChannelId;
        }
        int id = declareChannel(procedure.outputType, procedure.name, ChannelKind.INTER_PROCEDURE);
        channels.get(id).producer = procedureId;
        procedure.outputChannelId = id;
        return id;
    }

    private static String describe(Class<?> c) {
        return c == byte[].class ? "byte[]" : c.getName();
    }

    // ---- Phantom builder records -------------------------------------------

    /** Mutable internal record for a channel declaration. */
    static final class ChannelEntry {
        String name;
        ChannelKind kind;
        Class<?> payloadType;
        Integer producer;
        final List<ConsumerEdge> consumers = new ArrayList<>();
        String pluginName;
        Function<Object, byte[]> pluginSerializer;
        String pluginSinkName;
        Function<Object, byte[]> pluginSinkSerializer;
        OverflowPolicy overflowPolicy;
        int retryMaxAttempts = com.flezha.sdk.channel.RetryPolicy.DEFAULT.maxAttempts();
        long retryBackoffBaseMs = com.flezha.sdk.channel.RetryPolicy.DEFAULT.backoffBaseMs();
        boolean keyed;
        Function<Object, byte[]> interProcEncoder;
        Function<byte[], Object> interProcDecoder;
        /** Entry channels only: wire bytes → typed payload. */
        Function<byte[], Object> entryParser;
        /**
         * Entry channels whose parser is a real one (typed JSON or a custom
         * {@code input(type, name, parser)}). A failing parse rides the
         * channel's retry→DLQ ladder; {@code byte[]}/{@code String}
         * passthrough entries keep the pre-change drop-and-log behaviour.
         */
        boolean entryParsed;
    }

    /**
     * Apply a {@code .retryMax(...)} declaration, rejecting a zero budget at
     * registration time like every other declaration error.
     */
    private void setRetryMax(int channelId, int maxAttempts) {
        ChannelEntry entry = channels.get(channelId);
        if (maxAttempts <= 0) {
            errors.add("channel `" + entry.name + "` declared retryMax = " + maxAttempts
                    + "; at least one attempt is required (throw ProcedureException.NonRetryable"
                    + " to park without retries)");
            return;
        }
        entry.retryMaxAttempts = maxAttempts;
    }

    private static OverflowPolicy defaultPolicyForKind(ChannelKind kind) {
        return kind == ChannelKind.ENTRY ? OverflowPolicy.DROP_NEWEST : OverflowPolicy.BLOCK;
    }

    /** Mutable internal record for a node registration. */
    static final class ProcedureEntry {
        /**
         * Node identity: the procedure's {@code name()}, or the explicit node
         * name given at wiring time. Feeds everything {@code name()} feeds —
         * state directories, journal entries, the derived channel name,
         * diagnostics.
         */
        String name;

        /** Whether this node was created with an explicit node name. */
        boolean named;

        Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass;
        List<String> entityKeys;
        boolean entitiesForOverridden;
        boolean entityKeyOverridden;
        Class<?> inputType;
        Class<?> outputType;
        Class<? extends com.flezha.sdk.state.Change> changeType;
        Class<? extends com.flezha.sdk.state.Clock> clockType;
        Class<?> configType;
        List<Class<?>> kryoTypes;
        int schemaVersion;
        long compactAfterBytes;
        Integer inputChannelId;
        Integer outputChannelId;
        int mailboxCapacityDeclared;
        boolean mailboxCapacityOverridden;
        boolean compensateOverridden;
        java.util.Optional<java.time.Duration> entityIdleTtl = java.util.Optional.empty();
    }

    // ---- Builder handle types ----------------------------------------------

    private final class EntryChannelImpl<T> implements EntryChannel<T> {
        private final int channelId;
        private final String name;

        EntryChannelImpl(int channelId, String name) {
            this.channelId = channelId;
            this.name = name;
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass) {
            return wire(procedureClass, null);
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(
                @Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, @Nonnull String nodeName) {
            return wire(procedureClass, nodeName);
        }

        private <O> Stream<O> wire(Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, String nodeName) {
            checkMutable();
            int node = wireConsumer(channelId, procedureClass, nodeName);
            return new StreamImpl<>(node);
        }

        @Override
        @Nonnull
        public EntryChannel<T> onFull(@Nonnull OverflowPolicy policy) {
            checkMutable();
            channels.get(channelId).overflowPolicy = policy;
            return this;
        }

        @Override
        @Nonnull
        public EntryChannel<T> retryMax(int maxAttempts) {
            checkMutable();
            setRetryMax(channelId, maxAttempts);
            return this;
        }

        @Override
        @Nonnull
        public EntryChannel<T> retryBackoffMs(long baseMs) {
            checkMutable();
            channels.get(channelId).retryBackoffBaseMs = baseMs;
            return this;
        }

        @Override
        public String toString() {
            return "EntryChannel(" + name + ")";
        }
    }

    /**
     * A merged subscription under construction: the source channels declared
     * so far, each with the mapping that lifts its payload into {@code I}.
     */
    private final class MergeImpl<I> implements Merge<I> {
        private final Class<I> mergedType;
        private final List<MergeSourceEntry> sources = new ArrayList<>();
        /**
         * Every {@code from} the author wrote, including ones already refused.
         * The arity check reads this so a duplicated source is reported once,
         * as a duplicate — not also as a merge that declared too few.
         */
        private int declared;

        MergeImpl(Class<I> mergedType) {
            this.mergedType = mergedType;
        }

        @Override
        @Nonnull
        public <S> Merge<I> from(@Nonnull EntryChannel<S> source, @Nonnull Function<S, I> map) {
            return addSource(sourceChannelId(source), map);
        }

        @Override
        @Nonnull
        public <S> Merge<I> from(@Nonnull Stream<S> source, @Nonnull Function<S, I> map) {
            return addSource(sourceChannelId(source), map);
        }

        /**
         * The channel a merge source subscribes on, whichever handle kind it
         * is — the erasure-side stand-in for Rust's {@code MergeSource} trait,
         * which the two public {@code from} overloads exist to cover.
         */
        private int sourceChannelId(Object source) {
            if (source instanceof UnboundedOperativeTopologyBuilder.EntryChannelImpl<?> entry) {
                return entry.channelId;
            }
            if (source instanceof UnboundedOperativeTopologyBuilder.StreamImpl<?> stream) {
                return stream.channelId();
            }
            throw new IllegalArgumentException("merge source was not declared on this operative's builder");
        }

        @SuppressWarnings("unchecked")
        private <S> Merge<I> addSource(int channelId, Function<S, I> map) {
            checkMutable();
            declared++;
            if (sources.stream().anyMatch(src -> src.channelId() == channelId)) {
                errors.add(UnboundedOperativeConfigurationException.duplicateMergeSource(channels.get(channelId).name)
                        .getMessage());
                return this;
            }
            sources.add(new MergeSourceEntry(channelId, value -> map.apply((S) value)));
            return this;
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(@Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure) {
            return wire(procedure, null);
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(
                @Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure, @Nonnull String nodeName) {
            return wire(procedure, nodeName);
        }

        private <O> Stream<O> wire(Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedureClass, String nodeName) {
            checkMutable();
            // Register the consumer before its sources — as Rust's `Merge::wire`
            // does — so the arity refusal can name it even with no source to
            // wire it. `registerNode` resolves by identity, so wiring below
            // returns this same node.
            int node = registerNode(procedureClass, nodeName);
            if (declared < 2) {
                errors.add(
                        UnboundedOperativeConfigurationException.mergeTooFewSources(procedures.get(node).name, declared)
                                .getMessage());
            }
            for (MergeSourceEntry source : sources) {
                wireConsumer(source.channelId(), procedureClass, nodeName, mergedType, source.map());
            }
            return new StreamImpl<>(node);
        }
    }

    /** One declared merge source: the channel and the mapping off it. */
    private record MergeSourceEntry(int channelId, Function<Object, Object> map) {}

    /**
     * The procedure's output stream handle: a node id into the enclosing
     * builder. The channel out of the node is created lazily on first use.
     */
    private final class StreamImpl<T> extends Stream<T> {
        private final int node;

        StreamImpl(int node) {
            this.node = node;
        }

        private int channelId() {
            return ensureOutputChannel(node);
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(@Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass) {
            return wire(procedureClass, null);
        }

        @Override
        @Nonnull
        public <O> Stream<O> into(
                @Nonnull Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, @Nonnull String nodeName) {
            return wire(procedureClass, nodeName);
        }

        private <O> Stream<O> wire(Class<? extends Procedure<T, ?, O, ?, ?, ?>> procedureClass, String nodeName) {
            checkMutable();
            int consumer = wireConsumer(channelId(), procedureClass, nodeName);
            return new StreamImpl<>(consumer);
        }

        @Override
        @Nonnull
        protected Stream<T> branchArms(@Nonnull List<java.util.function.Consumer<Stream<T>>> arms) {
            checkMutable();
            if (arms.size() < 2) {
                errors.add(UnboundedOperativeConfigurationException.branchTooFewArms(arms.size())
                        .getMessage());
            }
            for (java.util.function.Consumer<Stream<T>> arm : arms) {
                arm.accept(this);
            }
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> named(@Nonnull String name) {
            checkMutable();
            channels.get(channelId()).name = name;
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> keyed() {
            checkMutable();
            channels.get(channelId()).keyed = true;
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> codec() {
            // Reuse the SDK's default JSON serialization — the same Jackson
            // mapper a plugin-bound channel falls back to.
            return codecWith(defaultSerializer(payloadType()), defaultDeserializer(payloadType()));
        }

        @Override
        @Nonnull
        public Stream<T> codecWith(@Nonnull Function<T, byte[]> encoder, @Nonnull Function<byte[], T> decoder) {
            checkMutable();
            ChannelEntry entry = channels.get(channelId());
            entry.interProcEncoder = value -> encoder.apply(cast(value));
            entry.interProcDecoder = bytes -> decoder.apply(bytes);
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> onFull(@Nonnull OverflowPolicy policy) {
            checkMutable();
            channels.get(channelId()).overflowPolicy = policy;
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> retryMax(int maxAttempts) {
            checkMutable();
            setRetryMax(channelId(), maxAttempts);
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> retryBackoffMs(long baseMs) {
            checkMutable();
            channels.get(channelId()).retryBackoffBaseMs = baseMs;
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> alsoOutput(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer) {
            checkMutable();
            ChannelEntry entry = channels.get(channelId());
            if (entry.pluginSinkName != null) {
                errors.add(UnboundedOperativeConfigurationException.duplicatePluginSink(entry.name)
                        .getMessage());
                return this;
            }
            entry.pluginSinkName = pluginName;
            entry.pluginSinkSerializer = value -> serializer.apply(cast(value));
            return this;
        }

        @Override
        @Nonnull
        public Stream<T> alsoOutput(@Nonnull String pluginName) {
            return alsoOutput(pluginName, defaultSerializer(payloadType()));
        }

        @Override
        @Nonnull
        public Channel<T> output(@Nonnull String pluginName) {
            return output(pluginName, defaultSerializer(payloadType()));
        }

        @Override
        @Nonnull
        public Channel<T> output(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer) {
            checkMutable();
            int cid = channelId();
            ChannelEntry entry = channels.get(cid);
            entry.kind = ChannelKind.PLUGIN_BOUND;
            entry.pluginName = pluginName;
            entry.pluginSerializer = value -> serializer.apply(cast(value));
            return new Channel<>(cid, payloadType());
        }

        /** The producer node's declared output type: this stream's payload type. */
        @SuppressWarnings("unchecked")
        private Class<T> payloadType() {
            return (Class<T>) procedures.get(node).outputType;
        }

        @SuppressWarnings("unchecked")
        private T cast(Object value) {
            return (T) value;
        }
    }

    /**
     * The default entry decode for {@code T}: passthrough for {@code byte[]},
     * UTF-8 for {@code String} (both byte-identical to the pre-typed-entry
     * contract), the SDK's default JSON codec otherwise.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Function<byte[], Object> defaultEntryParser(Class<?> payloadType) {
        if (payloadType == byte[].class) {
            return bytes -> bytes;
        }
        if (payloadType == String.class) {
            return bytes -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return bytes -> {
            try {
                return DEFAULT_MAPPER.readValue(bytes, (Class) payloadType);
            } catch (Exception e) {
                throw new RuntimeException("entry channel: JSON deserialization failed: " + e.getMessage(), e);
            }
        };
    }

    /** Mirror of {@link #defaultSerializer}: the decode half of a default {@code codec()}. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Function<byte[], T> defaultDeserializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<byte[], String>) (b -> new String(b, java.nio.charset.StandardCharsets.UTF_8));
        }
        return bytes -> {
            try {
                return DEFAULT_MAPPER.readValue(bytes, payloadType);
            } catch (Exception e) {
                throw new RuntimeException("inter-proc codec (Jackson) decode failed: " + e.getMessage(), e);
            }
        };
    }

    /**
     * Default plugin-bound serializer, shared by {@code output(...)} and
     * {@code alsoOutput(...)} so the two produce identical bytes for the same
     * value. {@code byte[]} is passthrough; {@code String} is UTF-8 encoded;
     * everything else goes through Jackson JSON.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Function<T, byte[]> defaultSerializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<String, byte[]>) (s -> s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return value -> {
            try {
                return DEFAULT_MAPPER.writeValueAsBytes(value);
            } catch (Exception e) {
                throw new RuntimeException("plugin serializer (Jackson) failed: " + e.getMessage(), e);
            }
        };
    }
}
