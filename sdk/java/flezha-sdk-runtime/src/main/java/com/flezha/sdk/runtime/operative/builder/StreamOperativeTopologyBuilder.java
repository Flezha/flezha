package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.EntryChannel;
import com.flezha.sdk.channel.Merge;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.channel.RoutedDocument;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.runtime.operative.CheckpointPolicy;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.EdgeYield;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Clock;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.LogicalClock;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/**
 * Implements the producer-first graph {@link ChannelBuilder} (and the
 * {@link EntryChannel} / {@link Stream} handles). Tracks declared channels
 * and node registrations, validates the graph at {@link #build(String)}
 * time, and produces a frozen {@link StreamOperativeTopology}.
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
 * <p>All failures accumulate and throw {@link StreamOperativeConfigurationException}
 * at build time. After {@link #build(String)} the topology is frozen: a
 * retained handle used afterwards throws {@link IllegalStateException}
 * naming the operative.
 */
public final class StreamOperativeTopologyBuilder implements ChannelBuilder {

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final List<ProcedureEntry> procedures = new ArrayList<>();
    /**
     * Transform/key chains under construction, indexed by the id the stream
     * handles carry. A chain neither superseded nor consumed is dangling.
     */
    private final List<PendingEdge> pending = new ArrayList<>();

    private final Map<String, Integer> procedureNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();
    private boolean effectivelyOnce;
    /** {@code null} until {@code checkpointAck()} (or an option implying it). */
    private CheckpointPolicy checkpoint;

    private long correctionWindowMs;
    private BeyondWindowPolicy beyondWindowPolicy = BeyondWindowPolicy.PROCESS_AS_ARRIVAL;

    /** Set at {@link #build(String)}: every mutating path refuses afterwards. */
    private boolean frozen;

    private String frozenFactoryId = "<unbuilt>";

    /**
     * Run the supplied {@link StreamOperative#channels(ChannelBuilder)} once, validate,
     * and freeze the result into a {@link StreamOperativeTopology}.
     *
     * @throws StreamOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static StreamOperativeTopology build(@Nonnull StreamOperative operative) {
        return build(operative.factoryId(), operative::channels);
    }

    /**
     * Run an authoring lambda against a fresh builder, validate, and freeze the
     * result into a {@link StreamOperativeTopology}. Shared by stream
     * {@link StreamOperative#channels} authoring and workflow
     * {@code procedures(...)} authoring — both declare Procedures / Channels /
     * Entities on the same {@link ChannelBuilder}.
     *
     * @throws StreamOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static StreamOperativeTopology build(
            @Nonnull String factoryId, @Nonnull Consumer<ChannelBuilder> authoring) {
        StreamOperativeTopologyBuilder b = new StreamOperativeTopologyBuilder();
        authoring.accept(b);
        return b.build(factoryId);
    }

    @Override
    @Nonnull
    public <T> EntryChannel<T> input(@Nonnull Class<T> type, @Nonnull String name) {
        checkMutable();
        boolean parsed = type != byte[].class && type != String.class;
        return declareEntry(type, name, PayloadCodecs.defaultEntryParser(type), parsed);
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
        return new EntryChannelImpl<>(this, id, name, null, type);
    }

    @Override
    @Nonnull
    public <I> Merge<I> merge(@Nonnull Class<I> type) {
        checkMutable();
        return new MergeImpl<>(this, type);
    }

    @Override
    @Nonnull
    public <D, T> Stream<RoutedDocument<D>> routeLatest(
            @Nonnull Class<D> documentType,
            @Nonnull EdgeStream<D> documents,
            @Nonnull EdgeStream<T> traffic,
            @Nonnull Function<T, String> keyFn) {
        return wireRouteLatest(documentType, documents, traffic, keyFn, null);
    }

    @Override
    @Nonnull
    public <D, T> Stream<RoutedDocument<D>> routeLatest(
            @Nonnull Class<D> documentType,
            @Nonnull EdgeStream<D> documents,
            @Nonnull EdgeStream<T> traffic,
            @Nonnull Function<T, String> keyFn,
            @Nonnull String nodeName) {
        return wireRouteLatest(documentType, documents, traffic, keyFn, nodeName);
    }

    /**
     * The expansion both {@code routeLatest} spellings share: the key function
     * is an edge transform on the traffic stream (so the traffic payload is
     * replaced by its key and never reaches the router), and the two streams are
     * one merge into the packaged procedure.
     */
    @SuppressWarnings("unchecked")
    private <D, T> Stream<RoutedDocument<D>> wireRouteLatest(
            Class<D> documentType,
            EdgeStream<D> documents,
            EdgeStream<T> traffic,
            Function<T, String> keyFn,
            String nodeName) {
        checkMutable();
        // Resolve the node before wiring — `registerNode` is by identity, so the
        // merge below returns this same node — and register the document's own
        // class with its state codec: the router retains the document in
        // journaled state but is generic over its type, so only the wiring can
        // name it. That is what the `documentType` token is for.
        registerRouterDocumentType(registerNode(LatestDocumentRouter.class, nodeName), documentType);
        EdgeStream<String> keys = traffic.map(String.class, keyFn);
        MergeImpl<LatestDocumentRouter.Input> merge = new MergeImpl<>(this, LatestDocumentRouter.Input.class);
        merge.from(documents, LatestDocumentRouter.Input::ofDocument);
        merge.from(keys, LatestDocumentRouter.Input::ofTrafficKey);
        return (Stream<RoutedDocument<D>>) (Stream<?>) merge.wire(LatestDocumentRouter.class, nodeName);
    }

    private void registerRouterDocumentType(int node, Class<?> documentType) {
        ProcedureEntry entry = procedures.get(node);
        if (entry.kryoTypes.contains(documentType)) {
            return;
        }
        List<Class<?>> types = new ArrayList<>(entry.kryoTypes);
        types.add(documentType);
        entry.kryoTypes = List.copyOf(types);
    }

    @Override
    public void effectivelyOnce() {
        checkMutable();
        this.effectivelyOnce = true;
    }

    @Override
    public void checkpointAck() {
        amendCheckpoint(policy -> policy);
    }

    @Override
    public void checkpointEvery(int inputs) {
        amendCheckpoint(policy -> policy.withEvery(inputs));
    }

    @Override
    public void outputsAtCheckpoint(long maxDelayMs) {
        amendCheckpoint(policy -> policy.withOutputsAtCheckpoint(maxDelayMs));
    }

    /**
     * Turn the checkpoint granularity on (idempotent) and amend its policy, so
     * each option is one call that preserves the others.
     */
    private void amendCheckpoint(UnaryOperator<CheckpointPolicy> amend) {
        checkMutable();
        this.checkpoint = amend.apply(this.checkpoint == null ? CheckpointPolicy.defaults() : this.checkpoint);
    }

    @Override
    public void correctionWindow(long windowMs) {
        checkMutable();
        this.correctionWindowMs = windowMs;
    }

    @Override
    public void beyondWindowPolicy(@Nonnull BeyondWindowPolicy policy) {
        checkMutable();
        this.beyondWindowPolicy = policy;
    }

    @Nonnull
    public StreamOperativeTopology build(@Nonnull String factoryId) {
        // Freeze first: any retained handle used from here on throws naming
        // the operative, whatever build's own outcome.
        frozenFactoryId = factoryId;
        frozen = true;
        TopologyRules.Checked checked = new TopologyRules(
                        channels, procedures, pending, effectivelyOnce, checkpoint, correctionWindowMs, errors)
                .check();
        if (!checked.errors().isEmpty()) {
            throw new StreamOperativeConfigurationException(List.copyOf(checked.errors()));
        }
        return TopologyMaterializer.materialize(
                factoryId,
                channels,
                procedures,
                checked,
                procedureNameToId,
                effectivelyOnce,
                checkpoint,
                correctionWindowMs,
                beyondWindowPolicy);
    }

    // ---- Internals ----------------------------------------------------------

    void checkMutable() {
        if (frozen) {
            throw new IllegalStateException("operative `" + frozenFactoryId
                    + "`: the topology is frozen; stream handles are only usable inside channels()/procedures()");
        }
    }

    /** The staging entry for a declared channel, by id. */
    ChannelEntry channel(int channelId) {
        return channels.get(channelId);
    }

    /** The staging entry for a registered node, by id. */
    ProcedureEntry procedure(int procedureId) {
        return procedures.get(procedureId);
    }

    /** Record one refusal; {@link #build(String)} throws the whole accumulated list. */
    void error(String message) {
        errors.add(message);
    }

    private int declareChannel(Class<?> payloadType, String name, ChannelKind kind) {
        int id = channels.size();
        ChannelEntry entry = new ChannelEntry();
        entry.name = name;
        entry.kind = kind;
        entry.payloadType = payloadType;
        // DROP_NOTHING for every kind: a channel drops only when the operative declared it.
        entry.overflowPolicy = OverflowPolicy.DROP_NOTHING;
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
    int registerNode(Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass, String nodeName) {
        Procedure<?, ?, ?, ?, ?, ?> probe = instantiate(procedureClass);
        String identity = nodeName != null ? nodeName : probe.name();
        Integer existing = resolveIdentity(identity, procedureClass, nodeName != null);
        if (existing != null) {
            return existing;
        }
        ProcedureEntry entry = probeDeclarations(probe, identity);
        entry.procedureClass = procedureClass;
        entry.name = identity;
        entry.named = nodeName != null;
        int id = procedures.size();
        procedures.add(entry);
        procedureNameToId.put(identity, id);
        return id;
    }

    /**
     * The node already registered under {@code identity}, or {@code null} for a
     * fresh one. A claim by a different class, or by the other of the
     * named/unnamed spellings, is the collision.
     */
    private Integer resolveIdentity(
            String identity, Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass, boolean named) {
        Integer existing = procedureNameToId.get(identity);
        if (existing == null) {
            return null;
        }
        ProcedureEntry existingEntry = procedures.get(existing);
        if (!existingEntry.procedureClass.equals(procedureClass) || existingEntry.named != named) {
            errors.add(StreamOperativeConfigurationException.nodeIdentityCollision(identity)
                    .getMessage());
        }
        return existing;
    }

    /**
     * Everything a fresh node records off one probe instance: the type tokens,
     * the Entity-mode and durability declarations, and the two clock refusals.
     *
     * <p>Every clock in the sealed hierarchy is wired. What Rust rejects at
     * compile time (a {@code TimeExtractor<Input>} bound, an unservable clock)
     * is rejected here instead — erasure precludes the compile-time form, so
     * the same defect is caught at registration.
     */
    private ProcedureEntry probeDeclarations(Procedure<?, ?, ?, ?, ?, ?> probe, String identity) {
        ProcedureEntry entry = new ProcedureEntry();
        entry.entityKeys = List.copyOf(probe.entityKeys());
        entry.entitiesForOverridden = probe.entitiesForOverridden();
        entry.inputType = probe.inputType();
        entry.outputType = probe.outputType();
        entry.changeType = probe.changeType();
        entry.clockType = probe.clockType();
        entry.configType = probe.configType();
        entry.kryoTypes = List.copyOf(probe.kryoTypes());
        entry.schemaVersion = probe.schemaVersion();
        entry.minReadableSchemaVersion = probe.minReadableSchemaVersion();
        entry.compactAfterBytes = probe.compactAfterBytes();
        entry.mailboxCapacityDeclared = probe.mailboxCapacity();
        entry.mailboxCapacityOverridden = probe.mailboxCapacityOverridden();
        entry.compensateOverridden = probe.compensateOverridden();
        entry.entityIdleTtl = probe.entityIdleTtl();
        entry.maxEntities = probe.maxEntitiesOverridden() ? OptionalLong.of(probe.maxEntities()) : OptionalLong.empty();
        if (entry.clockType == EventTimeFrom.class && !probe.eventTimeOverridden()) {
            errors.add(StreamOperativeConfigurationException.eventTimeExtractorMissing(identity)
                    .getMessage());
        }
        if (entry.clockType == LogicalClock.class && entry.changeType == NoChange.class) {
            errors.add(StreamOperativeConfigurationException.unservableClock(identity)
                    .getMessage());
        }
        return entry;
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

    private static final Procedure<Object, Object, Object, NoChange, WallClock, Void> UNINSTANTIABLE =
            new Procedure<>() {
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
                public Class<NoChange> changeType() {
                    return NoChange.class;
                }

                @Override
                public Class<WallClock> clockType() {
                    return WallClock.class;
                }

                @Override
                public Class<Void> configType() {
                    return Void.class;
                }

                @Override
                public Object initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
                    throw new UnsupportedOperationException("uninstantiable procedure");
                }

                @Override
                public void run(Object input, StateHandle<Object, NoChange> state, ProcedureCtx ctx) {
                    throw new UnsupportedOperationException("uninstantiable procedure");
                }
            };

    /**
     * Wire {@code channelId} into a consumer node (created/resolved by
     * identity), closing {@code pendingChain} onto the resulting edge. Returns
     * the consumer's node id so its handle can continue the chain.
     */
    int wireConsumer(
            int channelId,
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            String nodeName,
            Integer pendingChain) {
        return wireConsumer(channelId, procedureClass, nodeName, null, pendingChain);
    }

    /**
     * A transform/key chain declared on a stream handle but not yet closed by
     * {@code into(...)}. Every link pushes a NEW entry composing the previous
     * one, which is what makes reusing a handle mid-chain branch cleanly
     * instead of aliasing. Mirror of the Rust SDK's {@code PendingEdge}.
     */
    static final class PendingEdge {
        int channel;
        /** The payload type after this chain's transforms. */
        Class<?> payloadType;
        /**
         * Every transform declared so far, composed into ONE closure.
         * Composition is the whole of "fusion": the runtime applies one closure
         * per edge and never learns that a chain existed.
         */
        Function<Object, EdgeYield> transform;

        Function<Object, String> key;
        /** Set when a later link built on this one, so only a tip can dangle. */
        boolean superseded;
        /** Set when {@code into(...)} closed the chain onto an edge. */
        boolean consumed;
    }

    /**
     * Extend the chain at {@code base} (or open one on {@code channel}) with
     * one more link, returning the new chain's id.
     */
    int extendPending(
            int channel,
            Integer base,
            Class<?> payloadType,
            Function<Object, EdgeYield> link,
            Function<Object, String> key) {
        ChannelEntry entry = channels.get(channel);
        if (entry.pluginSinkName != null) {
            errors.add(StreamOperativeConfigurationException.transformAfterTerminal(entry.name)
                    .getMessage());
        }
        Function<Object, EdgeYield> priorTransform = null;
        Function<Object, String> priorKey = null;
        if (base != null) {
            PendingEdge prior = pending.get(base);
            prior.superseded = true;
            priorTransform = prior.transform;
            priorKey = prior.key;
        }
        // The key is evaluated on the final post-transform value, so a
        // transform declared after it would hand it a value of a type it was
        // never written against.
        if (priorKey != null && link != null) {
            errors.add(StreamOperativeConfigurationException.transformAfterKeyBy(entry.name)
                    .getMessage());
        }
        if (priorKey != null && key != null) {
            errors.add(StreamOperativeConfigurationException.duplicateKeyBy(entry.name)
                    .getMessage());
        }
        PendingEdge chain = new PendingEdge();
        chain.channel = channel;
        chain.payloadType = payloadType;
        chain.transform = compose(priorTransform, link);
        chain.key = key != null ? key : priorKey;
        pending.add(chain);
        return pending.size() - 1;
    }

    /** Take a chain's closures and mark it closed; {@code null} for a bare handle. */
    private PendingEdge closePending(Integer pendingChain) {
        if (pendingChain == null) {
            return null;
        }
        PendingEdge chain = pending.get(pendingChain);
        chain.consumed = true;
        return chain;
    }

    /**
     * Compose two edge transforms into one. A drop short-circuits; a fan-out
     * feeds each of its values through the next link and concatenates.
     */
    private static Function<Object, EdgeYield> compose(
            Function<Object, EdgeYield> first, Function<Object, EdgeYield> second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return value -> {
            EdgeYield yielded = first.apply(value);
            List<Object> values = yielded.values();
            if (values.isEmpty()) {
                return EdgeYield.DROP;
            }
            if (values.size() == 1) {
                return second.apply(values.get(0));
            }
            List<Object> out = new ArrayList<>(values.size());
            for (Object v : values) {
                out.addAll(second.apply(v).values());
            }
            return EdgeYield.many(out);
        };
    }

    /**
     * Wire {@code channelId} into a consumer node. A merge subscription passes
     * the declared merged input type and the mapping that lifts this channel's
     * payload into it; what Rust checks at the {@code .into::<Proc>()} call site
     * with an {@code Input = I} bound is checked here instead — erasure
     * precludes the compile-time form, so the same defect is caught at
     * registration.
     */
    int wireConsumer(
            int channelId,
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            String nodeName,
            Class<?> deliveredTypeOverride,
            Integer pendingChain) {
        int procedureId = registerNode(procedureClass, nodeName);
        ProcedureEntry procedure = procedures.get(procedureId);
        ChannelEntry channel = channels.get(channelId);
        Class<?> chainType = pendingChain == null ? null : pending.get(pendingChain).payloadType;
        refuseTypeMismatch(channel, procedure, deliveredTypeOverride, chainType);
        if (!attachEdge(channel, procedureId, procedure.name, closePending(pendingChain))) {
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
     * The delivered payload is what the edge's transforms produce, not what the
     * channel carries — a merge mapping and a {@code .map(...)} chain are the
     * same thing here, and both reshape what the consumer observes.
     */
    private void refuseTypeMismatch(
            ChannelEntry channel, ProcedureEntry procedure, Class<?> deliveredTypeOverride, Class<?> chainType) {
        Class<?> declared = deliveredTypeOverride != null ? deliveredTypeOverride : chainType;
        Class<?> deliveredType = declared != null ? declared : channel.payloadType;
        if (deliveredType.equals(procedure.inputType)) {
            return;
        }
        String producedBy = deliveredTypeOverride != null
                ? "merge maps into"
                : chainType != null
                        ? "edge transforms produce"
                        : channel.producer == null
                                ? "payload type"
                                : "producer procedure `" + procedures.get(channel.producer).name + "` outputType() =";
        errors.add("channel `" + channel.name + "`: " + producedBy + " "
                + describe(deliveredType)
                + " does not match consumer procedure `" + procedure.name
                + "` inputType() = " + describe(procedure.inputType));
    }

    /**
     * Add the consumer edge, or refuse it. Re-declaring an existing bare edge is
     * the fan-in / repeated {@code .into} case and stays a no-op. Re-declaring
     * one where either side carries a transform or a key is refused rather than
     * silently dropped — the edge would otherwise keep whichever closure was
     * declared first and deliver the wrong value, to the wrong Entity, forever.
     *
     * @return {@code false} when the edge was refused as already wired.
     */
    private boolean attachEdge(ChannelEntry channel, int procedureId, String procedureName, PendingEdge chain) {
        boolean declares = chain != null && (chain.transform != null || chain.key != null);
        Optional<ConsumerEdge> existing = channel.consumers.stream()
                .filter(e -> e.procedure() == procedureId)
                .findFirst();
        if (existing.isEmpty()) {
            channel.consumers.add(new ConsumerEdge(
                    procedureId,
                    Optional.ofNullable(chain == null ? null : chain.transform),
                    Optional.ofNullable(chain == null ? null : chain.key)));
            return true;
        }
        if (existing.get().transform().isPresent() || existing.get().key().isPresent() || declares) {
            errors.add(StreamOperativeConfigurationException.edgeAlreadyWired(channel.name, procedureName)
                    .getMessage());
            return false;
        }
        return true;
    }

    /**
     * The node's output channel, created lazily on first use so a leaf node
     * never carries a phantom channel. Named by the node's identity until a
     * {@code .named(...)} override; payload type = the node's declared
     * {@code outputType()}.
     */
    int ensureOutputChannel(int procedureId) {
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
        String pluginSinkName;
        Function<Object, byte[]> pluginSinkSerializer;
        OverflowPolicy overflowPolicy;
        int retryMaxAttempts = RetryPolicy.DEFAULT.maxAttempts();
        long retryBackoffBaseMs = RetryPolicy.DEFAULT.backoffBaseMs();
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
    void setRetryMax(int channelId, int maxAttempts) {
        ChannelEntry entry = channels.get(channelId);
        if (maxAttempts <= 0) {
            errors.add("channel `" + entry.name + "` declared retryMax = " + maxAttempts
                    + "; at least one attempt is required (throw ProcedureException.NonRetryable"
                    + " to park without retries)");
            return;
        }
        entry.retryMaxAttempts = maxAttempts;
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
        /**
         * Set during validation when some consumer edge into this node carries
         * a key function — Entity mode is read off the WIRING now, not off the
         * procedure.
         */
        boolean dynamic;

        Class<?> inputType;
        Class<?> outputType;
        Class<? extends Change> changeType;
        Class<? extends Clock> clockType;
        Class<?> configType;
        List<Class<?>> kryoTypes;
        int schemaVersion;
        int minReadableSchemaVersion;
        long compactAfterBytes;
        Integer inputChannelId;
        Integer outputChannelId;
        int mailboxCapacityDeclared;
        boolean mailboxCapacityOverridden;
        boolean compensateOverridden;
        Optional<Duration> entityIdleTtl = Optional.empty();
        OptionalLong maxEntities = OptionalLong.empty();
    }
}
