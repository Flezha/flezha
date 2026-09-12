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
import com.flezha.sdk.runtime.core.HostClassDeclaration;
import com.flezha.sdk.runtime.core.HostLink;
import com.flezha.sdk.runtime.operative.EdgeYield;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.Clock;
import com.flezha.sdk.state.EventTimeFrom;
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
 * <p>The builder JUDGES nothing about the graph: it records the links each
 * handle was given and submits that record to {@code flz_core}'s one check,
 * which answers the derived declaration or the verdict. What it still refuses
 * itself is only what erasure makes and no record could carry — a payload type
 * that does not reach its consumer's input, a type argument left generic at the
 * procedure class, a missing {@code eventTime} extractor, a procedure class
 * with no public no-arg constructor. After {@link #build(String)} the topology
 * is frozen: a retained handle used afterwards throws
 * {@link IllegalStateException} naming the operative.
 */
public final class StreamOperativeTopologyBuilder implements ChannelBuilder {

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final List<ProcedureEntry> procedures = new ArrayList<>();
    /**
     * Every chain declared on a handle, indexed by the id the stream handles
     * carry. Each link pushes a NEW entry superseding the previous one, so a
     * chain's tip is the only entry that names the whole sequence.
     */
    private final List<PendingEdge> pending = new ArrayList<>();

    private final Map<String, Integer> procedureNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();
    /** Ties the sources of one merge together across the channels they sit on. */
    private int mergeGroups;

    private boolean effectivelyOnce;
    /**
     * The three checkpoint knobs, each written raw. The core derives the
     * acknowledgement granularity from them — any one of them implies it —
     * which is the rule six per-SDK helpers each carried before (KI-122).
     */
    private boolean checkpointAck;

    private Integer checkpointEvery;
    private Long checkpointMaxDelayMs;

    /** {@code null} until {@code correctionWindow(...)}; 0 is a declared window. */
    private Long correctionWindowMs;

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
        checkMutable();
        this.checkpointAck = true;
    }

    @Override
    public void checkpointEvery(int inputs) {
        checkMutable();
        this.checkpointEvery = inputs;
    }

    @Override
    public void outputsAtCheckpoint(long maxDelayMs) {
        checkMutable();
        this.checkpointMaxDelayMs = maxDelayMs;
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
        if (!errors.isEmpty()) {
            // The record's edges are meaningless once a payload type does not
            // reach its consumer, so the core is never asked about them.
            throw refused(errors);
        }
        // A superseded entry is an intermediate link the chain it grew into
        // already carries; only a tip names a whole declared sequence.
        for (PendingEdge chain : pending) {
            if (!chain.superseded) {
                channels.get(chain.channel).chains.add(chain);
            }
        }
        int[] capacities = new int[procedures.size()];
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry procedure = procedures.get(i);
            capacities[i] = MailboxCapacityResolver.resolve(
                    procedure.mailboxCapacityDeclared, procedure.mailboxCapacityOverridden);
        }
        markKeyedTargetsDynamic();

        Map<String, Object> record = HostClassDeclaration.forStream(
                factoryId,
                new ClassFacts(
                        effectivelyOnce,
                        checkpointAck,
                        checkpointEvery,
                        checkpointMaxDelayMs,
                        correctionWindowMs,
                        beyondWindowPolicy),
                channels,
                procedures,
                capacities);
        // An empty declaration is the workflow run-sugar shape: no topology to
        // derive, and the class facts are the core's own at declare time.
        if (channels.isEmpty() && procedures.isEmpty()) {
            return TopologyMaterializer.empty(factoryId, record);
        }
        return TopologyMaterializer.materialize(
                factoryId, channels, procedures, capacities, record, HostClassDeclaration.check(record));
    }

    /**
     * Entity mode is read off the WIRING, and the record states it: a
     * procedure that names its own keys is static, one a keyed chain closes
     * onto is dynamic. A procedure declaring both is NOT re-stated as dynamic
     * — its keys are its declaration, and the contradiction with the keyed
     * edge is the core's to name. Reading the builder's own record back is not
     * a judgement — what an unkeyed sibling edge into one MEANS is the core's
     * answer.
     */
    private void markKeyedTargetsDynamic() {
        for (PendingEdge chain : pending) {
            if (chain.superseded || chain.key == null) {
                continue;
            }
            for (HostLink link : chain.links) {
                Integer target = link.target();
                if (target != null && procedures.get(target).entityKeys.isEmpty()) {
                    procedures.get(target).dynamic = true;
                }
            }
        }
    }

    /**
     * The refusals erasure makes and no record could carry. Rendered and named
     * the way the core renders and names its own — {@code Declaration} is the
     * vocabulary's one unnamed shape — so a reader branches on the same names
     * whichever side refused.
     */
    private static StreamOperativeConfigurationException refused(List<String> sentences) {
        List<String> names = java.util.Collections.nCopies(sentences.size(), "Declaration");
        StringBuilder out = new StringBuilder("topology rejected [");
        out.append(String.join(", ", names)).append("]:");
        for (String sentence : sentences) {
            out.append("\n  - ").append(sentence);
        }
        return new StreamOperativeConfigurationException(out.toString(), names);
    }

    /** The class-level facts the record states, exactly as declared. */
    public record ClassFacts(
            boolean effectivelyOnce,
            boolean checkpointAck,
            Integer checkpointEvery,
            Long checkpointMaxDelayMs,
            Long correctionWindowMs,
            BeyondWindowPolicy beyondWindowPolicy) {}

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
        // A claim by a different class, or by the other of the named/unnamed
        // spellings, is a SECOND node under one identity: it is recorded as
        // one, and the core answers NodeIdentityCollision off the record.
        boolean sameNode = existingEntry.procedureClass.equals(procedureClass) && existingEntry.named == named;
        return sameNode ? existing : null;
    }

    /**
     * Everything a fresh node records off one probe instance: the type tokens,
     * the Entity-mode and durability declarations, and the two clock refusals.
     *
     * <p>Every clock in the sealed hierarchy is wired. The missing
     * {@code eventTime(Input)} extractor is refused here because the record
     * carries no such fact: Rust states it as a {@code TimeExtractor<Input>}
     * bound at compile time, and erasure precludes that form.
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
     * Record {@code channelId}'s chain as closing onto a consumer node
     * (created/resolved by identity). Returns the consumer's node id so its
     * handle can continue the chain.
     */
    int wireConsumer(
            int channelId,
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            String nodeName,
            Integer pendingChain) {
        return wireConsumer(channelId, procedureClass, nodeName, null, pendingChain, null);
    }

    /**
     * A transform/key chain declared on a stream handle but not yet closed by
     * {@code into(...)}. Every link pushes a NEW entry composing the previous
     * one, which is what makes reusing a handle mid-chain branch cleanly
     * instead of aliasing. Mirror of the Rust SDK's {@code PendingEdge}.
     */
    public static final class PendingEdge {
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
        /** This chain's links, in the order the author declared them. */
        public List<HostLink> links = new ArrayList<>();
        /** Set when a later link built on this one, so only a tip is recorded. */
        boolean superseded;

        /** The transform this chain's edge applies, or {@code null}. */
        Function<Object, EdgeYield> transform() {
            return transform;
        }

        /** The key this chain's edge applies, or {@code null}. */
        Function<Object, String> key() {
            return key;
        }
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
        Function<Object, EdgeYield> priorTransform = null;
        Function<Object, String> priorKey = null;
        List<HostLink> priorLinks = List.of();
        if (base != null) {
            PendingEdge prior = pending.get(base);
            prior.superseded = true;
            priorTransform = prior.transform;
            priorKey = prior.key;
            priorLinks = prior.links;
        }
        PendingEdge chain = new PendingEdge();
        chain.channel = channel;
        chain.payloadType = payloadType;
        chain.transform = compose(priorTransform, link);
        chain.key = key != null ? key : priorKey;
        chain.links = new ArrayList<>(priorLinks);
        chain.links.add(link != null ? HostLink.transform() : HostLink.key());
        pending.add(chain);
        return pending.size() - 1;
    }

    /**
     * Close {@code base} on a terminator link, or open the one-link chain a
     * bare handle declares. The chain is recorded either way; what closing it
     * MEANS is the core's answer.
     */
    void closeChain(int channel, Integer base, HostLink terminator) {
        if (base != null) {
            pending.get(base).links.add(terminator);
            return;
        }
        PendingEdge chain = new PendingEdge();
        chain.channel = channel;
        chain.links.add(terminator);
        pending.add(chain);
    }

    /** One merge's group id, which ties its sources across their channels. */
    int nextMergeGroup() {
        return mergeGroups++;
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
     * Record {@code channelId}'s chain as closing onto a consumer node. A merge
     * subscription passes the declared merged input type, the mapping that
     * lifts this channel's payload into it, and the group tying its sources
     * together; what Rust checks at the {@code .into::<Proc>()} call site with
     * an {@code Input = I} bound is checked here instead — erasure precludes
     * the compile-time form, so the same defect is caught at registration.
     */
    int wireConsumer(
            int channelId,
            Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass,
            String nodeName,
            Class<?> deliveredTypeOverride,
            Integer pendingChain,
            Integer mergeGroup) {
        int procedureId = registerNode(procedureClass, nodeName);
        ProcedureEntry procedure = procedures.get(procedureId);
        ChannelEntry channel = channels.get(channelId);
        Class<?> chainType = pendingChain == null ? null : pending.get(pendingChain).payloadType;
        refuseTypeMismatch(channel, procedure, deliveredTypeOverride, chainType);
        closeChain(
                channelId,
                pendingChain,
                mergeGroup == null ? HostLink.into(procedureId) : HostLink.merge(procedureId, mergeGroup));
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
    public static final class ChannelEntry {
        public String name;
        public ChannelKind kind;
        Class<?> payloadType;
        public Integer producer;
        /** Every chain declared on this channel's handles, in author order. */
        public final List<PendingEdge> chains = new ArrayList<>();

        Function<Object, byte[]> pluginSinkSerializer;
        public OverflowPolicy overflowPolicy;
        public int retryMaxAttempts = RetryPolicy.DEFAULT.maxAttempts();
        public long retryBackoffBaseMs = RetryPolicy.DEFAULT.backoffBaseMs();
        public boolean keyed;
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

    /** Record a {@code .retryMax(...)} declaration; the core refuses a zero budget. */
    void setRetryMax(int channelId, int maxAttempts) {
        channels.get(channelId).retryMaxAttempts = maxAttempts;
    }

    /** Mutable internal record for a node registration. */
    public static final class ProcedureEntry {
        /**
         * Node identity: the procedure's {@code name()}, or the explicit node
         * name given at wiring time. Feeds everything {@code name()} feeds —
         * state directories, journal entries, the derived channel name,
         * diagnostics.
         */
        public String name;

        /** Whether this node was created with an explicit node name. */
        boolean named;

        Class<? extends Procedure<?, ?, ?, ?, ?, ?>> procedureClass;
        public List<String> entityKeys;
        public boolean entitiesForOverridden;
        /**
         * Set when some chain closing onto this node carries a key function —
         * Entity mode is read off the WIRING, not off the procedure, and the
         * record states it so the core can judge the sibling edges.
         */
        public boolean dynamic;

        Class<?> inputType;
        Class<?> outputType;
        public Class<? extends Change> changeType;
        public Class<? extends Clock> clockType;
        Class<?> configType;
        List<Class<?>> kryoTypes;
        public int schemaVersion;
        public int minReadableSchemaVersion;
        public long compactAfterBytes;
        Integer outputChannelId;
        int mailboxCapacityDeclared;
        boolean mailboxCapacityOverridden;
        public boolean compensateOverridden;
        public Optional<Duration> entityIdleTtl = Optional.empty();
        public OptionalLong maxEntities = OptionalLong.empty();
    }
}
