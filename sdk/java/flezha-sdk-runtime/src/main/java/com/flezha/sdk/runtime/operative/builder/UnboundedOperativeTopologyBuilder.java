package com.flezha.sdk.runtime.operative.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.Channel;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelDecl;
import com.flezha.sdk.channel.ChannelDeclWithProducer;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.EntryChannel;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.operative.UnboundedOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.OperationDescriptor;
import com.flezha.sdk.runtime.operative.PluginSink;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * Implements {@link ChannelBuilder} (and the inner {@link EntryChannel} /
 * {@link ChannelDecl} / {@link ChannelDeclWithProducer} interfaces). Tracks
 * declared channels and operation registrations, validates wiring at
 * {@link #build(String)} time, and produces a frozen {@link UnboundedOperativeTopology}.
 *
 * <p>Validation rules (mirror of `operative-mvp-rust/src/operative/builder.rs`):
 * <ul>
 *   <li>Entry channels accept only {@code byte[]} or {@code String}.</li>
 *   <li>Channel payload type must match producer Operation's
 *       {@link Operation#outputType()} and consumer Operation(s)'
 *       {@link Operation#inputType()}.</li>
 *   <li>Channels declared with {@code .channel(...)} but never wired with
 *       {@code .from(...)} are rejected.</li>
 *   <li>Operations declaring non-empty {@link Operation#operationKeys()} but with
 *       {@code segmentForOverridden() == false} are rejected.</li>
 *   <li>Operations with {@code operationKeyOverridden() == true} but empty
 *       {@code operationKeys()} are rejected (dynamic-mode reservation).</li>
 *   <li>Channel names must be unique within the UnboundedOperative.</li>
 * </ul>
 *
 * <p>All failures throw {@link UnboundedOperativeConfigurationException}.
 */
public final class UnboundedOperativeTopologyBuilder implements ChannelBuilder {

    private static final Set<Class<?>> ENTRY_CHANNEL_TYPES = Set.of(byte[].class, String.class);

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final Map<String, Integer> nameToId = new LinkedHashMap<>();
    private final List<OperationEntry> operations = new ArrayList<>();
    private final Map<String, Integer> operationNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();
    private boolean effectivelyOnce;
    private boolean checkpointAck;
    private long correctionWindowMs;
    private com.flezha.sdk.channel.BeyondWindowPolicy beyondWindowPolicy =
            com.flezha.sdk.channel.BeyondWindowPolicy.PROCESS_AS_ARRIVAL;

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
     * {@code operations(...)} authoring — both declare Operations / Channels /
     * Segments on the same {@link ChannelBuilder}.
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
        if (!ENTRY_CHANNEL_TYPES.contains(type)) {
            errors.add("entry channel `" + name + "`: type " + type.getName()
                    + " is not allowed; entry channels accept only byte[] or String in v1");
        }
        int id = declareChannel(type, name, ChannelKind.ENTRY);
        return new EntryChannelImpl<>(id, type, name);
    }

    @Override
    @Nonnull
    public <T> ChannelDecl<T> channel(@Nonnull Class<T> type, @Nonnull String name) {
        int id = declareChannel(type, name, ChannelKind.INTER_OPERATION);
        return new ChannelDeclImpl<>(id, type, name);
    }

    @Override
    public void effectivelyOnce() {
        this.effectivelyOnce = true;
    }

    @Override
    public void checkpointAck() {
        this.checkpointAck = true;
    }

    @Override
    public void correctionWindow(long windowMs) {
        this.correctionWindowMs = windowMs;
    }

    @Override
    public void beyondWindowPolicy(@Nonnull com.flezha.sdk.channel.BeyondWindowPolicy policy) {
        this.beyondWindowPolicy = policy;
    }

    @Nonnull
    public UnboundedOperativeTopology build(@Nonnull String factoryId) {
        // Detect channels declared without `.from(...)` (still classified as
        // INTER_OPERATION with no producer).
        for (ChannelEntry entry : channels) {
            if (entry.kind != ChannelKind.ENTRY && entry.producer == null) {
                errors.add("channel `" + entry.name + "` was declared but never wired with .from(...)");
            }
        }

        // Validate each Operation's static-mode / dynamic-mode declaration.
        for (OperationEntry operation : operations) {
            if (!operation.segmentKeys.isEmpty() && !operation.segmentForOverridden) {
                errors.add("operation `" + operation.name
                        + "` declares non-empty operationKeys() but does not override segmentFor "
                        + "(set segmentForOverridden() to return true)");
            }
            if (operation.segmentForOverridden && operation.segmentKeys.isEmpty()) {
                errors.add("operation `" + operation.name
                        + "` overrode segmentFor — single-mode operations have one implicit segment");
            }
        }

        // Resolve mailbox capacity for each Operation (Operation override → env var →
        // default), accumulating any range errors into the errors list.
        int[] resolvedCapacities = new int[operations.size()];
        for (int i = 0; i < operations.size(); i++) {
            OperationEntry operation = operations.get(i);
            try {
                resolvedCapacities[i] = MailboxCapacityResolver.resolve(
                        operation.name, operation.mailboxCapacityDeclared, operation.mailboxCapacityOverridden);
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
                    operations.stream().anyMatch(o -> o.clockType == com.flezha.sdk.state.EventTimeFrom.class);
            if (!anyEventTime) {
                errors.add(UnboundedOperativeConfigurationException.correctionWindowRequires(
                                "an `EventTimeFrom` clock on at least one operation")
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
            // inter-operation channel has nowhere to carry it.
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
                    Optional.ofNullable(entry.pluginSerializer),
                    pluginSink,
                    entry.overflowPolicy,
                    new com.flezha.sdk.channel.RetryPolicy(entry.retryMaxAttempts, entry.retryBackoffBaseMs),
                    entry.keyed,
                    entry.interOpEncoder == null
                            ? Optional.empty()
                            : Optional.of(
                                    new ChannelDescriptor.InterOpCodec(entry.interOpEncoder, entry.interOpDecoder))));
        }

        // Materialize OperationDescriptors.
        List<OperationDescriptor> operationDescriptors = new ArrayList<>(operations.size());
        for (int i = 0; i < operations.size(); i++) {
            OperationEntry entry = operations.get(i);
            operationDescriptors.add(new OperationDescriptor(
                    entry.name,
                    entry.operationClass,
                    entry.segmentKeys,
                    entry.segmentForOverridden,
                    entry.operationKeyOverridden,
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
                    entry.segmentIdleTtl));
        }

        return new UnboundedOperativeTopology(
                factoryId,
                channelDescriptors,
                Map.copyOf(nameToId),
                operationDescriptors,
                Map.copyOf(operationNameToId),
                effectivelyOnce,
                checkpointAck,
                correctionWindowMs,
                beyondWindowPolicy);
    }

    // ---- Internals ----------------------------------------------------------

    private int declareChannel(Class<?> payloadType, String name, ChannelKind kind) {
        if (nameToId.containsKey(name)) {
            errors.add("channel `" + name + "` declared more than once on this operative");
        }
        int id = channels.size();
        ChannelEntry entry = new ChannelEntry();
        entry.name = name;
        entry.kind = kind;
        entry.payloadType = payloadType;
        entry.overflowPolicy = defaultPolicyForKind(kind);
        channels.add(entry);
        nameToId.put(name, id);
        return id;
    }

    private int registerOperation(Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        Operation<?, ?, ?, ?, ?, ?> probe = instantiate(operationClass);
        String operationName = probe.name();
        Integer existing = operationNameToId.get(operationName);
        if (existing != null) {
            return existing;
        }
        int id = operations.size();
        OperationEntry entry = new OperationEntry();
        entry.operationClass = operationClass;
        entry.name = operationName;
        entry.segmentKeys = List.copyOf(probe.operationKeys());
        entry.segmentForOverridden = probe.segmentForOverridden();
        entry.operationKeyOverridden = probe.operationKeyOverridden();
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
        entry.segmentIdleTtl = probe.segmentIdleTtl();
        // Every clock in the sealed hierarchy is wired. What Rust rejects at
        // compile time (a `TimeExtractor<Input>` bound, an unservable clock) is
        // rejected here instead — erasure precludes the compile-time form, so
        // the same defect is caught at registration.
        if (entry.clockType == com.flezha.sdk.state.EventTimeFrom.class && !probe.eventTimeOverridden()) {
            errors.add(UnboundedOperativeConfigurationException.eventTimeExtractorMissing(operationName)
                    .getMessage());
        }
        if (entry.clockType == com.flezha.sdk.state.LogicalClock.class
                && entry.changeType == com.flezha.sdk.state.NoChange.class) {
            errors.add(UnboundedOperativeConfigurationException.unservableClock(operationName)
                    .getMessage());
        }
        operations.add(entry);
        operationNameToId.put(operationName, id);
        return id;
    }

    private Operation<?, ?, ?, ?, ?, ?> instantiate(Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        try {
            return operationClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            errors.add("operation class `" + operationClass.getName() + "` must have a public no-arg constructor: "
                    + e.toString());
            // Return a placeholder so the rest of the build can collect more
            // errors before failing; the exception throws at build() time.
            return UNINSTANTIABLE;
        }
    }

    private static final Operation<
                    Object, Object, Object, com.flezha.sdk.state.NoChange, com.flezha.sdk.state.WallClock, Void>
            UNINSTANTIABLE = new Operation<>() {
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
                        Optional<String> operationKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
                    throw new UnsupportedOperationException("uninstantiable operation");
                }

                @Override
                public void run(
                        Object input,
                        com.flezha.sdk.state.StateHandle<Object, com.flezha.sdk.state.NoChange> state,
                        com.flezha.sdk.operation.OperationCtx ctx) {
                    throw new UnsupportedOperationException("uninstantiable operation");
                }
            };

    private void wireConsumer(
            int channelId,
            String channelName,
            Class<?> channelPayloadType,
            Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        int operationId = registerOperation(operationClass);
        OperationEntry operation = operations.get(operationId);
        if (!channelPayloadType.equals(operation.inputType)) {
            errors.add("channel `" + channelName + "`: payload type "
                    + describe(channelPayloadType)
                    + " does not match consumer operation `" + operation.name
                    + "` inputType() = " + describe(operation.inputType));
        }
        ChannelEntry channel = channels.get(channelId);
        if (!channel.consumers.contains(operationId)) {
            channel.consumers.add(operationId);
        }
        // Cache input channel id on the operation. If the operation already has a
        // different input channel wired, both stay reachable via the
        // channel descriptor; the dispatcher uses the channel-side mapping.
        if (operation.inputChannelId == null) {
            operation.inputChannelId = channelId;
        }
    }

    private void wireProducer(
            int channelId,
            String channelName,
            Class<?> channelPayloadType,
            Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass) {
        int operationId = registerOperation(operationClass);
        OperationEntry operation = operations.get(operationId);
        if (!channelPayloadType.equals(operation.outputType)) {
            errors.add("channel `" + channelName + "`: payload type "
                    + describe(channelPayloadType)
                    + " does not match producer operation `" + operation.name
                    + "` outputType() = " + describe(operation.outputType));
        }
        ChannelEntry channel = channels.get(channelId);
        if (channel.producer != null) {
            errors.add("channel `" + channelName + "` already has a producer; multi-producer is not allowed");
        } else {
            channel.producer = operationId;
        }
        if (operation.outputChannelId == null) {
            operation.outputChannelId = channelId;
        }
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
        final List<Integer> consumers = new ArrayList<>();
        String pluginName;
        Function<Object, byte[]> pluginSerializer;
        String pluginSinkName;
        Function<Object, byte[]> pluginSinkSerializer;
        OverflowPolicy overflowPolicy;
        int retryMaxAttempts = com.flezha.sdk.channel.RetryPolicy.DEFAULT.maxAttempts();
        long retryBackoffBaseMs = com.flezha.sdk.channel.RetryPolicy.DEFAULT.backoffBaseMs();
        boolean keyed;
        Function<Object, byte[]> interOpEncoder;
        Function<byte[], Object> interOpDecoder;
    }

    /**
     * Apply a {@code .retryMax(...)} declaration, rejecting a zero budget at
     * registration time like every other declaration error.
     */
    private void setRetryMax(int channelId, int maxAttempts) {
        ChannelEntry entry = channels.get(channelId);
        if (maxAttempts <= 0) {
            errors.add("channel `" + entry.name + "` declared retryMax = " + maxAttempts
                    + "; at least one attempt is required (throw OperationException.NonRetryable"
                    + " to park without retries)");
            return;
        }
        entry.retryMaxAttempts = maxAttempts;
    }

    private static OverflowPolicy defaultPolicyForKind(ChannelKind kind) {
        return kind == ChannelKind.ENTRY ? OverflowPolicy.DROP_NEWEST : OverflowPolicy.BLOCK;
    }

    /** Mutable internal record for a operation registration. */
    static final class OperationEntry {
        String name;
        Class<? extends Operation<?, ?, ?, ?, ?, ?>> operationClass;
        List<String> segmentKeys;
        boolean segmentForOverridden;
        boolean operationKeyOverridden;
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
        java.util.Optional<java.time.Duration> segmentIdleTtl = java.util.Optional.empty();
    }

    // ---- Builder phantom types ---------------------------------------------

    private final class EntryChannelImpl<T> implements EntryChannel<T> {
        private final int channelId;
        private final Class<T> payloadType;
        private final String name;

        EntryChannelImpl(int channelId, Class<T> payloadType, String name) {
            this.channelId = channelId;
            this.payloadType = payloadType;
            this.name = name;
        }

        @Override
        @Nonnull
        public Channel<T> into(@Nonnull Class<? extends Operation<T, ?, ?, ?, ?, ?>> operationClass) {
            wireConsumer(channelId, name, payloadType, operationClass);
            return new Channel<>(channelId, payloadType);
        }

        @Override
        @Nonnull
        public EntryChannel<T> onFull(@Nonnull OverflowPolicy policy) {
            channels.get(channelId).overflowPolicy = policy;
            return this;
        }

        @Override
        @Nonnull
        public EntryChannel<T> retryMax(int maxAttempts) {
            setRetryMax(channelId, maxAttempts);
            return this;
        }

        @Override
        @Nonnull
        public EntryChannel<T> retryBackoffMs(long baseMs) {
            channels.get(channelId).retryBackoffBaseMs = baseMs;
            return this;
        }
    }

    private final class ChannelDeclImpl<T> implements ChannelDecl<T> {
        private final int channelId;
        private final Class<T> payloadType;
        private final String name;

        ChannelDeclImpl(int channelId, Class<T> payloadType, String name) {
            this.channelId = channelId;
            this.payloadType = payloadType;
            this.name = name;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> from(@Nonnull Class<? extends Operation<?, ?, T, ?, ?, ?>> operationClass) {
            wireProducer(channelId, name, payloadType, operationClass);
            return new ChannelDeclWithProducerImpl<>(channelId, payloadType, name);
        }

        @Override
        @Nonnull
        public ChannelDecl<T> onFull(@Nonnull OverflowPolicy policy) {
            channels.get(channelId).overflowPolicy = policy;
            return this;
        }

        @Override
        @Nonnull
        public ChannelDecl<T> retryMax(int maxAttempts) {
            setRetryMax(channelId, maxAttempts);
            return this;
        }

        @Override
        @Nonnull
        public ChannelDecl<T> retryBackoffMs(long baseMs) {
            channels.get(channelId).retryBackoffBaseMs = baseMs;
            return this;
        }
    }

    private final class ChannelDeclWithProducerImpl<T> implements ChannelDeclWithProducer<T> {
        private final int channelId;
        private final Class<T> payloadType;
        private final String name;

        ChannelDeclWithProducerImpl(int channelId, Class<T> payloadType, String name) {
            this.channelId = channelId;
            this.payloadType = payloadType;
            this.name = name;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> into(@Nonnull Class<? extends Operation<T, ?, ?, ?, ?, ?>> operationClass) {
            wireConsumer(channelId, name, payloadType, operationClass);
            return this;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> keyed() {
            channels.get(channelId).keyed = true;
            return this;
        }

        @Override
        @Nonnull
        @SuppressWarnings("unchecked")
        public ChannelDeclWithProducer<T> codec() {
            // Reuse the SDK's default JSON serialization — the same Jackson
            // mapper a plugin-bound channel falls back to.
            return codecWith(defaultSerializer(payloadType), defaultDeserializer(payloadType));
        }

        @Override
        @Nonnull
        @SuppressWarnings("unchecked")
        public ChannelDeclWithProducer<T> codecWith(
                @Nonnull Function<T, byte[]> encoder, @Nonnull Function<byte[], T> decoder) {
            ChannelEntry entry = channels.get(channelId);
            entry.interOpEncoder = value -> encoder.apply((T) value);
            entry.interOpDecoder = bytes -> decoder.apply(bytes);
            return this;
        }

        @Override
        @Nonnull
        public Channel<T> toPlugin(@Nonnull String pluginName) {
            return toPlugin(pluginName, defaultSerializer(payloadType));
        }

        @Override
        @Nonnull
        public Channel<T> toPlugin(@Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer) {
            ChannelEntry entry = channels.get(channelId);
            entry.kind = ChannelKind.PLUGIN_BOUND;
            entry.pluginName = pluginName;
            @SuppressWarnings("unchecked")
            Function<Object, byte[]> erased = (Function<Object, byte[]>) (Function<?, byte[]>) serializer;
            entry.pluginSerializer = erased;
            return new Channel<>(channelId, payloadType);
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> alsoToPluginWith(
                @Nonnull String pluginName, @Nonnull Function<T, byte[]> serializer) {
            ChannelEntry entry = channels.get(channelId);
            if (entry.pluginSinkName != null) {
                errors.add(UnboundedOperativeConfigurationException.duplicatePluginSink(entry.name)
                        .getMessage());
                return this;
            }
            entry.pluginSinkName = pluginName;
            @SuppressWarnings("unchecked")
            Function<Object, byte[]> erased = (Function<Object, byte[]>) (Function<?, byte[]>) serializer;
            entry.pluginSinkSerializer = erased;
            return this;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> onFull(@Nonnull OverflowPolicy policy) {
            channels.get(channelId).overflowPolicy = policy;
            return this;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> retryMax(int maxAttempts) {
            setRetryMax(channelId, maxAttempts);
            return this;
        }

        @Override
        @Nonnull
        public ChannelDeclWithProducer<T> retryBackoffMs(long baseMs) {
            channels.get(channelId).retryBackoffBaseMs = baseMs;
            return this;
        }
    }

    /**
     * Default plugin-bound serializer. {@code byte[]} is passthrough;
     * {@code String} is UTF-8 encoded; everything else goes through Jackson
     * JSON ({@code design.md} Open Question 4).
     */
    /** Mirror of {@link #defaultSerializer}: the decode half of a default {@code codec()}. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Function<byte[], T> defaultDeserializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<byte[], String>) (b -> new String(b, java.nio.charset.StandardCharsets.UTF_8));
        }
        ObjectMapper mapper = new ObjectMapper();
        return bytes -> {
            try {
                return mapper.readValue(bytes, payloadType);
            } catch (Exception e) {
                throw new RuntimeException("inter-op codec (Jackson) decode failed: " + e.getMessage(), e);
            }
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> Function<T, byte[]> defaultSerializer(Class<T> payloadType) {
        if (payloadType == byte[].class) {
            return (Function) (Function<byte[], byte[]>) (b -> b);
        }
        if (payloadType == String.class) {
            return (Function) (Function<String, byte[]>) (s -> s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        ObjectMapper mapper = new ObjectMapper();
        return value -> {
            try {
                return mapper.writeValueAsBytes(value);
            } catch (Exception e) {
                throw new RuntimeException("plugin serializer (Jackson) failed: " + e.getMessage(), e);
            }
        };
    }
}
