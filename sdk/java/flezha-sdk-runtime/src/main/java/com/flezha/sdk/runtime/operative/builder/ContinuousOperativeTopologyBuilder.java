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
import com.flezha.sdk.operative.ContinuousOperative;
import com.flezha.sdk.operative.ContinuousOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.OperationDescriptor;
import com.flezha.sdk.runtime.operative.PluginSink;
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
 * {@link #build(String)} time, and produces a frozen {@link ContinuousOperativeTopology}.
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
 *   <li>Channel names must be unique within the ContinuousOperative.</li>
 * </ul>
 *
 * <p>All failures throw {@link ContinuousOperativeConfigurationException}.
 */
public final class ContinuousOperativeTopologyBuilder implements ChannelBuilder {

    private static final Set<Class<?>> ENTRY_CHANNEL_TYPES = Set.of(byte[].class, String.class);

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final Map<String, Integer> nameToId = new LinkedHashMap<>();
    private final List<OperationEntry> operations = new ArrayList<>();
    private final Map<String, Integer> operationNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();

    /**
     * Run the supplied {@link ContinuousOperative#channels(ChannelBuilder)} once, validate,
     * and freeze the result into a {@link ContinuousOperativeTopology}.
     *
     * @throws ContinuousOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static ContinuousOperativeTopology build(@Nonnull ContinuousOperative operative) {
        return build(operative.factoryId(), operative::channels);
    }

    /**
     * Run an authoring lambda against a fresh builder, validate, and freeze the
     * result into a {@link ContinuousOperativeTopology}. Shared by continuous
     * {@link ContinuousOperative#channels} authoring and bounded
     * {@code operations(...)} authoring — both declare Operations / Channels /
     * Segments on the same {@link ChannelBuilder}.
     *
     * @throws ContinuousOperativeConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static ContinuousOperativeTopology build(
            @Nonnull String factoryId, @Nonnull java.util.function.Consumer<ChannelBuilder> authoring) {
        ContinuousOperativeTopologyBuilder b = new ContinuousOperativeTopologyBuilder();
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

    @Nonnull
    public ContinuousOperativeTopology build(@Nonnull String factoryId) {
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
            if (operation.operationKeyOverridden && operation.segmentKeys.isEmpty()) {
                errors.add("operation `" + operation.name
                        + "` overrides operationKey() (dynamic-mode marker) without declaring "
                        + "operationKeys(); dynamic mode is reserved for "
                        + "`operative-dynamic-segmenting` and rejected in v1");
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
            } catch (ContinuousOperativeConfigurationException e) {
                errors.add(e.getMessage());
                resolvedCapacities[i] = MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY;
            }
        }

        if (!errors.isEmpty()) {
            throw new ContinuousOperativeConfigurationException(List.copyOf(errors));
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
                    entry.overflowPolicy));
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
                    entry.mailboxCapacityOverridden));
        }

        return new ContinuousOperativeTopology(
                factoryId,
                channelDescriptors,
                Map.copyOf(nameToId),
                operationDescriptors,
                Map.copyOf(operationNameToId));
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
        // v1 wires only WallClock; declaring LogicalClock or EventTimeFrom
        // fails ContinuousOperative registration loud-and-clear (per design.md Decision 4).
        if (entry.clockType != null
                && entry.clockType != com.flezha.sdk.state.WallClock.class
                && entry.clockType != com.flezha.sdk.state.Clock.class) {
            errors.add("operation `" + operationName + "`: "
                    + ContinuousOperativeConfigurationException.clockNotImplementedYet(entry.clockType.getSimpleName())
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
                errors.add(ContinuousOperativeConfigurationException.duplicatePluginSink(entry.name)
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
    }

    /**
     * Default plugin-bound serializer. {@code byte[]} is passthrough;
     * {@code String} is UTF-8 encoded; everything else goes through Jackson
     * JSON ({@code design.md} Open Question 4).
     */
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
