package com.flezha.sdk.runtime.relay.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.Channel;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.ChannelDecl;
import com.flezha.sdk.channel.ChannelDeclWithProducer;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.channel.EntryChannel;
import com.flezha.sdk.channel.OverflowPolicy;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayConfigurationException;
import com.flezha.sdk.runtime.relay.ChannelDescriptor;
import com.flezha.sdk.runtime.relay.PluginSink;
import com.flezha.sdk.runtime.relay.RelayTopology;
import com.flezha.sdk.runtime.relay.ShardDescriptor;
import com.flezha.sdk.shard.Shard;
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
 * declared channels and shard registrations, validates wiring at
 * {@link #build(String)} time, and produces a frozen {@link RelayTopology}.
 *
 * <p>Validation rules (mirror of `relay-mvp-rust/src/relay/builder.rs`):
 * <ul>
 *   <li>Entry channels accept only {@code byte[]} or {@code String}.</li>
 *   <li>Channel payload type must match producer Shard's
 *       {@link Shard#outputType()} and consumer Shard(s)'
 *       {@link Shard#inputType()}.</li>
 *   <li>Channels declared with {@code .channel(...)} but never wired with
 *       {@code .from(...)} are rejected.</li>
 *   <li>Shards declaring non-empty {@link Shard#shardKeys()} but with
 *       {@code partitionForOverridden() == false} are rejected.</li>
 *   <li>Shards with {@code shardKeyOverridden() == true} but empty
 *       {@code shardKeys()} are rejected (dynamic-mode reservation).</li>
 *   <li>Channel names must be unique within the Relay.</li>
 * </ul>
 *
 * <p>All failures throw {@link RelayConfigurationException}.
 */
public final class RelayTopologyBuilder implements ChannelBuilder {

    private static final Set<Class<?>> ENTRY_CHANNEL_TYPES = Set.of(byte[].class, String.class);

    private final List<ChannelEntry> channels = new ArrayList<>();
    private final Map<String, Integer> nameToId = new LinkedHashMap<>();
    private final List<ShardEntry> shards = new ArrayList<>();
    private final Map<String, Integer> shardNameToId = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();

    /**
     * Run the supplied {@link Relay#channels(ChannelBuilder)} once, validate,
     * and freeze the result into a {@link RelayTopology}.
     *
     * @throws RelayConfigurationException if any validation rule fails.
     */
    @Nonnull
    public static RelayTopology build(@Nonnull Relay relay) {
        RelayTopologyBuilder b = new RelayTopologyBuilder();
        relay.channels(b);
        return b.build(relay.factoryId());
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
        int id = declareChannel(type, name, ChannelKind.INTER_SHARD);
        return new ChannelDeclImpl<>(id, type, name);
    }

    @Nonnull
    public RelayTopology build(@Nonnull String factoryId) {
        // Detect channels declared without `.from(...)` (still classified as
        // INTER_SHARD with no producer).
        for (ChannelEntry entry : channels) {
            if (entry.kind != ChannelKind.ENTRY && entry.producer == null) {
                errors.add("channel `" + entry.name + "` was declared but never wired with .from(...)");
            }
        }

        // Validate each Shard's static-mode / dynamic-mode declaration.
        for (ShardEntry shard : shards) {
            if (!shard.partitionKeys.isEmpty() && !shard.partitionForOverridden) {
                errors.add("shard `" + shard.name
                        + "` declares non-empty shardKeys() but does not override partitionFor "
                        + "(set partitionForOverridden() to return true)");
            }
            if (shard.shardKeyOverridden && shard.partitionKeys.isEmpty()) {
                errors.add("shard `" + shard.name
                        + "` overrides shardKey() (dynamic-mode marker) without declaring "
                        + "shardKeys(); dynamic mode is reserved for "
                        + "`relay-dynamic-partitioning` and rejected in v1");
            }
            if (shard.partitionForOverridden && shard.partitionKeys.isEmpty()) {
                errors.add("shard `" + shard.name
                        + "` overrode partitionFor — single-mode shards have one implicit partition");
            }
        }

        // Resolve mailbox capacity for each Shard (Shard override → env var →
        // default), accumulating any range errors into the errors list.
        int[] resolvedCapacities = new int[shards.size()];
        for (int i = 0; i < shards.size(); i++) {
            ShardEntry shard = shards.get(i);
            try {
                resolvedCapacities[i] = MailboxCapacityResolver.resolve(
                        shard.name, shard.mailboxCapacityDeclared, shard.mailboxCapacityOverridden);
            } catch (RelayConfigurationException e) {
                errors.add(e.getMessage());
                resolvedCapacities[i] = MailboxCapacityResolver.DEFAULT_MAILBOX_CAPACITY;
            }
        }

        if (!errors.isEmpty()) {
            throw new RelayConfigurationException(List.copyOf(errors));
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

        // Materialize ShardDescriptors.
        List<ShardDescriptor> shardDescriptors = new ArrayList<>(shards.size());
        for (int i = 0; i < shards.size(); i++) {
            ShardEntry entry = shards.get(i);
            shardDescriptors.add(new ShardDescriptor(
                    entry.name,
                    entry.shardClass,
                    entry.partitionKeys,
                    entry.partitionForOverridden,
                    entry.shardKeyOverridden,
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

        return new RelayTopology(
                factoryId, channelDescriptors, Map.copyOf(nameToId), shardDescriptors, Map.copyOf(shardNameToId));
    }

    // ---- Internals ----------------------------------------------------------

    private int declareChannel(Class<?> payloadType, String name, ChannelKind kind) {
        if (nameToId.containsKey(name)) {
            errors.add("channel `" + name + "` declared more than once on this relay");
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

    private int registerShard(Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass) {
        Shard<?, ?, ?, ?, ?, ?> probe = instantiate(shardClass);
        String shardName = probe.name();
        Integer existing = shardNameToId.get(shardName);
        if (existing != null) {
            return existing;
        }
        int id = shards.size();
        ShardEntry entry = new ShardEntry();
        entry.shardClass = shardClass;
        entry.name = shardName;
        entry.partitionKeys = List.copyOf(probe.shardKeys());
        entry.partitionForOverridden = probe.partitionForOverridden();
        entry.shardKeyOverridden = probe.shardKeyOverridden();
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
        // fails Relay registration loud-and-clear (per design.md Decision 4).
        if (entry.clockType != null
                && entry.clockType != com.flezha.sdk.state.WallClock.class
                && entry.clockType != com.flezha.sdk.state.Clock.class) {
            errors.add("shard `" + shardName + "`: "
                    + RelayConfigurationException.clockNotImplementedYet(entry.clockType.getSimpleName())
                            .getMessage());
        }
        shards.add(entry);
        shardNameToId.put(shardName, id);
        return id;
    }

    private Shard<?, ?, ?, ?, ?, ?> instantiate(Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass) {
        try {
            return shardClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            errors.add("shard class `" + shardClass.getName() + "` must have a public no-arg constructor: "
                    + e.toString());
            // Return a placeholder so the rest of the build can collect more
            // errors before failing; the exception throws at build() time.
            return UNINSTANTIABLE;
        }
    }

    private static final Shard<
                    Object, Object, Object, com.flezha.sdk.state.NoChange, com.flezha.sdk.state.WallClock, Void>
            UNINSTANTIABLE = new Shard<>() {
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
                        Optional<String> shardKey, Optional<com.flezha.sdk.state.RestoreInitPayload> initPayload) {
                    throw new UnsupportedOperationException("uninstantiable shard");
                }

                @Override
                public void run(
                        Object input,
                        com.flezha.sdk.state.StateHandle<Object, com.flezha.sdk.state.NoChange> state,
                        com.flezha.sdk.shard.ShardCtx ctx) {
                    throw new UnsupportedOperationException("uninstantiable shard");
                }
            };

    private void wireConsumer(
            int channelId,
            String channelName,
            Class<?> channelPayloadType,
            Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass) {
        int shardId = registerShard(shardClass);
        ShardEntry shard = shards.get(shardId);
        if (!channelPayloadType.equals(shard.inputType)) {
            errors.add("channel `" + channelName + "`: payload type "
                    + describe(channelPayloadType)
                    + " does not match consumer shard `" + shard.name
                    + "` inputType() = " + describe(shard.inputType));
        }
        ChannelEntry channel = channels.get(channelId);
        if (!channel.consumers.contains(shardId)) {
            channel.consumers.add(shardId);
        }
        // Cache input channel id on the shard. If the shard already has a
        // different input channel wired, both stay reachable via the
        // channel descriptor; the dispatcher uses the channel-side mapping.
        if (shard.inputChannelId == null) {
            shard.inputChannelId = channelId;
        }
    }

    private void wireProducer(
            int channelId,
            String channelName,
            Class<?> channelPayloadType,
            Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass) {
        int shardId = registerShard(shardClass);
        ShardEntry shard = shards.get(shardId);
        if (!channelPayloadType.equals(shard.outputType)) {
            errors.add("channel `" + channelName + "`: payload type "
                    + describe(channelPayloadType)
                    + " does not match producer shard `" + shard.name
                    + "` outputType() = " + describe(shard.outputType));
        }
        ChannelEntry channel = channels.get(channelId);
        if (channel.producer != null) {
            errors.add("channel `" + channelName + "` already has a producer; multi-producer is not allowed");
        } else {
            channel.producer = shardId;
        }
        if (shard.outputChannelId == null) {
            shard.outputChannelId = channelId;
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

    /** Mutable internal record for a shard registration. */
    static final class ShardEntry {
        String name;
        Class<? extends Shard<?, ?, ?, ?, ?, ?>> shardClass;
        List<String> partitionKeys;
        boolean partitionForOverridden;
        boolean shardKeyOverridden;
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
        public Channel<T> into(@Nonnull Class<? extends Shard<T, ?, ?, ?, ?, ?>> shardClass) {
            wireConsumer(channelId, name, payloadType, shardClass);
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
        public ChannelDeclWithProducer<T> from(@Nonnull Class<? extends Shard<?, ?, T, ?, ?, ?>> shardClass) {
            wireProducer(channelId, name, payloadType, shardClass);
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
        public ChannelDeclWithProducer<T> into(@Nonnull Class<? extends Shard<T, ?, ?, ?, ?, ?>> shardClass) {
            wireConsumer(channelId, name, payloadType, shardClass);
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
                errors.add(RelayConfigurationException.duplicatePluginSink(entry.name)
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
