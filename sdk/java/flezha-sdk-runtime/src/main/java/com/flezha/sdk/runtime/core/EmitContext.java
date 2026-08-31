package com.flezha.sdk.runtime.core;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.procedure.EmitKeys;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.procedure.StableId;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ConsumerEdge;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.state.TimerSession;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.TimerKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Per-invocation {@link ProcedureCtx}: serves the item's core-evaluated
 * clock, the replay-stable id, and evaluates emits producer-side into
 * schema {@link Output}s — edge transforms, keys and the consumer's
 * event-time extractor run here, so live delivery and replay cannot drift.
 */
final class EmitContext implements ProcedureCtx {

    private final OperativeInstance instance;
    private final EntityCell cell;
    private final long invocationSeq;
    private final long now;
    private final boolean replay;

    final List<Output> outputs = new ArrayList<>();
    boolean checkpointed;

    @Nullable
    TimerSession timers;

    private byte[] identityBytes;
    private int stableIdOrdinal;
    private int emitOrdinal;

    EmitContext(OperativeInstance instance, EntityCell cell, long invocationSeq, long now, boolean replay) {
        this.instance = instance;
        this.cell = cell;
        this.invocationSeq = invocationSeq;
        this.now = now;
        this.replay = replay;
    }

    @Override
    public long now() {
        return now;
    }

    @Override
    @Nonnull
    public StableId stableId() {
        if (identityBytes == null) {
            identityBytes = cell.id.encodedBytes();
        }
        return StableId.derive(identityBytes, invocationSeq, stableIdOrdinal++);
    }

    @Override
    public void checkpoint() {
        checkpointed = true;
    }

    @Override
    public boolean replaying() {
        return replay;
    }

    @Override
    @Nonnull
    public Optional<String> entityKey() {
        // The cell stores a single-entity Procedure's absent key as "";
        // that is not a key to emit under.
        return cell.entityKey.isEmpty() ? Optional.empty() : Optional.of(cell.entityKey);
    }

    private TimerSession session() throws ProcedureException {
        if (timers == null) {
            throw new ProcedureException.TimersUnavailable();
        }
        return timers;
    }

    @Override
    public long timerAtEventTime(long scheduledAt) throws ProcedureException {
        return session().registerAtEventTime(scheduledAt);
    }

    @Override
    public long timerAfter(@Nonnull Duration delay) throws ProcedureException {
        return session().registerAfter(delay);
    }

    @Override
    public boolean cancelEventTimeTimer(long scheduledAt) throws ProcedureException {
        return session().cancel(TimerKind.EVENT_TIME, scheduledAt);
    }

    @Override
    public boolean cancelProcessingTimeTimer(long scheduledAt) throws ProcedureException {
        return session().cancel(TimerKind.PROCESSING_TIME, scheduledAt);
    }

    @Override
    public <C> void emit(@Nonnull C value) throws ProcedureException {
        doEmit(defaultOutputChannel(), value, null, null);
    }

    @Override
    public <C> void emit(@Nonnull String channelName, @Nonnull C value) throws ProcedureException {
        doEmit(resolveChannel(channelName), value, null, null);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) throws ProcedureException {
        doEmit(defaultOutputChannel(), value, meta, null);
    }

    @Override
    public <C> void emitWithMeta(@Nonnull String channelName, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException {
        doEmit(resolveChannel(channelName), value, meta, null);
    }

    @Override
    public <C> void emitKeyed(@Nonnull String key, @Nonnull C value) throws ProcedureException {
        doEmit(defaultOutputChannel(), value, null, key);
    }

    @Override
    public <C> void emitKeyedWithMeta(@Nonnull String key, @Nonnull C value, @Nonnull Map<String, String> meta)
            throws ProcedureException {
        doEmit(defaultOutputChannel(), value, meta, key);
    }

    private int defaultOutputChannel() throws ProcedureException {
        ProcedureDescriptor desc = instance.topology.procedure(cell.procedureId);
        if (desc == null || desc.outputChannelId().isEmpty()) {
            throw new ProcedureException.UnknownChannel("<no output channel declared on this procedure>");
        }
        return desc.outputChannelId().getAsInt();
    }

    private int resolveChannel(String name) throws ProcedureException {
        Integer id = instance.topology.lookupChannelId(name);
        if (id == null) {
            throw new ProcedureException.UnknownChannel(name);
        }
        return id;
    }

    /**
     * The single emit funnel: keyed-channel contract, reserved-metadata
     * refusal, then producer-side routing into schema outputs. Supplying no
     * key on a keyed channel resolves to this Entity's own key here, so the
     * defaulted emit and an explicit {@code emitKeyed} are one path from
     * this point on and their stamping cannot diverge.
     */
    private void doEmit(int channelId, Object value, @Nullable Map<String, String> meta, @Nullable String key)
            throws ProcedureException {
        ChannelDescriptor descriptor = instance.topology.channel(channelId);
        if (descriptor == null) {
            throw new ProcedureException.UnknownChannel("<channel " + channelId + ">");
        }
        if (descriptor.kind() == ChannelKind.ENTRY) {
            throw new ProcedureException("cannot emit on entry channel `" + descriptor.name() + "` from a procedure");
        }
        key = EmitKeys.resolve(descriptor.name(), descriptor.keyed(), key, entityKey());
        if (meta != null) {
            for (String k : meta.keySet()) {
                if (k.startsWith(ProcedureCtx.RESERVED_METADATA_PREFIX)) {
                    throw new ProcedureException.ReservedMetadataKey(k);
                }
            }
            if (descriptor.pluginSink().isEmpty()) {
                throw new ProcedureException.MetadataNotAllowedOnInterProcedureChannel(descriptor.name());
            }
        }
        // Fan out to the consumer edges, then to the plugin sink if the
        // channel has one. The plugin-bound terminal is the same path with
        // no consumer edges, which is why there is no second arm for it.
        collectEdgeOutputs(channelId, descriptor, value);
        if (descriptor.pluginSink().isPresent()) {
            byte[] bytes = serializeOrThrow(descriptor.pluginSink().get().serializer(), value, descriptor.name());
            collectPluginOutput(descriptor, bytes, meta, key);
        }
    }

    private static byte[] serializeOrThrow(Function<Object, byte[]> serializer, Object value, String channel)
            throws ProcedureException {
        try {
            return serializer.apply(value);
        } catch (RuntimeException | Error e) {
            throw new ProcedureException.PluginSerializerFailure(channel, e.toString(), e);
        }
    }

    /**
     * Producer-side edge evaluation: transform chain, edge key, consumer
     * event time. That is the STREAM shape — the core routes each output
     * on {@code (edge, key)} and reads the consumer.
     *
     * <p>A workflow stage is folded, not routed: {@code fold_effect}
     * ({@code flz_core::host::workflow}) discards the consumer and collects
     * every output as ONE released value, which the driver then fans out to
     * each child itself. One output per consumer would therefore multiply
     * the stage's released batch — and every downstream node's work — by
     * the channel's consumer count. So a structured workflow releases the
     * value once, untransformed and unkeyed; each consumer applies its own
     * edge on delivery in {@link EntityRouting#valuesForEntity}, which is the
     * seam a live run and a journal re-seed both cross. Mirrors the Rust
     * SDK's {@code WorkflowCollectAdapter::emit}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void collectEdgeOutputs(int channelId, ChannelDescriptor descriptor, Object value) {
        // A channel with no consumer releases nothing: on a workflow that
        // is a plugin-bound terminal, and seeding the next stage from it
        // would hand the driver a terminal output.
        if (descriptor.consumers().isEmpty()) {
            return;
        }
        if (instance.workflow.isStructuredWorkflow()) {
            outputs.add(new Output(
                    // The consumer the fold discards; naming the channel's
                    // first keeps the target a well-formed Edge.
                    new OutputTarget.Edge(
                            channelId, descriptor.consumers().get(0).procedure()),
                    Optional.empty(),
                    OptionalLong.empty(),
                    List.of(),
                    instance.edgeCodec.serialize(value)));
            return;
        }
        for (ConsumerEdge edge : descriptor.consumers()) {
            List<Object> values = EdgeTransforms.valuesOf(edge, value);
            ProcedureDescriptor consumer = instance.topology.procedure(edge.procedure());
            boolean eventTimeClocked = consumer != null && consumer.clockType() == EventTimeFrom.class;
            for (Object v : values) {
                OptionalLong eventTime = OptionalLong.empty();
                if (eventTimeClocked) {
                    Procedure router = instance.routers.get(edge.procedure());
                    eventTime = OptionalLong.of(router.eventTime(v));
                }
                outputs.add(new Output(
                        new OutputTarget.Edge(channelId, edge.procedure()),
                        Optional.ofNullable(edge.keyOf(v)),
                        eventTime,
                        List.of(),
                        instance.edgeCodec.serialize(v)));
            }
        }
    }

    /**
     * A plugin output. Non-tier keyed channels carry the identity stamps
     * the pre-core frame writer minted; under the tier the controller
     * derives them at flush time, so none ride here — both byte-shapes
     * preserved.
     */
    private void collectPluginOutput(
            ChannelDescriptor descriptor, byte[] bytes, @Nullable Map<String, String> meta, @Nullable String key) {
        List<Map.Entry<String, String>> metadata = new ArrayList<>();
        if (meta != null) {
            for (Map.Entry<String, String> e : meta.entrySet()) {
                if (!"channel".equals(e.getKey())) {
                    metadata.add(Map.entry(e.getKey(), e.getValue()));
                }
            }
        }
        if (descriptor.keyed() && !instance.topology.effectivelyOnce()) {
            metadata.add(Map.entry("flz.entity", cell.id.encoded()));
            metadata.add(Map.entry("flz.seq", invocationSeq + "." + emitOrdinal));
            metadata.add(Map.entry("flz.key", key));
        }
        emitOrdinal++;
        outputs.add(new Output(
                new OutputTarget.Plugin(descriptor.name()),
                Optional.ofNullable(key),
                OptionalLong.empty(),
                metadata,
                bytes));
    }
}
